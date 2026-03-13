from django.shortcuts import render, get_object_or_404, redirect
from datetime import date, timedelta
import statistics
import json
from django.db.models import OuterRef, Subquery
from django.db.utils import OperationalError, ProgrammingError

from .models import Basic, BokIo, CompanyStock, IndBok, IndIo, Report

def home(request):
    count = Report.objects.count()
    return render(request, 'home.html', {'count': count})

def search(request):
    query = request.GET.get('q', '')
    
    if query:
        # 기업명에 검색어가 포함된 데이터를 찾음. 여기서 활성화 된 애만 검색 가능함
        # 여기서 원하는 속성만 가져오려면 뒤에 .values('corp_name', 'stock_code') 처럼 쓰면 됨
        results = Basic.objects.filter(corp_name__icontains=query, is_active=True) # corp_name에서 대소문자 구분 없이, query가 포함되고, is_active=True인 데이터 가져옴
        
        # 중복이 없으므로, 결과가 딱 1개라면 바로 AI 페이지로 이동
        if results.count() == 1:
            return redirect('ai_page', stock_code=results.first().stock_code)
    else:
        results = []

    # 결과가 없거나 2개 이상(부분 일치 등)일 때만 검색 결과 리스트를 보여줌
    return render(request, 'search.html', {'results': results, 'query': query})

def ai_page(request, stock_code):
    company = get_object_or_404(Basic, stock_code=stock_code) # BASIC 테이블에 있는 stock_code만 가져오게 함
    report = Report.objects.filter(stock_code=stock_code).first() # 해당 종목의 report 테이블을 가져옴

    # 데이터 유효성 검사 (None, 'None', '[]' 등 체크)
    def is_invalid(value):
        return value in [None, 'None', '[]', 'NULL', [], 'null', '', 'NULL']

    # 주요 연혁 및 제품 비중 노출 여부 판단
    show_history = report and not is_invalid(report.history_ai) # report가 있으면서 history_ai랑 product_ratio_ai는 None이 아니어야 홈페이지에 띄우기 위한 용도
    show_ratio = report and not is_invalid(report.product_ratio_ai)

    ratio_data = []
    if show_ratio:
        try:
            # JSON 문자열 파싱, 딕셔너리 형태로 전환해서 넘김
            ratio_data = json.loads(report.product_ratio_ai)
        except (json.JSONDecodeError, TypeError):
            show_ratio = False # json 못가져오면 show_ratio False로 해서 안보여줌

    context = {
        'company': company,
        'report': report,
        'show_history': show_history,
        'show_ratio': show_ratio,
        'ratio_data': ratio_data, # 템플릿의 json_script에서 사용
    }
    return render(request, 'ai_page.html', context)

def overview(request):
    return render(request, 'overview.html')

def finance(request, stock_code=None):
    if not stock_code:
        # URL에 stock_code가 없을 경우 안전장치
        company = Basic.objects.filter(is_active=True).first()
        if company:
            stock_code = company.stock_code
        else:
            return render(request, 'finance.html')

    company = get_object_or_404(Basic.objects.select_related('ind_code'), stock_code=stock_code)

    # 날짜 계산: 어제 날짜 기준으로 가장 최근 개장일 찾기
    today = date.today() 
    yesterday = today - timedelta(days=1)

    # 최신 데이터 (어제 이하 기준 가장 최근)
    latest_stock = CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__lte=yesterday
    ).order_by('-reference_date').first()

    base_date = latest_stock.reference_date if latest_stock else yesterday
    ninety_days_ago = base_date - timedelta(days=90)

    # 차트용 데이터 (90일 전 ~ 기준일), 날짜 오름차순(과거->최신)
    stock_qs = CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__gte=ninety_days_ago,
        reference_date__lte=base_date
    ).order_by('reference_date')

    chart_data = []
    for row in stock_qs:
        # open_price가 결측치인 경우 시고저종 캔들차트 생성을 위해 close_price로 대체
        o_price = float(row.open_price) if row.open_price else (float(row.close_price) if row.close_price else 0)
        h_price = float(row.high_price) if row.high_price else o_price
        l_price = float(row.low_price) if row.low_price else o_price
        c_price = float(row.close_price) if row.close_price else o_price

        chart_data.append({
            'date': row.reference_date.strftime('%Y-%m-%d'),
            'open': o_price,
            'high': h_price,
            'low': l_price,
            'close': c_price,
            'volume': float(row.acc_trdvol) if row.acc_trdvol else 0,
            'trading_value': float(row.acc_trdval) if row.acc_trdval else 0,
        })

    # 피어그룹 중앙값 계산 및 배당성향 계산 로직
    peer_codes = Basic.objects.filter(ind_code=company.ind_code, is_active=True).values_list('stock_code', flat=True)
    
    per_list, pbr_list, div_list, payout_list = [], [], [], []

    for code in peer_codes:
        peer_stock = CompanyStock.objects.filter(
            stock_code=code, 
            reference_date__lte=yesterday
        ).order_by('-reference_date').first()

        if peer_stock:
            p_eps = float(peer_stock.eps) if peer_stock.eps is not None else None
            p_dps = float(peer_stock.dps) if peer_stock.dps is not None else None

            # EPS가 양수인 기업들 중에서만 중앙값을 계산
            if p_eps is not None and p_eps > 0:
                if peer_stock.per is not None: per_list.append(float(peer_stock.per))
                if peer_stock.pbr is not None: pbr_list.append(float(peer_stock.pbr))
                if peer_stock.dividend_yield is not None: div_list.append(float(peer_stock.dividend_yield))
                
                if p_dps is not None:
                    payout_list.append((p_dps / p_eps) * 100)

    def get_median(lst):
        return f"{round(statistics.median(lst), 1)}" if lst else '-'

    peer_count = len(peer_codes) # 전체 피어의 개수 기준

    peer_medians = {
        'per': get_median(per_list),
        'pbr': get_median(pbr_list),
        'dividend_yield': get_median(div_list),
        'payout_ratio': get_median(payout_list),
        'count': peer_count
    }

    # 현재 기업 배당성향 포맷팅 로직
    company_payout = '-'
    if latest_stock:
        eps = float(latest_stock.eps) if latest_stock.eps is not None else None
        dps = float(latest_stock.dps) if latest_stock.dps is not None else None
        
        if eps is not None and eps < 0:
            company_payout = 'N/A'
        elif dps is not None and dps == 0:
            company_payout = '-'
        elif eps is not None and dps is not None and eps > 0:
            company_payout = f"{round((dps / eps) * 100, 1)}%"

    # 최신 데이터 라운딩 처리
    if latest_stock:
        if latest_stock.per is not None: latest_stock.per = f"{float(latest_stock.per):.1f}"
        if latest_stock.pbr is not None: latest_stock.pbr = f"{float(latest_stock.pbr):.1f}"
        if latest_stock.dividend_yield is not None: latest_stock.dividend_yield = f"{float(latest_stock.dividend_yield):.1f}"

    context = {
        'company': company,
        'latest_stock': latest_stock,
        'chart_data_json': chart_data,
        'peer_medians': peer_medians,
        'company_payout': company_payout,
    }
    return render(request, 'finance.html', context)

def industry(request, stock_code=None):
    def to_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def format_mktcap(value):
        if value is None:
            return '-'
        try:
            return f"{int(value):,}"
        except (TypeError, ValueError):
            return '-'

    def format_change_and_rate(price_change, fluc_rt):
        if price_change is None or fluc_rt is None:
            return None, 'flat'

        try:
            change_value = int(price_change)
            rate_value = float(fluc_rt)
        except (TypeError, ValueError):
            return None, 'flat'

        if change_value > 0:
            sign = '+'
            css_class = 'up'
        elif change_value < 0:
            sign = '-'
            css_class = 'down'
        else:
            sign = ''
            css_class = 'flat'

        return f"{sign}{abs(change_value):,}({abs(rate_value):.2f}%)", css_class

    def company_topic_particle(name):
        text = str(name).strip() if name is not None else ''
        if not text:
            return '은'

        last_char = text[-1]
        code = ord(last_char)
        if 0xAC00 <= code <= 0xD7A3:
            return '은' if (code - 0xAC00) % 28 != 0 else '는'

        return '는'

    def empty_growth_context():
        default_title = '데이터 부족'
        default_desc = '최근 3개년 데이터가 충분하지 않아 추세를 판단하기 어렵습니다.'
        return {
            'growth_chart_labels': [],
            'industry_asset_growth': [],
            'industry_sales_growth': [],
            'benchmark_asset_growth': [],
            'benchmark_sales_growth': [],
            'asset_trend_title': default_title,
            'asset_trend_desc': default_desc,
            'sales_trend_title': default_title,
            'sales_trend_desc': default_desc,
        }

    def empty_structure_context():
        return {
            'industry_structure_options': [],
            'industry_structure_map': {},
            'selected_io_code': '',
            'structure_year': None,
        }

    def get_trend_message(values, message_map):
        valid = [v for v in values if v is not None]
        if len(valid) < 3:
            return '데이터 부족', '최근 3개년 데이터가 충분하지 않아 추세를 판단하기 어렵습니다.'

        prev2, prev1, recent = valid[-3], valid[-2], valid[-1]
        first = '상승' if prev1 >= prev2 else '하락'
        second = '상승' if recent >= prev1 else '하락'

        return message_map.get(
            (first, second),
            ('데이터 부족', '최근 3개년 데이터가 충분하지 않아 추세를 판단하기 어렵습니다.'),
        )

    def get_industry_growth_data(company_obj):
        empty = empty_growth_context()
        if not company_obj.ind_code or not company_obj.ind_code.bok_code:
            return empty

        industry_rows = list(
            IndBok.objects.filter(bok_code=company_obj.ind_code.bok_code).order_by('-year')[:3]
        )
        if not industry_rows:
            return empty

        industry_rows = sorted(industry_rows, key=lambda row: row.year)
        years = [row.year for row in industry_rows]
        industry_asset = [to_float(row.asset_growth_rate) for row in industry_rows]
        industry_sales = [to_float(row.sales_growth_rate) for row in industry_rows]

        benchmark_rows = list(IndBok.objects.filter(bok_code='ZZZ00').order_by('-year')[:3])
        benchmark_map = {row.year: row for row in benchmark_rows}
        benchmark_asset = [
            to_float(benchmark_map[year].asset_growth_rate) if year in benchmark_map else None
            for year in years
        ]
        benchmark_sales = [
            to_float(benchmark_map[year].sales_growth_rate) if year in benchmark_map else None
            for year in years
        ]

        asset_message_map = {
            ('상승', '상승'): ('📈 자산 성장 지속', '최근 산업의 자산 규모가 계속 커지고 있습니다.'),
            (
                '상승',
                '하락',
            ): (
                '📉 자산 성장 둔화',
                '이전 기간 대비 증가율이 낮아졌습니다.\n산업 성장 속도가 다소 둔화된 흐름입니다.',
            ),
            ('하락', '상승'): ('🔄 자산 회복 신호', '줄어들던 자산이 다시 증가세로 전환되었습니다.'),
            ('하락', '하락'): ('📉 자산 감소 지속', '산업 자산 규모가 계속 줄어드는 흐름입니다.'),
        }

        sales_message_map = {
            (
                '상승',
                '상승',
            ): (
                '📈 매출 성장 지속',
                '최근 산업의 매출 규모가 계속 확대되고 있습니다.\n시장 수요가 유지되며 성장 흐름이 이어지는 모습입니다.',
            ),
            (
                '상승',
                '하락',
            ): (
                '📉 매출 성장 둔화',
                '이전 기간 대비 매출 증가율이 낮아졌습니다.\n산업 매출 성장 속도가 다소 완만해진 흐름입니다.',
            ),
            (
                '하락',
                '상승',
            ): (
                '🔄 매출 회복 신호',
                '감소하던 매출 증가율이 다시 상승세로 전환되었습니다.\n시장 수요가 회복되는 흐름일 수 있습니다.',
            ),
            (
                '하락',
                '하락',
            ): (
                '📉 매출 감소 지속',
                '산업 매출 증가율이 계속 낮아지고 있습니다.\n시장 수요가 약화되는 흐름일 수 있습니다.',
            ),
        }

        asset_trend_title, asset_trend_desc = get_trend_message(industry_asset, asset_message_map)
        sales_trend_title, sales_trend_desc = get_trend_message(industry_sales, sales_message_map)

        return {
            'growth_chart_labels': years,
            'industry_asset_growth': industry_asset,
            'industry_sales_growth': industry_sales,
            'benchmark_asset_growth': benchmark_asset,
            'benchmark_sales_growth': benchmark_sales,
            'asset_trend_title': asset_trend_title,
            'asset_trend_desc': asset_trend_desc,
            'sales_trend_title': sales_trend_title,
            'sales_trend_desc': sales_trend_desc,
        }

    def get_industry_marketcap_data(company_obj):
        latest_stock = CompanyStock.objects.filter(stock_code=OuterRef('stock_code')).order_by(
            '-reference_date'
        )

        industry_qs = Basic.objects.filter(ind_code=company_obj.ind_code).annotate(
            latest_mktcap=Subquery(latest_stock.values('mktcap')[:1]),
            latest_price_change=Subquery(latest_stock.values('price_change')[:1]),
            latest_fluc_rt=Subquery(latest_stock.values('fluc_rt')[:1]),
        )
        industry_with_mktcap = industry_qs.filter(latest_mktcap__isnull=False)

        top10_rows = list(industry_with_mktcap.order_by('-latest_mktcap', 'corp_name')[:10])
        top10 = []
        for idx, row in enumerate(top10_rows, start=1):
            change_text, change_class = format_change_and_rate(
                row.latest_price_change, row.latest_fluc_rt
            )
            top10.append(
                {
                    'rank': idx,
                    'corp_name': row.corp_name,
                    'stock_code': row.stock_code,
                    'mktcap': format_mktcap(row.latest_mktcap),
                    'change_text': change_text,
                    'change_class': change_class,
                }
            )

        current_row = industry_qs.filter(stock_code=company_obj.stock_code).first()
        company_rank = None
        if current_row and current_row.latest_mktcap is not None:
            company_rank = (
                industry_with_mktcap.filter(latest_mktcap__gt=current_row.latest_mktcap).count() + 1
            )

        return {
            'industry_total_count': industry_qs.count(),
            'company_rank': company_rank,
            'industry_top10_left': top10[:5],
            'industry_top10_right': top10[5:],
        }

    def build_io_name_map():
        io_name_map = {}
        rows = (
            BokIo.objects.exclude(io_code__isnull=True)
            .exclude(io_code='')
            .exclude(io_name__isnull=True)
            .values('io_code', 'io_name')
            .order_by('io_code')
        )
        for row in rows:
            code = row.get('io_code')
            name = row.get('io_name')
            if code and code not in io_name_map and name:
                io_name_map[code] = name
        return io_name_map

    def build_structure_rows(rows, counterpart_field, io_name_map):
        result = []
        for row in rows:
            counterpart_code = getattr(row, counterpart_field, None)
            trade_vol = to_float(row.trade_vol)
            if not counterpart_code or trade_vol is None:
                continue

            result.append(
                {
                    'io_code': counterpart_code,
                    'io_name': io_name_map.get(counterpart_code) or counterpart_code,
                    'trade_vol': trade_vol,
                }
            )
        return result

    def get_industry_structure_data(company_obj, selected_io_code=None):
        empty = empty_structure_context()
        if not company_obj.ind_code:
            return empty

        option_rows = (
            BokIo.objects.filter(ind_code=company_obj.ind_code)
            .exclude(io_code__isnull=True)
            .exclude(io_code='')
            .values('io_code', 'io_name')
            .order_by('io_name', 'io_code')
        )

        options = []
        seen_codes = set()
        for row in option_rows:
            code = row.get('io_code')
            name = row.get('io_name') or code
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            options.append({'io_code': code, 'io_name': name})

        if not options:
            return empty

        valid_codes = {option['io_code'] for option in options}
        selected = selected_io_code if selected_io_code in valid_codes else options[0]['io_code']

        latest_year = IndIo.objects.order_by('-year').values_list('year', flat=True).first()
        if latest_year is None:
            return {
                'industry_structure_options': options,
                'industry_structure_map': {},
                'selected_io_code': selected,
                'structure_year': None,
            }

        io_name_map = build_io_name_map()
        structure_map = {}
        for option in options:
            code = option['io_code']

            supplier_rows = list(
                IndIo.objects.filter(year=latest_year, out_io_code=code, trade_vol__isnull=False)
                .order_by('-trade_vol')[:3]
            )
            buyer_rows = list(
                IndIo.objects.filter(year=latest_year, in_io_code=code, trade_vol__isnull=False)
                .order_by('-trade_vol')[:3]
            )

            structure_map[code] = {
                'io_code': code,
                'io_name': option['io_name'] or io_name_map.get(code) or code,
                'suppliers': build_structure_rows(supplier_rows, 'in_io_code', io_name_map),
                'buyers': build_structure_rows(buyer_rows, 'out_io_code', io_name_map),
            }

        return {
            'industry_structure_options': options,
            'industry_structure_map': structure_map,
            'selected_io_code': selected,
            'structure_year': latest_year,
        }

    if not stock_code:
        try:
            company_obj = Basic.objects.filter(is_active=True).order_by('corp_name').first()
            if not company_obj:
                company_obj = Basic.objects.order_by('corp_name').first()
            if company_obj:
                return redirect('industry', stock_code=company_obj.stock_code)
        except (ProgrammingError, OperationalError):
            pass

        context = {
            'company': None,
            'ind_name': None,
            'ind_def': None,
            'industry_total_count': 0,
            'company_rank': None,
            'industry_top10_left': [],
            'industry_top10_right': [],
            'company_topic_particle': '은',
            **empty_structure_context(),
            **empty_growth_context(),
        }
        return render(request, 'industry.html', context)

    company = get_object_or_404(Basic.objects.select_related('ind_code'), stock_code=stock_code)
    ind_name = company.ind_code.ind_name if company.ind_code else None
    ind_def = company.ind_code.ind_def if company.ind_code else None

    industry_total_count = 0
    company_rank = None
    industry_top10_left = []
    industry_top10_right = []
    growth_context = empty_growth_context()
    structure_context = empty_structure_context()
    selected_io_code = request.GET.get('io_code')

    try:
        if company.ind_code:
            marketcap_data = get_industry_marketcap_data(company)
            industry_total_count = marketcap_data['industry_total_count']
            company_rank = marketcap_data['company_rank']
            industry_top10_left = marketcap_data['industry_top10_left']
            industry_top10_right = marketcap_data['industry_top10_right']
    except (ProgrammingError, OperationalError):
        pass

    try:
        if company.ind_code:
            growth_context = get_industry_growth_data(company)
    except (ProgrammingError, OperationalError):
        pass

    try:
        if company.ind_code:
            structure_context = get_industry_structure_data(
                company,
                selected_io_code=selected_io_code,
            )
    except (ProgrammingError, OperationalError):
        pass

    context = {
        'company': company,
        'ind_name': ind_name,
        'ind_def': ind_def,
        'industry_total_count': industry_total_count,
        'company_rank': company_rank,
        'industry_top10_left': industry_top10_left,
        'industry_top10_right': industry_top10_right,
        'company_topic_particle': company_topic_particle(company.corp_name),
        **structure_context,
        **growth_context,
    }
    return render(request, 'industry.html', context)


# 여기서 함수 이름이랑 .html 이름 나중에 수정할 것, urls.py에서 연결할 주소 명칭도 적절하게 바꿔야됨
def a(request):
    return render(request, 'a.html')


def about(request):
    return render(request, 'about.html')


def stats(request):
    return render(request, 'stats.html')

