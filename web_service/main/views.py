from django.db.models import OuterRef, Subquery
from django.db.utils import OperationalError, ProgrammingError
from django.shortcuts import get_object_or_404, redirect, render

from .models import Basic, CompanyStock, IndBok, Report


def home(request):
    try:
        count = Report.objects.count()
    except (ProgrammingError, OperationalError):
        count = 0
    return render(request, 'home.html', {'count': count})


def search(request):
    query = request.GET.get('q', '')

    if query:
        # 기업명에 검색어가 포함된 데이터를 찾음. 여기서 활성화 된 애만 검색 가능함
        # 여기서 원하는 속성만 가져오려면 뒤에 .values('corp_name', 'stock_code') 처럼 쓰면 됨
        results = Basic.objects.filter(corp_name__icontains=query, is_active=True)

        # 중복이 없으므로, 결과가 딱 1개라면 바로 AI 페이지로 이동
        if results.count() == 1:
            return redirect('ai_page', stock_code=results.first().stock_code)
    else:
        results = []

    # 결과가 없거나 2개 이상(부분 일치 등)일 때만 검색 결과 리스트를 보여줌
    return render(request, 'search.html', {'results': results, 'query': query})


def ai_page(request, stock_code):
    # 종목코드로 기본 정보와 리포트를 가져옵니다.
    company = get_object_or_404(Basic, stock_code=stock_code)
    report = Report.objects.filter(stock_code=stock_code).first()

    context = {
        'company': company,
        'report': report,
    }
    return render(request, 'ai_page.html', context)


def overview(request):
    return render(request, 'overview.html')


def ai_page_preview(request):
    # 탭 연결용 미리보기 페이지
    context = {
        'company': {'corp_name': '기업명', 'stock_code': '종목코드'},
        'report': None,
    }
    return render(request, 'ai_page.html', context)


def finance(request):
    return render(request, 'finance.html')


def _format_mktcap(value):
    if value is None:
        return '-'

    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return '-'


def _to_float(value):
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _empty_growth_context():
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


def _get_trend_message(values, message_map):
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


def _get_industry_growth_data(company):
    empty = _empty_growth_context()
    if not company.ind_code or not company.ind_code.bok_code:
        return empty

    industry_rows = list(
        IndBok.objects.filter(bok_code=company.ind_code.bok_code).order_by('-year')[:3]
    )
    if not industry_rows:
        return empty

    industry_rows = sorted(industry_rows, key=lambda row: row.year)
    years = [row.year for row in industry_rows]
    industry_asset = [_to_float(row.asset_growth_rate) for row in industry_rows]
    industry_sales = [_to_float(row.sales_growth_rate) for row in industry_rows]

    benchmark_rows = list(IndBok.objects.filter(bok_code='ZZZ00').order_by('-year')[:3])
    benchmark_map = {row.year: row for row in benchmark_rows}
    benchmark_asset = [
        _to_float(benchmark_map[year].asset_growth_rate) if year in benchmark_map else None
        for year in years
    ]
    benchmark_sales = [
        _to_float(benchmark_map[year].sales_growth_rate) if year in benchmark_map else None
        for year in years
    ]

    asset_message_map = {
        ('상승', '상승'): ('📈 성장 지속', '최근 산업의 자산 규모가 계속 커지고 있습니다.'),
        (
            '상승',
            '하락',
        ): (
            '📉 성장 둔화',
            '이전 기간 대비 증가율이 낮아졌습니다.\n산업 성장 속도가 다소 둔화된 흐름입니다.',
        ),
        ('하락', '상승'): ('🔄 회복 신호', '줄어들던 자산이 다시 증가세로 전환되었습니다.'),
        ('하락', '하락'): ('📉 감소 지속', '산업 자산 규모가 계속 줄어드는 흐름입니다.'),
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
            '감소하던 매출 증가율이 다시 상승세로 전환되었습니다.\n산업 수요가 회복되는 흐름일 수 있습니다.',
        ),
        (
            '하락',
            '하락',
        ): (
            '📉 매출 감소 지속',
            '산업 매출 증가율이 계속 낮아지고 있습니다.\n시장 수요가 약화되는 흐름일 수 있습니다.',
        ),
    }

    asset_trend_title, asset_trend_desc = _get_trend_message(industry_asset, asset_message_map)
    sales_trend_title, sales_trend_desc = _get_trend_message(industry_sales, sales_message_map)

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


def _get_industry_marketcap_data(company):
    latest_stock = CompanyStock.objects.filter(stock_code=OuterRef('stock_code')).order_by('-reference_date')

    industry_qs = Basic.objects.filter(ind_code=company.ind_code).annotate(
        latest_mktcap=Subquery(latest_stock.values('mktcap')[:1]),
    )
    industry_with_mktcap = industry_qs.filter(latest_mktcap__isnull=False)

    top10_rows = list(industry_with_mktcap.order_by('-latest_mktcap', 'corp_name')[:10])
    top10 = []
    for idx, row in enumerate(top10_rows, start=1):
        top10.append(
            {
                'rank': idx,
                'corp_name': row.corp_name,
                'stock_code': row.stock_code,
                'mktcap': _format_mktcap(row.latest_mktcap),
            }
        )

    current_row = industry_qs.filter(stock_code=company.stock_code).first()
    company_rank = None
    if current_row and current_row.latest_mktcap is not None:
        company_rank = industry_with_mktcap.filter(latest_mktcap__gt=current_row.latest_mktcap).count() + 1

    return {
        'industry_total_count': industry_qs.count(),
        'company_rank': company_rank,
        'industry_top10_left': top10[:5],
        'industry_top10_right': top10[5:],
    }


def industry_default(request):
    try:
        company = Basic.objects.filter(is_active=True).order_by('corp_name').first() or Basic.objects.order_by('corp_name').first()
        if company:
            return redirect('industry', stock_code=company.stock_code)
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
        **_empty_growth_context(),
    }
    return render(request, 'industry.html', context)


def industry(request, stock_code):
    company = get_object_or_404(Basic.objects.select_related('ind_code'), stock_code=stock_code)
    ind_name = company.ind_code.ind_name if company.ind_code else None
    ind_def = company.ind_code.ind_def if company.ind_code else None

    industry_total_count = 0
    company_rank = None
    industry_top10_left = []
    industry_top10_right = []
    growth_context = _empty_growth_context()

    try:
        if company.ind_code:
            marketcap_data = _get_industry_marketcap_data(company)
            industry_total_count = marketcap_data['industry_total_count']
            company_rank = marketcap_data['company_rank']
            industry_top10_left = marketcap_data['industry_top10_left']
            industry_top10_right = marketcap_data['industry_top10_right']
    except (ProgrammingError, OperationalError):
        pass

    try:
        if company.ind_code:
            growth_context = _get_industry_growth_data(company)
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