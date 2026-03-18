from django.shortcuts import render, get_object_or_404, redirect
from datetime import date, timedelta
import math
import threading
import statistics
import json
import pandas as pd
from django.db.models import OuterRef, Subquery
from django.db.utils import OperationalError, ProgrammingError

from .models import Basic, BokIo, CompanyFinance, CompanyStock, IndBok, IndIo, Report, MarketIndex


COMPETITIVENESS_METRICS = [
    {'key': 'roe', 'label': 'ROE', 'higher_is_better': True},
    {'key': 'gross_margin_pct', 'label': '매출총이익률', 'higher_is_better': True},
    {'key': 'debt_ratio_pct', 'label': '부채비율', 'higher_is_better': False},
    {'key': 'sales_growth_rate_pct', 'label': '매출성장률', 'higher_is_better': True},
    {'key': 'cashholding_ratio_pct', 'label': '현금보유비율', 'higher_is_better': True},
]
COMPETITIVENESS_ANALYSIS_COPY = {
    'roe': {
        'strong': '경쟁사 평균보다 자본을 활용해 더 높은 수익을 창출하고 있어요',
        'weak': '경쟁사 평균보다 자본 대비 수익성이 낮은 편이에요',
    },
    'gross_margin_pct': {
        'strong': '경쟁사 평균보다 마진율이 높아요',
        'weak': '경쟁사 평균보다 마진율이 낮은 편이에요',
    },
    'debt_ratio_pct': {
        'strong': '경쟁사 평균보다 재무 구조가 안정적이에요',
        'weak': '경쟁사 평균보다 부채 부담이 높은 편이에요',
    },
    'sales_growth_rate_pct': {
        'strong': '경쟁사 평균보다 매출이 빠르게 성장하고 있어요',
        'weak': '경쟁사 평균보다 매출 성장 속도가 느린 편이에요',
    },
    'cashholding_ratio_pct': {
        'strong': '경쟁사 평균보다 현금 여력이 충분해요',
        'weak': '경쟁사 평균보다 현금 여력이 낮은 편이에요',
    },
}
COMPETITIVENESS_METRIC_KEYS = [metric['key'] for metric in COMPETITIVENESS_METRICS]
COMPETITIVENESS_CACHE_COLUMNS = ['stock_code', 'ind_code', 'biz_year', *COMPETITIVENESS_METRIC_KEYS]
COMPETITIVENESS_YEAR_CONFIRM_RATIO = 0.7
_COMPETITIVENESS_FINANCE_DF = None
_COMPETITIVENESS_FINANCE_LOCK = threading.Lock()
_COMPETITIVENESS_CONFIRMED_YEARS = []
_COMPETITIVENESS_ACTIVE_COMPANY_COUNT = 0
FINANCIAL_METRIC_DESCRIPTIONS = {
    '매출성장률': '기업의 매출이 얼마나 빠르게 증가하고 있는지를 보여주는 지표',
    'ROE': '기업이 자기자본을 활용해 얼마나 효율적으로 이익을 내는지를 보여주는 지표',
    '순이익률': '매출 대비 실제로 얼마나 이익을 남기는지를 보여주는 지표',
    '부채비율': '기업이 얼마나 많은 부채를 활용하고 있는지를 보여주는 지표',
    '유동비율': '기업이 단기 부채를 상환할 수 있는 능력을 보여주는 지표',
    '자기자본비율': '기업의 자산 중 자기자본이 차지하는 비중을 보여주는 지표',
    'EPS 성장률': '주당순이익(EPS)이 얼마나 증가했는지를 보여주는 지표 (1년 전 대비)',
}
DEBT_RATIO_FOOTNOTE = '※ 부채비율은 낮을수록 안정성이 높은 지표로, 해당 기준에 따라 순위가 산정되었습니다.'

def home(request):
    count = Report.objects.count()
    top_stocks = []

    # 활성화된 기업의 stock_code만 리스트로 쫙 뽑아옴
    active_codes = list(Basic.objects.filter(is_active=True).values_list('stock_code', flat=True))

    if active_codes:
        # 가장 최근 거래일 찾기
        latest_stock = CompanyStock.objects.order_by('-reference_date').first()
        
        if latest_stock:
            # 거래대금(bas_trdval) 기준 내림차순 10개 추출
            top_10_qs = CompanyStock.objects.filter(reference_date=latest_stock.reference_date, stock_code__in=active_codes).order_by('-bas_trdval')[:10]
            
            for stock in top_10_qs:
                # 이름 찾기 로직
                raw_code = getattr(stock, 'stock_code_id', stock.stock_code) # 해당 주식의 stock_code 가져옴
                stock_code_str = str(raw_code).strip() # stock_code 문자열 변환 후 좌우 공백 다듬기
                
                # DB에서 이름 직접 가져오기
                company = Basic.objects.filter(stock_code=stock_code_str).first()
                company_name = company.corp_name if company else '이름 없음'

                # 가격 및 등락률 텍스트/색상 가공
                try:
                    price_change = int(stock.price_change) if stock.price_change else 0
                    change_rate = float(stock.fluc_rt) if stock.fluc_rt else 0.0
                    close_price = int(stock.close_price) if stock.close_price else 0
                except (ValueError, TypeError):
                    price_change, change_rate, close_price = 0, 0.0, 0

                if price_change > 0:
                    css_class, sign = 'up', '+'
                elif price_change < 0:
                    css_class, sign = 'down', '' # 음수는 이미 -가 붙어 있음
                else:
                    css_class, sign = 'flat', ''

                top_stocks.append({
                    'code': stock_code_str,
                    'name': company_name,
                    'price': f"{close_price:,}",
                    'change_text': f"{sign}{price_change:,}원 ({change_rate:.2f}%)",
                    'css_class': css_class
                })

    return render(request, 'home.html', {'count': count, 'top_stocks': top_stocks})

def search(request):
    query = request.GET.get('q', '').strip()
    
    if query:
        if len(query) == 6 and query.isdigit():
            exact_company = Basic.objects.filter(stock_code=query, is_active=True).first()
            if exact_company:
                return redirect('overview', stock_code=exact_company.stock_code)

        exact_name_results = Basic.objects.filter(corp_name__iexact=query, is_active=True)
        if exact_name_results.count() == 1:
            return redirect('overview', stock_code=exact_name_results.first().stock_code)

        # 기업명에 검색어가 포함된 데이터를 찾음. 여기서 활성화 된 애만 검색 가능함
        # 여기서 원하는 속성만 가져오려면 뒤에 .values('corp_name', 'stock_code') 처럼 쓰면 됨
        results = Basic.objects.filter(corp_name__icontains=query, is_active=True) # corp_name에서 대소문자 구분 없이, query가 포함되고, is_active=True인 데이터 가져옴
        
        # 중복이 없으므로, 결과가 딱 1개라면 바로 요약 페이지로 이동
        if results.count() == 1:
            return redirect('overview', stock_code=results.first().stock_code)
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

def overview(request, stock_code=None):
    if not stock_code:
        # URL에 stock_code가 없을 경우 첫 번째 활성화된 기업으로 리다이렉트
        company = Basic.objects.filter(is_active=True).first()
        if company:
            return redirect('overview', stock_code=company.stock_code)
        return render(request, 'overview.html')

    company = get_object_or_404(Basic.objects.select_related('ind_code'), stock_code=stock_code)

    # 1. COMPANY_STOCK과 MARKET_INDEX의 reference_date, date 교집합 중 가장 최신 날짜 찾기
    latest_overlap = CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__in=MarketIndex.objects.values('date')
    ).order_by('-reference_date').first()

    latest_date = latest_overlap.reference_date if latest_overlap else date.today()

    # 2. 기본 정보 데이터 페치
    stock_data = CompanyStock.objects.filter(stock_code=stock_code, reference_date=latest_date).first()
    index_data = MarketIndex.objects.filter(date=latest_date).first()
    finance_data = CompanyFinance.objects.filter(stock_code=stock_code).order_by('-biz_year').first()

    # 3. 차트용 데이터 (최신 교집합 날짜 기준 역순 60일치 가져와서 정방향 정렬)
    past_60_stocks = list(CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__lte=latest_date
    ).order_by('-reference_date')[:60])
    past_60_stocks.reverse() # 과거 -> 최신 순으로 정렬

    chart_dates = [s.reference_date for s in past_60_stocks]
    market_indices = MarketIndex.objects.filter(date__in=chart_dates)
    market_index_map = {m.date: m.kosdaq for m in market_indices}

    chart_data = []
    for s in past_60_stocks:
        chart_data.append({
            'date': s.reference_date.strftime('%Y-%m-%d'),
            'close': float(s.close_price) if s.close_price else None,
            'kosdaq': float(market_index_map.get(s.reference_date, 0)) if market_index_map.get(s.reference_date) else None
        })

    context = {
        'company': company,
        'stock_data': stock_data,
        'index_data': index_data,
        'finance_data': finance_data,
        'chart_data_json': chart_data,
    }
    return render(request, 'overview.html', context)

def finance(request, stock_code=None):
    def empty_competitiveness_radar_context(target_stock_code=None):
        metric_payload = []
        for metric in COMPETITIVENESS_METRICS:
            metric_payload.append({
                'key': metric['key'],
                'label': metric['label'],
                'label_with_value': f"{metric['label']}(-)",
                'company_value': None,
                'company_display': '-',
                'company_score': None,
                'peer_average_score': None,
            })

        return {
            'available': False,
            'stock_code': target_stock_code,
            'target_year': None,
            'peer_year': None,
            'peer_count': 0,
            'industry_company_count': 0,
            'comparison_label': '피어그룹 평균',
            'uses_market_average_fallback': False,
            'labels': [item['label'] for item in metric_payload],
            'labels_with_values': [item['label_with_value'] for item in metric_payload],
            'company_scores': [item['company_score'] for item in metric_payload],
            'peer_average_scores': [item['peer_average_score'] for item in metric_payload],
            'metrics': metric_payload,
            'analysis_items': [],
        }

    def format_percent_metric(value):
        if value is None or pd.isna(value):
            return '-'
        return f"{float(value):.1f}%"

    def round_score(value):
        if value is None or pd.isna(value):
            return None
        return round(float(value), 1)

    def get_confirmed_competitiveness_years(finance_df, active_company_count):
        if finance_df is None or finance_df.empty or active_company_count <= 0:
            return []

        required_count = max(1, math.ceil(active_company_count * COMPETITIVENESS_YEAR_CONFIRM_RATIO))
        year_counts = finance_df.groupby('biz_year')['stock_code'].nunique()
        confirmed_years = year_counts[year_counts >= required_count].index.tolist()
        return sorted(int(year) for year in confirmed_years)

    def build_competitiveness_finance_dataframe():
        global _COMPETITIVENESS_CONFIRMED_YEARS, _COMPETITIVENESS_ACTIVE_COMPANY_COUNT

        basic_records = list(
            Basic.objects.filter(is_active=True).values('stock_code', 'ind_code')
        )
        finance_records = list(
            CompanyFinance.objects.values('stock_code', 'biz_year', *COMPETITIVENESS_METRIC_KEYS)
        )
        _COMPETITIVENESS_ACTIVE_COMPANY_COUNT = len({record['stock_code'] for record in basic_records})

        if not basic_records or not finance_records:
            _COMPETITIVENESS_CONFIRMED_YEARS = []
            return pd.DataFrame(columns=COMPETITIVENESS_CACHE_COLUMNS)

        basic_df = pd.DataFrame.from_records(basic_records)
        finance_df = pd.DataFrame.from_records(finance_records)
        merged_df = basic_df.merge(finance_df, on='stock_code', how='inner')

        for column in ['biz_year', *COMPETITIVENESS_METRIC_KEYS]:
            merged_df[column] = pd.to_numeric(merged_df[column], errors='coerce')

        merged_df = merged_df.dropna(subset=['stock_code', 'ind_code', 'biz_year']).copy()
        if merged_df.empty:
            _COMPETITIVENESS_CONFIRMED_YEARS = []
            return pd.DataFrame(columns=COMPETITIVENESS_CACHE_COLUMNS)

        merged_df['biz_year'] = merged_df['biz_year'].astype(int)
        merged_df['ind_code'] = merged_df['ind_code'].astype(str)
        merged_df = (
            merged_df
            .sort_values(['stock_code', 'biz_year'], ascending=[True, False])
            .drop_duplicates(subset=['stock_code', 'biz_year'], keep='first')
            .reset_index(drop=True)
        )

        final_df = merged_df[COMPETITIVENESS_CACHE_COLUMNS]
        _COMPETITIVENESS_CONFIRMED_YEARS = get_confirmed_competitiveness_years(
            final_df,
            _COMPETITIVENESS_ACTIVE_COMPANY_COUNT,
        )
        return final_df

    def get_competitiveness_finance_dataframe():
        global _COMPETITIVENESS_FINANCE_DF

        if _COMPETITIVENESS_FINANCE_DF is None:
            with _COMPETITIVENESS_FINANCE_LOCK:
                if _COMPETITIVENESS_FINANCE_DF is None:
                    _COMPETITIVENESS_FINANCE_DF = build_competitiveness_finance_dataframe()
        return _COMPETITIVENESS_FINANCE_DF

    def get_latest_confirmed_company_finance_row(finance_df, target_stock_code, confirmed_years):
        if not confirmed_years:
            return None

        company_df = finance_df[
            (finance_df['stock_code'] == target_stock_code)
            & (finance_df['biz_year'].isin([int(year) for year in confirmed_years]))
        ]
        if company_df.empty:
            return None

        return company_df.sort_values('biz_year', ascending=False).iloc[0]

    def get_unique_rows_for_year(finance_df, biz_year):
        if biz_year is None:
            return finance_df.iloc[0:0].copy()

        year_df = finance_df[finance_df['biz_year'] == int(biz_year)]
        if year_df.empty:
            return year_df.copy()

        return (
            year_df
            .sort_values(['stock_code', 'biz_year'], ascending=[True, False])
            .drop_duplicates(subset=['stock_code'], keep='first')
            .reset_index(drop=True)
        )

    def select_latest_year_with_min_companies(industry_df, minimum_company_count):
        if industry_df.empty:
            return None

        year_counts = industry_df.groupby('biz_year')['stock_code'].nunique()
        eligible_years = year_counts[year_counts >= minimum_company_count]
        if not eligible_years.empty:
            return int(eligible_years.index.max())

        max_count = int(year_counts.max()) if not year_counts.empty else 0
        if max_count <= 0:
            return None

        return int(year_counts[year_counts == max_count].index.max())

    def calculate_percentile_score(series, value, higher_is_better=True):
        if value is None or pd.isna(value):
            return None

        valid_series = pd.Series(series).dropna()
        if valid_series.empty:
            return None

        percentile = float((valid_series <= float(value)).mean() * 100)
        if not higher_is_better:
            percentile = 100 - percentile

        percentile = max(0.0, min(100.0, percentile))
        return round_score(percentile)

    def build_competitiveness_analysis_items(metrics_payload, use_market_average=False):
        analysis_items = []

        for index, metric in enumerate(metrics_payload):
            company_score = metric.get('company_score')
            peer_average_score = metric.get('peer_average_score')

            if company_score is None or peer_average_score is None:
                continue

            score_gap = float(company_score) - float(peer_average_score)
            gap_magnitude = abs(score_gap)
            is_strong = score_gap >= 0
            status = 'strong' if is_strong else 'weak'
            copy_map = COMPETITIVENESS_ANALYSIS_COPY.get(metric['key'], {})
            message = copy_map.get(status, '')
            if use_market_average:
                message = message.replace('경쟁사 평균보다', 'KOSDAQ 평균보다')

            analysis_items.append({
                'key': metric['key'],
                'label': metric['label'],
                'status': status,
                'status_label': 'Strong' if is_strong else 'Weak',
                'message': message,
                'score_gap': round_score(score_gap),
                'gap_magnitude': gap_magnitude,
                'sort_order': index,
            })

        analysis_items.sort(key=lambda item: (-item['gap_magnitude'], item['sort_order']))
        return analysis_items[:2]

    def resolve_competitiveness_years(finance_df, target_stock_code, confirmed_years=None):
        confirmed_years = [int(year) for year in (confirmed_years or [])]
        target_row = get_latest_confirmed_company_finance_row(finance_df, target_stock_code, confirmed_years)
        if target_row is None:
            return None

        target_year = int(target_row['biz_year'])
        industry_full_df = finance_df[finance_df['ind_code'] == target_row['ind_code']].copy()
        industry_company_count = int(industry_full_df['stock_code'].nunique())
        industry_df = industry_full_df[industry_full_df['biz_year'].isin(confirmed_years)].copy()
        if industry_df.empty:
            return None

        same_year_peer_df = get_unique_rows_for_year(industry_df, target_year)
        same_year_peer_count = int(same_year_peer_df['stock_code'].nunique())

        if industry_company_count <= 3:
            common_year = select_latest_year_with_min_companies(industry_df, industry_company_count)
            comparison_peer_df = get_unique_rows_for_year(industry_df, common_year)
            if comparison_peer_df.empty:
                comparison_peer_df = same_year_peer_df
            return {
                'company_row': target_row,
                'company_year': target_year,
                'peer_year': int(common_year) if common_year is not None else target_year,
                'peer_df': comparison_peer_df,
                'industry_company_count': industry_company_count,
                'same_year_peer_count': same_year_peer_count,
            }

        if same_year_peer_count >= 3:
            return {
                'company_row': target_row,
                'company_year': target_year,
                'peer_year': target_year,
                'peer_df': same_year_peer_df,
                'industry_company_count': industry_company_count,
                'same_year_peer_count': same_year_peer_count,
            }

        fallback_peer_year = select_latest_year_with_min_companies(industry_df, 3)
        fallback_peer_df = get_unique_rows_for_year(industry_df, fallback_peer_year)
        if fallback_peer_df.empty:
            fallback_peer_df = same_year_peer_df

        return {
            'company_row': target_row,
            'company_year': target_year,
            'peer_year': int(fallback_peer_year) if fallback_peer_year is not None else target_year,
            'peer_df': fallback_peer_df,
            'industry_company_count': industry_company_count,
            'same_year_peer_count': same_year_peer_count,
        }

    def build_competitiveness_radar_context(finance_df, target_stock_code, confirmed_years=None):
        empty_context = empty_competitiveness_radar_context(target_stock_code)
        if finance_df is None or finance_df.empty:
            return empty_context

        if confirmed_years is None:
            confirmed_years = get_confirmed_competitiveness_years(
                finance_df,
                int(finance_df['stock_code'].nunique()),
            )
        else:
            confirmed_years = [int(year) for year in confirmed_years]

        resolved = resolve_competitiveness_years(finance_df, target_stock_code, confirmed_years=confirmed_years)
        if not resolved:
            return empty_context

        company_row = resolved['company_row']
        peer_df = resolved['peer_df']
        company_population_df = get_unique_rows_for_year(finance_df, resolved['company_year'])
        peer_population_df = get_unique_rows_for_year(finance_df, resolved['peer_year'])
        raw_peer_count = int(peer_df['stock_code'].nunique()) if not peer_df.empty else 0
        uses_market_average_fallback = raw_peer_count <= 1
        comparison_df = company_population_df if uses_market_average_fallback else peer_df
        comparison_population_df = company_population_df if uses_market_average_fallback else peer_population_df
        comparison_label = 'KOSDAQ 기업 평균' if uses_market_average_fallback else '피어그룹 평균'

        metrics_payload = []
        for metric in COMPETITIVENESS_METRICS:
            key = metric['key']
            company_value = company_row.get(key)
            company_display = format_percent_metric(company_value)
            company_score = calculate_percentile_score(
                company_population_df[key],
                company_value,
                higher_is_better=metric['higher_is_better'],
            )

            comparison_scores = []
            comparison_raw_values = comparison_df[key].dropna()
            for comparison_value in comparison_raw_values:
                comparison_score = calculate_percentile_score(
                    comparison_population_df[key],
                    comparison_value,
                    higher_is_better=metric['higher_is_better'],
                )
                if comparison_score is not None:
                    comparison_scores.append(comparison_score)

            peer_average_value = float(comparison_raw_values.mean()) if not comparison_raw_values.empty else None
            peer_average_score = round_score(sum(comparison_scores) / len(comparison_scores)) if comparison_scores else None

            metrics_payload.append({
                'key': key,
                'label': metric['label'],
                'label_with_value': f"{metric['label']}({company_display})",
                'higher_is_better': metric['higher_is_better'],
                'company_value': None if pd.isna(company_value) else float(company_value),
                'company_display': company_display,
                'company_score': company_score,
                'peer_average_value': peer_average_value,
                'peer_average_display': format_percent_metric(peer_average_value),
                'peer_average_score': peer_average_score,
            })

        return {
            'available': any(metric['company_score'] is not None for metric in metrics_payload),
            'stock_code': target_stock_code,
            'target_year': resolved['company_year'],
            'peer_year': resolved['peer_year'],
            'peer_count': raw_peer_count,
            'industry_company_count': resolved['industry_company_count'],
            'comparison_label': comparison_label,
            'uses_market_average_fallback': uses_market_average_fallback,
            'labels': [metric['label'] for metric in metrics_payload],
            'labels_with_values': [metric['label_with_value'] for metric in metrics_payload],
            'company_scores': [metric['company_score'] for metric in metrics_payload],
            'peer_average_scores': [metric['peer_average_score'] for metric in metrics_payload],
            'metrics': metrics_payload,
            'analysis_items': build_competitiveness_analysis_items(
                metrics_payload,
                use_market_average=uses_market_average_fallback,
            ),
        }

    def get_competitiveness_radar_context(target_stock_code):
        try:
            finance_df = get_competitiveness_finance_dataframe()
        except (OperationalError, ProgrammingError):
            return empty_competitiveness_radar_context(target_stock_code)

        return build_competitiveness_radar_context(
            finance_df,
            target_stock_code,
            confirmed_years=_COMPETITIVENESS_CONFIRMED_YEARS,
        )

    def build_chart_data(stock_rows):
        chart_rows = []
        for row in stock_rows:
            o_price = float(row.open_price) if row.open_price else (float(row.close_price) if row.close_price else 0)
            h_price = float(row.high_price) if row.high_price else o_price
            l_price = float(row.low_price) if row.low_price else o_price
            c_price = float(row.close_price) if row.close_price else o_price

            chart_rows.append({
                'date': row.reference_date.strftime('%Y-%m-%d'),
                'open': o_price,
                'high': h_price,
                'low': l_price,
                'close': c_price,
                'volume': float(row.acc_trdvol) if row.acc_trdvol else 0,
                'trading_value': float(row.acc_trdval) if row.acc_trdval else 0,
            })
        return chart_rows

    def get_median(values):
        return f"{round(statistics.median(values), 1)}" if values else '-'

    def build_peer_medians(company_obj, reference_day):
        peer_codes = list(
            Basic.objects.filter(ind_code=company_obj.ind_code, is_active=True)
            .values_list('stock_code', flat=True)
        )

        if not peer_codes or reference_day is None:
            return {
                'per': '-',
                'pbr': '-',
                'dividend_yield': '-',
                'payout_ratio': '-',
                'count': 0,
            }

        peer_stocks = list(
            CompanyStock.objects.filter(
                stock_code__in=peer_codes,
                reference_date=reference_day,
            )
            .values('eps', 'dps', 'per', 'pbr', 'dividend_yield')
        )
        peer_count = len(peer_stocks)

        per_list, pbr_list, div_list, payout_list = [], [], [], []
        for peer_stock in peer_stocks:
            p_eps = float(peer_stock['eps']) if peer_stock.get('eps') is not None else None
            p_dps = float(peer_stock['dps']) if peer_stock.get('dps') is not None else None

            if p_eps is not None and p_eps > 0:
                if peer_stock.get('per') is not None:
                    per_list.append(float(peer_stock['per']))
                if peer_stock.get('pbr') is not None:
                    pbr_list.append(float(peer_stock['pbr']))
                if peer_stock.get('dividend_yield') is not None:
                    div_list.append(float(peer_stock['dividend_yield']))
                if p_dps is not None:
                    payout_list.append((p_dps / p_eps) * 100)

        return {
            'per': get_median(per_list),
            'pbr': get_median(pbr_list),
            'dividend_yield': get_median(div_list),
            'payout_ratio': get_median(payout_list),
            'count': peer_count,
        }

    def calculate_position_percentile(series, value):
        if value is None or pd.isna(value):
            return None

        valid_series = pd.Series(series).dropna()
        if valid_series.empty:
            return None

        percentile = float((valid_series <= float(value)).mean() * 100)
        percentile = max(0.0, min(100.0, percentile))
        return round_score(percentile)

    def score_to_top_percent(score):
        if score is None or pd.isna(score):
            return None

        top_percent = int(round(100 - float(score)))
        return min(100, max(1, top_percent if top_percent > 0 else 1))

    def get_rank_tone(top_percent):
        if top_percent is None or pd.isna(top_percent):
            return 'neutral'
        if float(top_percent) <= 30:
            return 'green'
        if float(top_percent) <= 70:
            return 'yellow'
        return 'red'

    def get_text_rank_tone(top_percent):
        if top_percent is None or pd.isna(top_percent):
            return 'neutral'
        if float(top_percent) <= 50:
            return 'green'
        return 'yellow'

    def build_unavailable_metric(label, message=None):
        if message is None:
            message = f"확인 가능한 {label} 정보가 없습니다."
        return {
            'label': label,
            'available': False,
            'guide': FINANCIAL_METRIC_DESCRIPTIONS.get(label, ''),
            'footnote': DEBT_RATIO_FOOTNOTE if label == '부채비율' else None,
            'message': message,
        }

    def get_median_numeric_value(series):
        valid_series = pd.Series(series).dropna()
        if valid_series.empty:
            return None
        return float(valid_series.median())

    def build_percentile_metric(
        label,
        series,
        company_value,
        higher_is_better=True,
        benchmark_value=None,
        benchmark_label='피어그룹 중앙값',
    ):
        valid_series = pd.Series(series).dropna()
        if company_value is None or pd.isna(company_value) or valid_series.empty:
            return None

        company_score = calculate_percentile_score(valid_series, company_value, higher_is_better=higher_is_better)
        benchmark_score = calculate_percentile_score(valid_series, benchmark_value, higher_is_better=higher_is_better)
        company_position = company_score
        benchmark_position = benchmark_score

        if company_position is None or benchmark_position is None or company_score is None or benchmark_score is None:
            return None

        company_value_display = format_percent_metric(company_value)
        company_top_percent = score_to_top_percent(company_score)
        benchmark_top_percent = score_to_top_percent(benchmark_score)

        return {
            'label': label,
            'available': True,
            'guide': FINANCIAL_METRIC_DESCRIPTIONS.get(label, ''),
            'footnote': DEBT_RATIO_FOOTNOTE if label == '부채비율' else None,
            'benchmark_label': benchmark_label,
            'company_value_display': company_value_display,
            'benchmark_value_display': format_percent_metric(benchmark_value),
            'company_position': company_position,
            'benchmark_position': benchmark_position,
            'company_top_percent': company_top_percent,
            'benchmark_top_percent': benchmark_top_percent,
            'company_rank_tone': get_rank_tone(company_top_percent),
            'benchmark_rank_tone': get_rank_tone(benchmark_top_percent),
            'company_text_rank_tone': get_text_rank_tone(company_top_percent),
            'description': f"{company.corp_name}의 {label}은 {company_value_display}로, 코스닥 내 상위 {company_top_percent}%입니다.",
        }

    def resolve_latest_finance_year(finance_df, target_stock_code, required_fields):
        if finance_df.empty or active_company_count <= 0:
            return None

        valid_df = finance_df[finance_df[required_fields].notna().any(axis=1)].copy()
        if valid_df.empty:
            return None

        required_count = max(1, math.ceil(active_company_count * COMPETITIVENESS_YEAR_CONFIRM_RATIO))
        year_counts = valid_df.groupby('biz_year')['stock_code'].nunique()
        target_years = (
            valid_df.loc[valid_df['stock_code'] == target_stock_code, 'biz_year']
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        candidate_years = [year for year in target_years if int(year_counts.get(year, 0)) >= required_count]
        return max(candidate_years) if candidate_years else None

    def build_eps_growth_dataframe(stock_codes):
        stock_records = list(
            CompanyStock.objects.filter(stock_code__in=stock_codes)
            .values('stock_code', 'reference_date', 'eps', 'eps_1yearago')
            .order_by('stock_code', 'reference_date')
        )
        if not stock_records:
            return pd.DataFrame(columns=['stock_code', 'reference_date', 'eps', 'eps_1yearago', 'eps_growth_rate_pct'])

        stock_df = pd.DataFrame.from_records(stock_records)
        stock_df['stock_code'] = stock_df['stock_code'].astype(str)
        stock_df['reference_date'] = pd.to_datetime(stock_df['reference_date'], errors='coerce')
        stock_df['eps'] = pd.to_numeric(stock_df['eps'], errors='coerce')
        stock_df['eps_1yearago'] = pd.to_numeric(stock_df['eps_1yearago'], errors='coerce')
        stock_df = stock_df.dropna(subset=['stock_code', 'reference_date']).copy()
        stock_df = stock_df.sort_values(['stock_code', 'reference_date']).reset_index(drop=True)
        stock_df['eps_growth_rate_pct'] = ((stock_df['eps'] - stock_df['eps_1yearago']) / stock_df['eps_1yearago'].abs()) * 100
        stock_df.loc[
            stock_df['eps'].isna() | stock_df['eps_1yearago'].isna() | (stock_df['eps_1yearago'].abs() == 0),
            'eps_growth_rate_pct'
        ] = pd.NA
        return stock_df

    def resolve_latest_reference_date(metric_df, target_stock_code, metric_column):
        if metric_df.empty:
            return None

        valid_df = metric_df.dropna(subset=[metric_column]).copy()
        if valid_df.empty:
            return None

        target_dates = (
            valid_df.loc[valid_df['stock_code'] == target_stock_code, 'reference_date']
            .dropna()
            .tolist()
        )

        return max(target_dates) if target_dates else None

    def build_financial_percentile_sections(target_stock_code):
        target_stock_code = str(target_stock_code)
        comparison_stock_codes = sorted(set(active_codes) | {target_stock_code})
        peer_stock_codes = set(
            Basic.objects.filter(is_active=True, ind_code=company.ind_code)
            .values_list('stock_code', flat=True)
        )
        peer_stock_codes.add(target_stock_code)
        finance_records = list(
            CompanyFinance.objects.filter(stock_code__in=comparison_stock_codes)
            .values(
                'stock_code',
                'biz_year',
                'sales_growth_rate_pct',
                'roe',
                'net_margin_pct',
                'debt_ratio_pct',
                'current_ratio_pct',
                'equity',
                'total_assets',
                'revenue_latest',
                'revenue_1y_ago',
            )
        )

        finance_columns = [
            'sales_growth_rate_pct',
            'roe',
            'net_margin_pct',
            'debt_ratio_pct',
            'current_ratio_pct',
            'equity',
            'total_assets',
            'revenue_latest',
            'revenue_1y_ago',
        ]

        if finance_records:
            finance_df = pd.DataFrame.from_records(finance_records)
            finance_df['stock_code'] = finance_df['stock_code'].astype(str)
            finance_df['biz_year'] = pd.to_numeric(finance_df['biz_year'], errors='coerce')
            for column in finance_columns:
                finance_df[column] = pd.to_numeric(finance_df[column], errors='coerce')
            finance_df = finance_df.dropna(subset=['stock_code', 'biz_year']).copy()
            finance_df['biz_year'] = finance_df['biz_year'].astype(int)
            finance_df['equity_ratio_pct'] = (finance_df['equity'] / finance_df['total_assets']) * 100
            finance_df.loc[
                finance_df['equity'].isna() | finance_df['total_assets'].isna() | (finance_df['total_assets'] == 0),
                'equity_ratio_pct'
            ] = pd.NA
            finance_df['revenue_growth_custom_pct'] = ((finance_df['revenue_latest'] - finance_df['revenue_1y_ago']) / finance_df['revenue_1y_ago']) * 100
            finance_df.loc[
                finance_df['revenue_latest'].isna() | finance_df['revenue_1y_ago'].isna() | (finance_df['revenue_1y_ago'] == 0),
                'revenue_growth_custom_pct'
            ] = pd.NA
        else:
            finance_df = pd.DataFrame(columns=['stock_code', 'biz_year', *finance_columns, 'equity_ratio_pct', 'revenue_growth_custom_pct'])

        finance_required_fields = [
            'sales_growth_rate_pct',
            'roe',
            'net_margin_pct',
            'debt_ratio_pct',
            'current_ratio_pct',
            'equity_ratio_pct',
            'revenue_growth_custom_pct',
        ]
        resolved_finance_year = resolve_latest_finance_year(finance_df, target_stock_code, finance_required_fields)

        if resolved_finance_year is not None:
            finance_year_df = (
                finance_df[finance_df['biz_year'] == int(resolved_finance_year)]
                .sort_values(['stock_code', 'biz_year'], ascending=[True, False])
                .drop_duplicates(subset=['stock_code'], keep='first')
                .reset_index(drop=True)
            )
            peer_finance_year_df = finance_year_df[finance_year_df['stock_code'].isin(peer_stock_codes)].copy()
            company_finance_row = finance_year_df[finance_year_df['stock_code'] == target_stock_code]
            company_finance_row = company_finance_row.iloc[0] if not company_finance_row.empty else None
        else:
            finance_year_df = finance_df.iloc[0:0].copy()
            peer_finance_year_df = finance_df.iloc[0:0].copy()
            company_finance_row = None

        eps_growth_df = build_eps_growth_dataframe(comparison_stock_codes)
        resolved_eps_date = resolve_latest_reference_date(eps_growth_df, target_stock_code, 'eps_growth_rate_pct')
        if resolved_eps_date is not None:
            eps_date_df = (
                eps_growth_df[eps_growth_df['reference_date'] == resolved_eps_date]
                .sort_values(['stock_code', 'reference_date'], ascending=[True, False])
                .drop_duplicates(subset=['stock_code'], keep='first')
                .reset_index(drop=True)
            )
            peer_eps_date_df = eps_date_df[eps_date_df['stock_code'].isin(peer_stock_codes)].copy()
            company_eps_row = eps_date_df[eps_date_df['stock_code'] == target_stock_code]
            company_eps_row = company_eps_row.iloc[0] if not company_eps_row.empty else None
        else:
            eps_date_df = eps_growth_df.iloc[0:0].copy()
            peer_eps_date_df = eps_growth_df.iloc[0:0].copy()
            company_eps_row = None

        profitability_metrics = []
        stability_metrics = []
        revenue_growth_metric = None

        if company_finance_row is not None:
            profitability_metrics.extend([
                build_percentile_metric(
                    'ROE',
                    finance_year_df['roe'],
                    company_finance_row.get('roe'),
                    benchmark_value=get_median_numeric_value(peer_finance_year_df['roe']),
                ),
                build_percentile_metric(
                    '순이익률',
                    finance_year_df['net_margin_pct'],
                    company_finance_row.get('net_margin_pct'),
                    benchmark_value=get_median_numeric_value(peer_finance_year_df['net_margin_pct']),
                ),
            ])
            stability_metrics.extend([
                build_percentile_metric(
                    '부채비율',
                    finance_year_df['debt_ratio_pct'],
                    company_finance_row.get('debt_ratio_pct'),
                    higher_is_better=False,
                    benchmark_value=get_median_numeric_value(peer_finance_year_df['debt_ratio_pct']),
                ),
                build_percentile_metric(
                    '자기자본비율',
                    finance_year_df['equity_ratio_pct'],
                    company_finance_row.get('equity_ratio_pct'),
                    benchmark_value=get_median_numeric_value(peer_finance_year_df['equity_ratio_pct']),
                ),
                build_percentile_metric(
                    '유동비율',
                    finance_year_df['current_ratio_pct'],
                    company_finance_row.get('current_ratio_pct'),
                    benchmark_value=get_median_numeric_value(peer_finance_year_df['current_ratio_pct']),
                ),
            ])
            revenue_growth_metric = build_percentile_metric(
                '매출성장률',
                finance_year_df['revenue_growth_custom_pct'],
                company_finance_row.get('revenue_growth_custom_pct'),
                benchmark_value=get_median_numeric_value(peer_finance_year_df['revenue_growth_custom_pct']),
            )

        if not profitability_metrics:
            profitability_metrics = [
                build_unavailable_metric('ROE'),
                build_unavailable_metric('순이익률'),
            ]
        else:
            profitability_metrics = [metric or build_unavailable_metric(label) for metric, label in zip(
                profitability_metrics,
                ['ROE', '순이익률']
            )]

        if not stability_metrics:
            stability_metrics = [
                build_unavailable_metric('부채비율'),
                build_unavailable_metric('유동비율'),
                build_unavailable_metric('자기자본비율'),
            ]
        else:
            stability_metrics = [metric or build_unavailable_metric(label) for metric, label in zip(
                stability_metrics,
                ['부채비율', '자기자본비율', '유동비율']
            )]

        eps_metric = None
        if company_eps_row is not None:
            eps_metric = build_percentile_metric(
                'EPS 성장률',
                eps_date_df['eps_growth_rate_pct'],
                company_eps_row.get('eps_growth_rate_pct'),
                benchmark_value=get_median_numeric_value(peer_eps_date_df['eps_growth_rate_pct']),
            )
        growth_metrics = [
            revenue_growth_metric or build_unavailable_metric('매출성장률'),
            eps_metric or build_unavailable_metric('EPS 성장률'),
        ]

        growth_note_parts = []
        if resolved_finance_year is not None:
            growth_note_parts.append(f"재무 기준 {resolved_finance_year}년")
        if resolved_eps_date is not None:
            growth_note_parts.append(f"EPS 기준 {resolved_eps_date.strftime('%Y-%m-%d')}")

        return [
            {
                'title': '수익성 지표',
                'note': f"기준 사업연도 {resolved_finance_year}년" if resolved_finance_year is not None else None,
                'metrics': profitability_metrics,
                'column_count': 2,
                'footnote': None,
            },
            {
                'title': '안정성 지표',
                'note': f"기준 사업연도 {resolved_finance_year}년" if resolved_finance_year is not None else None,
                'metrics': stability_metrics,
                'column_count': 2,
                'footnote': None,
            },
            {
                'title': '성장성 지표',
                'note': ' / '.join(growth_note_parts) if growth_note_parts else None,
                'metrics': growth_metrics,
                'column_count': 2,
                'footnote': None,
            },
        ]

    if not stock_code:
        # URL에 stock_code가 없을 경우 안전장치
        company = Basic.objects.filter(is_active=True).first()
        if company:
            stock_code = company.stock_code
        else:
            return render(request, 'finance.html')

    company = get_object_or_404(Basic.objects.select_related('ind_code'), stock_code=stock_code)
    active_codes = list(Basic.objects.filter(is_active=True).values_list('stock_code', flat=True))
    active_company_count = len(active_codes)

    # 날짜 계산: 어제 날짜 기준으로 가장 최근 개장일 찾기
    today = date.today() 
    yesterday = today - timedelta(days=1)

    # 최신 데이터 (어제 이하 기준 가장 최근)
    latest_stock = CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__lte=yesterday
    ).order_by('-reference_date').first()
    latest_company_finance = CompanyFinance.objects.filter(
        stock_code=stock_code
    ).values(
        'biz_year', 'equity', 'net_income'
    ).exclude(
        biz_year__isnull=True
    ).order_by(
        '-biz_year'
    ).first()

    base_date = latest_stock.reference_date if latest_stock else yesterday
    ninety_days_ago = base_date - timedelta(days=90)

    # 차트용 데이터 (90일 전 ~ 기준일), 날짜 오름차순(과거->최신)
    stock_qs = CompanyStock.objects.filter(
        stock_code=stock_code,
        reference_date__gte=ninety_days_ago,
        reference_date__lte=base_date
    ).order_by('reference_date')
    chart_data = build_chart_data(stock_qs)
    peer_medians = build_peer_medians(company, base_date)

    per_info_missing = True
    pbr_info_missing = True
    payout_info_missing = True

    # 현재 기업 배당성향 포맷팅 로직
    company_payout = '-'
    dividend_info_missing = True
    if latest_stock:
        per_info_missing = latest_stock.per is None
        pbr_info_missing = latest_stock.pbr is None
        eps = float(latest_stock.eps) if latest_stock.eps is not None else None
        dps = float(latest_stock.dps) if latest_stock.dps is not None else None
        dividend_info_missing = dps is None or dps == 0
        payout_info_missing = eps is None or dps is None
        
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

    latest_finance_equity = float(latest_company_finance['equity']) if latest_company_finance and latest_company_finance.get('equity') is not None else None
    latest_finance_net_income = float(latest_company_finance['net_income']) if latest_company_finance and latest_company_finance.get('net_income') is not None else None
    valuation_alerts = {
        'per_is_net_loss': latest_finance_net_income is not None and latest_finance_net_income < 0,
        'pbr_is_complete_capital_impairment': latest_finance_equity is not None and latest_finance_equity < 0,
        'biz_year': latest_company_finance.get('biz_year') if latest_company_finance else None,
    }
    financial_percentile_sections = build_financial_percentile_sections(stock_code)

    context = {
        'company': company,
        'latest_stock': latest_stock,
        'chart_data_json': chart_data,
        'peer_medians': peer_medians,
        'company_payout': company_payout,
        'per_info_missing': per_info_missing,
        'pbr_info_missing': pbr_info_missing,
        'dividend_info_missing': dividend_info_missing,
        'payout_info_missing': payout_info_missing,
        'valuation_alerts': valuation_alerts,
        'competitiveness_radar': get_competitiveness_radar_context(stock_code),
        'financial_percentile_sections': financial_percentile_sections,
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
            return f"{int(value):,}원"
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

        return f"{sign}{abs(change_value):,}원 ({abs(rate_value):.2f}%)", css_class

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
            'asset_trend_tone': 'neutral',
            'sales_trend_title': default_title,
            'sales_trend_desc': default_desc,
            'sales_trend_tone': 'neutral',
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
            return '데이터 부족', '최근 3개년 데이터가 충분하지 않아 추세를 판단하기 어렵습니다.', 'neutral'

        prev2, prev1, recent = valid[-3], valid[-2], valid[-1]
        first = '상승' if prev1 >= prev2 else '하락'
        second = '상승' if recent >= prev1 else '하락'

        return message_map.get(
            (first, second),
            ('데이터 부족', '최근 3개년 데이터가 충분하지 않아 추세를 판단하기 어렵습니다.', 'neutral'),
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
            ('상승', '상승'): ('자산 성장 지속', '최근 산업의 자산 규모가 계속 커지고 있습니다.', 'positive'),
            (
                '상승',
                '하락',
            ): (
                '자산 성장 둔화',
                '이전 기간 대비 증가율이 낮아졌습니다.\n산업 성장 속도가 다소 둔화된 흐름입니다.',
                'negative',
            ),
            ('하락', '상승'): ('자산 회복 신호', '줄어들던 자산이 다시 증가세로 전환되었습니다.', 'positive'),
            ('하락', '하락'): ('자산 감소 지속', '산업 자산 규모가 계속 줄어드는 흐름입니다.', 'negative'),
        }

        sales_message_map = {
            (
                '상승',
                '상승',
            ): (
                '매출 성장 지속',
                '최근 산업의 매출 규모가 계속 확대되고 있습니다.\n시장 수요가 유지되며 성장 흐름이 이어지는 모습입니다.',
                'positive',
            ),
            (
                '상승',
                '하락',
            ): (
                '매출 성장 둔화',
                '이전 기간 대비 매출 증가율이 낮아졌습니다.\n산업 매출 성장 속도가 다소 완만해진 흐름입니다.',
                'negative',
            ),
            (
                '하락',
                '상승',
            ): (
                '매출 회복 신호',
                '감소하던 매출 증가율이 다시 상승세로 전환되었습니다.\n시장 수요가 회복되는 흐름일 수 있습니다.',
                'positive',
            ),
            (
                '하락',
                '하락',
            ): (
                '매출 감소 지속',
                '산업 매출 증가율이 계속 낮아지고 있습니다.\n시장 수요가 약화되는 흐름일 수 있습니다.',
                'negative',
            ),
        }

        asset_trend_title, asset_trend_desc, asset_trend_tone = get_trend_message(industry_asset, asset_message_map)
        sales_trend_title, sales_trend_desc, sales_trend_tone = get_trend_message(industry_sales, sales_message_map)

        return {
            'growth_chart_labels': years,
            'industry_asset_growth': industry_asset,
            'industry_sales_growth': industry_sales,
            'benchmark_asset_growth': benchmark_asset,
            'benchmark_sales_growth': benchmark_sales,
            'asset_trend_title': asset_trend_title,
            'asset_trend_desc': asset_trend_desc,
            'asset_trend_tone': asset_trend_tone,
            'sales_trend_title': sales_trend_title,
            'sales_trend_desc': sales_trend_desc,
            'sales_trend_tone': sales_trend_tone,
        }

    def get_industry_marketcap_data(company_obj):
        peer_codes = list(
            Basic.objects.filter(
                ind_code=company_obj.ind_code,
                is_active=True,
            ).values_list('stock_code', flat=True)
        )
        reference_date = (
            CompanyStock.objects.filter(stock_code=company_obj.stock_code)
            .order_by('-reference_date')
            .values_list('reference_date', flat=True)
            .first()
        )
        if reference_date is None and peer_codes:
            reference_date = (
                CompanyStock.objects.filter(stock_code__in=peer_codes)
                .order_by('-reference_date')
                .values_list('reference_date', flat=True)
                .first()
            )

        industry_qs = Basic.objects.filter(
            ind_code=company_obj.ind_code,
            is_active=True,
        ).annotate(
            latest_mktcap=Subquery(
                CompanyStock.objects.filter(
                    stock_code=OuterRef('stock_code'),
                    reference_date=reference_date,
                ).values('mktcap')[:1]
            ),
            latest_price_change=Subquery(
                CompanyStock.objects.filter(
                    stock_code=OuterRef('stock_code'),
                    reference_date=reference_date,
                ).values('price_change')[:1]
            ),
            latest_fluc_rt=Subquery(
                CompanyStock.objects.filter(
                    stock_code=OuterRef('stock_code'),
                    reference_date=reference_date,
                ).values('fluc_rt')[:1]
            ),
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
            'reference_date': reference_date,
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
            'industry_reference_date': None,
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
    industry_reference_date = None
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
            industry_reference_date = marketcap_data['reference_date']
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
        'industry_reference_date': industry_reference_date,
        'company_topic_particle': company_topic_particle(company.corp_name),
        **structure_context,
        **growth_context,
    }
    return render(request, 'industry.html', context)
