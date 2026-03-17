from django.shortcuts import render, get_object_or_404, redirect
from django.views.decorators.cache import cache_page
from datetime import date, timedelta
import math
import threading
import statistics
import json
import pandas as pd
from django.db.models import OuterRef, Subquery
from django.db.utils import OperationalError, ProgrammingError

from .models import Basic, BokIo, CompanyFinance, CompanyStock, IndBok, IndIo, Report


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
PAGE_CACHE_SECONDS = 60 * 15

@cache_page(PAGE_CACHE_SECONDS, key_prefix='home_page')
def home(request):
    count = Report.objects.count()
    return render(request, 'home.html', {'count': count})

@cache_page(PAGE_CACHE_SECONDS, key_prefix='search_page')
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

@cache_page(PAGE_CACHE_SECONDS, key_prefix='ai_page')
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

@cache_page(PAGE_CACHE_SECONDS, key_prefix='overview_page')
def overview(request):
    return render(request, 'overview.html')

@cache_page(PAGE_CACHE_SECONDS, key_prefix='finance_page')
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
        peer_count = len(peer_codes)

        if not peer_codes:
            return {
                'per': '-',
                'pbr': '-',
                'dividend_yield': '-',
                'payout_ratio': '-',
                'count': 0,
            }

        latest_reference_subquery = (
            CompanyStock.objects.filter(
                stock_code=OuterRef('stock_code'),
                reference_date__lte=reference_day,
            )
            .order_by('-reference_date')
            .values('reference_date')[:1]
        )

        peer_stocks = (
            CompanyStock.objects.filter(
                stock_code__in=peer_codes,
                reference_date=Subquery(latest_reference_subquery),
            )
            .values('eps', 'dps', 'per', 'pbr', 'dividend_yield')
        )

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
    peer_medians = build_peer_medians(company, yesterday)

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
    }
    return render(request, 'finance.html', context)

@cache_page(PAGE_CACHE_SECONDS, key_prefix='industry_page')
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

        industry_qs = Basic.objects.filter(
            ind_code=company_obj.ind_code,
            is_active=True,
        ).annotate(
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
@cache_page(PAGE_CACHE_SECONDS, key_prefix='a_page')
def a(request):
    return render(request, 'a.html')


@cache_page(PAGE_CACHE_SECONDS, key_prefix='about_page')
def about(request):
    return render(request, 'about.html')


@cache_page(PAGE_CACHE_SECONDS, key_prefix='stats_page')
def stats(request):
    return render(request, 'stats.html')
