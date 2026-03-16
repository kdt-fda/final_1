import pandas as pd
from django.test import SimpleTestCase

from .views import _build_competitiveness_radar_context, _calculate_percentile_score


class CompetitivenessRadarLogicTests(SimpleTestCase):
    def test_debt_ratio_percentile_is_inverted(self):
        series = pd.Series([10, 20, 30, 40])
        score = _calculate_percentile_score(series, 10, higher_is_better=False)
        self.assertEqual(score, 75.0)

    def test_large_industry_falls_back_to_latest_peer_year(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2024, 'roe': 40, 'gross_margin_pct': 30, 'debt_ratio_pct': 20, 'sales_growth_rate_pct': 15, 'cashholding_ratio_pct': 12},
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2023, 'roe': 35, 'gross_margin_pct': 28, 'debt_ratio_pct': 25, 'sales_growth_rate_pct': 14, 'cashholding_ratio_pct': 11},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2023, 'roe': 10, 'gross_margin_pct': 18, 'debt_ratio_pct': 80, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 4},
            {'stock_code': 'A2', 'ind_code': 'A', 'biz_year': 2023, 'roe': 15, 'gross_margin_pct': 20, 'debt_ratio_pct': 70, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'A3', 'ind_code': 'A', 'biz_year': 2023, 'roe': 20, 'gross_margin_pct': 22, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'A4', 'ind_code': 'A', 'biz_year': 2023, 'roe': 25, 'gross_margin_pct': 24, 'debt_ratio_pct': 50, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 18, 'gross_margin_pct': 19, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 9, 'cashholding_ratio_pct': 8},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 22, 'gross_margin_pct': 21, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 10, 'cashholding_ratio_pct': 9},
            {'stock_code': 'B3', 'ind_code': 'B', 'biz_year': 2023, 'roe': 16, 'gross_margin_pct': 17, 'debt_ratio_pct': 65, 'sales_growth_rate_pct': 11, 'cashholding_ratio_pct': 10},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T', confirmed_years=[2023, 2024])

        self.assertTrue(context['available'])
        self.assertEqual(context['target_year'], 2024)
        self.assertEqual(context['peer_year'], 2023)
        self.assertEqual(context['peer_count'], 5)

    def test_confirmed_year_rule_uses_latest_confirmed_target_year(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2025, 'roe': 45, 'gross_margin_pct': 31, 'debt_ratio_pct': 18, 'sales_growth_rate_pct': 16, 'cashholding_ratio_pct': 13},
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2024, 'roe': 40, 'gross_margin_pct': 30, 'debt_ratio_pct': 20, 'sales_growth_rate_pct': 15, 'cashholding_ratio_pct': 12},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2024, 'roe': 10, 'gross_margin_pct': 18, 'debt_ratio_pct': 80, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 4},
            {'stock_code': 'A2', 'ind_code': 'A', 'biz_year': 2024, 'roe': 15, 'gross_margin_pct': 20, 'debt_ratio_pct': 70, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'A3', 'ind_code': 'A', 'biz_year': 2024, 'roe': 20, 'gross_margin_pct': 22, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 18, 'gross_margin_pct': 19, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 9, 'cashholding_ratio_pct': 8},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 22, 'gross_margin_pct': 21, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 10, 'cashholding_ratio_pct': 9},
            {'stock_code': 'B3', 'ind_code': 'B', 'biz_year': 2024, 'roe': 16, 'gross_margin_pct': 17, 'debt_ratio_pct': 65, 'sales_growth_rate_pct': 11, 'cashholding_ratio_pct': 10},
            {'stock_code': 'B4', 'ind_code': 'B', 'biz_year': 2024, 'roe': 14, 'gross_margin_pct': 16, 'debt_ratio_pct': 68, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T')

        self.assertTrue(context['available'])
        self.assertEqual(context['target_year'], 2024)
        self.assertEqual(context['peer_year'], 2024)
        self.assertEqual(context['peer_count'], 4)

    def test_context_is_unavailable_when_target_has_only_unconfirmed_year(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2025, 'roe': 45, 'gross_margin_pct': 31, 'debt_ratio_pct': 18, 'sales_growth_rate_pct': 16, 'cashholding_ratio_pct': 13},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2024, 'roe': 10, 'gross_margin_pct': 18, 'debt_ratio_pct': 80, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 4},
            {'stock_code': 'A2', 'ind_code': 'A', 'biz_year': 2024, 'roe': 15, 'gross_margin_pct': 20, 'debt_ratio_pct': 70, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'A3', 'ind_code': 'A', 'biz_year': 2024, 'roe': 20, 'gross_margin_pct': 22, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 18, 'gross_margin_pct': 19, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 9, 'cashholding_ratio_pct': 8},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 22, 'gross_margin_pct': 21, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 10, 'cashholding_ratio_pct': 9},
            {'stock_code': 'B3', 'ind_code': 'B', 'biz_year': 2024, 'roe': 16, 'gross_margin_pct': 17, 'debt_ratio_pct': 65, 'sales_growth_rate_pct': 11, 'cashholding_ratio_pct': 10},
            {'stock_code': 'B4', 'ind_code': 'B', 'biz_year': 2024, 'roe': 14, 'gross_margin_pct': 16, 'debt_ratio_pct': 68, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T')

        self.assertFalse(context['available'])
        self.assertIsNone(context['target_year'])
        self.assertIsNone(context['peer_year'])
        self.assertEqual(context['peer_count'], 0)

    def test_small_industry_uses_latest_common_year_for_everyone(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'S1', 'ind_code': 'S', 'biz_year': 2024, 'roe': 45, 'gross_margin_pct': 30, 'debt_ratio_pct': 25, 'sales_growth_rate_pct': 18, 'cashholding_ratio_pct': 15},
            {'stock_code': 'S1', 'ind_code': 'S', 'biz_year': 2023, 'roe': 30, 'gross_margin_pct': 24, 'debt_ratio_pct': 35, 'sales_growth_rate_pct': 10, 'cashholding_ratio_pct': 9},
            {'stock_code': 'S2', 'ind_code': 'S', 'biz_year': 2023, 'roe': 20, 'gross_margin_pct': 19, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
            {'stock_code': 'S3', 'ind_code': 'S', 'biz_year': 2023, 'roe': 10, 'gross_margin_pct': 15, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 35, 'gross_margin_pct': 28, 'debt_ratio_pct': 30, 'sales_growth_rate_pct': 12, 'cashholding_ratio_pct': 10},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2023, 'roe': 25, 'gross_margin_pct': 22, 'debt_ratio_pct': 40, 'sales_growth_rate_pct': 9, 'cashholding_ratio_pct': 8},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'S1')

        self.assertEqual(context['target_year'], 2023)
        self.assertEqual(context['peer_year'], 2023)
        self.assertEqual(context['peer_count'], 3)
        self.assertEqual(context['metrics'][0]['company_display'], '30.0%')

    def test_peer_average_uses_score_average_not_raw_average(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2024, 'roe': 50, 'gross_margin_pct': 30, 'debt_ratio_pct': 20, 'sales_growth_rate_pct': 15, 'cashholding_ratio_pct': 12},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2024, 'roe': 10, 'gross_margin_pct': 10, 'debt_ratio_pct': 80, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 4},
            {'stock_code': 'A2', 'ind_code': 'A', 'biz_year': 2024, 'roe': 20, 'gross_margin_pct': 15, 'debt_ratio_pct': 70, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 30, 'gross_margin_pct': 20, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 40, 'gross_margin_pct': 25, 'debt_ratio_pct': 50, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T')

        self.assertEqual(context['peer_year'], 2024)
        self.assertEqual(context['metrics'][0]['peer_average_score'], 53.3)

    def test_analysis_items_use_top_two_percentile_gaps(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2024, 'roe': 50, 'gross_margin_pct': 12, 'debt_ratio_pct': 90, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 3},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2024, 'roe': 20, 'gross_margin_pct': 20, 'debt_ratio_pct': 40, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 5},
            {'stock_code': 'A2', 'ind_code': 'A', 'biz_year': 2024, 'roe': 25, 'gross_margin_pct': 18, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 6},
            {'stock_code': 'A3', 'ind_code': 'A', 'biz_year': 2024, 'roe': 30, 'gross_margin_pct': 16, 'debt_ratio_pct': 50, 'sales_growth_rate_pct': 9, 'cashholding_ratio_pct': 7},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 35, 'gross_margin_pct': 24, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 10, 'cashholding_ratio_pct': 8},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 40, 'gross_margin_pct': 26, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 11, 'cashholding_ratio_pct': 9},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T')
        expected_top_two = []
        for index, metric in enumerate(context['metrics']):
            if metric['company_score'] is None or metric['peer_average_score'] is None:
                continue
            score_gap = round(metric['company_score'] - metric['peer_average_score'], 1)
            expected_top_two.append((index, metric['key'], score_gap))

        expected_top_two.sort(key=lambda item: (-abs(item[2]), item[0]))
        expected_top_two = expected_top_two[:2]

        self.assertEqual(len(context['analysis_items']), 2)
        self.assertEqual(
            [item['key'] for item in context['analysis_items']],
            [item[1] for item in expected_top_two],
        )
        self.assertEqual(
            [item['status'] for item in context['analysis_items']],
            ['strong' if item[2] >= 0 else 'weak' for item in expected_top_two],
        )

    def test_single_company_peer_group_falls_back_to_kosdaq_average(self):
        finance_df = pd.DataFrame.from_records([
            {'stock_code': 'T', 'ind_code': 'A', 'biz_year': 2024, 'roe': 50, 'gross_margin_pct': 30, 'debt_ratio_pct': 20, 'sales_growth_rate_pct': 15, 'cashholding_ratio_pct': 12},
            {'stock_code': 'A1', 'ind_code': 'A', 'biz_year': 2023, 'roe': 15, 'gross_margin_pct': 18, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'B1', 'ind_code': 'B', 'biz_year': 2024, 'roe': 10, 'gross_margin_pct': 20, 'debt_ratio_pct': 60, 'sales_growth_rate_pct': 5, 'cashholding_ratio_pct': 4},
            {'stock_code': 'B2', 'ind_code': 'B', 'biz_year': 2024, 'roe': 20, 'gross_margin_pct': 22, 'debt_ratio_pct': 55, 'sales_growth_rate_pct': 6, 'cashholding_ratio_pct': 5},
            {'stock_code': 'B3', 'ind_code': 'B', 'biz_year': 2024, 'roe': 30, 'gross_margin_pct': 24, 'debt_ratio_pct': 50, 'sales_growth_rate_pct': 7, 'cashholding_ratio_pct': 6},
            {'stock_code': 'B4', 'ind_code': 'B', 'biz_year': 2024, 'roe': 40, 'gross_margin_pct': 26, 'debt_ratio_pct': 45, 'sales_growth_rate_pct': 8, 'cashholding_ratio_pct': 7},
        ])

        context = _build_competitiveness_radar_context(finance_df, 'T')

        self.assertTrue(context['uses_market_average_fallback'])
        self.assertEqual(context['comparison_label'], 'KOSDAQ 기업 평균')
        self.assertEqual(context['peer_count'], 1)
        self.assertEqual(context['metrics'][0]['peer_average_score'], 60.0)
        self.assertTrue(all('KOSDAQ 평균보다' in item['message'] for item in context['analysis_items']))
