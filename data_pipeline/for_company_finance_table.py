from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests


temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

from common.setting import get_connection, BASE_DIR  # noqa: E402

data_dir = BASE_DIR / "data"
data_dir.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://opendart.fss.or.kr/api"
TIMEOUT = 30
REQUEST_SLEEP = 0.12


class OpenDartClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("DART_API 환경변수가 없습니다.")
        self.api_key = api_key
        self.session = requests.Session()

    def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
        params = params or {}
        params["crtfc_key"] = self.api_key
        url = f"{BASE_URL}/{path}"

        # 간단 재시도
        last_error = None
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
                if attempt < 2:
                    time.sleep(0.4 * (attempt + 1))
                else:
                    raise last_error

    def list_reports(
        self,
        corp_code: str,
        bgn_de: str = "20200101",
        end_de: str = "20301231",
        page_count: int = 100,
    ) -> pd.DataFrame:
        rows: List[dict] = []
        page_no = 1

        while True:
            data = self._get_json(
                "list.json",
                {
                    "corp_code": corp_code,
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "page_no": page_no,
                    "page_count": page_count,
                },
            )

            status = data.get("status")
            if status == "013":
                break
            if status != "000":
                raise RuntimeError(
                    f"DART list 조회 실패: corp_code={corp_code}, status={status}, message={data.get('message')}"
                )

            batch = data.get("list", [])
            if not batch:
                break

            rows.extend(batch)

            total_count = int(data.get("total_count", 0) or 0)
            if page_no * page_count >= total_count:
                break

            page_no += 1
            time.sleep(REQUEST_SLEEP)

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def get_financial_statement_all(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
        fs_div: str = "CFS",
    ) -> pd.DataFrame:
        data = self._get_json(
            "fnlttSinglAcntAll.json",
            {
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )

        status = data.get("status")
        if status == "013":
            return pd.DataFrame()
        if status != "000":
            raise RuntimeError(
                f"재무제표 조회 실패: corp_code={corp_code}, bsns_year={bsns_year}, status={status}, message={data.get('message')}"
            )

        rows = data.get("list", [])
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def get_major_shareholders(
        self,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
    ) -> dict:
        data = self._get_json(
            "hyslrSttus.json",
            {
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
        )
        return data


def get_table_columns(conn, table_name: str) -> List[str]:
    sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND UPPER(TABLE_NAME) = UPPER(%s)
        ORDER BY ORDINAL_POSITION
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (table_name,))
        rows = cursor.fetchall()

    cols: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            cols.append(row["COLUMN_NAME"])
        else:
            cols.append(row[0])
    return cols


def assert_company_finance_schema(conn):
    cols = [c.lower() for c in get_table_columns(conn, "COMPANY_FINANCE")]
    required = [
        "stock_code",
        "corp_code",
        "biz_year",
        "currency",
        "source_report_num",
        "source_report_nm",
        "source_report_date",
        "match_status",
    ]
    missing = [c for c in required if c not in cols]
    if missing:
        raise RuntimeError(
            f"COMPANY_FINANCE 스키마 불일치. 누락 컬럼: {missing}. "
            f"테이블 drop 후 create_company_finance()로 다시 생성하세요."
        )


def create_company_finance(conn):
    create_sql = """
        CREATE TABLE IF NOT EXISTS COMPANY_FINANCE (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(10) NOT NULL,
            corp_code VARCHAR(15) NOT NULL,
            biz_year INT NOT NULL,
            currency VARCHAR(20),
            total_assets DECIMAL(20,0),
            cash_and_equivalents DECIMAL(20,0),
            current_assets DECIMAL(20,0),
            accounts_receivable DECIMAL(20,0),
            liabilities DECIMAL(20,0),
            current_liabilities DECIMAL(20,0),
            equity DECIMAL(20,0),
            capital_stock DECIMAL(20,0),
            revenue_latest DECIMAL(20,0),
            revenue_1y_ago DECIMAL(20,0),
            revenue_2y_ago DECIMAL(20,0),
            gross_profit DECIMAL(20,0),
            net_income DECIMAL(20,0),
            cashholding_ratio_pct DECIMAL(20,6),
            sales_growth_rate_pct DECIMAL(20,6),
            gross_margin_pct DECIMAL(20,6),
            roe DECIMAL(20,6),
            net_margin_pct DECIMAL(20,6),
            debt_ratio_pct DECIMAL(20,6),
            current_ratio_pct DECIMAL(20,6),
            maj_shareholders MEDIUMTEXT,
            source_report_num VARCHAR(20),
            source_report_nm VARCHAR(255),
            source_report_date DATE,
            match_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_company_finance (corp_code, biz_year),
            KEY idx_company_finance_stock_code (stock_code),
            KEY idx_company_finance_corp_code (corp_code),
            KEY idx_company_finance_report_num (source_report_num)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cursor:
        cursor.execute(create_sql)
    print("✅ COMPANY_FINANCE CREATE 확인 완료")


def safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"none", "nan", "nat"}:
        return None
    return s


def to_decimal_number(v: Any) -> Optional[float]:
    s = safe_str(v)
    if s is None:
        return None
    s = s.replace(",", "").replace(" ", "")
    if s in {"-", "--"}:
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None


def fetch_basic_targets(conn, kosdaq_only: bool = True, limit: Optional[int] = None) -> pd.DataFrame:
    cols = get_table_columns(conn, "BASIC")
    lower_map = {c.lower(): c for c in cols}

    required = ["corp_code", "stock_code"]
    for req in required:
        if req not in lower_map:
            raise ValueError(f"BASIC 테이블에 '{req}' 컬럼이 없습니다.")

    corp_code_col = lower_map["corp_code"]
    stock_code_col = lower_map["stock_code"]
    corp_name_col = lower_map.get("corp_name") or lower_map.get("corp_nm")
    is_active_col = lower_map.get("is_active")
    market_col = lower_map.get("market")
    corp_cls_col = lower_map.get("corp_cls")

    select_cols = [
        f"{corp_code_col} AS corp_code",
        f"{stock_code_col} AS stock_code",
        f"{corp_name_col} AS corp_name" if corp_name_col else "NULL AS corp_name",
    ]

    where_parts = [
        f"{stock_code_col} IS NOT NULL",
        f"{stock_code_col} <> ''",
        f"{corp_code_col} IS NOT NULL",
        f"{corp_code_col} <> ''",
    ]
    params: List[Any] = []

    if is_active_col:
        where_parts.append(f"COALESCE({is_active_col}, 0) = 1")

    if kosdaq_only:
        market_cond_parts = []
        if market_col:
            market_cond_parts.append(f"UPPER(COALESCE({market_col}, '')) = 'KOSDAQ'")
        if corp_cls_col:
            market_cond_parts.append(f"UPPER(COALESCE({corp_cls_col}, '')) = 'K'")
        if market_cond_parts:
            where_parts.append("(" + " OR ".join(market_cond_parts) + ")")

    sql = f"SELECT {', '.join(select_cols)} FROM BASIC WHERE {' AND '.join(where_parts)} ORDER BY {stock_code_col}"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["corp_code", "stock_code", "corp_name"])

    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    return df


def fetch_report_hints(conn) -> Dict[str, str]:
    cols = get_table_columns(conn, "REPORT")
    if not cols:
        print("?? REPORT ???? ?? ?? report_num ?? ?? ?????.")
        return {}

    lower_map = {c.lower(): c for c in cols}
    stock_code_col = lower_map.get("stock_code")
    report_num_col = lower_map.get("report_num") or lower_map.get("rcept_no")
    rcept_dt_col = lower_map.get("rcept_dt") or lower_map.get("report_date") or lower_map.get("rcp_dt")

    if not stock_code_col or not report_num_col:
        print("?? REPORT ???? stock_code/report_num ?? ??? ??? ?? ?? ?????.")
        return {}

    select_cols = [f"{stock_code_col} AS stock_code", f"{report_num_col} AS report_num"]
    if rcept_dt_col:
        select_cols.append(f"{rcept_dt_col} AS rcept_dt")

    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM REPORT
        WHERE {stock_code_col} IS NOT NULL
          AND {report_num_col} IS NOT NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    if not rows:
        return {}

    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df["report_num"] = df["report_num"].astype(str)
    sort_cols = ["stock_code", "report_num"]
    ascending = [True, False]
    if "rcept_dt" in df.columns:
        df["rcept_dt"] = pd.to_datetime(df["rcept_dt"], errors="coerce")
        sort_cols = ["stock_code", "rcept_dt", "report_num"]
        ascending = [True, False, False]
    df = df.sort_values(sort_cols, ascending=ascending)
    df = df.drop_duplicates(subset=["stock_code"], keep="first")
    return df.set_index("stock_code")["report_num"].to_dict()

def fetch_existing_company_finance_map(conn) -> Dict[str, str]:
    cols = get_table_columns(conn, "COMPANY_FINANCE")
    if not cols:
        return {}

    lower_map = {c.lower(): c for c in cols}
    stock_code_col = lower_map.get("stock_code")
    source_report_num_col = lower_map.get("source_report_num")
    updated_at_col = lower_map.get("updated_at") or lower_map.get("created_at")

    if not stock_code_col or not source_report_num_col:
        return {}

    select_cols = [
        f"{stock_code_col} AS stock_code",
        f"{source_report_num_col} AS source_report_num",
    ]
    if updated_at_col:
        select_cols.append(f"{updated_at_col} AS updated_at")

    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM COMPANY_FINANCE
        WHERE {stock_code_col} IS NOT NULL
          AND {source_report_num_col} IS NOT NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    if not rows:
        return {}

    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df["source_report_num"] = df["source_report_num"].astype(str)
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df = df.sort_values(["stock_code", "updated_at"], ascending=[True, False])
    df = df.drop_duplicates(subset=["stock_code"], keep="first")
    return df.set_index("stock_code")["source_report_num"].to_dict()

def parse_bsns_year_from_report_nm(report_nm: Optional[str], rcept_dt: Optional[str]) -> Optional[int]:
    s = safe_str(report_nm)
    if s:
        m = re.search(r"(20\d{2})\.(?:12|09|06|03)", s)
        if m:
            return int(m.group(1))
    dt = safe_str(rcept_dt)
    if dt and len(dt) >= 4:
        return int(dt[:4]) - 1
    return None


def report_kind_priority(report_nm: Optional[str]) -> int:
    s = safe_str(report_nm) or ""
    if "사업보고서" in s and "기재정정" not in s and "정정" not in s:
        return 1
    if "사업보고서" in s and "정정" in s and "기재정정" not in s:
        return 2
    if "기재정정" in s and "사업보고서" in s:
        return 3
    if "사업보고서" in s:
        return 4
    return 99


def build_business_candidates(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["rcept_no", "report_nm", "rcept_dt", "bsns_year", "priority"])

    df = report_df.copy()
    if "report_nm" not in df.columns or "rcept_no" not in df.columns:
        return pd.DataFrame(columns=["rcept_no", "report_nm", "rcept_dt", "bsns_year", "priority"])

    df = df[df["report_nm"].astype(str).str.contains("사업보고서", na=False)].copy()
    if df.empty:
        return pd.DataFrame(columns=["rcept_no", "report_nm", "rcept_dt", "bsns_year", "priority"])

    df["bsns_year"] = df.apply(lambda r: parse_bsns_year_from_report_nm(r.get("report_nm"), r.get("rcept_dt")), axis=1)
    df["priority"] = df["report_nm"].apply(report_kind_priority)
    df["rcept_dt"] = df["rcept_dt"].astype(str)
    df["rcept_no"] = df["rcept_no"].astype(str)
    df = df.sort_values(["bsns_year", "priority", "rcept_dt", "rcept_no"], ascending=[False, True, False, False])
    return df.reset_index(drop=True)


def choose_report_candidate(
    business_df: pd.DataFrame,
    hinted_report_num: Optional[str] = None,
    target_year: Optional[int] = None,
) -> Tuple[Optional[pd.Series], str]:
    if business_df.empty:
        return None, "no_business_report_candidate"

    df = business_df.copy()
    if target_year is not None:
        df_year = df[df["bsns_year"] == target_year].copy()
        if not df_year.empty:
            df = df_year

    hinted_report_num = safe_str(hinted_report_num)
    if hinted_report_num:
        exact = df[df["rcept_no"] == hinted_report_num]
        if not exact.empty:
            return exact.iloc[0], "exact_match"

    if hinted_report_num and len(hinted_report_num) >= 4:
        same_prefix = df[df["rcept_no"].str[:4] == hinted_report_num[:4]]
        if not same_prefix.empty:
            row = same_prefix.iloc[0]
            return row, f"same_year_prefix_{int(row.get('priority', 99))}"

    row = df.iloc[0]
    status_map = {
        1: "fallback_business_report",
        2: "fallback_business_report_amended",
        3: "fallback_business_report_correction",
        4: "fallback_business_report_other",
    }
    return row, status_map.get(int(row.get("priority", 99)), "fallback_business_report_other")


def normalize_account_rows(fs_df: pd.DataFrame) -> pd.DataFrame:
    if fs_df.empty:
        return fs_df
    out = fs_df.copy()
    for c in ["account_nm", "account_id", "sj_nm", "thstrm_amount", "fs_div", "currency"]:
        if c not in out.columns:
            out[c] = None
    out["account_nm"] = out["account_nm"].astype(str)
    out["account_id"] = out["account_id"].astype(str)
    out["sj_nm"] = out["sj_nm"].astype(str)
    out["currency"] = out["currency"].astype(str)
    return out


def extract_currency(fs_df: pd.DataFrame) -> Optional[str]:
    if fs_df.empty:
        return None
    df = normalize_account_rows(fs_df)

    vals = df["currency"].dropna().astype(str).str.strip()
    vals = vals[~vals.isin(["", "nan", "None", "null"])]
    if vals.empty:
        return None
    return vals.iloc[0]


def account_amount(
    fs_df: pd.DataFrame,
    account_ids: List[str],
    account_name_keywords: List[str],
    sj_keywords: Optional[List[str]] = None,
) -> Optional[float]:
    if fs_df.empty:
        return None
    df = normalize_account_rows(fs_df)

    id_mask = df["account_id"].isin(account_ids)
    nm_mask = pd.Series(False, index=df.index)
    for kw in account_name_keywords:
        nm_mask = nm_mask | df["account_nm"].str.contains(kw, na=False)

    mask = id_mask | nm_mask
    if sj_keywords:
        sj_mask = pd.Series(False, index=df.index)
        for kw in sj_keywords:
            sj_mask = sj_mask | df["sj_nm"].str.contains(kw, na=False)
        mask = mask & sj_mask

    candidates = df.loc[mask].copy()
    if candidates.empty:
        return None

    candidates["id_priority"] = candidates["account_id"].isin(account_ids).astype(int)
    candidates = candidates.sort_values(["id_priority"], ascending=[False])

    for _, row in candidates.iterrows():
        val = to_decimal_number(row.get("thstrm_amount"))
        if val is not None:
            return val
    return None


def extract_major_shareholders_text(data: dict) -> Optional[str]:
    rows = data.get("list", [])
    if not rows:
        return None

    texts = []
    for row in rows:
        nm = row.get("nm")
        stock_knd = row.get("stock_knd")
        stock_co = row.get("trmend_posesn_stock_co")
        share_rt = row.get("trmend_posesn_stock_qota_rt")

        part = []
        if nm:
            part.append(str(nm).strip())
        if stock_knd:
            part.append(f"({str(stock_knd).strip()})")
        if stock_co:
            part.append(f"{stock_co}주")
        if share_rt:
            part.append(f"{share_rt}%")

        text = " ".join(part).strip()
        if text:
            texts.append(text)

    return " | ".join(texts) if texts else None


def get_statement_snapshot(
    client: OpenDartClient,
    corp_code: str,
    business_df: pd.DataFrame,
    target_year: int,
    hinted_report_num: Optional[str],
) -> Dict[str, Any]:
    candidate, match_status = choose_report_candidate(
        business_df,
        hinted_report_num=hinted_report_num,
        target_year=target_year,
    )
    if candidate is None:
        return {
            "biz_year": target_year,
            "report_num": None,
            "report_nm": None,
            "report_date": None,
            "match_status": match_status,
            "fs_df": pd.DataFrame(),
        }

    fs_df = client.get_financial_statement_all(
        corp_code=corp_code,
        bsns_year=target_year,
        reprt_code="11011",
        fs_div="CFS",
    )
    if fs_df.empty:
        fs_df = client.get_financial_statement_all(
            corp_code=corp_code,
            bsns_year=target_year,
            reprt_code="11011",
            fs_div="OFS",
        )
        if not fs_df.empty:
            match_status = match_status + "_ofs"

    return {
        "biz_year": target_year,
        "report_num": candidate.get("rcept_no"),
        "report_nm": candidate.get("report_nm"),
        "report_date": candidate.get("rcept_dt"),
        "match_status": match_status,
        "fs_df": fs_df,
    }


def build_company_finance_row(
    client: OpenDartClient,
    corp_code: str,
    stock_code: str,
    snapshot_curr: Dict[str, Any],
    snapshot_prev1: Dict[str, Any],
    snapshot_prev2: Dict[str, Any],
) -> Dict[str, Any]:
    fs = snapshot_curr["fs_df"]
    if fs.empty:
        raise ValueError(
            f"재무제표 없음: corp_code={corp_code}, biz_year={snapshot_curr['biz_year']}, match_status={snapshot_curr['match_status']}"
        )

    currency = extract_currency(fs)

    total_assets = account_amount(fs, ["ifrs-full_Assets", "ifrs_Assets"], ["자산총계"], ["재무상태표"])
    cash_and_equivalents = account_amount(fs, ["ifrs-full_CashAndCashEquivalents", "ifrs_CashAndCashEquivalents"], ["현금및현금성자산"], ["재무상태표"])
    current_assets = account_amount(fs, ["ifrs-full_CurrentAssets", "ifrs_CurrentAssets"], ["유동자산"], ["재무상태표"])
    accounts_receivable = account_amount(
        fs,
        ["ifrs-full_TradeAndOtherCurrentReceivables", "dart_ShortTermTradeReceivable", "ifrs_TradeReceivables"],
        ["매출채권"],
        ["재무상태표"],
    )
    liabilities = account_amount(fs, ["ifrs-full_Liabilities", "ifrs_Liabilities"], ["부채총계"], ["재무상태표"])
    current_liabilities = account_amount(fs, ["ifrs-full_CurrentLiabilities", "ifrs_CurrentLiabilities"], ["유동부채"], ["재무상태표"])
    equity = account_amount(fs, ["ifrs-full_Equity", "ifrs_Equity"], ["자본총계"], ["재무상태표"])
    capital_stock = account_amount(fs, ["ifrs-full_IssuedCapital", "dart_IssuedCapitalOfCommonStock"], ["자본금"], ["재무상태표"])

    revenue_latest = account_amount(fs, ["ifrs-full_Revenue", "ifrs_Revenue", "dart_OperatingRevenue"], ["매출액", "영업수익"], ["손익계산서", "포괄손익계산서"])
    gross_profit = account_amount(fs, ["ifrs-full_GrossProfit"], ["매출총이익"], ["손익계산서", "포괄손익계산서"])
    net_income = account_amount(fs, ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"], ["당기순이익", "당기순손익"], ["손익계산서", "포괄손익계산서"])

    fs_prev1 = snapshot_prev1["fs_df"]
    fs_prev2 = snapshot_prev2["fs_df"]
    revenue_1y_ago = account_amount(fs_prev1, ["ifrs-full_Revenue", "ifrs_Revenue", "dart_OperatingRevenue"], ["매출액", "영업수익"], ["손익계산서", "포괄손익계산서"])
    revenue_2y_ago = account_amount(fs_prev2, ["ifrs-full_Revenue", "ifrs_Revenue", "dart_OperatingRevenue"], ["매출액", "영업수익"], ["손익계산서", "포괄손익계산서"])

    cashholding_ratio_pct = (cash_and_equivalents / total_assets * 100.0) if cash_and_equivalents is not None and total_assets not in (None, 0) else None
    sales_growth_rate_pct = ((revenue_latest - revenue_1y_ago) / abs(revenue_1y_ago) * 100.0) if revenue_latest is not None and revenue_1y_ago not in (None, 0) else None
    gross_margin_pct = (gross_profit / revenue_latest * 100.0) if gross_profit is not None and revenue_latest not in (None, 0) else None
    roe = (net_income / equity * 100.0) if net_income is not None and equity not in (None, 0) else None
    net_margin_pct = (net_income / revenue_latest * 100.0) if net_income is not None and revenue_latest not in (None, 0) else None
    debt_ratio_pct = (liabilities / equity * 100.0) if liabilities is not None and equity not in (None, 0) else None
    current_ratio_pct = (current_assets / current_liabilities * 100.0) if current_assets is not None and current_liabilities not in (None, 0) else None

    try:
        major_shareholders_data = client.get_major_shareholders(
            corp_code=corp_code,
            bsns_year=int(snapshot_curr["biz_year"]),
            reprt_code="11011",
        )
        maj_shareholders = (
            extract_major_shareholders_text(major_shareholders_data)
            if major_shareholders_data.get("status") == "000"
            else None
        )
    except Exception:
        maj_shareholders = None

    return {
        "stock_code": stock_code,
        "corp_code": corp_code,
        "biz_year": snapshot_curr["biz_year"],
        "currency": currency,
        "total_assets": total_assets,
        "cash_and_equivalents": cash_and_equivalents,
        "current_assets": current_assets,
        "accounts_receivable": accounts_receivable,
        "liabilities": liabilities,
        "current_liabilities": current_liabilities,
        "equity": equity,
        "capital_stock": capital_stock,
        "revenue_latest": revenue_latest,
        "revenue_1y_ago": revenue_1y_ago,
        "revenue_2y_ago": revenue_2y_ago,
        "gross_profit": gross_profit,
        "net_income": net_income,
        "cashholding_ratio_pct": cashholding_ratio_pct,
        "sales_growth_rate_pct": sales_growth_rate_pct,
        "gross_margin_pct": gross_margin_pct,
        "roe": roe,
        "net_margin_pct": net_margin_pct,
        "debt_ratio_pct": debt_ratio_pct,
        "current_ratio_pct": current_ratio_pct,
        "maj_shareholders": maj_shareholders,
        "source_report_num": snapshot_curr["report_num"],
        "source_report_nm": snapshot_curr["report_nm"],
        "source_report_date": snapshot_curr["report_date"],
        "match_status": snapshot_curr["match_status"],
    }


def upsert_company_finance(conn, rows: List[Dict[str, Any]], chunk_size: int = 200):
    if not rows:
        print("ℹ️ 적재할 행이 없습니다.")
        return

    assert_company_finance_schema(conn)

    sql = """
        INSERT INTO COMPANY_FINANCE (
            stock_code, corp_code, biz_year, currency,
            total_assets, cash_and_equivalents, current_assets, accounts_receivable,
            liabilities, current_liabilities, equity, capital_stock,
            revenue_latest, revenue_1y_ago, revenue_2y_ago,
            gross_profit, net_income,
            cashholding_ratio_pct, sales_growth_rate_pct, gross_margin_pct,
            roe, net_margin_pct, debt_ratio_pct, current_ratio_pct,
            maj_shareholders,
            source_report_num, source_report_nm, source_report_date, match_status
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            stock_code = VALUES(stock_code),
            currency = VALUES(currency),
            total_assets = VALUES(total_assets),
            cash_and_equivalents = VALUES(cash_and_equivalents),
            current_assets = VALUES(current_assets),
            accounts_receivable = VALUES(accounts_receivable),
            liabilities = VALUES(liabilities),
            current_liabilities = VALUES(current_liabilities),
            equity = VALUES(equity),
            capital_stock = VALUES(capital_stock),
            revenue_latest = VALUES(revenue_latest),
            revenue_1y_ago = VALUES(revenue_1y_ago),
            revenue_2y_ago = VALUES(revenue_2y_ago),
            gross_profit = VALUES(gross_profit),
            net_income = VALUES(net_income),
            cashholding_ratio_pct = VALUES(cashholding_ratio_pct),
            sales_growth_rate_pct = VALUES(sales_growth_rate_pct),
            gross_margin_pct = VALUES(gross_margin_pct),
            roe = VALUES(roe),
            net_margin_pct = VALUES(net_margin_pct),
            debt_ratio_pct = VALUES(debt_ratio_pct),
            current_ratio_pct = VALUES(current_ratio_pct),
            maj_shareholders = VALUES(maj_shareholders),
            source_report_num = VALUES(source_report_num),
            source_report_nm = VALUES(source_report_nm),
            source_report_date = VALUES(source_report_date),
            match_status = VALUES(match_status),
            updated_at = CURRENT_TIMESTAMP
    """

    data = [
        (
            r["stock_code"], r["corp_code"], r["biz_year"], r["currency"],
            r["total_assets"], r["cash_and_equivalents"], r["current_assets"], r["accounts_receivable"],
            r["liabilities"], r["current_liabilities"], r["equity"], r["capital_stock"],
            r["revenue_latest"], r["revenue_1y_ago"], r["revenue_2y_ago"],
            r["gross_profit"], r["net_income"],
            r["cashholding_ratio_pct"], r["sales_growth_rate_pct"], r["gross_margin_pct"],
            r["roe"], r["net_margin_pct"], r["debt_ratio_pct"], r["current_ratio_pct"],
            r["maj_shareholders"],
            r["source_report_num"], r["source_report_nm"], r["source_report_date"], r["match_status"],
        )
        for r in rows
    ]

    with conn.cursor() as cursor:
        for i in range(0, len(data), chunk_size):
            cursor.executemany(sql, data[i:i + chunk_size])
    conn.commit()
    print(f"✅ COMPANY_FINANCE upsert 완료: {len(data):,} rows")


def save_error_log(error_rows: List[Dict[str, Any]], path: str = "company_finance_error_log.csv"):
    if not error_rows:
        print("ℹ️ 에러 로그 없음")
        return
    df = pd.DataFrame(error_rows)
    df.to_csv(data_dir / path, index=False, encoding="utf-8-sig")
    print(f"⚠️ 에러 로그 저장: {path} ({len(df):,} rows)")


def main(
    kosdaq_only: bool = True,
    do_create_table: bool = True,
    do_insert: bool = True,
    limit: Optional[int] = None,
):
    dart_api = os.getenv("DART_API")
    if not dart_api:
        raise ValueError("DART_API 환경변수가 없습니다. .env 확인하세요.")

    conn = get_connection()
    client = OpenDartClient(dart_api)

    success_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []

    try:
        if do_create_table:
            create_company_finance(conn)
            conn.commit()

        assert_company_finance_schema(conn)

        basic_df = fetch_basic_targets(conn, kosdaq_only=kosdaq_only, limit=limit)
        if basic_df.empty:
            raise ValueError("BASIC에서 대상 종목을 찾지 못했습니다.")

        hint_map = fetch_report_hints(conn)
        existing_map = fetch_existing_company_finance_map(conn)

        total = len(basic_df)
        skipped_count = 0

        for idx, row in enumerate(basic_df.itertuples(index=False), start=1):
            stock_code = str(row.stock_code).zfill(6)
            corp_code = str(row.corp_code).zfill(8)
            corp_name = getattr(row, "corp_name", None)

            hinted_report_num = safe_str(hint_map.get(stock_code))
            existing_report_num = safe_str(existing_map.get(stock_code))

            # 1) 이미 같은 보고서번호로 적재되어 있으면 API 호출 자체를 스킵
            if hinted_report_num and existing_report_num and hinted_report_num == existing_report_num:
                skipped_count += 1
                print(
                    f"[{idx}/{total}] ⏭️ {stock_code} {corp_name or ''} 스킵 "
                    f"(REPORT.report_num={hinted_report_num} == COMPANY_FINANCE.source_report_num)"
                )
                continue

            try:
                report_df = client.list_reports(
                    corp_code=corp_code,
                    bgn_de="20200101",
                    end_de="20301231"
                )
                business_df = build_business_candidates(report_df)
                if business_df.empty:
                    raise ValueError(f"사업보고서 후보 없음: corp_code={corp_code}")

                current_candidate, _ = choose_report_candidate(
                    business_df,
                    hinted_report_num=hinted_report_num,
                    target_year=None,
                )
                if current_candidate is None:
                    raise ValueError(f"현재 사업연도 후보 선택 실패: corp_code={corp_code}")

                biz_year = int(current_candidate["bsns_year"])

                snapshot_curr = get_statement_snapshot(client, corp_code, business_df, biz_year, hinted_report_num)
                snapshot_prev1 = get_statement_snapshot(client, corp_code, business_df, biz_year - 1, None)
                snapshot_prev2 = get_statement_snapshot(client, corp_code, business_df, biz_year - 2, None)

                result_row = build_company_finance_row(
                    client=client,
                    corp_code=corp_code,
                    stock_code=stock_code,
                    snapshot_curr=snapshot_curr,
                    snapshot_prev1=snapshot_prev1,
                    snapshot_prev2=snapshot_prev2,
                )
                success_rows.append(result_row)

                print(
                    f"[{idx}/{total}] ✅ {stock_code} {corp_name or ''} 적재 준비 완료 "
                    f"(biz_year={result_row['biz_year']}, "
                    f"match_status={result_row['match_status']}, "
                    f"currency={result_row['currency']}, "
                    f"source_report_num={result_row['source_report_num']})"
                )

            except Exception as e:
                msg = str(e)
                print(f"[{idx}/{total}] ❌ {stock_code} {corp_name or ''} 실패: {msg}")
                error_rows.append(
                    {
                        "stock_code": stock_code,
                        "corp_code": corp_code,
                        "corp_name": corp_name,
                        "report_num": hinted_report_num,
                        "error": msg,
                    }
                )

            time.sleep(REQUEST_SLEEP)

        if do_insert and success_rows:
            upsert_company_finance(conn, success_rows)

        save_error_log(error_rows)

        print(
            f"🏁 완료: 전체 {total:,}건 / 스킵 {skipped_count:,}건 / 신규·갱신 준비 {len(success_rows):,}건 / 실패 {len(error_rows):,}건"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main(kosdaq_only=True, do_create_table=True, do_insert=True, limit=None)