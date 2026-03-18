from __future__ import annotations

import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv


temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

from common.setting import get_connection  # noqa: E402


BASE_URL = "https://opendart.fss.or.kr/api"
TIMEOUT = 30
REQUEST_SLEEP = 0.12
START_DATE = "20150101"
END_DATE = date.today().strftime("%Y%m%d")


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
        resp = self.session.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def list_reports(
        self,
        corp_code: str,
        bgn_de: str = START_DATE,
        end_de: str = END_DATE,
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


def create_feature_raw_d(conn):
    sql = """
        CREATE TABLE IF NOT EXISTS FEATURE_RAW_D (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(10) NOT NULL,
            corp_code VARCHAR(15) NOT NULL,
            net_income DECIMAL(24,6),
            equity DECIMAL(24,6),
            op_profit DECIMAL(24,6),
            revenue DECIMAL(24,6),
            disclosure_date DATE,
            biz_year INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_feature_raw_d (corp_code, biz_year),
            KEY idx_feature_raw_d_stock_code (stock_code),
            KEY idx_feature_raw_d_corp_code (corp_code),
            KEY idx_feature_raw_d_disclosure_date (disclosure_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    print("✅ FEATURE_RAW_D CREATE 확인 완료")


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
    ]
    select_cols.append(f"{corp_name_col} AS corp_name" if corp_name_col else "NULL AS corp_name")
    select_cols.append(f"{is_active_col} AS is_active" if is_active_col else "1 AS is_active")
    select_cols.append(f"{market_col} AS market" if market_col else "NULL AS market")
    select_cols.append(f"{corp_cls_col} AS corp_cls" if corp_cls_col else "NULL AS corp_cls")

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

    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM BASIC
        WHERE {' AND '.join(where_parts)}
        ORDER BY {stock_code_col}
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["corp_code", "stock_code", "corp_name", "is_active", "market", "corp_cls"])

    df = pd.DataFrame(rows)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    return df


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
    if "report_nm" not in df.columns:
        return pd.DataFrame(columns=["rcept_no", "report_nm", "rcept_dt", "bsns_year", "priority"])

    df = df[df["report_nm"].astype(str).str.contains("사업보고서", na=False)].copy()
    if df.empty:
        return pd.DataFrame(columns=["rcept_no", "report_nm", "rcept_dt", "bsns_year", "priority"])

    if "rcept_no" not in df.columns:
        df["rcept_no"] = None
    if "rcept_dt" not in df.columns:
        df["rcept_dt"] = None

    df["bsns_year"] = df.apply(lambda r: parse_bsns_year_from_report_nm(r.get("report_nm"), r.get("rcept_dt")), axis=1)
    df = df[df["bsns_year"].notna()].copy()
    df["bsns_year"] = df["bsns_year"].astype(int)
    df["priority"] = df["report_nm"].apply(report_kind_priority)
    df["rcept_dt"] = pd.to_datetime(df["rcept_dt"], errors="coerce")
    df["rcept_no"] = df["rcept_no"].astype(str)

    df = df.sort_values(
        ["bsns_year", "priority", "rcept_dt", "rcept_no"],
        ascending=[False, True, False, False],
    )
    return df.reset_index(drop=True)


def pick_latest_business_reports_by_year(report_df: pd.DataFrame) -> pd.DataFrame:
    business_df = build_business_candidates(report_df)
    if business_df.empty:
        return business_df

    business_df = business_df.sort_values(
        ["bsns_year", "priority", "rcept_dt", "rcept_no"],
        ascending=[True, True, False, False],
    )
    return business_df.drop_duplicates(subset=["bsns_year"], keep="first").reset_index(drop=True)


def normalize_account_rows(fs_df: pd.DataFrame) -> pd.DataFrame:
    if fs_df.empty:
        return fs_df

    out = fs_df.copy()
    for c in ["account_nm", "account_id", "sj_nm", "thstrm_amount"]:
        if c not in out.columns:
            out[c] = None

    out["account_nm"] = out["account_nm"].astype(str)
    out["account_id"] = out["account_id"].astype(str)
    out["sj_nm"] = out["sj_nm"].astype(str)
    return out


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




def classify_missing_fs_reason(
    stock_code: str,
    biz_year: int,
    report_nm: Optional[str],
    corp_name: Optional[str] = None,
) -> Tuple[str, str]:
    report_nm_s = safe_str(report_nm) or ""
    corp_name_s = safe_str(corp_name) or ""

    if biz_year < 2015:
        return (
            "UNSUPPORTED_BEFORE_2015",
            "OpenDART fnlttSinglAcntAll 미지원 사업연도(2015 이전)",
        )

    if str(stock_code).startswith(("9", "95")):
        return (
            "FOREIGN_OR_OVERSEAS_ENTITY",
            "외국법인/해외기업 계열 가능성 높음: 정형 재무제표 API 미제공 가능성",
        )

    if "기재정정" in report_nm_s:
        return (
            "AMENDED_REPORT",
            "기재정정 사업보고서: 원문/정정본 기준 재확인 필요",
        )

    if "정정" in report_nm_s or "첨부추가" in report_nm_s:
        return (
            "ATTACHMENT_OR_AMENDED_REPORT",
            "정정/첨부추가 보고서: 원문/첨부 기준 재확인 필요",
        )

    biotech_keywords = ["바이오", "헬스", "제약", "리서치", "이노베이션", "온코"]
    if any(kw in corp_name_s for kw in biotech_keywords):
        return (
            "GENERAL_CORP_OR_PRELISTING_POSSIBLE",
            "상장 전 일반법인/기술특례 준비구간 공시 가능성: 정형 재무제표 API 미제공 가능성",
        )

    return (
        "API_UNAVAILABLE_OR_RAW_PARSING_NEEDED",
        "OpenDART 정형 재무제표 API 미제공 가능성: 상장 전·코넥스·일반법인·원문파싱 필요",
    )

def get_statement_snapshot(
    client: OpenDartClient,
    corp_code: str,
    biz_year: int,
) -> Tuple[pd.DataFrame, str]:
    if biz_year < 2015:
        return pd.DataFrame(), "UNSUPPORTED_BEFORE_2015"

    fs_df = client.get_financial_statement_all(
        corp_code=corp_code,
        bsns_year=biz_year,
        reprt_code="11011",
        fs_div="CFS",
    )
    if not fs_df.empty:
        return fs_df, "CFS"

    fs_df = client.get_financial_statement_all(
        corp_code=corp_code,
        bsns_year=biz_year,
        reprt_code="11011",
        fs_div="OFS",
    )
    if not fs_df.empty:
        return fs_df, "OFS"

    return pd.DataFrame(), "NONE"


def build_feature_raw_d_row(
    corp_code: str,
    stock_code: str,
    biz_year: int,
    disclosure_date: Any,
    fs_df: pd.DataFrame,
) -> Dict[str, Any]:
    if fs_df.empty:
        raise ValueError(f"재무제표 없음: corp_code={corp_code}, biz_year={biz_year}")

    revenue = account_amount(
        fs_df,
        ["ifrs-full_Revenue", "ifrs_Revenue", "dart_OperatingRevenue"],
        ["매출액", "영업수익"],
        ["손익계산서", "포괄손익계산서"],
    )

    op_profit = account_amount(
        fs_df,
        ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        ["영업이익", "영업손익"],
        ["손익계산서", "포괄손익계산서"],
    )

    net_income = account_amount(
        fs_df,
        ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"],
        ["당기순이익", "당기순손익"],
        ["손익계산서", "포괄손익계산서"],
    )

    equity = account_amount(
        fs_df,
        ["ifrs-full_Equity", "ifrs_Equity"],
        ["자본총계"],
        ["재무상태표"],
    )

    disclosure_date = pd.to_datetime(disclosure_date, errors="coerce")
    disclosure_date = None if pd.isna(disclosure_date) else disclosure_date.date()

    return {
        "stock_code": stock_code,
        "corp_code": corp_code,
        "net_income": net_income,
        "equity": equity,
        "op_profit": op_profit,
        "revenue": revenue,
        "disclosure_date": disclosure_date,
        "biz_year": biz_year,
    }


def upsert_feature_raw_d(conn, rows: List[Dict[str, Any]], chunk_size: int = 200):
    if not rows:
        print("ℹ️ 적재할 행이 없습니다.")
        return

    sql = """
        INSERT INTO FEATURE_RAW_D (
            stock_code, corp_code, net_income, equity, op_profit, revenue, disclosure_date, biz_year
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            stock_code = VALUES(stock_code),
            net_income = VALUES(net_income),
            equity = VALUES(equity),
            op_profit = VALUES(op_profit),
            revenue = VALUES(revenue),
            disclosure_date = VALUES(disclosure_date),
            updated_at = CURRENT_TIMESTAMP
    """

    data = [
        (
            r["stock_code"],
            r["corp_code"],
            r["net_income"],
            r["equity"],
            r["op_profit"],
            r["revenue"],
            r["disclosure_date"],
            r["biz_year"],
        )
        for r in rows
    ]

    with conn.cursor() as cursor:
        for i in range(0, len(data), chunk_size):
            cursor.executemany(sql, data[i:i + chunk_size])

    conn.commit()
    print(f"✅ FEATURE_RAW_D upsert 완료: {len(data):,} rows")


def save_error_log(error_rows: List[Dict[str, Any]], path: str = "feature_raw_d_error_log.csv"):
    if not error_rows:
        print("ℹ️ 에러 로그 없음")
        return

    df = pd.DataFrame(error_rows)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"⚠️ 에러 로그 저장: {path} ({len(df):,} rows)")



def fetch_existing_feature_raw_d_keys(conn) -> set[tuple[str, int]]:
    sql = """
        SELECT corp_code, biz_year
        FROM FEATURE_RAW_D
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception:
            return set()

    existing_keys: set[tuple[str, int]] = set()
    for row in rows:
        if isinstance(row, dict):
            corp_code = str(row.get("corp_code", "")).zfill(8)
            biz_year = row.get("biz_year")
        else:
            corp_code = str(row[0]).zfill(8)
            biz_year = row[1]

        if corp_code and biz_year is not None:
            existing_keys.add((corp_code, int(biz_year)))

    return existing_keys

def main(
    kosdaq_only: bool = True,
    do_create_table: bool = True,
    do_insert: bool = True,
    limit: Optional[int] = None,
    skip_existing: bool = True,
):
    load_dotenv()
    dart_api = os.getenv("DART_API")
    if not dart_api:
        raise ValueError("DART_API 환경변수가 없습니다. .env 확인하세요.")

    conn = get_connection()
    client = OpenDartClient(dart_api)

    success_rows: List[Dict[str, Any]] = []
    error_rows: List[Dict[str, Any]] = []

    try:
        if do_create_table:
            create_feature_raw_d(conn)
            conn.commit()

        existing_keys = fetch_existing_feature_raw_d_keys(conn) if skip_existing else set()
        if skip_existing:
            print(f"ℹ️ 기존 적재건 skip 대상: {len(existing_keys):,}개 (corp_code, biz_year)")

        basic_df = fetch_basic_targets(conn, kosdaq_only=kosdaq_only, limit=limit)
        if basic_df.empty:
            raise ValueError("BASIC에서 대상 종목을 찾지 못했습니다.")

        total = len(basic_df)

        for idx, row in enumerate(basic_df.itertuples(index=False), start=1):
            stock_code = str(row.stock_code).zfill(6)
            corp_code = str(row.corp_code).zfill(8)
            corp_name = getattr(row, "corp_name", None)

            try:
                report_df = client.list_reports(
                    corp_code=corp_code,
                    bgn_de=START_DATE,
                    end_de=END_DATE,
                )

                yearly_business_reports = pick_latest_business_reports_by_year(report_df)
                if yearly_business_reports.empty:
                    raise ValueError(f"사업보고서 후보 없음: corp_code={corp_code}")

                company_rows = []

                for _, rpt in yearly_business_reports.iterrows():
                    biz_year = int(rpt["bsns_year"])

                    if biz_year < 2015:
                        print(
                            f"[{idx}/{total}] ⏭️ {stock_code} {corp_name or ''} "
                            f"biz_year={biz_year} 정책상 skip (2015 이전 비지원)"
                        )
                        continue
                    
                    if skip_existing and (corp_code, biz_year) in existing_keys:
                        print(
                            f"[{idx}/{total}] ⏭️ {stock_code} {corp_name or ''} "
                            f"biz_year={biz_year} 기존 적재건 skip"
                        )
                        continue

                    fs_df, fs_type = get_statement_snapshot(client, corp_code, biz_year)

                    if fs_df.empty:
                        reason_code, reason_detail = classify_missing_fs_reason(
                            stock_code=stock_code,
                            biz_year=biz_year,
                            report_nm=rpt.get("report_nm"),
                            corp_name=corp_name,
                        )
                        error_rows.append(
                            {
                                "stock_code": stock_code,
                                "corp_code": corp_code,
                                "corp_name": corp_name,
                                "biz_year": biz_year,
                                "report_nm": rpt.get("report_nm"),
                                "rcept_no": rpt.get("rcept_no"),
                                "error_code": reason_code,
                                "error": reason_detail,
                            }
                        )
                        print(
                            f"[{idx}/{total}] ⚠️ {stock_code} {corp_name or ''} "
                            f"biz_year={biz_year} 미적재: {reason_code}"
                        )
                        continue

                    result_row = build_feature_raw_d_row(
                        corp_code=corp_code,
                        stock_code=stock_code,
                        biz_year=biz_year,
                        disclosure_date=rpt.get("rcept_dt"),
                        fs_df=fs_df,
                    )
                    company_rows.append(result_row)

                    print(
                        f"[{idx}/{total}] {stock_code} {corp_name or ''} "
                        f"biz_year={biz_year} 저장준비 완료 ({fs_type})"
                    )

                    time.sleep(REQUEST_SLEEP)

                success_rows.extend(company_rows)

            except Exception as e:
                msg = str(e)
                print(f"[{idx}/{total}] ❌ {stock_code} {corp_name or ''} 실패: {msg}")
                error_rows.append(
                    {
                        "stock_code": stock_code,
                        "corp_code": corp_code,
                        "corp_name": corp_name,
                        "biz_year": None,
                        "report_nm": None,
                        "rcept_no": None,
                        "error_code": "EXCEPTION",
                        "error": msg,
                    }
                )

            time.sleep(REQUEST_SLEEP)

        if do_insert and success_rows:
            upsert_feature_raw_d(conn, success_rows)

        save_error_log(error_rows)

    finally:
        conn.close()


if __name__ == "__main__":
    main(kosdaq_only=True, do_create_table=True, do_insert=True, limit=None)