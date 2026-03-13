import argparse
import io
import os
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from pykrx import stock

# Add project root so common package can be imported.
TEMP_BASE = Path(__file__).resolve().parents[1]
if str(TEMP_BASE) not in sys.path:
    sys.path.append(str(TEMP_BASE))

# Delayed imports (initialized in main)
get_connection = None
login_krx = None


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Collect FEATURE_BASIC from DART + pykrx KOSDAQ and upsert day-by-day "
            "with resumable progress"
        )
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2010-01-01",
        help="Collection start date (YYYY-MM-DD or YYYYMMDD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Collection end date (YYYY-MM-DD or YYYYMMDD). default=today",
    )
    parser.add_argument(
        "--market",
        type=str,
        default="KOSDAQ",
        choices=["KOSDAQ"],
        help="Target market",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.01,
        help="Sleep seconds between day requests",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for pykrx day query",
    )
    parser.add_argument(
        "--retry-sleep-sec",
        type=float,
        default=0.4,
        help="Sleep seconds between retries",
    )
    parser.add_argument(
        "--log-every-days",
        type=int,
        default=20,
        help="Log progress every N processed days",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume-from-last-date behavior",
    )
    parser.add_argument(
        "--skip-login",
        action="store_true",
        help="Skip KRX login before pykrx collection",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect only, no DB upsert",
    )
    return parser.parse_args()


def parse_to_date(text: str) -> date:
    return pd.to_datetime(text).date()


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def iter_dates(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def create_feature_basic(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS FEATURE_BASIC (
                stock_code VARCHAR(10) NOT NULL COMMENT 'stock code',
                date DATE NOT NULL COMMENT 'trade date',
                corp_name VARCHAR(100) COMMENT 'company name',
                corp_code VARCHAR(8) COMMENT 'dart corp code',
                is_listed_on_date TINYINT(1) DEFAULT NULL COMMENT '1 listed / 0 unlisted / NULL unknown',
                is_active_now TINYINT(1) DEFAULT NULL COMMENT '1 active now / 0 delisted / NULL unknown',
                PRIMARY KEY (stock_code, date)
            ) ENGINE=InnoDB COMMENT='Base feature table for stock modeling';
            """
        )
    log("FEATURE_BASIC table is ready")


def get_resume_start_date(conn, requested_start: date) -> date:
    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(date) AS max_date FROM FEATURE_BASIC;")
        row = cursor.fetchone()

    max_date = row.get("max_date") if row else None
    if not max_date:
        return requested_start

    candidate = max_date + timedelta(days=1)
    return max(requested_start, candidate)


def fetch_dart_corp_map(api_key: str) -> pd.DataFrame:
    if not api_key:
        raise RuntimeError("DART_API is required")

    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    resp = requests.get(url, timeout=40)
    resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    xml_name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
    if xml_name is None:
        raise RuntimeError("No XML file found in DART corpCode ZIP")

    root = ET.fromstring(zf.read(xml_name))
    rows = []

    for node in root.findall("list"):
        corp_code = (node.findtext("corp_code") or "").strip()
        corp_name = (node.findtext("corp_name") or "").strip()
        stock_code = (node.findtext("stock_code") or "").strip()
        modify_date = (node.findtext("modify_date") or "").strip()

        if not stock_code:
            continue

        rows.append(
            {
                "stock_code": stock_code.zfill(6),
                "corp_code": corp_code.zfill(8),
                "corp_name": corp_name,
                "modify_date": modify_date,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No stock_code rows found in DART corpCode")

    df = df[df["stock_code"].str.fullmatch(r"\d{6}", na=False)].copy()
    df["modify_date"] = pd.to_datetime(df["modify_date"], errors="coerce")

    df = (
        df.sort_values(["stock_code", "modify_date"], ascending=[True, False])
        .drop_duplicates(subset=["stock_code"], keep="first")
        .reset_index(drop=True)
    )

    return df[["stock_code", "corp_code", "corp_name"]]


def build_dart_lookup(dart_df: pd.DataFrame) -> dict[str, tuple[str | None, str | None]]:
    lookup: dict[str, tuple[str | None, str | None]] = {}
    for row in dart_df.itertuples(index=False):
        code = str(row.stock_code).zfill(6)
        corp_code = row.corp_code if pd.notna(row.corp_code) else None
        corp_name = row.corp_name if pd.notna(row.corp_name) else None
        lookup[code] = (corp_code, corp_name)
    return lookup


def get_ticker_list_with_retry(
    yyyymmdd: str,
    market: str,
    retries: int,
    retry_sleep_sec: float,
) -> list[str]:
    last_err = None
    for _ in range(max(1, retries)):
        try:
            out = stock.get_market_ticker_list(yyyymmdd, market)
            return [str(x).zfill(6) for x in out]
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(max(0.0, retry_sleep_sec))

    log(f"[WARN] get_market_ticker_list failed: date={yyyymmdd}, err={last_err}")
    return []


def build_day_records(
    trade_date: date,
    tickers: list[str],
    dart_lookup: dict[str, tuple[str | None, str | None]],
):
    records = []

    for code in tickers:
        code = str(code).zfill(6)
        if not code.isdigit():
            continue

        dart_corp_code, dart_name = dart_lookup.get(code, (None, None))

        corp_name = dart_name

        records.append(
            (
                code,
                trade_date,
                corp_name,
                dart_corp_code,
                1,
                None,
            )
        )

    return records


def upsert_feature_basic_by_day(conn, records: list[tuple]) -> int:
    if not records:
        return 0

    sql = """
        INSERT INTO FEATURE_BASIC (
            stock_code,
            date,
            corp_name,
            corp_code,
            is_listed_on_date,
            is_active_now
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            corp_name = VALUES(corp_name),
            corp_code = VALUES(corp_code),
            is_listed_on_date = VALUES(is_listed_on_date),
            is_active_now = VALUES(is_active_now);
    """

    with conn.cursor() as cursor:
        cursor.executemany(sql, records)

    return len(records)


def resolve_end_date(end_date_text: str | None) -> date:
    if end_date_text:
        return parse_to_date(end_date_text)
    return datetime.now().date()


def main() -> None:
    args = parse_args()
    log(
        "start | "
        f"start_date={args.start_date}, end_date={args.end_date}, market={args.market}, "
        f"dry_run={args.dry_run}, no_resume={args.no_resume}"
    )

    requested_start = parse_to_date(args.start_date)
    requested_end = resolve_end_date(args.end_date)

    if requested_start > requested_end:
        raise ValueError(
            f"invalid date range: start={requested_start}, end={requested_end}"
        )

    log("loading common.setting ...")
    global get_connection, login_krx
    from common.setting import get_connection as _get_connection, login_krx as _login_krx

    get_connection = _get_connection
    login_krx = _login_krx

    if not args.skip_login:
        krx_id = os.getenv("ID")
        krx_pw = os.getenv("PW")
        if not krx_id or not krx_pw:
            raise RuntimeError("ID/PW env vars are required unless --skip-login is used")

        log("trying KRX login ...")
        if not login_krx(krx_id, krx_pw):
            raise RuntimeError("KRX login failed")
        log("KRX login succeeded")

    dart_api = os.getenv("DART_API")
    log("fetching DART corp map ...")
    dart_df = fetch_dart_corp_map(dart_api)
    dart_lookup = build_dart_lookup(dart_df)
    log(f"DART map loaded: {len(dart_lookup)} stock mappings")

    conn = get_connection()

    total_rows = 0
    processed_days = 0
    days_with_data = 0

    try:
        create_feature_basic(conn)

        run_start = requested_start
        if not args.no_resume and not args.dry_run:
            resume_start = get_resume_start_date(conn, requested_start)
            if resume_start > requested_start:
                log(
                    f"resume enabled: requested_start={requested_start}, "
                    f"resume_start={resume_start}"
                )
            run_start = resume_start

        if run_start > requested_end:
            log(
                "nothing to do: "
                f"run_start={run_start} > requested_end={requested_end}"
            )
            return

        log(f"effective range: {run_start} ~ {requested_end}")

        for d in iter_dates(run_start, requested_end):
            yyyymmdd = date_to_yyyymmdd(d)
            processed_days += 1

            tickers = get_ticker_list_with_retry(
                yyyymmdd,
                args.market,
                args.retries,
                args.retry_sleep_sec,
            )

            if not tickers:
                if processed_days % max(1, args.log_every_days) == 0:
                    log(
                        f"progress days={processed_days}, days_with_data={days_with_data}, "
                        f"rows={total_rows}, last={d} (no data)"
                    )
                time.sleep(max(0.0, args.sleep_sec))
                continue

            records = build_day_records(d, tickers, dart_lookup)
            if records:
                if args.dry_run:
                    total_rows += len(records)
                else:
                    upserted = upsert_feature_basic_by_day(conn, records)
                    conn.commit()
                    total_rows += upserted

                days_with_data += 1

            if processed_days % max(1, args.log_every_days) == 0:
                log(
                    f"progress days={processed_days}, days_with_data={days_with_data}, "
                    f"rows={total_rows}, last={d}"
                )

            time.sleep(max(0.0, args.sleep_sec))

        log(
            "done | "
            f"processed_days={processed_days}, days_with_data={days_with_data}, rows={total_rows}, "
            f"dry_run={args.dry_run}"
        )

    except Exception as e:  # noqa: BLE001
        if not args.dry_run:
            conn.rollback()
        log(f"failed with rollback (if applicable): {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
