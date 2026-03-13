import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pykrx import stock

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

# common.setting은 DB pool을 import 시점에 초기화하므로 main에서 지연 로드
get_connection = None
login_krx = None


def log(msg: str):
    print(msg, flush=True)


def default_worker_count() -> int:
    cpu = os.cpu_count() or 4
    # Network I/O workload: keep worker count conservative for stability
    return max(2, min(4, cpu // 4 if cpu >= 4 else 1))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect FEATURE_RAW from pykrx and upsert into DB"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD or YYYYMMDD). Default: 2009-12-31",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD or YYYYMMDD). Default: latest trading day",
    )
    parser.add_argument(
        "--market",
        type=str,
        default="KOSDAQ",
        choices=["KOSPI", "KOSDAQ", "KONEX", "ALL"],
        help="Market filter",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of target stocks from FEATURE_BASIC",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.12,
        help="Delay seconds between worker submit batches (throttle)",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=1,
        help="Commit interval by processed stock count",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_worker_count(),
        help="Thread worker count for collection (recommended: 2~4)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume mode (default: process only missing keys)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect/validate without DB write",
    )
    return parser.parse_args()


def yyyymmdd(date_text: str) -> str:
    return pd.to_datetime(date_text).strftime("%Y%m%d")


def resolve_latest_trading_date(base_date: datetime, market: str) -> str:
    cursor = base_date
    for _ in range(14):
        cand = cursor.strftime("%Y%m%d")
        try:
            tickers = stock.get_market_ticker_list(cand, market)
        except Exception:
            tickers = []

        if tickers:
            return cand

        cursor = cursor - timedelta(days=1)

    raise RuntimeError(
        f"최근 거래일을 찾지 못했습니다. base={base_date.strftime('%Y-%m-%d')}, market={market}"
    )


def resolve_date_range(start_date: str | None, end_date: str | None, market: str) -> tuple[str, str]:
    if end_date:
        to_dt = yyyymmdd(end_date)
    else:
        # 현재 시점 기준 하루 전 날짜에서 가장 가까운 이전 거래일
        yesterday = datetime.now() - timedelta(days=1)
        to_dt = resolve_latest_trading_date(yesterday, market)

    if start_date:
        from_dt = yyyymmdd(start_date)
    else:
        # 가능한 과거 전체
        from_dt = "20091231"

    if from_dt > to_dt:
        raise ValueError(f"잘못된 기간: from_dt={from_dt}, to_dt={to_dt}")

    return from_dt, to_dt


def empty_feature_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "stock_code",
            "date",
            "close",
            "trading_value",
            "foreign_netbuy_value",
            "inst_netbuy_value",
            "per",
            "pbr",
        ]
    )


def safe_pykrx_call(fn, *args, **kwargs) -> pd.DataFrame:
    try:
        out = fn(*args, **kwargs)
        if isinstance(out, pd.DataFrame):
            return out.copy()
    except Exception:
        pass
    return pd.DataFrame()


def pick_first_existing_col(df: pd.DataFrame, candidates: list[str]):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def create_feature_raw(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS FEATURE_RAW (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                close DECIMAL(24,6),
                trading_value DECIMAL(24,6),
                foreign_netbuy_value DECIMAL(24,6),
                inst_netbuy_value DECIMAL(24,6),
                per DECIMAL(24,6),
                pbr DECIMAL(24,6),
                CONSTRAINT fk_feature_raw_feature_basic
                    FOREIGN KEY (stock_code, date)
                    REFERENCES FEATURE_BASIC (stock_code, date)
                    ON UPDATE CASCADE
                    ON DELETE RESTRICT
            );
            """
        )
    log("FEATURE_RAW 테이블이 생성되었거나 이미 존재합니다.")


def ensure_feature_raw_unique_key(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'FEATURE_RAW'
              AND index_name = 'uq_feature_raw_stock_date';
            """
        )
        row = cursor.fetchone()
        exists = int(row["cnt"]) > 0
        if not exists:
            cursor.execute(
                """
                ALTER TABLE FEATURE_RAW
                ADD UNIQUE KEY uq_feature_raw_stock_date (stock_code, date);
                """
            )
            log("FEATURE_RAW 고유키(uq_feature_raw_stock_date) 생성 완료")


def ensure_feature_raw_decimal_capacity(conn):
    target_cols = [
        "close",
        "trading_value",
        "foreign_netbuy_value",
        "inst_netbuy_value",
        "per",
        "pbr",
    ]

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'FEATURE_RAW'
              AND column_name IN (
                  'close',
                  'trading_value',
                  'foreign_netbuy_value',
                  'inst_netbuy_value',
                  'per',
                  'pbr'
              );
            """
        )
        rows = cursor.fetchall()

        width_map = {row["column_name"]: row for row in rows}
        to_modify = []
        for col in target_cols:
            row = width_map.get(col)
            if row is None:
                continue
            prec = int(row["numeric_precision"]) if row["numeric_precision"] is not None else 0
            scale = int(row["numeric_scale"]) if row["numeric_scale"] is not None else 0
            if prec < 24 or scale < 6:
                to_modify.append(col)

        if not to_modify:
            return

        alter_sql = "ALTER TABLE FEATURE_RAW " + ", ".join(
            [f"MODIFY COLUMN `{col}` DECIMAL(24,6)" for col in to_modify]
        )
        cursor.execute(alter_sql)
        log(f"FEATURE_RAW decimal columns widened: {', '.join(to_modify)}")


def ensure_feature_raw_foreign_key(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.table_constraints
            WHERE table_schema = DATABASE()
              AND table_name = 'FEATURE_RAW'
              AND constraint_type = 'FOREIGN KEY'
              AND constraint_name = 'fk_feature_raw_feature_basic';
            """
        )
        row = cursor.fetchone()
        exists = int(row["cnt"]) > 0
        if not exists:
            cursor.execute(
                """
                ALTER TABLE FEATURE_RAW
                ADD CONSTRAINT fk_feature_raw_feature_basic
                    FOREIGN KEY (stock_code, date)
                    REFERENCES FEATURE_BASIC (stock_code, date)
                    ON UPDATE CASCADE
                    ON DELETE RESTRICT;
                """
            )
            log("FEATURE_RAW FK(fk_feature_raw_feature_basic) created")


def ensure_feature_basic_lookup_index(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = "FEATURE_BASIC"
              AND index_name = "idx_feature_basic_date_stock";
            """
        )
        row = cursor.fetchone()
        exists = int(row["cnt"]) > 0
        if not exists:
            cursor.execute(
                """
                ALTER TABLE FEATURE_BASIC
                ADD INDEX idx_feature_basic_date_stock (date, stock_code);
                """
            )
            log("FEATURE_BASIC index(idx_feature_basic_date_stock) created")


def load_feature_basic_stock_ranges(
    conn,
    from_dt: str,
    to_dt: str,
    limit: int | None = None,
    missing_only: bool = False,
) -> pd.DataFrame:
    start_date = pd.to_datetime(from_dt).date()
    end_date = pd.to_datetime(to_dt).date()

    with conn.cursor() as cursor:
        if missing_only:
            if limit is None:
                cursor.execute(
                    """
                    SELECT
                        fb.stock_code,
                        MIN(fb.date) AS min_date,
                        MAX(fb.date) AS max_date,
                        COUNT(*) AS target_rows
                    FROM FEATURE_BASIC fb
                    WHERE fb.date BETWEEN %s AND %s
                      AND fb.stock_code IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM FEATURE_RAW fr
                          WHERE fr.stock_code = fb.stock_code
                            AND fr.date = fb.date
                      )
                    GROUP BY fb.stock_code
                    ORDER BY fb.stock_code
                    """,
                    (start_date, end_date),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        fb.stock_code,
                        MIN(fb.date) AS min_date,
                        MAX(fb.date) AS max_date,
                        COUNT(*) AS target_rows
                    FROM FEATURE_BASIC fb
                    JOIN (
                        SELECT fb1.stock_code
                        FROM FEATURE_BASIC fb1
                        WHERE fb1.date BETWEEN %s AND %s
                          AND fb1.stock_code IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1
                              FROM FEATURE_RAW fr1
                              WHERE fr1.stock_code = fb1.stock_code
                                AND fr1.date = fb1.date
                          )
                        GROUP BY fb1.stock_code
                        ORDER BY fb1.stock_code
                        LIMIT %s
                    ) s ON fb.stock_code = s.stock_code
                    WHERE fb.date BETWEEN %s AND %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM FEATURE_RAW fr
                          WHERE fr.stock_code = fb.stock_code
                            AND fr.date = fb.date
                      )
                    GROUP BY fb.stock_code
                    ORDER BY fb.stock_code
                    """,
                    (start_date, end_date, int(limit), start_date, end_date),
                )
        else:
            if limit is None:
                cursor.execute(
                    """
                    SELECT
                        fb.stock_code,
                        MIN(fb.date) AS min_date,
                        MAX(fb.date) AS max_date,
                        COUNT(*) AS target_rows
                    FROM FEATURE_BASIC fb
                    WHERE fb.date BETWEEN %s AND %s
                      AND fb.stock_code IS NOT NULL
                    GROUP BY fb.stock_code
                    ORDER BY fb.stock_code
                    """,
                    (start_date, end_date),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        fb.stock_code,
                        MIN(fb.date) AS min_date,
                        MAX(fb.date) AS max_date,
                        COUNT(*) AS target_rows
                    FROM FEATURE_BASIC fb
                    JOIN (
                        SELECT stock_code
                        FROM FEATURE_BASIC
                        WHERE date BETWEEN %s AND %s
                          AND stock_code IS NOT NULL
                        GROUP BY stock_code
                        ORDER BY stock_code
                        LIMIT %s
                    ) s ON fb.stock_code = s.stock_code
                    WHERE fb.date BETWEEN %s AND %s
                    GROUP BY fb.stock_code
                    ORDER BY fb.stock_code
                    """,
                    (start_date, end_date, int(limit), start_date, end_date),
                )
        rows = cursor.fetchall()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df = df[df["stock_code"].str.fullmatch(r"\d{6}", na=False)].copy()
    df["min_date"] = pd.to_datetime(df["min_date"], errors="coerce").dt.date
    df["max_date"] = pd.to_datetime(df["max_date"], errors="coerce").dt.date
    df = df[df["min_date"].notna() & df["max_date"].notna()]
    df["target_rows"] = pd.to_numeric(df["target_rows"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values(["stock_code"]).reset_index(drop=True)
    return df


def load_feature_basic_dates_for_stock(
    conn,
    stock_code: str,
    start_date,
    end_date,
    missing_only: bool = False,
) -> set:
    with conn.cursor() as cursor:
        if missing_only:
            cursor.execute(
                """
                SELECT fb.date
                FROM FEATURE_BASIC fb
                WHERE fb.stock_code = %s
                  AND fb.date BETWEEN %s AND %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM FEATURE_RAW fr
                      WHERE fr.stock_code = fb.stock_code
                        AND fr.date = fb.date
                  )
                """,
                (stock_code, start_date, end_date),
            )
        else:
            cursor.execute(
                """
                SELECT fb.date
                FROM FEATURE_BASIC fb
                WHERE fb.stock_code = %s
                  AND fb.date BETWEEN %s AND %s
                """,
                (stock_code, start_date, end_date),
            )
        rows = cursor.fetchall()

    if not rows:
        return set()

    out = set()
    for row in rows:
        v = row.get("date")
        if v is None:
            continue
        if hasattr(v, "date"):
            v = v.date()
        out.add(v)
    return out


def collect_single_ticker(
    stock_code: str,
    from_dt: str,
    to_dt: str,
    progress_tag: str | None = None,
):
    issue = {
        "stock_code": stock_code,
        "ohlcv_empty": True,
        "investor_empty": True,
        "fundamental_empty": True,
    }

    tag = progress_tag or stock_code

    def _stage_start(name: str) -> float:
        log(f"[{tag}] {name} start")
        return time.perf_counter()

    def _stage_done(name: str, t0: float, extra: str = "") -> None:
        elapsed = time.perf_counter() - t0
        suffix = f" | {extra}" if extra else ""
        log(f"[{tag}] {name} done ({elapsed:.2f}s){suffix}")

    t_ohlcv = _stage_start("OHLCV")
    ohlcv = safe_pykrx_call(
        stock.get_market_ohlcv_by_date, from_dt, to_dt, stock_code, adjusted=False
    )
    _stage_done("OHLCV", t_ohlcv, f"rows={len(ohlcv)}")

    if ohlcv.empty:
        log(f"[{tag}] OHLCV empty -> skip")
        return empty_feature_frame(), issue

    issue["ohlcv_empty"] = False

    base = pd.DataFrame(index=ohlcv.index.copy())
    base["close"] = pd.to_numeric(ohlcv.get("종가"), errors="coerce")

    trading_value_series = ohlcv.get("거래대금")
    if trading_value_series is not None:
        base["trading_value"] = pd.to_numeric(trading_value_series, errors="coerce")
    else:
        t_cap = _stage_start("MarketCapFallback")
        cap_df = safe_pykrx_call(stock.get_market_cap_by_date, from_dt, to_dt, stock_code)
        _stage_done("MarketCapFallback", t_cap, f"rows={len(cap_df)}")
        if (not cap_df.empty) and ("거래대금" in cap_df.columns):
            base["trading_value"] = pd.to_numeric(
                cap_df["거래대금"], errors="coerce"
            ).reindex(base.index)
        else:
            volume_series = pd.to_numeric(ohlcv.get("거래량"), errors="coerce")
            base["trading_value"] = base["close"] * volume_series

    t_investor = _stage_start("InvestorTradingValue")
    investor = safe_pykrx_call(
        stock.get_market_trading_value_by_date, from_dt, to_dt, stock_code, on="순매수"
    )
    _stage_done("InvestorTradingValue", t_investor, f"rows={len(investor)}")

    if not investor.empty:
        issue["investor_empty"] = False
        foreign_col = pick_first_existing_col(investor, ["외국인합계", "외국인"])
        inst_col = pick_first_existing_col(investor, ["기관합계", "기관"])
        base["foreign_netbuy_value"] = (
            pd.to_numeric(investor[foreign_col], errors="coerce")
            if foreign_col
            else np.nan
        )
        base["inst_netbuy_value"] = (
            pd.to_numeric(investor[inst_col], errors="coerce") if inst_col else np.nan
        )
    else:
        base["foreign_netbuy_value"] = np.nan
        base["inst_netbuy_value"] = np.nan

    t_fund = _stage_start("Fundamental")
    fundamental = safe_pykrx_call(
        stock.get_market_fundamental_by_date, from_dt, to_dt, stock_code
    )
    _stage_done("Fundamental", t_fund, f"rows={len(fundamental)}")

    if not fundamental.empty:
        issue["fundamental_empty"] = False
        per_col = pick_first_existing_col(fundamental, ["PER", "per"])
        pbr_col = pick_first_existing_col(fundamental, ["PBR", "pbr"])

        per_series = (
            pd.to_numeric(fundamental[per_col], errors="coerce")
            if per_col
            else pd.Series(np.nan, index=base.index)
        )
        pbr_series = (
            pd.to_numeric(fundamental[pbr_col], errors="coerce")
            if pbr_col
            else pd.Series(np.nan, index=base.index)
        )

        base["per"] = per_series.mask(per_series.eq(0))
        base["pbr"] = pbr_series.mask(pbr_series.eq(0))
    else:
        base["per"] = np.nan
        base["pbr"] = np.nan

    base.index = pd.to_datetime(base.index)
    base = base.reset_index()
    first_col = base.columns[0]
    if first_col != "date":
        base = base.rename(columns={first_col: "date"})

    base["date"] = pd.to_datetime(base["date"]).dt.date
    base["stock_code"] = stock_code

    ordered_columns = empty_feature_frame().columns.tolist()
    out = base[ordered_columns]
    log(f"[{tag}] collect done | rows={len(out)}")
    return out, issue


def collect_task_worker(
    stock_code: str,
    from_dt: str,
    to_dt: str,
    progress_tag: str,
):
    return collect_single_ticker(
        stock_code,
        from_dt,
        to_dt,
        progress_tag=progress_tag,
    )


def to_sql_value(v):
    if pd.isna(v):
        return None
    return float(v)


def upsert_feature_raw(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append(
            (
                str(row["stock_code"]),
                row["date"],
                to_sql_value(row["close"]),
                to_sql_value(row["trading_value"]),
                to_sql_value(row["foreign_netbuy_value"]),
                to_sql_value(row["inst_netbuy_value"]),
                to_sql_value(row["per"]),
                to_sql_value(row["pbr"]),
            )
        )

    sql = """
        INSERT INTO FEATURE_RAW (
            stock_code,
            date,
            close,
            trading_value,
            foreign_netbuy_value,
            inst_netbuy_value,
            per,
            pbr
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            close = VALUES(close),
            trading_value = VALUES(trading_value),
            foreign_netbuy_value = VALUES(foreign_netbuy_value),
            inst_netbuy_value = VALUES(inst_netbuy_value),
            per = VALUES(per),
            pbr = VALUES(pbr);
    """

    with conn.cursor() as cursor:
        cursor.executemany(sql, records)
    return len(records)


def cleanup_feature_basic_missing_dates_for_collected_stocks(conn) -> int:
    sql = """
        DELETE fb
        FROM FEATURE_BASIC fb
        JOIN (
            SELECT DISTINCT stock_code
            FROM FEATURE_RAW
            WHERE close IS NOT NULL
               OR trading_value IS NOT NULL
               OR foreign_netbuy_value IS NOT NULL
               OR inst_netbuy_value IS NOT NULL
               OR per IS NOT NULL
               OR pbr IS NOT NULL
        ) ds ON ds.stock_code = fb.stock_code
        LEFT JOIN FEATURE_RAW fr
          ON fr.stock_code = fb.stock_code
         AND fr.date = fb.date
        WHERE fr.stock_code IS NULL;
    """

    with conn.cursor() as cursor:
        affected = cursor.execute(sql)
    return int(affected or 0)


def cleanup_feature_basic_missing_dates_for_stock_if_collected(conn, stock_code: str) -> int:
    sql = """
        DELETE fb
        FROM FEATURE_BASIC fb
        LEFT JOIN FEATURE_RAW fr
          ON fr.stock_code = fb.stock_code
         AND fr.date = fb.date
        WHERE fb.stock_code = %s
          AND fr.stock_code IS NULL
          AND EXISTS (
              SELECT 1
              FROM FEATURE_RAW fr2
              WHERE fr2.stock_code = fb.stock_code
                AND (
                    fr2.close IS NOT NULL
                    OR fr2.trading_value IS NOT NULL
                    OR fr2.foreign_netbuy_value IS NOT NULL
                    OR fr2.inst_netbuy_value IS NOT NULL
                    OR fr2.per IS NOT NULL
                    OR fr2.pbr IS NOT NULL
                )
          );
    """

    with conn.cursor() as cursor:
        affected = cursor.execute(sql, (stock_code,))
    return int(affected or 0)


def main():
    args = parse_args()
    log(
        f"실행 시작 | start={args.start_date}, end={args.end_date}, "
        f"market={args.market}, limit={args.limit}, workers={args.workers}, "
        f"commit_every={args.commit_every}, no_resume={args.no_resume}, dry_run={args.dry_run}"
    )

    log("common.setting 로드 중...")
    global get_connection, login_krx
    from common.setting import get_connection as _get_connection, login_krx as _login_krx

    get_connection = _get_connection
    login_krx = _login_krx
    log("common.setting 로드 완료")

    krx_id = os.getenv("ID")
    krx_pw = os.getenv("PW")
    if not krx_id or not krx_pw:
        raise RuntimeError(".env에 ID, PW가 필요합니다.")

    log("KRX 로그인 시도...")
    if not login_krx(krx_id, krx_pw):
        raise RuntimeError("KRX 로그인 실패")
    log("KRX 로그인 성공")

    from_dt, to_dt = resolve_date_range(args.start_date, args.end_date, args.market)
    log(f"수집 기간 확정: {from_dt} ~ {to_dt}")

    log("DB 연결 시도...")
    conn = get_connection()
    log("DB 연결 성공")

    total_rows = 0
    total_deleted_rows = 0
    issue_totals = {"ohlcv_empty": 0, "investor_empty": 0, "fundamental_empty": 0}

    try:
        if not args.dry_run:
            create_feature_raw(conn)
            ensure_feature_raw_unique_key(conn)
            ensure_feature_raw_decimal_capacity(conn)
            ensure_feature_basic_lookup_index(conn)
            ensure_feature_raw_foreign_key(conn)

        resume_mode = (not args.no_resume) and (not args.dry_run)
        if resume_mode:
            log("resume mode: process only missing FEATURE_RAW keys")
            pre_deleted = cleanup_feature_basic_missing_dates_for_collected_stocks(conn)
            total_deleted_rows += pre_deleted
            conn.commit()
            log(f"resume pre-cleanup deleted_rows={pre_deleted}")
        stock_range_df = load_feature_basic_stock_ranges(
            conn,
            from_dt,
            to_dt,
            args.limit,
            missing_only=resume_mode,
        )
        if stock_range_df.empty:
            log("No target rows found in FEATURE_BASIC for the requested range.")
            return

        total_target_rows = int(stock_range_df["target_rows"].sum())
        total = len(stock_range_df)

        log(
            f"FEATURE_BASIC ranges loaded | stocks={total}, "
            f"target_rows={total_target_rows}"
        )

        sample_printed = False
        collect_jobs = []

        for idx, row in enumerate(stock_range_df.itertuples(index=False), start=1):
            code = str(row.stock_code).zfill(6)
            stock_from_dt = row.min_date.strftime("%Y%m%d")
            stock_to_dt = row.max_date.strftime("%Y%m%d")
            log(
                f"[{idx}/{total}] stock={code} range={stock_from_dt}~{stock_to_dt} "
                f"target_rows={int(row.target_rows)}"
            )

            target_dates = load_feature_basic_dates_for_stock(
                conn,
                code,
                row.min_date,
                row.max_date,
                missing_only=resume_mode,
            )
            if not target_dates:
                continue

            collect_jobs.append(
                {
                    "idx": idx,
                    "total": total,
                    "code": code,
                    "from_dt": stock_from_dt,
                    "to_dt": stock_to_dt,
                    "target_dates": target_dates,
                }
            )

        if not collect_jobs:
            log("No collectable targets after date filtering.")
            return

        worker_count = max(1, int(args.workers))
        job_total = len(collect_jobs)
        log(f"parallel collect enabled | workers={worker_count}, jobs={job_total}")

        processed = 0
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {}
            for submit_idx, job in enumerate(collect_jobs, start=1):
                future = executor.submit(
                    collect_task_worker,
                    job["code"],
                    job["from_dt"],
                    job["to_dt"],
                    f"{job['idx']}/{job['total']}:{job['code']}",
                )
                future_map[future] = job
                if args.sleep_sec > 0 and (submit_idx % worker_count == 0):
                    time.sleep(args.sleep_sec)

            for future in as_completed(future_map):
                job = future_map[future]
                processed += 1

                try:
                    frame, issue = future.result()
                except Exception as worker_error:
                    log(
                        f"[{job['idx']}/{job['total']}:{job['code']}] "
                        f"collect failed: {worker_error}"
                    )
                    if (processed % max(1, args.commit_every) == 0) and (not args.dry_run):
                        conn.commit()
                    if processed % 20 == 0 or processed == job_total:
                        log(f"[{processed}/{job_total}] processed | accumulated_rows={total_rows}, cleanup_deleted={total_deleted_rows}")
                    continue

                for k in issue_totals:
                    issue_totals[k] += int(bool(issue.get(k, False)))

                if not frame.empty:
                    frame = frame[frame["date"].isin(job["target_dates"])]
                    frame = frame.sort_values(["stock_code", "date"]).reset_index(drop=True)
                    if args.dry_run:
                        total_rows += len(frame)
                        if not sample_printed:
                            log("dry-run sample (first 10 rows):")
                            print(frame.head(10).to_string(index=False), flush=True)
                            sample_printed = True
                    else:
                        upserted = upsert_feature_raw(conn, frame)
                        total_rows += upserted
                        if upserted > 0:
                            deleted = cleanup_feature_basic_missing_dates_for_stock_if_collected(
                                conn,
                                job["code"],
                            )
                            total_deleted_rows += deleted
                            if deleted > 0:
                                log(
                                    f"[{job['idx']}/{job['total']}:{job['code']}] "
                                    f"FEATURE_BASIC cleanup deleted_rows={deleted}"
                                )

                if (processed % max(1, args.commit_every) == 0) and (not args.dry_run):
                    conn.commit()

                if processed % 20 == 0 or processed == job_total:
                    log(f"[{processed}/{job_total}] processed | accumulated_rows={total_rows}, cleanup_deleted={total_deleted_rows}")

        if not args.dry_run:
            conn.commit()

        log(f"done rows: {total_rows}, cleanup_deleted={total_deleted_rows}")
        log("수집 이슈 요약:")
        print(pd.Series(issue_totals).to_string(), flush=True)

        if args.dry_run:
            log("dry-run 모드: DB 적재 생략")
        else:
            log(f"FEATURE_RAW upsert 완료: {total_rows}건")

    except Exception as e:
        conn.rollback()
        log(f"(롤백됨) FEATURE_RAW 적재 중 오류: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()









