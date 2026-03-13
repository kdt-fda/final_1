import argparse
import os
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pykrx import stock

# Add project root for common.setting import
TEMP_BASE = Path(__file__).resolve().parents[1]
if str(TEMP_BASE) not in sys.path:
    sys.path.append(str(TEMP_BASE))

get_connection = None
login_krx = None


def log(msg: str) -> None:
    print(msg, flush=True)


def default_worker_count() -> int:
    cpu = os.cpu_count() or 4
    return max(2, min(4, cpu // 4 if cpu >= 4 else 1))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill FEATURE_RAW.mkt_cap using pykrx market cap data"
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
        help="Market filter used only for latest trading date resolution",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of target stocks",
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
        help="Thread worker count for market cap fetch (recommended: 2~4)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect/validate without DB update",
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
        f"Failed to resolve latest trading date. base={base_date.strftime('%Y-%m-%d')}, market={market}"
    )


def resolve_date_range(start_date: str | None, end_date: str | None, market: str) -> tuple[str, str]:
    if end_date:
        to_dt = yyyymmdd(end_date)
    else:
        yesterday = datetime.now() - timedelta(days=1)
        to_dt = resolve_latest_trading_date(yesterday, market)

    if start_date:
        from_dt = yyyymmdd(start_date)
    else:
        from_dt = "20091231"

    if from_dt > to_dt:
        raise ValueError(f"Invalid date range: from_dt={from_dt}, to_dt={to_dt}")

    return from_dt, to_dt


def safe_pykrx_call(fn, *args, **kwargs) -> pd.DataFrame:
    try:
        out = fn(*args, **kwargs)
        if isinstance(out, pd.DataFrame):
            return out.copy()
    except Exception as e:  # noqa: BLE001
        name = getattr(fn, "__name__", str(fn))
        log(f"[WARN] pykrx call failed: fn={name}, err={e}")
    return pd.DataFrame()


def ensure_mkt_cap_column(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'FEATURE_RAW'
              AND column_name = 'mkt_cap';
            """
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                ALTER TABLE FEATURE_RAW
                ADD COLUMN mkt_cap DECIMAL(24,6) NULL AFTER pbr;
                """
            )
            log("FEATURE_RAW.mkt_cap column created")
            return

        prec = int(row["numeric_precision"]) if row["numeric_precision"] is not None else 0
        scale = int(row["numeric_scale"]) if row["numeric_scale"] is not None else 0
        if prec < 24 or scale < 6:
            cursor.execute(
                """
                ALTER TABLE FEATURE_RAW
                MODIFY COLUMN mkt_cap DECIMAL(24,6) NULL;
                """
            )
            log("FEATURE_RAW.mkt_cap widened to DECIMAL(24,6)")


def load_target_stock_ranges(
    conn,
    from_dt: str,
    to_dt: str,
    limit: int | None = None,
) -> pd.DataFrame:
    start_date = pd.to_datetime(from_dt).date()
    end_date = pd.to_datetime(to_dt).date()

    with conn.cursor() as cursor:
        if limit is None:
            cursor.execute(
                """
                SELECT
                    stock_code,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(*) AS target_rows
                FROM FEATURE_RAW
                WHERE date BETWEEN %s AND %s
                  AND stock_code IS NOT NULL
                  AND mkt_cap IS NULL
                GROUP BY stock_code
                ORDER BY stock_code;
                """,
                (start_date, end_date),
            )
        else:
            cursor.execute(
                """
                SELECT
                    stock_code,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(*) AS target_rows
                FROM FEATURE_RAW
                WHERE date BETWEEN %s AND %s
                  AND stock_code IS NOT NULL
                  AND mkt_cap IS NULL
                GROUP BY stock_code
                ORDER BY stock_code
                LIMIT %s;
                """,
                (start_date, end_date, int(limit)),
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


def fetch_market_cap_by_date(stock_code: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    fn = stock.get_market_cap_by_date if hasattr(stock, "get_market_cap_by_date") else stock.get_market_cap
    cap_df = safe_pykrx_call(fn, from_dt, to_dt, stock_code)
    if cap_df.empty:
        return pd.DataFrame(columns=["date", "mkt_cap"])

    mkt_col = None
    for cand in ["시가총액", "mkt_cap", "MKT_CAP"]:
        if cand in cap_df.columns:
            mkt_col = cand
            break

    if mkt_col is None:
        return pd.DataFrame(columns=["date", "mkt_cap"])

    out = pd.DataFrame(index=cap_df.index.copy())
    out["mkt_cap"] = pd.to_numeric(cap_df[mkt_col], errors="coerce")

    out.index = pd.to_datetime(out.index)
    out = out.reset_index()
    first_col = out.columns[0]
    if first_col != "date":
        out = out.rename(columns={first_col: "date"})

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out[out["date"].notna()].copy()
    return out[["date", "mkt_cap"]]


def fetch_market_cap_with_timing(stock_code: str, from_dt: str, to_dt: str):
    t0 = time.perf_counter()
    df = fetch_market_cap_by_date(stock_code, from_dt, to_dt)
    return df, (time.perf_counter() - t0)


def to_sql_value(v):
    if pd.isna(v):
        return None
    return float(v)


def ensure_tmp_mkt_cap_table(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS TMP_MKT_CAP_BACKFILL (
                stock_code VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                mkt_cap DECIMAL(24,6) NULL,
                PRIMARY KEY (stock_code, date)
            ) ENGINE=InnoDB;
            """
        )


def update_mkt_cap(conn, stock_code: str, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    records = []
    for row in frame.itertuples(index=False):
        records.append((stock_code, row.date, to_sql_value(row.mkt_cap)))

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE TMP_MKT_CAP_BACKFILL;")
        cursor.executemany(
            """
            INSERT INTO TMP_MKT_CAP_BACKFILL (stock_code, date, mkt_cap)
            VALUES (%s, %s, %s)
            """,
            records,
        )
        cursor.execute(
            """
            UPDATE FEATURE_RAW fr
            JOIN TMP_MKT_CAP_BACKFILL t
              ON fr.stock_code = t.stock_code
             AND fr.date = t.date
            SET fr.mkt_cap = t.mkt_cap
            WHERE fr.mkt_cap IS NULL
              AND t.mkt_cap IS NOT NULL;
            """
        )
        affected = cursor.rowcount

    return int(affected or 0)


def main() -> None:
    args = parse_args()
    log(
        f"start | start={args.start_date}, end={args.end_date}, market={args.market}, "
        f"limit={args.limit}, workers={args.workers}, dry_run={args.dry_run}"
    )

    log("loading common.setting ...")
    global get_connection, login_krx
    from common.setting import get_connection as _get_connection, login_krx as _login_krx

    get_connection = _get_connection
    login_krx = _login_krx

    krx_id = os.getenv("ID")
    krx_pw = os.getenv("PW")
    if not krx_id or not krx_pw:
        raise RuntimeError("ID/PW env vars are required")

    log("trying KRX login ...")
    if not login_krx(krx_id, krx_pw):
        raise RuntimeError("KRX login failed")
    log("KRX login succeeded")

    from_dt, to_dt = resolve_date_range(args.start_date, args.end_date, args.market)
    log(f"effective date range: {from_dt} ~ {to_dt}")

    conn = get_connection()

    total_updated = 0
    processed_stocks = 0
    empty_cap_stocks = 0
    total_fetch_sec = 0.0
    total_update_sec = 0.0
    total_commit_sec = 0.0

    try:
        if not args.dry_run:
            ensure_mkt_cap_column(conn)
            ensure_tmp_mkt_cap_table(conn)
            t_commit = time.perf_counter()
            conn.commit()
            sec_commit = time.perf_counter() - t_commit
            total_commit_sec += sec_commit
            log(f"[timing] commit schema/setup: {sec_commit:.3f}s")

        stock_range_df = load_target_stock_ranges(conn, from_dt, to_dt, args.limit)
        if stock_range_df.empty:
            log("No target rows with mkt_cap IS NULL")
            return

        total = len(stock_range_df)
        total_targets = int(stock_range_df["target_rows"].sum())
        log(f"targets loaded | stocks={total}, rows={total_targets}")

        worker_count = max(1, int(args.workers))
        log(f"parallel fetch enabled | workers={worker_count}, jobs={total}")

        processed = 0
        submitted = 0
        next_idx = 0
        rows = list(stock_range_df.itertuples(index=False))

        def _submit_one(executor, row_obj, idx_num, fut_map):
            nonlocal submitted
            code = str(row_obj.stock_code).zfill(6)
            stock_from_dt = row_obj.min_date.strftime("%Y%m%d")
            stock_to_dt = row_obj.max_date.strftime("%Y%m%d")
            log(
                f"[{idx_num}/{total}] stock={code} range={stock_from_dt}~{stock_to_dt} "
                f"target_rows={int(row_obj.target_rows)}"
            )
            future = executor.submit(fetch_market_cap_with_timing, code, stock_from_dt, stock_to_dt)
            fut_map[future] = {
                "idx": idx_num,
                "total": total,
                "code": code,
            }
            submitted += 1
            if args.sleep_sec > 0 and (submitted % worker_count == 0):
                time.sleep(args.sleep_sec)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {}

            while next_idx < len(rows) and len(future_map) < worker_count:
                _submit_one(executor, rows[next_idx], next_idx + 1, future_map)
                next_idx += 1

            while future_map:
                done_set, _ = wait(tuple(future_map.keys()), return_when=FIRST_COMPLETED)

                for future in done_set:
                    job = future_map.pop(future)
                    processed += 1

                    fetch_sec = 0.0
                    update_sec = 0.0
                    commit_sec = 0.0
                    updated_rows = 0
                    fetched_rows = 0

                    try:
                        cap_df, fetch_sec = future.result()
                        total_fetch_sec += fetch_sec
                    except Exception as fetch_error:
                        log(
                            f"[{job['idx']}/{job['total']}:{job['code']}] "
                            f"market cap fetch failed: {fetch_error}"
                        )
                        if (processed % max(1, args.commit_every) == 0) and (not args.dry_run):
                            t_commit = time.perf_counter()
                            conn.commit()
                            commit_sec = time.perf_counter() - t_commit
                            total_commit_sec += commit_sec
                            log(
                                f"[{job['idx']}/{job['total']}:{job['code']}] timing | "
                                f"fetch=ERR, update=0.000s, commit={commit_sec:.3f}s"
                            )
                        if processed % 20 == 0 or processed == total:
                            log(
                                f"[{processed}/{total}] processed | updated={total_updated}, "
                                f"empty_cap_stocks={empty_cap_stocks}, "
                                f"avg_fetch={total_fetch_sec / max(1, processed):.3f}s, "
                                f"avg_update={total_update_sec / max(1, processed):.3f}s, "
                                f"avg_commit={total_commit_sec / max(1, processed):.3f}s"
                            )
                        continue

                    if cap_df.empty:
                        empty_cap_stocks += 1
                        log(
                            f"[{job['idx']}/{job['total']}:{job['code']}] market cap empty | "
                            f"fetch={fetch_sec:.3f}s"
                        )
                    else:
                        cap_df = cap_df.sort_values(["date"]).reset_index(drop=True)
                        fetched_rows = len(cap_df)

                        if args.dry_run:
                            updated_rows = fetched_rows
                            total_updated += updated_rows
                        else:
                            t_update = time.perf_counter()
                            updated_rows = update_mkt_cap(conn, job["code"], cap_df)
                            update_sec = time.perf_counter() - t_update
                            total_update_sec += update_sec
                            total_updated += updated_rows

                        processed_stocks += 1

                    if (processed % max(1, args.commit_every) == 0) and (not args.dry_run):
                        t_commit = time.perf_counter()
                        conn.commit()
                        commit_sec = time.perf_counter() - t_commit
                        total_commit_sec += commit_sec

                    log(
                        f"[{job['idx']}/{job['total']}:{job['code']}] timing | "
                        f"fetch={fetch_sec:.3f}s, update={update_sec:.3f}s, commit={commit_sec:.3f}s, "
                        f"fetched_rows={fetched_rows}, updated_rows={updated_rows}"
                    )

                    if processed % 20 == 0 or processed == total:
                        log(
                            f"[{processed}/{total}] processed | updated={total_updated}, "
                            f"empty_cap_stocks={empty_cap_stocks}, "
                            f"avg_fetch={total_fetch_sec / max(1, processed):.3f}s, "
                            f"avg_update={total_update_sec / max(1, processed):.3f}s, "
                            f"avg_commit={total_commit_sec / max(1, processed):.3f}s"
                        )

                while next_idx < len(rows) and len(future_map) < worker_count:
                    _submit_one(executor, rows[next_idx], next_idx + 1, future_map)
                    next_idx += 1

        if not args.dry_run:
            t_commit = time.perf_counter()
            conn.commit()
            sec_commit = time.perf_counter() - t_commit
            total_commit_sec += sec_commit
            log(f"[timing] final commit: {sec_commit:.3f}s")

        log(
            f"done | updated={total_updated}, processed_stocks={processed_stocks}, "
            f"empty_cap_stocks={empty_cap_stocks}, dry_run={args.dry_run}, "
            f"total_fetch={total_fetch_sec:.3f}s, total_update={total_update_sec:.3f}s, "
            f"total_commit={total_commit_sec:.3f}s"
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
