from pykrx import stock
import pandas as pd
from pathlib import Path
import sys
import time

# 경로 잡기
TEMP_BASE = Path(__file__).resolve().parents[1]
if str(TEMP_BASE) not in sys.path:
    sys.path.append(str(TEMP_BASE))

from common.setting import get_connection


# -------------------------------------------------
# 1) LABEL 테이블 생성
# -------------------------------------------------
def create_label(conn):
    """
    LABEL: 종목(stock_code) x 기준일(asof_date) 별 alpha 저장
    - (stock_code, asof_date) UNIQUE 로 UPSERT 기준 잡음
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS LABEL (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL,
                    asof_date DATE NOT NULL,
                    alpha DECIMAL(18,6) NULL,
                    UNIQUE KEY uk_label_stock_date (stock_code, asof_date)
                );
                """
            )
        conn.commit()
        print("LABEL 테이블 준비 완료")
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) LABEL 테이블 생성 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 2) DB(FEATURE_BASIC) 기준 월말 거래일 생성
# -------------------------------------------------
def get_month_end_trading_dates_from_db(conn, start: str) -> list:
    """
    FEATURE_BASIC.date 기준으로 월별 마지막 거래일을 생성.
    pykrx 지수 API에 의존하지 않음.
    """
    sql = """
    SELECT MAX(date) AS month_end
    FROM FEATURE_BASIC
    WHERE date >= %s
    GROUP BY YEAR(date), MONTH(date)
    ORDER BY month_end
    """

    start_date = pd.to_datetime(start).date()

    with conn.cursor() as cursor:
        cursor.execute(sql, (start_date,))
        rows = cursor.fetchall()

    if not rows:
        raise ValueError("FEATURE_BASIC에서 월말 거래일을 찾지 못했습니다.")

    if isinstance(rows[0], dict):
        month_end_dates = [pd.to_datetime(r["month_end"]).date() for r in rows]
    else:
        month_end_dates = [pd.to_datetime(r[0]).date() for r in rows]

    print(f"DB 기준 월말 거래일 개수: {len(month_end_dates)}")
    print("첫 월말:", month_end_dates[0], "/ 마지막 월말:", month_end_dates[-1])

    return month_end_dates


# -------------------------------------------------
# 3) LABEL에 아직 없는 월말 기준일만 찾기
# -------------------------------------------------
def get_missing_month_end_dates(conn, start: str) -> list:
    all_month_end_dates = get_month_end_trading_dates_from_db(conn, start)

    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT asof_date FROM LABEL")
        rows = cursor.fetchall()

    if rows:
        if isinstance(rows[0], dict):
            existing_dates = {
                pd.to_datetime(r["asof_date"]).date()
                for r in rows
                if r["asof_date"] is not None
            }
        else:
            existing_dates = {
                pd.to_datetime(r[0]).date()
                for r in rows
                if r[0] is not None
            }
    else:
        existing_dates = set()

    missing_dates = [d for d in all_month_end_dates if d not in existing_dates]

    print("기존 LABEL 기준일 수:", len(existing_dates))
    print("이번에 새로 추가할 기준일 수:", len(missing_dates))
    if missing_dates:
        print("추가 대상 기준일 범위:", min(missing_dates), "~", max(missing_dates))

    return missing_dates


# -------------------------------------------------
# 4) 신규 기준일에 대해서만 label_base 생성
# -------------------------------------------------
def build_label_base_incremental(conn, start: str) -> pd.DataFrame:
    """
    FEATURE_BASIC 기준으로 LABEL에 아직 없는 월말 기준일만 생성
    - 매번 전체 월말을 다시 만들지 않음
    - 누락된 월말이 있으면 그 월말들만 보강 가능
    """
    missing_dates = get_missing_month_end_dates(conn, start)

    if not missing_dates:
        print("추가할 신규 월말 기준일이 없습니다.")
        return pd.DataFrame(columns=["stock_code", "asof_date"])

    placeholders = ",".join(["%s"] * len(missing_dates))

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT stock_code, date AS asof_date
            FROM FEATURE_BASIC
            WHERE is_listed_on_date = 1
              AND date IN ({placeholders})
            """,
            missing_dates,
        )
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_code", "asof_date"])

    if isinstance(rows[0], dict):
        label = pd.DataFrame(rows)
    else:
        label = pd.DataFrame(rows, columns=["stock_code", "asof_date"])

    label["stock_code"] = label["stock_code"].astype(str).str.strip().str.zfill(6)
    label["asof_date"] = pd.to_datetime(label["asof_date"]).dt.date
    label = label.drop_duplicates(subset=["stock_code", "asof_date"]).reset_index(drop=True)

    print("신규 label_base shape:", label.shape)
    print("신규 label_base 종목 수:", label["stock_code"].nunique())
    print("신규 label_base 기준일 수:", label["asof_date"].nunique())
    print(label.head())

    return label


# -------------------------------------------------
# 5) 신규 row만 LABEL에 적재
# -------------------------------------------------
def upload_to_label(conn, df: pd.DataFrame, chunk_size: int = 10000):
    """
    없는 (stock_code, asof_date)만 insert.
    기존 alpha는 절대 덮어쓰지 않음.
    """
    if df.empty:
        print("업로드할 신규 LABEL 데이터가 없습니다.")
        return

    sql = """
        INSERT INTO LABEL (stock_code, asof_date)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            stock_code = stock_code
    """

    df = df.copy()
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date
    data = [(r.stock_code, r.asof_date) for r in df.itertuples(index=False)]

    try:
        with conn.cursor() as cursor:
            for i in range(0, len(data), chunk_size):
                cursor.executemany(sql, data[i:i + chunk_size])
                print(f"LABEL 업서트 진행: {min(i + chunk_size, len(data)):,} / {len(data):,}")
        conn.commit()
        print(f"✅ LABEL upsert done: {len(data):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) LABEL 업로드 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 6) 필요한 날짜별 코스닥 전종목 종가 조회
# -------------------------------------------------
def get_kosdaq_prices_by_dates(dates, sleep_sec: float = 0.3, max_retry: int = 3) -> pd.DataFrame:
    all_price_frames = []
    dates = sorted(pd.to_datetime(list(dates)))

    if not dates:
        return pd.DataFrame(columns=["stock_code", "date", "close"])

    print("가격 조회 대상 날짜 수:", len(dates))

    for i, dt in enumerate(dates, start=1):
        ymd = dt.strftime("%Y%m%d")
        print(f"[{i}/{len(dates)}] 날짜별 전체 종목 가격 조회 중: {ymd}")

        daily = None
        last_err = None

        for trial in range(1, max_retry + 1):
            try:
                daily = stock.get_market_ohlcv_by_ticker(ymd, market="KOSDAQ")
                if daily is not None and not daily.empty:
                    break
                print(f"  - 재시도 {trial}/{max_retry}: 빈 데이터프레임")
            except Exception as e:
                last_err = e
                print(f"  - 재시도 {trial}/{max_retry}: 오류 발생 -> {e}")
            time.sleep(sleep_sec * trial)

        if daily is None or daily.empty:
            print(f"[경고] {ymd} 조회 실패. last_err={last_err}")
            continue

        daily = daily.reset_index()
        rename_map = {
            "티커": "stock_code",
            "ticker": "stock_code",
            "종목코드": "stock_code",
            "종가": "close",
            "close": "close",
        }
        daily = daily.rename(columns=rename_map)

        if "stock_code" not in daily.columns:
            raise ValueError(f"stock_code 컬럼을 찾을 수 없습니다. columns={daily.columns.tolist()}")
        if "close" not in daily.columns:
            raise ValueError(f"close 컬럼을 찾을 수 없습니다. columns={daily.columns.tolist()}")

        daily["stock_code"] = daily["stock_code"].astype(str).str.strip().str.zfill(6)
        daily["date"] = pd.to_datetime(dt)
        daily["close"] = pd.to_numeric(daily["close"], errors="coerce")

        all_price_frames.append(daily[["stock_code", "date", "close"]])
        time.sleep(sleep_sec)

    if not all_price_frames:
        return pd.DataFrame(columns=["stock_code", "date", "close"])

    price_all = pd.concat(all_price_frames, ignore_index=True)
    price_all["date"] = pd.to_datetime(price_all["date"])
    price_all["close"] = pd.to_numeric(price_all["close"], errors="coerce")

    print("수집된 전체 가격 행 수:", len(price_all))
    print("가격 데이터 날짜 수:", price_all["date"].nunique())
    print("가격 데이터 종목 수:", price_all["stock_code"].nunique())

    return price_all


# -------------------------------------------------
# 7) alpha 미완성 구간만 계산 대상 추출
# -------------------------------------------------
def get_alpha_pending_targets(conn) -> pd.DataFrame:
    """
    alpha가 NULL이면서, 다음 월말(next_asof_date)이 존재하는 구간만 반환.
    마지막 월말은 다음 월말이 생기기 전까지 계산 불가이므로 제외됨.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            WITH cal AS (
                SELECT
                    asof_date,
                    LEAD(asof_date) OVER (ORDER BY asof_date) AS next_asof_date
                FROM (
                    SELECT DISTINCT asof_date
                    FROM LABEL
                ) t
            )
            SELECT
                l.stock_code,
                l.asof_date,
                cal.next_asof_date
            FROM LABEL l
            JOIN cal
              ON l.asof_date = cal.asof_date
            WHERE l.alpha IS NULL
              AND cal.next_asof_date IS NOT NULL
            ORDER BY l.asof_date, l.stock_code
            """
        )
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_code", "asof_date", "next_asof_date"])

    if isinstance(rows[0], dict):
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(rows, columns=["stock_code", "asof_date", "next_asof_date"])

    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["asof_date"] = pd.to_datetime(df["asof_date"])
    df["next_asof_date"] = pd.to_datetime(df["next_asof_date"])

    return df


# -------------------------------------------------
# 8) alpha 실제값 채우기 (미완성 구간만)
# -------------------------------------------------
def fill_alpha_to_label(conn, sleep_sec: float = 0.3, max_retry: int = 3):
    """
    아직 alpha가 없는 구간 중, 다음 월말이 존재하는 구간만 계산.
    alpha = 개별종목 1개월 미래수익률 - KOSDAQ 1개월 미래수익률
    """
    df = get_alpha_pending_targets(conn)

    if df.empty:
        print("계산 가능한 alpha 미완성 구간이 없습니다.")
        return

    print("alpha 미계산 대상 행 수:", len(df))
    print("alpha 미계산 대상 종목 수:", df["stock_code"].nunique())
    print("alpha 미계산 대상 기준일 수:", df["asof_date"].nunique())
    print("계산 구간:", df["asof_date"].min().date(), "~", df["next_asof_date"].max().date())

    needed_dates = sorted(set(df["asof_date"]).union(set(df["next_asof_date"])))

    start = min(needed_dates).strftime("%Y%m%d")
    end = max(needed_dates).strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, end, "2001")
    if idx is None or idx.empty:
        raise ValueError("KOSDAQ 지수 데이터를 가져오지 못했습니다.")

    idx.index = pd.to_datetime(idx.index)
    idx_close_map = idx["종가"].to_dict()

    df["bench_start"] = df["asof_date"].map(idx_close_map)
    df["bench_end"] = df["next_asof_date"].map(idx_close_map)
    df["bench_ret"] = (df["bench_end"] / df["bench_start"]) - 1

    print("벤치마크 수익률 계산 가능 행 수:", df["bench_ret"].notna().sum())

    price_all = get_kosdaq_prices_by_dates(needed_dates, sleep_sec=sleep_sec, max_retry=max_retry)
    if price_all.empty:
        print("수집된 가격 데이터가 없습니다.")
        return

    start_price = price_all.rename(columns={"date": "asof_date", "close": "stock_start"})
    end_price = price_all.rename(columns={"date": "next_asof_date", "close": "stock_end"})

    df = df.merge(start_price, on=["stock_code", "asof_date"], how="left")
    df = df.merge(end_price, on=["stock_code", "next_asof_date"], how="left")

    before_drop = len(df)
    df = df.dropna(subset=["stock_start", "stock_end", "bench_ret"]).copy()

    print("가격/벤치마크 매핑 후 계산 가능 행 수:", len(df))
    print("제외된 행 수:", before_drop - len(df))

    if df.empty:
        print("계산 가능한 alpha가 없습니다.")
        return

    df["stock_ret"] = (df["stock_end"] / df["stock_start"]) - 1
    df["alpha"] = df["stock_ret"] - df["bench_ret"]

    df_alpha = df[["stock_code", "asof_date", "alpha"]].copy()
    df_alpha["asof_date"] = pd.to_datetime(df_alpha["asof_date"]).dt.date
    df_alpha["alpha"] = pd.to_numeric(df_alpha["alpha"], errors="coerce")

    print("계산된 alpha 행 수:", len(df_alpha))
    print(df_alpha.head())

    update_sql = """
        UPDATE LABEL
        SET alpha = %s
        WHERE stock_code = %s
          AND asof_date = %s
          AND alpha IS NULL
    """

    data = [(r.alpha, r.stock_code, r.asof_date) for r in df_alpha.itertuples(index=False)]

    try:
        with conn.cursor() as cursor:
            batch_size = 10000
            for i in range(0, len(data), batch_size):
                cursor.executemany(update_sql, data[i:i + batch_size])
                print(f"alpha 업데이트 진행: {min(i + batch_size, len(data)):,} / {len(data):,}")
        conn.commit()
        print(f"✅ alpha 업데이트 완료: {len(data):,} rows")
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) alpha 업데이트 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 9) sanity check
# -------------------------------------------------
def sanity_check_alpha(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM LABEL")
        total_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) AS cnt FROM LABEL WHERE alpha IS NOT NULL")
        alpha_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) AS cnt FROM LABEL WHERE alpha IS NULL")
        alpha_null_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(DISTINCT stock_code) AS cnt FROM LABEL")
        stock_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(DISTINCT asof_date) AS cnt FROM LABEL")
        date_cnt = cursor.fetchone()

    if isinstance(total_cnt, dict):
        print("LABEL 전체 행 수:", total_cnt["cnt"])
        print("LABEL alpha 존재 행 수:", alpha_cnt["cnt"])
        print("LABEL alpha NULL 행 수:", alpha_null_cnt["cnt"])
        print("LABEL 종목 수:", stock_cnt["cnt"])
        print("LABEL 기준일 수:", date_cnt["cnt"])
    else:
        print("LABEL 전체 행 수:", total_cnt[0])
        print("LABEL alpha 존재 행 수:", alpha_cnt[0])
        print("LABEL alpha NULL 행 수:", alpha_null_cnt[0])
        print("LABEL 종목 수:", stock_cnt[0])
        print("LABEL 기준일 수:", date_cnt[0])


# -------------------------------------------------
# 10) MAIN
# -------------------------------------------------
def main():
    print("[1] DB 연결 시작")
    conn = get_connection()

    try:
        print("[2] create_label 시작")
        create_label(conn)

        print("[3] build_label_base_incremental 시작")
        label_base = build_label_base_incremental(conn, start="20151001")

        print("[4] upload_to_label 시작")
        upload_to_label(conn, label_base)

        print("[5] fill_alpha_to_label 시작")
        fill_alpha_to_label(conn, sleep_sec=0.3, max_retry=3)

        print("[6] sanity_check_alpha 시작")
        sanity_check_alpha(conn)

        print("[7] 전체 완료")
    finally:
        conn.close()
        print("[8] DB 연결 종료")


if __name__ == "__main__":
    main()