from pykrx import stock
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
from pathlib import Path
import sys
import time

# 경로 잡기
temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

from common.setting import login_krx, get_connection


# -------------------------------------------------
# 1) LABEL 테이블 생성
# -------------------------------------------------
def create_label(conn):
    """
    LABEL: 종목(stock_code) x 기준일(asof_date) 별 alpha 저장
    - asof_date는 '달력 월말'
    - (stock_code, asof_date) UNIQUE 기준
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS LABEL (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL,
                    asof_date DATE NOT NULL,
                    alpha DECIMAL(18,6) NULL,
                    UNIQUE KEY uk_label_stock_date (stock_code, asof_date)
                );
            """)
        print("LABEL 테이블이 성공적으로 생성되었거나 이미 존재합니다.")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) LABEL 테이블 생성 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 2) KOSDAQ 거래일력 확보
# -------------------------------------------------
def get_kosdaq_trading_calendar(start: str) -> pd.DatetimeIndex:
    """
    start ~ today 까지의 KOSDAQ 실제 거래일 목록
    """
    today = datetime.today().strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, today, "2001", name_display=False)
    if idx is None or idx.empty:
        raise ValueError("KOSDAQ 지수 데이터가 비어 있습니다.")

    idx.index = pd.to_datetime(idx.index)
    trading_dates = pd.DatetimeIndex(idx.index.unique()).sort_values()

    print(f"KOSDAQ 거래일 수: {len(trading_dates)}")
    print("첫 거래일:", trading_dates.min().date(), "/ 마지막 거래일:", trading_dates.max().date())

    return trading_dates


# -------------------------------------------------
# 3) 월말 캘린더 생성
# -------------------------------------------------
def build_month_end_reference_calendar(start: str) -> pd.DataFrame:
    """
    asof_date는 '달력 월말'
    ref_trade_date는 '해당 월말까지의 최신 거래일'

    예:
    - asof_date = 2026-03-31
    - 오늘이 2026-03-23이면
    - ref_trade_date = 2026-03-23

    단, alpha 계산 시에는 다음 asof_date가 아직 미래이면 계산하지 않음.
    """
    trading_dates = get_kosdaq_trading_calendar(start)
    today_ts = pd.Timestamp(datetime.today().date())

    start_ts = pd.to_datetime(start)
    start_month_end = start_ts.to_period("M").to_timestamp("M")
    current_month_end = today_ts.to_period("M").to_timestamp("M")

    month_ends = pd.date_range(start=start_month_end, end=current_month_end, freq="ME")

    if len(month_ends) == 0:
        raise ValueError("생성된 월말 캘린더가 없습니다.")

    ref_rows = []

    for asof_ts in month_ends:
        cutoff = min(asof_ts, today_ts)

        eligible = trading_dates[trading_dates <= cutoff]
        if len(eligible) == 0:
            print(f"[스킵] {asof_ts.date()} 이전 거래일이 없어 제외")
            continue

        ref_trade_date = eligible.max()

        ref_rows.append({
            "asof_date": asof_ts.normalize(),
            "ref_trade_date": pd.Timestamp(ref_trade_date).normalize()
        })

    cal = pd.DataFrame(ref_rows)

    if cal.empty:
        raise ValueError("월말 참조 캘린더가 비었습니다.")

    cal["asof_date"] = pd.to_datetime(cal["asof_date"])
    cal["ref_trade_date"] = pd.to_datetime(cal["ref_trade_date"])

    print("월말 캘린더 개수:", len(cal))
    print("첫 월말:", cal["asof_date"].min().date(), "/ 마지막 월말:", cal["asof_date"].max().date())
    print("sample:")
    print(cal.head())

    return cal


# -------------------------------------------------
# 4) label_base 생성: FEATURE_BASIC 기준
# -------------------------------------------------
def build_label_base_easy(conn, start: str) -> pd.DataFrame:
    """
    FEATURE_BASIC 기준으로 label base 생성
    - LABEL.asof_date는 무조건 '달력 월말'
    - 실제 종목 포함 여부는 ref_trade_date 기준
    - 해당 ref_trade_date에 상장되어 있던 종목(is_listed_on_date = 1)만 포함
    """
    cal = build_month_end_reference_calendar(start)

    ref_dates = cal["ref_trade_date"].dt.date.tolist()
    if not ref_dates:
        raise ValueError("ref_trade_date 리스트가 비었습니다.")

    placeholders = ",".join(["%s"] * len(ref_dates))

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT stock_code, date AS ref_trade_date
            FROM FEATURE_BASIC
            WHERE is_listed_on_date = 1
              AND date IN ({placeholders})
        """, ref_dates)
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_code", "asof_date", "alpha"])

    if isinstance(rows[0], dict):
        base = pd.DataFrame(rows)
    else:
        base = pd.DataFrame(rows, columns=["stock_code", "ref_trade_date"])

    base["stock_code"] = base["stock_code"].astype(str).str.strip().str.zfill(6)
    base["ref_trade_date"] = pd.to_datetime(base["ref_trade_date"])

    # ref_trade_date -> asof_date 매핑
    ref_to_asof = cal.copy()
    merged = base.merge(ref_to_asof, on="ref_trade_date", how="left")

    label = merged[["stock_code", "asof_date"]].copy()
    label["asof_date"] = pd.to_datetime(label["asof_date"]).dt.date
    label["alpha"] = None

    label = label.drop_duplicates(subset=["stock_code", "asof_date"]).reset_index(drop=True)

    print("label_base shape:", label.shape)
    print("label_base 종목 수:", label["stock_code"].nunique())
    print("label_base 기준일 수:", label["asof_date"].nunique())
    print("label_base sample:")
    print(label.head())

    return label


# -------------------------------------------------
# 5) LABEL insert (기존 alpha 보존)
# -------------------------------------------------
def upload_to_label(conn, df: pd.DataFrame, chunk_size: int = 10000):
    """
    LABEL(stock_code, asof_date) UNIQUE 기준으로
    없는 row만 INSERT
    기존 alpha는 절대 덮어쓰지 않음
    """
    sql = """
        INSERT IGNORE INTO LABEL (stock_code, asof_date, alpha)
        VALUES (%s, %s, %s);
    """

    df = df.copy()
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.date

    if "alpha" not in df.columns:
        df["alpha"] = None

    data = [(r.stock_code, r.asof_date, r.alpha) for r in df.itertuples(index=False)]

    try:
        with conn.cursor() as cursor:
            for i in range(0, len(data), chunk_size):
                cursor.executemany(sql, data[i:i + chunk_size])
                print(f"insert 진행: {min(i + chunk_size, len(data)):,} / {len(data):,}")

        conn.commit()
        print(f"✅ LABEL insert done: {len(data):,} rows")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) LABEL 업로드 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 6) 필요한 날짜별 전체 종목 종가 조회
# -------------------------------------------------
def get_kosdaq_prices_by_dates(dates, sleep_sec: float = 0.3, max_retry: int = 3) -> pd.DataFrame:
    """
    필요한 날짜들에 대해 코스닥 전체 종목 종가를 조회
    반환 컬럼: stock_code, date, close
    """
    all_price_frames = []
    dates = sorted(pd.to_datetime(list(dates)).unique())

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
                else:
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
# 7) alpha 실제값 채우기 (alpha IS NULL 대상만)
# -------------------------------------------------
def fill_alpha_to_label(conn, start: str = "20151001", sleep_sec: float = 0.3, max_retry: int = 3):
    """
    아직 alpha가 없는 LABEL row만 대상으로 실제 미래 1개월 alpha 업데이트
    alpha = 개별종목 1개월 미래수익률 - KOSDAQ 1개월 미래수익률

    핵심 규칙:
    - LABEL.asof_date는 달력 월말
    - 실제 수익률 계산은 ref_trade_date 기준
    - next_asof_date가 '오늘 기준 아직 닫히지 않은 월말'이면 계산하지 않음
      -> 마지막 월말 alpha는 NULL 유지
    """

    today_date = datetime.today().date()

    # -------------------------------------------------
    # 1) LABEL 전체 월말 캘린더 확보
    # -------------------------------------------------
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT asof_date
            FROM LABEL
            ORDER BY asof_date
        """)
        cal_rows = cursor.fetchall()

    if not cal_rows:
        print("LABEL 테이블에 기준일 데이터가 없습니다.")
        return

    if isinstance(cal_rows[0], dict):
        label_cal = pd.DataFrame(cal_rows)
    else:
        label_cal = pd.DataFrame(cal_rows, columns=["asof_date"])

    label_cal["asof_date"] = pd.to_datetime(label_cal["asof_date"]).dt.normalize()
    unique_asof = sorted(label_cal["asof_date"].drop_duplicates().tolist())

    if len(unique_asof) < 2:
        print("기준일이 2개 미만이라 alpha 계산이 불가능합니다.")
        return

    # -------------------------------------------------
    # 2) asof_date -> ref_trade_date 매핑 캘린더 생성
    # -------------------------------------------------
    ref_cal = build_month_end_reference_calendar(start)
    ref_cal["asof_date"] = pd.to_datetime(ref_cal["asof_date"]).dt.normalize()
    ref_cal["ref_trade_date"] = pd.to_datetime(ref_cal["ref_trade_date"]).dt.normalize()

    ref_map_df = pd.DataFrame({"asof_date": unique_asof})
    ref_map_df = ref_map_df.merge(ref_cal, on="asof_date", how="left")

    if ref_map_df["ref_trade_date"].isna().any():
        missing = ref_map_df.loc[ref_map_df["ref_trade_date"].isna(), "asof_date"].dt.date.tolist()
        raise ValueError(f"ref_trade_date 매핑 실패 asof_date: {missing[:10]}")

    # next_asof_date 매핑
    date_map = {unique_asof[i]: unique_asof[i + 1] for i in range(len(unique_asof) - 1)}

    # -------------------------------------------------
    # 3) 아직 alpha가 없는 대상만 조회
    # -------------------------------------------------
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT stock_code, asof_date
            FROM LABEL
            WHERE alpha IS NULL
            ORDER BY stock_code, asof_date
        """)
        rows = cursor.fetchall()

    if not rows:
        print("이미 alpha가 전부 채워져 있습니다. 새로 계산할 row가 없습니다.")
        return

    if isinstance(rows[0], dict):
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(rows, columns=["stock_code", "asof_date"])

    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["asof_date"] = pd.to_datetime(df["asof_date"]).dt.normalize()

    print("alpha 미계산 대상 행 수:", len(df))
    print("alpha 미계산 대상 종목 수:", df["stock_code"].nunique())
    print("alpha 미계산 대상 기준일 수:", df["asof_date"].nunique())

    # -------------------------------------------------
    # 4) next_asof_date / ref_trade_date / next_ref_trade_date 매핑
    # -------------------------------------------------
    df["next_asof_date"] = df["asof_date"].map(date_map)

    df = df.merge(
        ref_map_df.rename(columns={"ref_trade_date": "ref_trade_date"}),
        on="asof_date",
        how="left"
    )

    next_ref_map_df = ref_map_df.rename(columns={
        "asof_date": "next_asof_date",
        "ref_trade_date": "next_ref_trade_date"
    })

    df = df.merge(
        next_ref_map_df,
        on="next_asof_date",
        how="left"
    )

    print("next_asof_date 없는 행 수(마지막 월말):", df["next_asof_date"].isna().sum())

    # -------------------------------------------------
    # 5) '다음 월말이 아직 안 닫힌' row 제외
    # -------------------------------------------------
    # 예: 오늘이 2026-03-23이면 next_asof_date=2026-03-31 은 아직 미래이므로 계산 제외
    before_filter = len(df)
    df = df[df["next_asof_date"].notna()].copy()
    df = df[df["next_asof_date"].dt.date <= today_date].copy()

    print("다음 월말 미도래로 제외된 행 수:", before_filter - len(df))

    if df.empty:
        print("오늘 기준 계산 가능한 alpha가 없습니다. 마지막 월말은 NULL 유지됩니다.")
        return

    # -------------------------------------------------
    # 6) 벤치마크 수익률 계산 (ref_trade_date 기준)
    # -------------------------------------------------
    start_idx = df["ref_trade_date"].min().strftime("%Y%m%d")
    end_idx = df["next_ref_trade_date"].max().strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start_idx, end_idx, "2001", name_display=False)
    if idx is None or idx.empty:
        raise ValueError("KOSDAQ 지수 데이터를 가져오지 못했습니다.")

    idx.index = pd.to_datetime(idx.index)
    idx_close_map = idx["종가"].to_dict()

    df["bench_start"] = df["ref_trade_date"].map(idx_close_map)
    df["bench_end"] = df["next_ref_trade_date"].map(idx_close_map)
    df["bench_ret"] = (df["bench_end"] / df["bench_start"]) - 1

    print("벤치마크 수익률 계산 가능 행 수:", df["bench_ret"].notna().sum())

    # -------------------------------------------------
    # 7) 필요한 ref_trade_date 가격만 조회
    # -------------------------------------------------
    needed_dates = sorted(set(df["ref_trade_date"]).union(set(df["next_ref_trade_date"])))
    price_all = get_kosdaq_prices_by_dates(
        needed_dates,
        sleep_sec=sleep_sec,
        max_retry=max_retry
    )

    if price_all.empty:
        print("수집된 가격 데이터가 없습니다.")
        return

    # 시작일 종가
    start_price = price_all.rename(columns={
        "date": "ref_trade_date",
        "close": "stock_start"
    })

    # 종료일 종가
    end_price = price_all.rename(columns={
        "date": "next_ref_trade_date",
        "close": "stock_end"
    })

    df = df.merge(
        start_price,
        on=["stock_code", "ref_trade_date"],
        how="left"
    )

    df = df.merge(
        end_price,
        on=["stock_code", "next_ref_trade_date"],
        how="left"
    )

    before_drop = len(df)
    df = df.dropna(subset=["stock_start", "stock_end", "bench_ret"]).copy()

    print("가격/벤치마크 매핑 후 계산 가능 행 수:", len(df))
    print("제외된 행 수:", before_drop - len(df))

    if df.empty:
        print("계산 가능한 alpha가 없습니다.")
        return

    # -------------------------------------------------
    # 8) alpha 계산
    # -------------------------------------------------
    df["stock_ret"] = (df["stock_end"] / df["stock_start"]) - 1
    df["alpha"] = df["stock_ret"] - df["bench_ret"]

    df_alpha = df[["stock_code", "asof_date", "alpha"]].copy()
    df_alpha["asof_date"] = pd.to_datetime(df_alpha["asof_date"]).dt.date
    df_alpha["alpha"] = pd.to_numeric(df_alpha["alpha"], errors="coerce")

    print("계산된 alpha 행 수:", len(df_alpha))
    print("alpha sample:")
    print(df_alpha.head())

    # -------------------------------------------------
    # 9) LABEL 업데이트
    # -------------------------------------------------
    update_sql = """
        UPDATE LABEL
        SET alpha = %s
        WHERE stock_code = %s
          AND asof_date = %s
          AND alpha IS NULL
    """

    data = [
        (r.alpha, r.stock_code, r.asof_date)
        for r in df_alpha.itertuples(index=False)
    ]

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
# 8) sanity check
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

        cursor.execute("""
            SELECT MIN(asof_date) AS min_date, MAX(asof_date) AS max_date
            FROM LABEL
        """)
        minmax = cursor.fetchone()

    if isinstance(total_cnt, dict):
        print("LABEL 전체 행 수:", total_cnt["cnt"])
        print("LABEL alpha 존재 행 수:", alpha_cnt["cnt"])
        print("LABEL alpha NULL 행 수:", alpha_null_cnt["cnt"])
        print("LABEL 종목 수:", stock_cnt["cnt"])
        print("LABEL 기준일 수:", date_cnt["cnt"])
        print("LABEL 최소/최대 기준일:", minmax["min_date"], "/", minmax["max_date"])
    else:
        print("LABEL 전체 행 수:", total_cnt[0])
        print("LABEL alpha 존재 행 수:", alpha_cnt[0])
        print("LABEL alpha NULL 행 수:", alpha_null_cnt[0])
        print("LABEL 종목 수:", stock_cnt[0])
        print("LABEL 기준일 수:", date_cnt[0])
        print("LABEL 최소/최대 기준일:", minmax[0], "/", minmax[1])


# -------------------------------------------------
# 9) MAIN
# -------------------------------------------------
def main():
    load_dotenv()

    krx_id = os.getenv("ID")
    krx_pw = os.getenv("PW")

    if not krx_id or not krx_pw:
        raise ValueError("환경변수 ID/PW가 비어있습니다. .env 확인하세요.")

    if login_krx(krx_id, krx_pw):
        print("KRX 로그인 성공!")
    else:
        raise RuntimeError("KRX 로그인 실패")

    conn = get_connection()
    try:
        create_label(conn)

        label_base = build_label_base_easy(conn, start="20151001")
        if label_base.empty:
            raise ValueError("label_base가 비었습니다. FEATURE_BASIC / 월말 캘린더 로직 확인")

        upload_to_label(conn, label_base)

        # alpha가 NULL인 것만 계산
        fill_alpha_to_label(conn, start="20151001", sleep_sec=0.3, max_retry=3)

        # sanity check
        sanity_check_alpha(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()