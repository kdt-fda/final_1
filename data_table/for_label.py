from pykrx import stock
import os
from dotenv import load_dotenv
import pymysql
from datetime import datetime
import pandas as pd
from pathlib import Path
import sys

# 경로 잡기
temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:  # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

from common.setting import login_krx, get_connection


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
# 2) 월별 "마지막 거래일" 리스트 생성 (KOSDAQ index 기반)
# -------------------------------------------------
def get_month_end_trading_dates(start: str) -> pd.DatetimeIndex:
    today = datetime.today().strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, today, "2001", name_display=False)
    if idx is None or idx.empty:
        raise ValueError("KOSDAQ 지수 데이터가 비어 있습니다.")

    idx.index = pd.to_datetime(idx.index)

    # 월별 마지막 "실제 거래일" (토/일/휴장일 방지)
    month_end = (
        idx.groupby(idx.index.to_period("M"))
           .apply(lambda x: x.index.max())
    )

    month_end_dates = pd.DatetimeIndex(pd.to_datetime(month_end.values))

    print(f"월말 실제 거래일 개수: {len(month_end_dates)}")
    if len(month_end_dates) > 0:
        print("첫 월말:", month_end_dates.min().date(), "/ 마지막 월말:", month_end_dates.max().date())

    return month_end_dates


# -------------------------------------------------
# 3) label_base 생성: FEATURE_BASIC 기준
# -------------------------------------------------
def build_label_base_easy(conn, start: str) -> pd.DataFrame:
    """
    FEATURE_BASIC 기준으로 label base 생성
    - 월말 실제 거래일만 사용
    - 해당 날짜에 상장되어 있던 종목(is_listed_on_date = 1)만 포함
    """
    dates = get_month_end_trading_dates(start)
    month_end_dates = [d.date() for d in dates]

    if not month_end_dates:
        raise ValueError("월말 거래일 리스트가 비었습니다.")

    placeholders = ",".join(["%s"] * len(month_end_dates))

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT stock_code, date AS asof_date
            FROM FEATURE_BASIC
            WHERE is_listed_on_date = 1
              AND date IN ({placeholders})
        """, month_end_dates)
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_code", "asof_date", "alpha"])

    if isinstance(rows[0], dict):
        label = pd.DataFrame(rows)
    else:
        label = pd.DataFrame(rows, columns=["stock_code", "asof_date"])

    label["stock_code"] = label["stock_code"].astype(str).str.strip().str.zfill(6)
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
# 4) LABEL 업서트
# -------------------------------------------------
def upload_to_label(conn, df: pd.DataFrame, chunk_size: int = 10000):
    """
    LABEL(stock_code, asof_date) UNIQUE 기준으로 UPSERT
    """
    sql = """
        INSERT INTO LABEL (stock_code, asof_date, alpha)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            alpha = VALUES(alpha);
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
                print(f"업서트 진행: {min(i + chunk_size, len(data)):,} / {len(data):,}")

        conn.commit()
        print(f"✅ LABEL upsert done: {len(data):,} rows")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) LABEL 업로드 중 오류 발생: {e}")
        raise


# -------------------------------------------------
# 5) alpha 실제값 채우기
# -------------------------------------------------
def fill_alpha_to_label(conn):
    """
    LABEL.alpha를 실제 미래 1개월 alpha로 업데이트
    alpha = 개별종목 1개월 미래수익률 - KOSDAQ 1개월 미래수익률

    기준:
    - asof_date: 월말 마지막 거래일
    - next_asof_date: 다음 월말 마지막 거래일
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT stock_code, asof_date
            FROM LABEL
            ORDER BY stock_code, asof_date
        """)
        rows = cursor.fetchall()

    if not rows:
        print("LABEL 테이블에 데이터가 없습니다.")
        return

    if isinstance(rows[0], dict):
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(rows, columns=["stock_code", "asof_date"])

    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["asof_date"] = pd.to_datetime(df["asof_date"])

    print("LABEL 전체 행 수:", len(df))
    print("LABEL 종목 수:", df["stock_code"].nunique())
    print("LABEL 기준일 수:", df["asof_date"].nunique())

    # 월말 기준일 -> 다음 월말 기준일 매핑
    unique_dates = sorted(df["asof_date"].drop_duplicates().tolist())
    date_map = {unique_dates[i]: unique_dates[i + 1] for i in range(len(unique_dates) - 1)}
    df["next_asof_date"] = df["asof_date"].map(date_map)

    print("next_asof_date 없는 행 수(마지막 월말):", df["next_asof_date"].isna().sum())

    # 마지막 월말은 다음 월말이 없으므로 제외
    df = df.dropna(subset=["next_asof_date"]).copy()

    if df.empty:
        print("다음 월말 기준일이 없어 alpha를 계산할 수 없습니다.")
        return

    # KOSDAQ 벤치마크 수익률 계산용 지수 데이터
    start = min(df["asof_date"]).strftime("%Y%m%d")
    end = max(df["next_asof_date"]).strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, end, "2001", name_display=False)
    if idx is None or idx.empty:
        raise ValueError("KOSDAQ 지수 데이터를 가져오지 못했습니다.")

    idx.index = pd.to_datetime(idx.index)
    idx_close_map = idx["종가"].to_dict()

    df["bench_start"] = df["asof_date"].map(idx_close_map)
    df["bench_end"] = df["next_asof_date"].map(idx_close_map)
    df["bench_ret"] = (df["bench_end"] / df["bench_start"]) - 1

    print("벤치마크 수익률 계산 가능 행 수:", df["bench_ret"].notna().sum())

    result_rows = []
    stock_groups = list(df.groupby("stock_code"))
    total_stocks = len(stock_groups)

    for idx_num, (stock_code, g) in enumerate(stock_groups, start=1):
        g = g.sort_values("asof_date").copy()

        if idx_num % 100 == 0 or idx_num == 1 or idx_num == total_stocks:
            print(f"[{idx_num}/{total_stocks}] 종목 처리 중: {stock_code}")

        try:
            price = stock.get_market_ohlcv_by_date(
                fromdate=g["asof_date"].min().strftime("%Y%m%d"),
                todate=g["next_asof_date"].max().strftime("%Y%m%d"),
                ticker=stock_code
            )
        except Exception as e:
            print(f"[건너뜀] {stock_code} 가격 조회 실패: {e}")
            continue

        if price is None or price.empty:
            continue

        price.index = pd.to_datetime(price.index)
        close_map = price["종가"].to_dict()

        g["stock_start"] = g["asof_date"].map(close_map)
        g["stock_end"] = g["next_asof_date"].map(close_map)

        g = g.dropna(subset=["stock_start", "stock_end", "bench_ret"]).copy()
        if g.empty:
            continue

        g["stock_ret"] = (g["stock_end"] / g["stock_start"]) - 1
        g["alpha"] = g["stock_ret"] - g["bench_ret"]

        result_rows.append(g[["stock_code", "asof_date", "alpha"]])

    if not result_rows:
        print("계산된 alpha가 없습니다.")
        return

    df_alpha = pd.concat(result_rows, ignore_index=True)
    df_alpha["asof_date"] = pd.to_datetime(df_alpha["asof_date"]).dt.date
    df_alpha["alpha"] = df_alpha["alpha"].astype(float)

    print("계산된 alpha 행 수:", len(df_alpha))
    print("alpha sample:")
    print(df_alpha.head())

    update_sql = """
        UPDATE LABEL
        SET alpha = %s
        WHERE stock_code = %s
          AND asof_date = %s
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
# 6) sanity check
# -------------------------------------------------
def sanity_check_alpha(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM LABEL")
        total_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) AS cnt FROM LABEL WHERE alpha IS NOT NULL")
        alpha_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(DISTINCT stock_code) AS cnt FROM LABEL")
        stock_cnt = cursor.fetchone()

        cursor.execute("SELECT COUNT(DISTINCT asof_date) AS cnt FROM LABEL")
        date_cnt = cursor.fetchone()

    if isinstance(total_cnt, dict):
        print("LABEL 전체 행 수:", total_cnt["cnt"])
        print("LABEL alpha 존재 행 수:", alpha_cnt["cnt"])
        print("LABEL 종목 수:", stock_cnt["cnt"])
        print("LABEL 기준일 수:", date_cnt["cnt"])
    else:
        print("LABEL 전체 행 수:", total_cnt[0])
        print("LABEL alpha 존재 행 수:", alpha_cnt[0])
        print("LABEL 종목 수:", stock_cnt[0])
        print("LABEL 기준일 수:", date_cnt[0])


# -------------------------------------------------
# 7) MAIN
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
            raise ValueError("label_base가 비었습니다. FEATURE_BASIC / 월말거래일 로직 확인")

        upload_to_label(conn, label_base)

        # alpha 실제값 채우기
        fill_alpha_to_label(conn)

        # sanity check
        sanity_check_alpha(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()