from pykrx import stock
import os
from dotenv import load_dotenv
import pymysql
from datetime import datetime
import pandas as pd
from pathlib import Path
import sys

temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

from common.setting import login_krx, get_connection


# -----------------------------
# MARKET_INDEX 테이블 생성
# -----------------------------
def create_market_index(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS MARKET_INDEX (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    KOSPI DECIMAL(18,6),
                    KOSDAQ DECIMAL(18,6),
                    UNIQUE KEY uk_market_index_date (date)
                );
            """)
        print("market_index 테이블이 성공적으로 생성되었거나 이미 존재합니다.")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
        raise


# -----------------------------
# 오늘 제외 직전 거래일 구하기
# -----------------------------
def get_prev_trading_date_excluding_today(lookback_days: int = 15) -> str:
    today = datetime.today().date()
    start = (pd.Timestamp(today) - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, end, "2001", name_display=False)

    if idx is None or idx.empty:
        raise ValueError(f"거래일 조회 실패: start={start}, end={end}")

    idx.index = pd.to_datetime(idx.index)
    trading_dates = sorted(idx.index.date)

    # 오늘 날짜 제외
    trading_dates = [d for d in trading_dates if d < today]

    if not trading_dates:
        raise ValueError("오늘을 제외한 직전 거래일을 찾지 못했습니다.")

    prev_date = max(trading_dates).strftime("%Y%m%d")
    return prev_date


# -----------------------------
# MARKET INDEX 데이터 가져오기
# -----------------------------
def fetch_market_index(start: str):
    end_date = get_prev_trading_date_excluding_today()
    print(f"오늘 제외 직전 거래일 기준으로 수집합니다: {end_date}")

    kospi = stock.get_index_ohlcv_by_date(start, end_date, "1001", name_display=False)[["종가"]]
    kosdaq = stock.get_index_ohlcv_by_date(start, end_date, "2001", name_display=False)[["종가"]]

    kospi = kospi.rename(columns={"종가": "KOSPI"})
    kosdaq = kosdaq.rename(columns={"종가": "KOSDAQ"})

    df = kospi.join(kosdaq, how="outer")

    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    for c in ["KOSPI", "KOSDAQ"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(6)

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    return df


# -----------------------------
# MARKET INDEX 업서트
# -----------------------------
def upload_to_market_index(df: pd.DataFrame):
    conn = get_connection()

    try:
        create_market_index(conn)

        sql = """
            INSERT INTO MARKET_INDEX (date, KOSPI, KOSDAQ)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            KOSPI = VALUES(KOSPI),
            KOSDAQ = VALUES(KOSDAQ);
        """

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date

        data = [(r.date, r.KOSPI, r.KOSDAQ) for r in df.itertuples(index=False)]

        with conn.cursor() as cursor:
            cursor.executemany(sql, data)

        conn.commit()
        print(f"✅ MARKET_INDEX upsert done: {len(data)} rows")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
        raise

    finally:
        conn.close()


# -----------------------------
# MAIN
# -----------------------------
def main():
    load_dotenv()

    krx_id = os.getenv("ID")
    krx_pw = os.getenv("PW")

    if not krx_id or not krx_pw:
        raise ValueError("환경변수 ID/PW가 없습니다. .env 확인하세요.")

    if login_krx(krx_id, krx_pw):
        print("KRX 로그인 성공!")
    else:
        raise RuntimeError("KRX 로그인 실패")

    df = fetch_market_index("20151001")

    if df is None or df.empty:
        raise ValueError("fetch_market_index 결과가 비었습니다. KRX 로그인/날짜/티커 확인")

    upload_to_market_index(df)


if __name__ == "__main__":
    main()