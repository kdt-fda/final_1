import sys
import pandas as pd
from pathlib import Path
import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ECOS_API_KEY")

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

# 공통 설정
from common.setting import get_connection, BASE_DIR


def create_bok_io_table(conn):
    """
    BOK_IO 테이블 생성
    """
    sql = """
    CREATE TABLE IF NOT EXISTS BOK_IO (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ind_code VARCHAR(10) NOT NULL,
        io_code VARCHAR(10) NOT NULL,
        io_name VARCHAR(100) NOT NULL,

        UNIQUE (ind_code, io_code),

        CONSTRAINT fk_bok_io_ind
            FOREIGN KEY (ind_code)
            REFERENCES IND_BASIC(ind_code)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    print("BOK_IO 테이블 확인/생성 완료")

def create_ind_io_table(conn):
    """
    IND_IO 테이블 생성
    """
    sql = """
    CREATE TABLE IF NOT EXISTS IND_IO (
        link_id INT AUTO_INCREMENT PRIMARY KEY,
        trade_vol DECIMAL(18,2) NOT NULL,
        year INT NOT NULL,
        out_io_code VARCHAR(10) NOT NULL,
        in_io_code VARCHAR(10) NOT NULL,
        UNIQUE (year, out_io_code, in_io_code)
    );
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    print("IND_IO 테이블 확인/생성 완료")


def load_bok_io(conn):
    """
    bok_io_map.csv → BOK_IO 적재
    """

    file_path = BASE_DIR / "data" / "bok_io_map.csv"

    df = pd.read_csv(
        file_path,
        encoding="utf-8-sig",
        dtype={
            "ind_code": "string",
            "io_code": "string",
            "io_name": "string",
        }
    )

    df = df[["ind_code", "io_code", "io_name"]].copy()

    # 문자열 정리
    for col in ["ind_code", "io_code", "io_name"]:
        df[col] = df[col].str.strip()

    # 결측 제거
    df = df.dropna(subset=["ind_code", "io_code", "io_name"])

    df = df[
        (df["ind_code"] != "") &
        (df["io_code"] != "") &
        (df["io_name"] != "")
    ]

    # 중복 제거
    df = df.drop_duplicates(subset=["ind_code", "io_code"])

    rows = list(
        df[["ind_code", "io_code", "io_name"]].itertuples(index=False, name=None)
    )

    sql = """
    INSERT INTO BOK_IO (ind_code, io_code, io_name)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        io_name = VALUES(io_name)
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    conn.commit()

    print(f"BOK_IO 데이터 적재 완료: {len(rows)}건")




def get_latest_ecos_year(stat_code: str = "271Y120") -> int:
    """
    StatisticItemList에서 최신 제공 연도 1개 추출
    """
    url = f"https://ecos.bok.or.kr/api/StatisticItemList/{API_KEY}/json/kr/1/500/{stat_code}"
    res = requests.get(url, timeout=30)
    res.raise_for_status()

    data = res.json()
    body = data.get("StatisticItemList") or data.get("root", {}).get("StatisticItemList")

    if not body or "row" not in body:
        raise ValueError(f"StatisticItemList 응답 구조 이상: {data}")

    df = pd.DataFrame(body["row"])
    df["END_TIME_NUM"] = pd.to_numeric(df["END_TIME"], errors="coerce")

    latest_year = df["END_TIME_NUM"].max()
    if pd.isna(latest_year):
        raise ValueError("최신 연도를 찾을 수 없습니다.")

    return int(latest_year)


def has_ind_io_year(conn, year: int) -> bool:
    """
    IND_IO에 해당 연도 데이터가 이미 존재하는지 확인
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM IND_IO
            WHERE year = %s
            LIMIT 1
            """,
            (year,)
        )
        return cur.fetchone() is not None


def fetch_ind_io_latest_df(
    stat_code: str = "271Y120",
    latest_year: int | None = None
) -> pd.DataFrame:
    """
    ECOS API에서 최신 연도 1개만 전체 조회해서
    IND_IO 적재용 DataFrame 반환
    반환 컬럼:
    trade_vol, year, out_io_code, in_io_code
    """
    if latest_year is None:
        latest_year = get_latest_ecos_year(stat_code)
        print(f"ECOS 최신 제공 연도: {latest_year}")

    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch/{API_KEY}/json/kr/1/10000/"
        f"{stat_code}/A/{latest_year}/{latest_year}"
    )

    res = requests.get(url, timeout=60)
    res.raise_for_status()

    data = res.json()
    body = data.get("StatisticSearch") or data.get("root", {}).get("StatisticSearch")

    if not body or "row" not in body:
        raise ValueError(f"StatisticSearch 응답 구조 이상: {data}")

    df = pd.DataFrame(body["row"])

    if df.empty:
        raise ValueError(f"{latest_year}년 데이터가 비어 있습니다.")

    # 적재용 컬럼으로 변경
    df = df.rename(columns={
        "ITEM_CODE1": "out_io_code",
        "ITEM_CODE2": "in_io_code",
        "TIME": "year",
        "DATA_VALUE": "trade_vol"
    })[["trade_vol", "year", "out_io_code", "in_io_code"]].copy()

    # 문자열 정리
    df["out_io_code"] = df["out_io_code"].astype("string").str.strip()
    df["in_io_code"] = df["in_io_code"].astype("string").str.strip()

    # 숫자 변환
    df["trade_vol"] = pd.to_numeric(df["trade_vol"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["out_io_code_num"] = pd.to_numeric(df["out_io_code"], errors="coerce")
    df["in_io_code_num"] = pd.to_numeric(df["in_io_code"], errors="coerce")

    # null 제거
    df = df.dropna(subset=["trade_vol", "year", "out_io_code_num", "in_io_code_num"]).copy()

    # 산업코드 1~83만
    df = df[
        (df["out_io_code_num"] >= 1) & (df["out_io_code_num"] <= 83) &
        (df["in_io_code_num"] >= 1) & (df["in_io_code_num"] <= 83)
    ].copy()

    # 코드 형식 통일: "01" -> "1"
    df["out_io_code"] = df["out_io_code_num"].astype(int).astype(str)
    df["in_io_code"] = df["in_io_code_num"].astype(int).astype(str)
    df["year"] = df["year"].astype(int)
    df["trade_vol"] = df["trade_vol"].round(2)

    # 최종 컬럼
    df = df[["trade_vol", "year", "out_io_code", "in_io_code"]]

    # 중복 제거
    df = df.drop_duplicates(subset=["year", "out_io_code", "in_io_code"])

    return df


def load_ind_io_latest(conn):
    """
    ECOS API 최신 연도 1개 조회 -> IND_IO 적재
    """
    latest_year = get_latest_ecos_year()
    print(f"ECOS 최신 제공 연도: {latest_year}")

    if has_ind_io_year(conn, latest_year):
        print(f"IND_IO {latest_year}년 데이터가 이미 존재하여 적재를 건너뜁니다.")
        return

    df = fetch_ind_io_latest_df(latest_year=latest_year)

    if df.empty:
        print("적재할 데이터가 없습니다.")
        return

    loaded_year = int(df["year"].iloc[0])

    rows = list(
        df[["trade_vol", "year", "out_io_code", "in_io_code"]]
        .itertuples(index=False, name=None)
    )

    sql = """
    INSERT INTO IND_IO (trade_vol, year, out_io_code, in_io_code)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        trade_vol = VALUES(trade_vol)
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    conn.commit()

    print(f"IND_IO 최신 연도({loaded_year}) 데이터 적재 완료: {len(rows)}건")


if __name__ == "__main__":

    conn = None

    try:
        conn = get_connection()

        # 1️⃣ 테이블 생성
        create_bok_io_table(conn)
        create_ind_io_table(conn)
        
        # 2️⃣ 데이터 적재
        load_bok_io(conn)
        load_ind_io_latest(conn)


    finally:
        if conn:
            conn.close()
