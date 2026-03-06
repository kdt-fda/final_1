import sys
import pandas as pd
from pathlib import Path

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
        UNIQUE (ind_code, io_code)
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

def load_ind_io(conn):
    """
    io_rawdata.csv -> IND_IO 적재 (2023년 데이터만, 산업코드 1~83만)
    """

    file_path = BASE_DIR / "data" / "io_rawdata.csv"

    df = pd.read_csv(
        file_path,
        encoding="utf-8-sig",
        dtype={
            "코드(수요부문(열))": "string",
            "코드(투입부문(행))": "string",
        }
    )

    # 필요한 컬럼만
    df = df[
        [
            "코드(수요부문(열))",
            "코드(투입부문(행))",
            "2023"
        ]
    ].copy()

    # 컬럼 이름 변경
    df = df.rename(
        columns={
            "코드(수요부문(열))": "out_io_code",
            "코드(투입부문(행))": "in_io_code",
            "2023": "trade_vol"
        }
    )

    # 문자열 정리
    df["out_io_code"] = df["out_io_code"].str.strip()
    df["in_io_code"] = df["in_io_code"].str.strip()

    # 빈값 제거
    df = df.dropna(subset=["out_io_code", "in_io_code", "trade_vol"])
    df = df[
        (df["out_io_code"] != "") &
        (df["in_io_code"] != "")
    ].copy()

    # 숫자 코드만 남기기
    df["out_io_code_num"] = pd.to_numeric(df["out_io_code"], errors="coerce")
    df["in_io_code_num"] = pd.to_numeric(df["in_io_code"], errors="coerce")

    df = df.dropna(subset=["out_io_code_num", "in_io_code_num"]).copy()

    # 산업코드 1~83만 남기기
    df = df[
        (df["out_io_code_num"] >= 1) & (df["out_io_code_num"] <= 83) &
        (df["in_io_code_num"] >= 1) & (df["in_io_code_num"] <= 83)
    ].copy()

    # DB에는 문자열 코드로 넣기
    df["out_io_code"] = df["out_io_code_num"].astype(int).astype(str)
    df["in_io_code"] = df["in_io_code_num"].astype(int).astype(str)

    # trade_vol 숫자 변환
    df["trade_vol"] = pd.to_numeric(df["trade_vol"], errors="coerce")

    # 결측 제거
    df = df.dropna(subset=["trade_vol"]).copy()

    # 연도 추가
    df["year"] = 2023

    # 소수점 정리
    df["trade_vol"] = df["trade_vol"].round(2)

    # 최종 컬럼만
    df = df[["trade_vol", "year", "out_io_code", "in_io_code"]]

    # 중복 제거
    df = df.drop_duplicates(subset=["year", "out_io_code", "in_io_code"])

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

    print(f"IND_IO 2023 데이터 적재 완료: {len(rows)}건")


if __name__ == "__main__":

    conn = None

    try:
        conn = get_connection()

        # 1️⃣ 테이블 생성
        create_bok_io_table(conn)
        create_ind_io_table(conn)
        
        # 2️⃣ 데이터 적재
        load_bok_io(conn)
        load_ind_io(conn)


    finally:
        if conn:
            conn.close()