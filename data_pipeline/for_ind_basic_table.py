import sys
import pandas as pd
from pathlib import Path

temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path:
    sys.path.append(str(temp_base))

from common.setting import get_connection, BASE_DIR


def create_ind_basic(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS IND_BASIC (
                    ind_code VARCHAR(10) PRIMARY KEY,
                    ind_name VARCHAR(60),
                    ind_def TEXT,
                    bok_code VARCHAR(10)
                );
            """)
        conn.commit()
        print("IND_BASIC 테이블이 성공적으로 생성되었거나 이미 존재합니다.")
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) IND_BASIC 테이블 생성 중 오류 발생: {e}")


def create_ind_bok(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS IND_BOK (
                    bok_code VARCHAR(10),
                    year INT,
                    asset_growth_rate DECIMAL(10,2),
                    sales_growth_rate DECIMAL(10,2),
                    PRIMARY KEY (bok_code, year)
                );
            """)
        conn.commit()
        print("IND_BOK 테이블이 성공적으로 생성되었거나 이미 존재합니다.")
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) IND_BOK 테이블 생성 중 오류 발생: {e}")


def load_ind_basic(conn):
    """
    ind_basic_filled.csv 를 IND_BASIC에 적재
    """
    try:
        file_path = BASE_DIR / "data" / "ind_basic_filled.csv"
        df = pd.read_csv(file_path, encoding="utf-8-sig")

        df = df[["ind_code", "ind_name", "ind_def", "bok_code"]].copy()

        for col in ["ind_code", "ind_name", "ind_def", "bok_code"]:
            df[col] = df[col].astype("string").str.strip()
        
        df = df.where(pd.notna(df), None)
        df = df[df["ind_code"].notna()]
        df = df[df["ind_code"] != ""]
        df = df.drop_duplicates(subset=["ind_code"])

        rows = []
        for _, row in df.iterrows():
            rows.append((
                None if pd.isna(row["ind_code"]) else str(row["ind_code"]).strip(),
                None if pd.isna(row["ind_name"]) else str(row["ind_name"]).strip(),
                None if pd.isna(row["ind_def"]) else str(row["ind_def"]).strip(),
                None if pd.isna(row["bok_code"]) else str(row["bok_code"]).strip()
            ))

        with conn.cursor() as cursor:
            sql = """
                INSERT INTO IND_BASIC (
                    ind_code, ind_name, ind_def, bok_code
                )
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ind_name = VALUES(ind_name),
                    ind_def = VALUES(ind_def),
                    bok_code = VALUES(bok_code)
            """
            cursor.executemany(sql, rows)

        conn.commit()
        print(f"IND_BASIC 적재 완료: {len(rows)}건")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) IND_BASIC 적재 중 오류 발생: {e}")

def transform_ind_bok_raw(year: int):
    """
    rate_ind.csv -> 특정 연도의 원본 pivot 결과 생성

    결과 컬럼:
    bok_code, year, asset_growth_rate, sales_growth_rate
    """
    file_path = BASE_DIR / "data" / "rate_ind.csv"
    df = pd.read_csv(file_path, encoding="utf-8-sig")

    year_col = str(year)

    if year_col not in df.columns:
        raise ValueError(f"rate_ind.csv에 {year_col} 컬럼이 없습니다.")

    df = df[
        (df["기업규모"] == "종합") &
        (df["계정항목"].isin(["총자산증가율", "매출액증가율"]))
    ].copy()

    df = df[["코드(업종코드)", "계정항목", year_col]].copy()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

    pivot_df = df.pivot(
        index="코드(업종코드)",
        columns="계정항목",
        values=year_col
    ).reset_index()

    pivot_df = pivot_df.rename(columns={
        "코드(업종코드)": "bok_code",
        "총자산증가율": "asset_growth_rate",
        "매출액증가율": "sales_growth_rate"
    })

    pivot_df["bok_code"] = pivot_df["bok_code"].astype("string").str.strip()
    pivot_df["year"] = year

    pivot_df = pivot_df[
        ["bok_code", "year", "asset_growth_rate", "sales_growth_rate"]
    ]

    return pivot_df


def transform_ind_bok_filled(year: int):
    """
    ind_basic_filled.csv의 bok_code를 기준으로
    특정 연도의 원본 pivot 결과를 확장 매핑한 최종 결과 생성

    규칙:
    1) 정확히 같은 bok_code 우선
    2) 없으면 첫 알파벳만 같은 raw bok_code 사용
    """
    master_path = BASE_DIR / "data" / "ind_basic_filled.csv"

    master_df = pd.read_csv(master_path, encoding="utf-8-sig")
    master_df = master_df[["bok_code"]].copy()
    master_df["bok_code"] = master_df["bok_code"].astype("string").str.strip()
    master_df = master_df[master_df["bok_code"].notna()]
    master_df = master_df[master_df["bok_code"] != ""]
    master_df = master_df.drop_duplicates(subset=["bok_code"])

    raw_df = transform_ind_bok_raw(year)

    exact_map = {}
    for _, row in raw_df.iterrows():
        exact_map[row["bok_code"]] = {
            "asset_growth_rate": row["asset_growth_rate"],
            "sales_growth_rate": row["sales_growth_rate"]
        }

    alpha_map = {}
    alpha_rows = raw_df[raw_df["bok_code"].str.len() == 1].copy()

    for _, row in alpha_rows.iterrows():
        alpha_map[row["bok_code"]] = {
            "asset_growth_rate": row["asset_growth_rate"],
            "sales_growth_rate": row["sales_growth_rate"]
        }

    result = []

    for _, row in master_df.iterrows():
        master_code = row["bok_code"]
        alpha = master_code[0]

        asset_value = None
        sales_value = None
        matched_by = None

        if master_code in exact_map:
            asset_value = exact_map[master_code]["asset_growth_rate"]
            sales_value = exact_map[master_code]["sales_growth_rate"]
            matched_by = "exact"
        elif alpha in alpha_map:
            asset_value = alpha_map[alpha]["asset_growth_rate"]
            sales_value = alpha_map[alpha]["sales_growth_rate"]
            matched_by = "alpha"

        result.append({
            "bok_code": master_code,
            "year": year,
            "asset_growth_rate": asset_value,
            "sales_growth_rate": sales_value,
            "matched_by": matched_by
        })

    if "ZZZ00" in exact_map:
        result.append({
            "bok_code": "ZZZ00",
            "year": year,
            "asset_growth_rate": exact_map["ZZZ00"]["asset_growth_rate"],
            "sales_growth_rate": exact_map["ZZZ00"]["sales_growth_rate"],
            "matched_by": "exact"
        })

    result_df = pd.DataFrame(result)
    return result_df


def load_ind_bok(conn, year: int):
    """
    특정 연도의 최종 확장 결과를 IND_BOK에 적재
    """
    try:
        result_df = transform_ind_bok_filled(year)

        rows = []
        for _, row in result_df.iterrows():
            rows.append((
                str(row["bok_code"]).strip(),
                int(row["year"]),
                None if pd.isna(row["asset_growth_rate"]) else float(row["asset_growth_rate"]),
                None if pd.isna(row["sales_growth_rate"]) else float(row["sales_growth_rate"])
            ))

        with conn.cursor() as cursor:
            sql = """
                INSERT INTO IND_BOK (
                    bok_code, year, asset_growth_rate, sales_growth_rate
                )
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    asset_growth_rate = VALUES(asset_growth_rate),
                    sales_growth_rate = VALUES(sales_growth_rate)
            """
            cursor.executemany(sql, rows)

        conn.commit()
        print(f"IND_BOK {year} 적재 완료: {len(rows)}건")

    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) IND_BOK {year} 적재 중 오류 발생: {e}")


def load_ind_bok_all(conn, years=(2022, 2023, 2024)):
    """
    여러 연도를 한 번에 적재
    """
    for year in years:
        load_ind_bok(conn, year)


if __name__ == "__main__":
    conn = get_connection()

    create_ind_basic(conn)
    create_ind_bok(conn)

    load_ind_basic(conn)
    load_ind_bok_all(conn)

    conn.close()