import sys
import pandas as pd
from pathlib import Path

# 루트 경로를 추가하여 common 패키지를 인식하게 함
BASE_DIR = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(BASE_DIR) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(BASE_DIR))

# 공통 설정에서 연결 함수 가져오기
from common.db_setting import get_connection # 이거는 그래서 sys.path 부분 이후로 선언해야 됨

def upload_to_basic():
    csv_path = BASE_DIR / "data" / "kosdaq_corp_map_final.csv" # BASE_DIR은 프로젝트 폴더 final_1까지의 경로를 의미
    if not csv_path.exists():
        print(f"오류: {csv_path}가 없습니다.")
        return

    df = pd.read_csv(csv_path, dtype=str)
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 테이블 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS basic (
                    stock_code VARCHAR(10) PRIMARY KEY,
                    corp_code VARCHAR(15),
                    corp_name VARCHAR(50),
                    est_dt VARCHAR(20),
                    ipo VARCHAR(20),
                    ind_code VARCHAR(10)
                );
            """)

            data_to_insert = [tuple(x) for x in df.values]

            # 삽입 및 업데이트 => 만약 중복된 키가 발견되면(기본키인 stock_code) 삽입 대신 업데이트
            sql = """
                INSERT INTO basic (stock_code, corp_code, corp_name, est_dt, ipo, ind_code)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    corp_name = VALUES(corp_name),
                    est_dt = VALUES(est_dt),
                    ipo = VALUES(ipo),
                    ind_code = VALUES(ind_code);
            """
            
            cursor.executemany(sql, data_to_insert)
            
            # 변경사항 확정
            conn.commit()
            print(f"basic table에 적재 완료 ({len(data_to_insert)}건)")

    except Exception as e:
        # 오류 발생 시 롤백
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
            
    finally:
        conn.close() # 풀에 연결 반납

def main():
    upload_to_basic()

if __name__ == "__main__":
    main()