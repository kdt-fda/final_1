import sys
import pandas as pd
import numpy as np
from pathlib import Path

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

# 공통 설정에서 연결 함수 가져오기
from common.setting import get_connection, BASE_DIR # 이거는 그래서 sys.path 부분 이후로 선언해야 됨

def create_basic(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS basic (
                    stock_code VARCHAR(10) PRIMARY KEY,
                    corp_code VARCHAR(15),
                    corp_name VARCHAR(100),
                    est_dt VARCHAR(20),
                    ipo VARCHAR(20),
                    ind_code VARCHAR(10),
                    is_active BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """) # updated_at 에서 데이터 수정시간 확인 가능
            # FOREIGN KEY (ind_code) REFERENCES industry(ind_code) 이거 맨 뒷줄에 추가해야 됨 나중에
            print("basic 테이블이 성공적으로 생성되었거나 이미 존재합니다.")

    except Exception as e:
        # 오류 발생 시 롤백
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
    
def upload_to_basic():
    csv_path = BASE_DIR / "data" / "kosdaq_corp_map_final.csv" # BASE_DIR은 프로젝트 폴더 final_1까지의 경로를 의미
    if not csv_path.exists():
        print(f"오류: {csv_path}가 없습니다.")
        return

    df = pd.read_csv(csv_path, dtype=str)
    df = df.replace({np.nan: None}) # NaN(결측치)을 SQL NULL 처리를 위해 None으로 변환
    
    conn = get_connection()
    try:
        create_basic(conn)
        
        with conn.cursor() as cursor:
            columns = ['stock_code', 'corp_code', 'corp_name', 'est_dt', 'ipo', 'ind_code']
            data_to_insert = [tuple(x) for x in df[columns].values]

            # 삽입 및 업데이트 => 만약 중복된 키가 발견되면(기본키인 stock_code) 삽입 대신 업데이트
            sql = """
                INSERT INTO basic (stock_code, corp_code, corp_name, est_dt, ipo, ind_code, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON DUPLICATE KEY UPDATE
                    corp_code = VALUES(corp_code),
                    corp_name = VALUES(corp_name),
                    est_dt = VALUES(est_dt),
                    ipo = VALUES(ipo),
                    ind_code = VALUES(ind_code),
                    is_active = TRUE;
            """
            
            cursor.executemany(sql, data_to_insert)
            
            current_stock_codes = df['stock_code'].tolist()
            
            if current_stock_codes:
                # IN 절에 들어갈 %s, %s, ... 문자열 동적 생성
                format_strings = ','.join(['%s'] * len(current_stock_codes))
                
                # 최신 리스트에 없는데 활성화 되어있는 데이터만 비활성화
                sql_soft_delete = f"""
                    UPDATE basic 
                    SET is_active = FALSE 
                    WHERE stock_code NOT IN ({format_strings}) AND is_active = TRUE;
                """
                cursor.execute(sql_soft_delete, tuple(current_stock_codes))
                soft_deleted_count = cursor.rowcount
            else:
                soft_deleted_count = 0
            
            # 변경사항 확정
            conn.commit()
            print(f"basic table에 적재 완료 ({len(data_to_insert)}건)")
            print(f"상장폐지 등 크롤링 누락 기업 비활성화(Soft Delete) 처리 완료: {soft_deleted_count}건")

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