import sys
from pathlib import Path

# 루트 경로를 추가하여 common 패키지를 인식하게 함
BASE_DIR = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(BASE_DIR) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(BASE_DIR))

# 공통 설정에서 연결 함수 가져오기
from common.db_setting import get_connection # 이거는 그래서 sys.path 부분 이후로 선언해야 됨

def show():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 테이블 내용 조회 SQL
            cursor.execute('select * from basic')
            
            tables = cursor.fetchall() # 수행한 내용을 가져옴 => 필드명 : 값 형태로
            
        if tables:
            for table in tables:
                print(list(table.values())) # 여기서 values()로 가져오기 때문에 value만 보여지게 됨    
            print(f'데이터 총 개수 : {len(tables)}개')
        else:
            print("결과값이 없습니다.")
            
    except Exception as e:
        # 오류 발생 시 롤백
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
            
    finally:
        conn.close() # 풀에 연결 반납

def main():
    show()

if __name__ == "__main__":
    main()