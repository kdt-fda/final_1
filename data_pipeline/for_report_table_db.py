import json
import sys
import csv
import logging
from pathlib import Path

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

# 공통 설정에서 연결 함수 가져오기
from common.setting import get_connection, init_gpt, BASE_DIR # 이거는 그래서 sys.path 부분 이후로 선언해야 됨

log_dir = BASE_DIR / "data"
log_dir.mkdir(parents=True, exist_ok=True) # 폴더가 없으면 생성
log_file_path = log_dir / "db_update_error.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a', encoding='utf-8') # mode='a'로 바꾸면 이어서 로그 작성됨
    ]
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
    
def upload_to_report_ai(gpt, batch_job):

    if not batch_job.output_file_id: # 이거는 complete 처리 됐는데도 결과물 파일이 없을 때를 대비한 조건문
        msg = f"출력 파일이 없습니다. (Job ID: {batch_job.id})"
        print(msg)
        logging.error(msg) # 로그 기록
        return

    # 결과 파일 내용 가져오기
    file_response = gpt.files.content(batch_job.output_file_id)
    results = file_response.text.strip().split('\n')
    
    success_count = 0
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for line in results:
                if not line.strip(): # 빈줄이면 넘김
                    continue
                
                data = json.loads(line)
                custom_id = data.get('custom_id', 'unknown')
                
                # API 에러 체크 (개별 요청들 중 에러인 부분 걸러냄)
                if data.get('error'):
                    msg = f"요청 에러 발생 ({custom_id}): {data['error']}"
                    print(msg)
                    logging.error(msg)
                    continue
                    
                status_code = data.get('response', {}).get('status_code') # status_code 확인을 통한 요청 실패 확인용
                if status_code != 200:
                    msg = f"응답 실패 ({custom_id}): HTTP {status_code}"
                    print(msg)
                    logging.error(msg)
                    continue
                
                try:
                    # custom_id 파싱 (key: 항목명, r_id: report 테이블의 id(기본키), corp: 기업명)
                    key, r_id, corp = custom_id.split('-', 2)
                    # GPT 답변 추출
                    content = data['response']['body']['choices'][0]['message']['content']
                    
                    # DB 업데이트 (항목별 동적 컬럼 지정)
                    column_name = f"{key}_ai"
                    sql = f"UPDATE REPORT SET {column_name} = %s WHERE id = %s"
                    cursor.execute(sql, (content, r_id))
                    success_count += 1
                    
                except Exception as e:
                    msg = f"데이터 db update 중 예외 발생 ({custom_id}): {e}"
                    print(msg)
                    logging.error(msg)
            
            conn.commit()
            print(f"{success_count}건 DB 적재됨 (Job ID: {batch_job.id})")

def check_batch_jobs():
    gpt = init_gpt()
    id_log_path = BASE_DIR / "data" / "batch_job_ids.txt"
    
    if not id_log_path.exists():
        print("배치 파일이 없습니다.")
        return

    pending_jobs = []
    
    # 저장된 Job ID 목록 읽기
    with open(id_log_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f) # { "created_at": "2023-10-24 12:00:00",  "file_name": "batch_input_1.jsonl", "job_id": "batch_abc123" } 이런식으로 변환
        jobs = list(reader) # 리스트에 담음

    if not jobs:
        print("기록된 Job ID가 없습니다.")
        return

    for row in jobs:
        job_id = row['job_id']       
        batch_job = gpt.batches.retrieve(job_id)
        status = batch_job.status
        
        if status == "completed":
            print(f"배치 작업 완료 ({job_id}), DB 적재 시작")
            upload_to_report_ai(gpt, batch_job)
        elif status in ["failed", "cancelled", "expired"]:
            msg = f"작업 실패 또는 중단됨 (Job ID: {job_id}, 상태: {status})"
            print(msg)
            logging.error(msg)
        else:
            print(f"현재 상태: {status} (Job ID: {job_id}) => 대기열에 유지")
            pending_jobs.append(row)

    # 처리가 완료되지 않은 작업만 파일에 다시 덮어쓰기
    if pending_jobs:
        with open(id_log_path, "w", encoding="utf-8", newline="") as f: # 쓰기 모드로 열어서 기존 내용 삭제
            writer = csv.DictWriter(f, fieldnames=jobs[0].keys())  # 기존 헤더 유지
            writer.writeheader() # 헤더 작성
            writer.writerows(pending_jobs) # db 적재 안된 job_id 작성
        print(f"\n남은 대기 작업 {len(pending_jobs)}건을 파일에 다시 저장했습니다.")
    else:
        # 모든 작업이 완료되었으면 파일 삭제
        id_log_path.unlink(missing_ok=True)
        print("\n모든 배치 작업 처리가 완료되어 목록 파일을 삭제했습니다.")

def main():
    check_batch_jobs()
    
if __name__ == "__main__":
    main()
