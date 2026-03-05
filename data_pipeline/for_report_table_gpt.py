import sys
import json
from pathlib import Path
import datetime

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

# 공통 설정에서 연결 함수 가져오기
from common.setting import get_connection, init_gpt, BASE_DIR, create_batch_task # 이거는 그래서 sys.path 부분 이후로 선언해야 됨
from common.prompts import prompt_history, prompt_outline, prompt_product, prompt_product_ratio, prompt_sales

def create_batch_file():
    batch_tasks = []
    # 생성 현황 추적용
    generation_report = {}
    
    # db 데이터 조회
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                    SELECT 
                        r.id,
                        r.stock_code, 
                        b.corp_name,
                        r.history_origin,
                        r.outline_origin,
                        r.product_origin, 
                        r.sales_origin 
                    FROM report r
                    JOIN basic b ON r.stock_code = b.stock_code
                    WHERE r.history_ai IS NULL OR r.history_ai = ''
                    OR r.outline_ai IS NULL OR r.outline_ai = ''
                    OR r.product_ai IS NULL OR r.product_ai = ''
                    OR r.product_ratio_ai IS NULL OR r.product_ratio_ai = ''
                    OR r.sales_ai IS NULL OR r.sales_ai = ''
                """ # LIMIT 3 추가해서 테스트 해보기

                cursor.execute(sql)
                rows = cursor.fetchall()
                print(f"조회된 데이터 개수: {len(rows)}개")
                
    except Exception as e:
        print((f"DB 연결 또는 쿼리 실행 중 오류 발생: {e}"))
        return None

    if not rows: 
        print("처리할 데이터(ai 부분이 빈 기업)가 없습니다.")
        return None

    # 배치 태스크 생성
    for row in rows:
        
        corp = row['corp_name']
        # 해당 기업의 리포트 초기화
        generation_report[corp] = {
            "history": 0,
            "outline": 0,
            "product": 0,
            "product_ratio": 0,
            "sales": 0
        }
        
        configs = [
            ('history', prompt_history, row['history_origin']),
            ('outline', prompt_outline, row['outline_origin']),
            ('product', prompt_product, row['product_origin']),
            ('product_ratio', prompt_product_ratio, row['product_origin']),
            ('sales', prompt_sales, row['sales_origin'])
        ]
        
        for key, prompt, content in configs:
            if content:
                task = create_batch_task(
                    custom_id=f"{key}-{row['id']}-{corp}",
                    system_prompt=prompt,
                    user_content=content
                )
                batch_tasks.append(task)
                generation_report[corp][key] = 1
            
    # 누락 데이터가 있는 기업 요약 출력 ---
    print("\n" + "="*50)
    print("[데이터 누락 기업 및 항목 리포트] ")
    print("="*50)
    missing_corp_count = 0

    for corp, status in generation_report.items():
        missing = [k for k, v in status.items() if v == 0]
        if missing:
            print(f"{corp:20} | 누락: {', '.join(missing)}")
            missing_corp_count += 1
    
    if missing_corp_count == 0:
        print("모든 데이터가 완벽합니다!")
        
    else:
        print("-" * 50)
        print(f"총 {missing_corp_count}개 기업에서 데이터 누락이 발견되었습니다.")
        print("="*50 + "\n")
    
    # .jsonl 파일 저장
    save_dir = BASE_DIR / "data"
    input_file_path = save_dir / "batch_input.jsonl"
    save_dir.mkdir(parents=True, exist_ok=True) # 폴더가 없으면 생성
    with open(input_file_path, "w", encoding="utf-8") as f:
        for task in batch_tasks:
            f.write(json.dumps(task, ensure_ascii=False) + "\n")
    
    print(f"총 {len(batch_tasks)}개의 태스크가 {input_file_path}에 저장되었습니다.")
    
    return str(input_file_path)


def submit_batch_file(file_path): # OpenAI Batch 업로드 및 실행
    gpt = init_gpt()
    try:
        with open(file_path, "rb") as f:
            batch_file = gpt.files.create(file=f, purpose="batch")
        
        batch_job = gpt.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        
        print(f"Batch Job 생성 성공! ID: {batch_job.id}")
        return batch_job.id
    except Exception as e:
        print(f"OpenAI Batch 업로드 중 오류 발생: {e}")
        return None

def main():
    input_file = create_batch_file()
    if not input_file:
        return
    
    job_id = submit_batch_file(input_file)
    if job_id:
        file_path = BASE_DIR / "data" / "batch_job_ids.txt"
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(file_path, "a", encoding="utf-8") as f: # "a" 모드는 기존 내용 뒤에 추가함
            f.write(f"[{current_time}] Job ID: {job_id}\n")
        print(f"작업 완료. Job ID: {job_id}가 {file_path}에 저장되었습니다.")
        
    else:
        print("Job ID를 생성하지 못했습니다.")

if __name__ == "__main__":
    main()