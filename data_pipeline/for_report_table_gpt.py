import sys
import json
from pathlib import Path
import time
import datetime
import shutil
import csv

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
                    OR r.sales_ai IS NULL OR r.sales_ai = ''
                """ # LIMIT 3 추가해서 테스트 해보기, product_ratio_ai는 빈 경우도 생길 수 있으므로 null 확인에서 제외

                cursor.execute(sql)
                rows = cursor.fetchall()
                
                if not rows: 
                    print("처리할 데이터(ai 부분이 빈 기업)가 없습니다.")
                    return None

                print(f"조회된 데이터 개수: {len(rows)}개")

                # 배치 태스크 생성
                total_rows = len(rows)
                for idx, row in enumerate(rows):
                    corp = row['corp_name']
                    if (idx + 1) % 10 == 0 or (idx + 1) == total_rows: # 10개마다 혹은 마지막에 진행 상황 출력
                        print(f"데이터 대조 및 계승 확인 중 ({idx + 1}/{total_rows})")
                    
                    # 해당 기업의 직전 보고서 조회 (기재정정 시 데이터 계승용)
                    prev_sql = """
                        SELECT history_origin, history_ai, outline_origin, outline_ai, 
                               product_origin, product_ai, product_ratio_ai, sales_origin, sales_ai
                        FROM report 
                        WHERE stock_code = %s AND id < %s 
                        ORDER BY id DESC LIMIT 1
                    """
                    cursor.execute(prev_sql, (row['stock_code'], row['id']))
                    prev_data = cursor.fetchone()
                    
                    # 해당 기업의 리포트 초기화 (0은 누락인거)
                    generation_report[corp] = {
                        "history": 0,
                        "outline": 0,
                        "product": 0,
                        "product_ratio": 0,
                        "sales": 0
                    }
                    
                    inheritance_updates = {} # 계승할 데이터를 담을 딕셔너리
                    
                    # 일반 섹션 설정 (history, outline, sales)
                    base_configs = [
                        ('history', prompt_history, row['history_origin'], 'history_ai'),
                        ('outline', prompt_outline, row['outline_origin'], 'outline_ai'),
                        ('sales', prompt_sales, row['sales_origin'], 'sales_ai')
                    ]
                    
                    for key, prompt, current_content, ai_col in base_configs:
                        if current_content:
                            # 이전 원문 및 AI 답변이 있으면 가져옴
                            prev_content = prev_data.get(f"{key}_origin") if prev_data else None
                            prev_ai = prev_data.get(ai_col) if prev_data else None
                            
                            # 내용 비교해서 일치하면 기존 AI 답변 계승
                            if prev_content and prev_ai and prev_content.strip() == current_content.strip():
                                inheritance_updates[ai_col] = prev_ai
                            else:
                                # 내용이 없거나 다르면 신규 태스크 생성
                                task = create_batch_task(
                                    custom_id=f"{key}-{row['id']}-{corp}",
                                    system_prompt=prompt,
                                    user_content=current_content
                                )
                                batch_tasks.append(task)
                                generation_report[corp][key] = 1 

                    # product 및 product_ratio 통합 처리 (원문 product_origin 공유 대응)
                    p_content = row['product_origin']
                    if p_content:
                        prev_p_content = prev_data.get("product_origin") if prev_data else None 
                        
                        # 원문이 같으면 product_ai와 product_ratio_ai 둘 다 계승
                        if prev_p_content and prev_p_content.strip() == p_content.strip() \
                           and prev_data.get("product_ai") and prev_data.get("product_ratio_ai"):
                            inheritance_updates["product_ai"] = prev_data["product_ai"]
                            inheritance_updates["product_ratio_ai"] = prev_data["product_ratio_ai"]
                        else:
                            # 원문이 다르면 각각의 프롬프트로 새로 요청
                            for k, p in [('product', prompt_product), ('product_ratio', prompt_product_ratio)]:
                                task = create_batch_task(
                                    custom_id=f"{k}-{row['id']}-{corp}",
                                    system_prompt=p,
                                    user_content=p_content
                                )
                                batch_tasks.append(task)
                                generation_report[corp][k] = 1

                    # 계승된 답변이 있다면 즉시 DB 업데이트
                    if inheritance_updates:
                        set_clause = ", ".join([f"{k} = %s" for k in inheritance_updates.keys()])
                        cursor.execute(f"UPDATE report SET {set_clause} WHERE id = %s", 
                                       list(inheritance_updates.values()) + [row['id']])
                    
                    # 각 기업별 처리가 끝날 때마다 커밋 (안전장치)
                    conn.commit()

    except Exception as e:
        print((f"DB 연결 또는 쿼리 실행 중 오류 발생: {e}"))
        return None

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
        
    # 새로 요청할 작업이 아예 없을 경우 빈 파일 생성 방지
    if not batch_tasks:
        print("새로 요청할 배치 작업이 없습니다.")
        return None

    # .jsonl 파일 저장
    save_dir = BASE_DIR / "data" / "batch_files"
    save_dir.mkdir(parents=True, exist_ok=True) # 폴더가 없으면 생성
    
    input_files = []
    current_file_idx = 1
    current_file_tokens = 0
    TOKEN_LIMIT = 800000 # 90만 제한보다 안전한 80만 설정
    
    current_file_path = save_dir / f"batch_input_{current_file_idx}.jsonl"
    f = open(current_file_path, "w", encoding="utf-8")
    input_files.append(str(current_file_path))

    for task in batch_tasks:
        task_str = json.dumps(task, ensure_ascii=False)
        estimated_tokens = len(task_str) * 2 # 한글/특수문자 고려: 글자 수 * 2를 토큰으로 가정

        if current_file_tokens + estimated_tokens > TOKEN_LIMIT:
            f.close()
            current_file_idx += 1
            current_file_tokens = 0
            current_file_path = save_dir / f"batch_input_{current_file_idx}.jsonl"
            f = open(current_file_path, "w", encoding="utf-8")
            input_files.append(str(current_file_path))
            print(f"토큰 제한으로 인해 새 파일 생성: {current_file_path.name}")

        f.write(task_str + "\n")
        current_file_tokens += estimated_tokens

    f.close()
    return input_files

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
    id_log_path = BASE_DIR / "data" / "batch_job_ids.txt"
    save_dir = BASE_DIR / "data" / "batch_files"
    gpt = init_gpt()
    
    while True:
        if id_log_path.exists():
            with open(id_log_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                jobs = list(reader)
            
            if jobs:
                job_id = jobs[0]['job_id']
                print(f"\n현재 대기 중인 작업이 있습니다. (Job ID: {job_id})")

                # 작업이 끝날 때까지 대기
                while True:
                    try:
                        batch_job = gpt.batches.retrieve(job_id)
                        status = batch_job.status
                        print(f"상태 : {status}")
                        
                        # 작업이 끝나거나 실패하면 DB 처리 스크립트 실행
                        if status in ["completed", "failed", "cancelled", "expired"]:
                            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 작업 상태가 '{status}'로 변경되었습니다.")
                            
                            # 외부 파이썬 파일인 for_report_table_db의 main 함수를 여기서 직접 실행
                            import for_report_table_db
                            for_report_table_db.main()
                            break # 대기 루프 탈출, 다음 파일 확인으로 넘어감
                    except Exception as e:
                        print(f"상태 확인 중 일시적인 오류 발생: {e}")
                    
                    # 아직 진행 중이면 60초 대기 후 다시 확인 (API 호출 제한 방지)
                    time.sleep(60)
                    
        # 대기 중인 .jsonl 파일이 있는지 확인
        input_files = []
        if save_dir.exists():
            input_files = sorted(list(save_dir.glob("*.jsonl")), key=lambda x: int(x.stem.split('_')[-1]))

        # .jsonl 파일이 없으면 생성
        if not input_files:
            print("현재 보유중인 batch 파일이 없어 데이터를 조회합니다.")
            generated_files = create_batch_file()
            
            # 여기서 생성된 파일이 아예 없다면 작업할 내용이 없으므로 최종 종료
            if not generated_files:
                print("db에 모든 내용이 적재되어 있어 종료합니다.")
                try:
                    if save_dir.exists():
                        shutil.rmtree(save_dir) # .jsonl 파일 삭제, 다썼으니깐
                except:
                    pass
                break # 전체 무한 루프 탈출
                
            input_files = [Path(p) for p in generated_files]

        if not input_files:
            break

        # 맨 앞의 1개만 선택해서 제출
        file_to_submit = input_files[0]
        print(f"파일 제출 시작: {file_to_submit.name}")
        job_id = submit_batch_file(str(file_to_submit))

        if job_id:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 새 작업 기록 저장
            with open(id_log_path, "w", encoding="utf-8") as f:
                f.write("created_at,file_name,job_id\n")
                f.write(f"{current_time},{file_to_submit.name},{job_id}\n")

            print(f"성공: {job_id} 저장 완료. (제출된 파일은 삭제됨)")
            
            # 제출이 완료된 파일은 중복 제출 방지를 위해 삭제
            file_to_submit.unlink(missing_ok=True)
            
        else:
            print(f"실패: {file_to_submit.name} 제출 중 오류 발생. 60초 후 재시도합니다.")
            time.sleep(60)


if __name__ == "__main__":
    main()