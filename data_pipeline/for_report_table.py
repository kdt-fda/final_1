import sys
import pandas as pd
import numpy as np
from pathlib import Path
import re
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from datetime import datetime
import logging

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

# 공통 설정에서 연결 함수 가져오기
from common.setting import get_connection, BASE_DIR, init_dart, init_solar, init_gpt, get_ai_answer, get_ai_answer_gpt # 이거는 그래서 sys.path 부분 이후로 선언해야 됨
from common.prompts import prompt_history, prompt_outline, prompt_product, prompt_product_ratio, prompt_sales

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log", mode='w', encoding='utf-8')
    ]
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
        
def fetch_report_html(dart, corp_code):
    try:
        current_year = datetime.now().year
        start_year = current_year - 3
        start_date = f"{start_year}-01-01"
        reports = dart.list(corp_code, kind='A', start=start_date)
        
        if reports.empty: 
            return None, "보고서를 찾을 수 없습니다.", None, None, None
        
        annual_reports = reports[reports['report_nm'].str.contains('사업보고서')]

        if annual_reports.empty:
            return None, "사업보고서를 찾을 수 없습니다.", None, None, None

        rcept_no = annual_reports.iloc[0]['rcept_no'] 
        rcept_dt = annual_reports.iloc[0]['rcept_dt'] 
        report_nm = annual_reports.iloc[0]['report_nm']
        rcept_no = annual_reports.iloc[0]['rcept_no']

        full_html = dart.document(rcept_no)
        return full_html, None, rcept_dt, report_nm, rcept_no
    except Exception as e:
        return None, f"DART 연결 에러: {e}", None, None, None

# HTML 표의 깨진 rowspan/colspan 값 오류 방지 및 오류 시 1을 반환
def safe_int(val, default=1):
    try: return int(val)
    except (ValueError, TypeError): return default

# 표 평탄화(샐 병합된 거를 ai가 읽기 쉽게 만드는 거임)
def parse_html_table(table_soup):
    rows = table_soup.find_all('tr') # 표에서 각 행부분에 해당
    if not rows: return []

    # 가장 칸이 많은 줄을 기준으로 표의 최대 가로 칸 수를 구함
    max_cols = max((sum(safe_int(cell.get('colspan', 1)) for cell in row.find_all(['th', 'td', 'tu', 'te'])) for row in rows), default=0)
    # (세로 줄 수 x 가로 칸 수) 만큼 텅 빈 2차원 리스트 만들기
    grid = [['' for _ in range(max_cols)] for _ in range(len(rows))]

    for r_idx, row in enumerate(rows): # 내용 채우는 부분
        c_idx = 0
        for cell in row.find_all(['th', 'td', 'tu', 'te']):
            while c_idx < max_cols and grid[r_idx][c_idx] != '':
                c_idx += 1
            if c_idx >= max_cols: break

            text = re.sub(r'\s+', ' ', cell.get_text(separator=" ", strip=True))
            rowspan, colspan = safe_int(cell.get('rowspan', 1)), safe_int(cell.get('colspan', 1))

            for r in range(rowspan):
                for c in range(colspan):
                    if r_idx + r < len(rows) and c_idx + c < max_cols:
                        grid[r_idx + r][c_idx + c] = text
            c_idx += colspan
    return grid

# 2차원 리스트 형태인 표를 마크다운화
def table_to_markdown(grid):
    if not grid: return ""
    md_lines = ["| " + " | ".join(grid[0]) + " |", "|" + "|".join(["---"] * len(grid[0])) + "|"]
    md_lines.extend("| " + " | ".join(row) + " |" for row in grid[1:])
    return "\n".join(md_lines) + "\n"

# 불필요한 태그(열 너비라던가 높이나, style요소나 기타 등등)
def process_dart_to_llm_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup(['script', 'style', 'colgroup']): tag.decompose()

    for table in soup.find_all('table'):
        md_table = table_to_markdown(parse_html_table(table)) # 여기서 실제 표 평탄화 및 마크다운화 진행
        table.insert_after(soup.new_string("\n\n" + md_table + "\n\n"))
        table.decompose()

    final_text = re.sub(r'\n{3,}', '\n\n', soup.get_text(separator='\n'))
    return final_text.strip() # 최종 내용 반환

# section 부분에 우리가 원하는 본문 내용이 들어 있어서 필요한 부분을 추출하는 함수
def extract_dart_sections_from_html(html_content, target_keywords):
    soup = BeautifulSoup(html_content, 'lxml') 
    sections = soup.find_all(re.compile("^section-\d+")) # section-1, section-2를 추출함. section-1은 'II. 사업의 내용' 같은 큰 제목, section-2는 '1. 사업의 개요' 같은 작은 제목 같음
    
    raw_html_dict = {key: "" for key in target_keywords}
    
    # 키워드별 HTML 내용 가져오기
    for section in sections:
        title_tag = section.find('title') # '회사의 연혁', '사업의 개요', '주요 제품 및 서비스', '매출 및 수주상황'이 section-2 안에 title에 들어가 있음
        if not title_tag: continue
        
        title_text = title_tag.get_text(strip=True)
        for keyword in target_keywords:# 회사의 연혁 같은 우리가 원하는 키워드를 가진 title이 있으면 그 안의 내용을 다 가져옴
            if keyword in title_text:
                raw_html_dict[keyword] += str(section) + "\n\n"
                break
                
    # 가져온 내용을 마크다운화 함수에 넣음
    extracted_data = {}
    for keyword in target_keywords:
        raw_html = raw_html_dict[keyword]
        extracted_data[keyword] = process_dart_to_llm_text(raw_html) if raw_html.strip() else None
        
    return extracted_data # (회사의 연혁 : 내용) 형태로 반환

def create_report(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS report (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10),
                    report_num VARCHAR(50) UNIQUE,
                    report_date VARCHAR(20),
                    history_origin MEDIUMTEXT,
                    outline_origin MEDIUMTEXT,
                    product_origin MEDIUMTEXT,
                    sales_origin MEDIUMTEXT,
                    history_ai MEDIUMTEXT,
                    outline_ai MEDIUMTEXT,
                    product_ai MEDIUMTEXT,
                    product_ratio_ai MEDIUMTEXT,
                    sales_ai MEDIUMTEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_report_stock_code 
                    FOREIGN KEY (stock_code) REFERENCES basic(stock_code)
                    ON DELETE CASCADE ON UPDATE CASCADE
                );
            """)
            print("report 테이블이 성공적으로 생성되었거나 이미 존재합니다.")

    except Exception as e:
        # 오류 발생 시 롤백
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")

def upload_to_report():
    dart = init_dart()
    client = init_solar()
    gpt = init_gpt()
    
    # csv_path = BASE_DIR / "data" / "kosdaq_corp_map_final.csv" # BASE_DIR은 프로젝트 폴더 final_1까지의 경로를 의미
    csv_path = BASE_DIR / "data" / "test.csv" # 수정필
    if not csv_path.exists():
        print(f"오류: {csv_path}가 없습니다.")
        return

    df = pd.read_csv(csv_path, dtype=str)
    df = df.replace({np.nan: None}) # NaN(결측치)을 SQL NULL 처리를 위해 None으로 변환
    
    conn = get_connection()
    
    try:
        create_report(conn)

        total_rows = len(df)
        
        for index, row in df.iterrows():
            current_num = index + 1
            stock_code = row['stock_code']
            corp_code = row['corp_code']
            corp_name = row['corp_name']
            
            print(f"[{current_num}/{total_rows}] {corp_name} 처리중") # 진행상황 확인용
            
            try:
                html, error, report_date, report_name, report_num = fetch_report_html(dart, corp_code) # 함수에 반환값을 4개로 줌(html, 에러 시 에러문구, 공시 접수 날짜, 보고서 이름)

                if html: # 사업보고서가 있으면 키워드로 내용 가져와서 마크다운화 하는 것  
                    # 키위드로 본문 내용 가져오는 부분
                    target_keywords = ['회사의 연혁', '사업의 개요', '주요 제품 및 서비스', '매출 및 수주상황']
                    
                    # 필요한 부분 추출 및 마크다운화까지 한번에 다
                    result_data = extract_dart_sections_from_html(html, target_keywords)
                    
                    # 추출된 내용을 solar ai에 넣기 편하게 변수화
                    history = result_data.get('회사의 연혁')
                    outline = result_data.get('사업의 개요')
                    product = result_data.get('주요 제품 및 서비스')
                    sales = result_data.get('매출 및 수주상황')
                    
                    print('history 처리중')
                    history_ai = get_ai_answer(client, prompt_history, history)
                    print('outline 처리중')
                    outline_ai = get_ai_answer(client, prompt_outline, outline)
                    print('product 처리중')
                    product_ai = get_ai_answer(client, prompt_product, product)
                    print('product_ratio 처리중')
                    product_ratio_ai = get_ai_answer(client, prompt_product_ratio, product)
                    print('sales 처리중')
                    sales_ai = get_ai_answer_gpt(gpt, prompt_sales, sales)
                    
                    with conn.cursor() as cursor:
                        # 삽입 및 업데이트, 동일한 보고서번호이면, ai 생성 부분만 업데이트(이거는 이 코드를 여러 번 수행했을 때 ai 내용만 바꾸기 위한 부분)
                        sql = """
                            INSERT INTO report (
                                stock_code, report_num, report_date, 
                                history_origin, outline_origin, product_origin, sales_origin, 
                                history_ai, outline_ai, product_ai, product_ratio_ai, sales_ai
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                history_ai = VALUES(history_ai),
                                outline_ai = VALUES(outline_ai),
                                product_ai = VALUES(product_ai),
                                product_ratio_ai = VALUES(product_ratio_ai),
                                sales_ai = VALUES(sales_ai);
                        """
                        
                        # 실행할 파라미터 튜플 생성
                        val = (
                            stock_code, report_num, report_date, 
                            history, outline, product, sales, 
                            history_ai, outline_ai, product_ai, product_ratio_ai, sales_ai
                        )
                        
                        cursor.execute(sql, val)
                        conn.commit()
                else:
                    print(f"HTML 사업보고서 수집 실패: {error}")
                    logging.warning(f"[{current_num}/{total_rows}] {corp_name} 건너뜀(보고서 없음): {error}")
                    
            except Exception as e: # 한 종목에서 에러 발생 시
                print(f"{corp_name} 처리 중 오류 발생: {e}")
                logging.error(f"[{current_num}/{total_rows}] {corp_name} 처리 중 에러: {str(e)}")
                conn.rollback() # 해당 건만 롤백하고
                continue # 다음 종목(for문의 다음 index)으로 진행
            
    except Exception as e:
        # 오류 발생 시 롤백
        conn.rollback()
        print(f"(롤백됨)데이터베이스 작업 중 오류 발생: {e}")
        logging.critical(f"데이터베이스 전체 작업 중 치명적 오류: {str(e)}")
            
    finally:
        conn.close() # 풀에 연결 반납

def main():
    upload_to_report()
    print('===== 완료 =====')

if __name__ == "__main__":
    main()