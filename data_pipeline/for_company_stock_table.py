from pykrx import stock
import os
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import requests
import datetime
import time

# 루트 경로를 추가하여 common 패키지를 인식하게 함
temp_base = Path(__file__).resolve().parents[1] # parents[0]은 data_pipeline 폴더를 의미함
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

# 공통 설정에서 연결 함수 가져오기
from common.setting import get_connection, login_krx, BASE_DIR # 이거는 그래서 sys.path 부분 이후로 선언해야 됨

FACE_VALUE_CACHE = {} # 캐싱용

# 잉여 파일 저장할 경로
data_dir = BASE_DIR / "data"
data_dir.mkdir(parents=True, exist_ok=True)

# COMPANY_STOCK 테이블 생성
def create_company_stock(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS COMPANY_STOCK (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL,
                    reference_date DATE NOT NULL,
                    open_price INT,
                    prev_close_price INT,
                    close_price INT,
                    price_change INT,
                    high_price INT,
                    low_price INT,
                    wk52_high INT,
                    wk52_low INT,
                    book_value INT,
                    mktcap DECIMAL(20,0),
                    shares_btj DECIMAL(20,0),
                    trdvol DECIMAL(20,0),
                    acc_trdvol DECIMAL(20,0),
                    acc_trdval DECIMAL(20,0),
                    bas_trdval DECIMAL(20,0),
                    foreign_ratio DECIMAL(9,4),
                    fluc_rt DECIMAL(9,4),
                    dps DECIMAL(18,4),
                    eps DECIMAL(18,4),
                    dividend_yield DECIMAL(6,3),
                    per DECIMAL(10,4),
                    pbr DECIMAL(10,4),
                    UNIQUE KEY uk_company_stock (stock_code, reference_date),
                    CONSTRAINT fk_company_stock_stock_code
                    FOREIGN KEY (stock_code)
                    REFERENCES BASIC (stock_code)
                    ON DELETE CASCADE ON UPDATE CASCADE
                );
            """)
        conn.commit()
        print("COMPANY_STOCK 테이블이 성공적으로 생성되었거나 이미 존재합니다.")
        
    except Exception as e:
        conn.rollback()
        print(f"(롤백됨) 데이터베이스 작업 중 오류 발생: {e}")
        raise


# 숫자 데이터에 포함된 컨마 제거, 빈 문자열이나 결측치를 None으로 변환
def clean_numeric(x):
    if pd.isna(x):
        return None
    if isinstance(x, str):
        x = x.strip()
        if x == "":
            return None
        x = x.replace(",", "")
    return x

# 텍스트나 소수점 형태의 데이터를 안전하게 정수로 변환. 에러나면 None 반환
def safe_to_int(x):
    x = clean_numeric(x)
    if x is None:
        return None
    try:
        return int(float(x))
    except Exception:
        return None

# 데이터를 실수로 변환, ndigits 만큼까지 반올림
def safe_to_float(x, ndigits=None):
    x = clean_numeric(x)
    if x is None:
        return None
    try:
        value = float(x)
        return round(value, ndigits) if ndigits is not None else value
    except Exception:
        return None

# 입력받은 날짜 데이터를 YYYYMMDD 형태의 문자열로 통일하여 반환
def normalize_date(date):
    return pd.to_datetime(date).strftime("%Y%m%d")

# krx에서 액면가 가져오는 함수 (날짜별 데이터 전체 캐싱 최적화, 특정날짜의 모든 종목 액면가 가져와서 저장해둬서 api 호출 줄임)
def get_kosdaq_face_value_cached(target_date, ticker):
    target_date_str = target_date.replace("-", "")
    
    if target_date_str not in FACE_VALUE_CACHE:
        url = "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info"
        auth_key = os.getenv('KRX_AUTH_KEY')
        
        if not auth_key:
            FACE_VALUE_CACHE[target_date_str] = pd.DataFrame()
        else:
            params = {'basDd': target_date_str}
            headers = {'AUTH_KEY': auth_key}
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                if 'OutBlock_1' in data:
                    df = pd.DataFrame(data['OutBlock_1'])
                    df['ISU_SRT_CD'] = df['ISU_SRT_CD'].astype(str)
                    FACE_VALUE_CACHE[target_date_str] = df
                else:
                    FACE_VALUE_CACHE[target_date_str] = pd.DataFrame()
            except Exception:
                FACE_VALUE_CACHE[target_date_str] = pd.DataFrame()

    df = FACE_VALUE_CACHE[target_date_str]
    if df.empty:
        return None

    target_stock = df[df['ISU_SRT_CD'] == str(ticker)]
    if not target_stock.empty:
        return target_stock['PARVAL'].values[0]
    return None

# DB에 이미 존재하는 (종목코드, 기준일) 쌍을 가져와서 반환. 중복 저장 회피
def get_existing_records(conn, start_date_str: str, end_date_str: str) -> set:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT stock_code, reference_date
            FROM COMPANY_STOCK
            WHERE reference_date >= %s AND reference_date <= %s
        """, (start_date_str, end_date_str))
        rows = cursor.fetchall()

    existing = set()
    for r in rows: # 딕셔너리 형태이긴 할건데 혹시 모를 else 처리
        if isinstance(r, dict):
            sc = str(r['stock_code']).zfill(6)
            rd = r['reference_date'].strftime("%Y-%m-%d")
        else:
            sc = str(r[0]).zfill(6)
            rd = r[1].strftime("%Y-%m-%d")
        existing.add((sc, rd))
    return existing

# 여러 날짜에 대해 한 종목의 피쳐들을 한 번에 파싱하는 함수
def assemble_company_stock_rows(stock_code: str, missing_dates: list, start_dt, end_dt, finance_map: dict) -> pd.DataFrame:
    if not missing_dates:
        return pd.DataFrame()
    
    # 누락된 날짜 중 가장 빠른 날짜와 늦은 날짜 구하기
    min_missing_dt = pd.to_datetime(min(missing_dates))
    max_missing_dt = pd.to_datetime(max(missing_dates))
    
    fetch_start_ymd = min_missing_dt.strftime("%Y%m%d")
    fetch_end_ymd = max_missing_dt.strftime("%Y%m%d")
    
    # 누적 이평선 및 52주 최고/최저가를 위해 OHLCV만 누락일 기준 1년 전까지 포괄
    start_history_ymd = (min_missing_dt - pd.Timedelta(days=365)).strftime("%Y%m%d")
    
    # 90일 고정이 아닌, 실제 필요한 최소 구간(fetch_start_ymd ~ fetch_end_ymd)만 API 호출
    df_ohlcv = stock.get_market_ohlcv_by_date(start_history_ymd, fetch_end_ymd, stock_code, adjusted=False) # 시가, 고가, 저가, 종가, 거래량, 거래대금, 등략률 있음(날짜 인덱스)
    df_cap = stock.get_market_cap_by_date(fetch_start_ymd, fetch_end_ymd, stock_code) # 시가총액, 거래량, 거래대금, 상장주식수 있음(날짜 인덱스)
    df_fund = stock.get_market_fundamental_by_date(fetch_start_ymd, fetch_end_ymd, stock_code) # BPS, PER, PBR, EPS, DIV, DPS 있음 (날짜 인덱스)
    df_fore = stock.get_exhaustion_rates_of_foreign_investment_by_date(fetch_start_ymd, fetch_end_ymd, stock_code) # 상장주식수, 외국인보유수량, 외국인 지분율, 외국인한도수량, 외국인한도소진률 있음 (날짜 인덱스)

    results = [] # 각 날짜별로 정리된 데이터를 담기 위한 빈 리스트
    for m_date_str in missing_dates: # db에 빠져 있는 날짜들 하나씩 가져오기
        m_dt = pd.to_datetime(m_date_str)
        
        # OHLCV 데이터 구성
        if df_ohlcv is not None and not df_ohlcv.empty and m_dt in df_ohlcv.index: # OHLCV 데이터가 존재하고, 현재 처리하려는 날짜가 데이터에 있는지 확인
            curr_ohlcv = df_ohlcv.loc[:m_dt] # 전체 데이터 중 시작일부터 for문에 걸린 날짜까지만 슬라이싱
            row_day = curr_ohlcv.iloc[-1] # 데이터 중 가장 마지막 줄 가져옴
            df_60 = curr_ohlcv.tail(60) # 최근 60거래일 데이터를 추출
            df_252 = curr_ohlcv.tail(252) # 252가 약 1년 영업일 (이거는 한번에 455일치를 조회하기 때문에 대략적인 52주 데이터를 개장일을 252로 잡아서 가져온 것)
            
            prev_close = safe_to_int(curr_ohlcv.iloc[-2].get("종가")) if len(curr_ohlcv) >= 2 else None # 전날 종가 추출
            close_price = safe_to_int(row_day.get("종가")) # 오늘 종가
            
            ohlcv_feats = {
                "open_price": safe_to_int(row_day.get("시가")),
                "close_price": close_price, # 종가
                "high_price": safe_to_int(row_day.get("고가")),
                "low_price": safe_to_int(row_day.get("저가")),
                "trdvol": safe_to_float(row_day.get("거래량"), 0), # 거래량
                "acc_trdvol": safe_to_float(df_60["거래량"].mean(), 0) if "거래량" in df_60.columns else None, # 60일 평균 거래량
                "acc_trdval": safe_to_float(df_60["거래대금"].mean(), 0) if "거래대금" in df_60.columns else None,
                "bas_trdval": safe_to_float(row_day.get("거래대금"), 0),
                "fluc_rt": safe_to_float(row_day.get("등락률"), 4),
                "prev_close_price": prev_close, # 전날 종가
                "price_change": (close_price - prev_close) if close_price is not None and prev_close is not None else None, # 등락금액
                "wk52_high": safe_to_int(df_252["종가"].max()), # 1년 종가중 최고가
                "wk52_low": safe_to_int(df_252["종가"].min()) # 1년 종가중 최저가
            }
        else:
            ohlcv_feats = {k: None for k in ["open_price", "close_price", "high_price", "low_price", 
                                             "trdvol", "acc_trdvol", "acc_trdval", "bas_trdval", "fluc_rt", 
                                             "prev_close_price", "price_change", "wk52_high", "wk52_low"]}

        face_val = get_kosdaq_face_value_cached(m_date_str, stock_code) # 해당 날짜의 액면가
        ohlcv_feats["book_value"] = safe_to_int(face_val)

        # 시총 피쳐 구성
        cap_feats = {"mktcap": None, "shares_btj": None}
        if df_cap is not None and not df_cap.empty and m_dt in df_cap.index: # cap 데이터가 존재하고, 현재 처리하려는 날짜가 데이터에 있는지 확인
            cap_row = df_cap.loc[m_dt]
            cap_feats = {
                "mktcap": safe_to_float(cap_row.get("시가총액"), 0),
                "shares_btj": safe_to_float(cap_row.get("상장주식수"), 0), # 발행주식수(보통주)
            }

        # 펀더멘털 피쳐 구성
        fund_feats = {"dps": None, "eps": None, "dividend_yield": None, "per": None, "pbr": None}
        if df_fund is not None and not df_fund.empty and m_dt in df_fund.index:
            fund_row = df_fund.loc[m_dt]
            
            raw_per = safe_to_float(fund_row.get("PER"), 4)
            raw_pbr = safe_to_float(fund_row.get("PBR"), 4)
            
            mktcap = cap_feats.get("mktcap")
            finance_info = finance_map.get(stock_code, {})
            
            # PER 대체 계산
            if raw_per in (None, 0, 0.0) and mktcap is not None:
                net_income = finance_info.get("net_income")
                if net_income is not None and float(net_income) != 0: 
                    raw_per = safe_to_float(mktcap / float(net_income), 4)

            # PBR 대체 계산
            if raw_pbr in (None, 0, 0.0) and mktcap is not None:
                equity = finance_info.get("equity")
                if equity is not None and float(equity) != 0: 
                    raw_pbr = safe_to_float(mktcap / float(equity), 4)
            
            fund_feats = {
                "dps": safe_to_float(fund_row.get("DPS"), 4),
                "eps": safe_to_float(fund_row.get("EPS"), 4),
                "dividend_yield": safe_to_float(fund_row.get("DIV"), 3),
                "per": raw_per,
                "pbr": raw_pbr
            }

        # 외국인 지분율 피쳐 구성
        fore_feats = {"foreign_ratio": None}
        if df_fore is not None and not df_fore.empty and m_dt in df_fore.index:
            fore_row = df_fore.loc[m_dt]
            for col in fore_row.index:
                if "지분율" in str(col):
                    fore_feats["foreign_ratio"] = safe_to_float(fore_row.get(col), 4)
                    break

        # 병합
        row_data = {"stock_code": stock_code, "reference_date": m_dt.date()}
        row_data.update(ohlcv_feats)
        row_data.update(cap_feats)
        row_data.update(fund_feats)
        row_data.update(fore_feats)
        
        results.append(row_data)

    return pd.DataFrame(results) if results else pd.DataFrame()

# COMPANY_FINANCE에서 종목별 최신 사업년도인 데이터의 stock_code, net_income, equity를 가져와 딕셔너리로 반환
def fetch_latest_finance_data(conn):
    sql = """
        SELECT a.stock_code, a.net_income, a.equity
        FROM COMPANY_FINANCE a
        INNER JOIN (
            SELECT stock_code, MAX(biz_year) AS max_biz_year
            FROM COMPANY_FINANCE
            GROUP BY stock_code
        ) b ON a.stock_code = b.stock_code AND a.biz_year = b.max_biz_year
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    finance_map = {}
    for row in rows:
        if isinstance(row, dict): # 딕셔너리 형태일 때 (stock_code를 키로 하고, 당기순이익, 자본총계를 딕셔너리 형태의 value로)
            sc = str(row['stock_code']).zfill(6)
            finance_map[sc] = {
                "net_income": row.get('net_income'),
                "equity": row.get('equity')
            }
        else: # 딕셔너리 형태겠지만 나중에 바뀔 것을 대비한 방어용 코드
            sc = str(row[0]).zfill(6)
            finance_map[sc] = {
                "net_income": row[1],
                "equity": row[2]
            }
    return finance_map

# is_active가 활성화된 종목의 종목코드, 상장일 반환
def fetch_listing_from_basic(conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT stock_code, ipo, is_active
            FROM BASIC
            WHERE stock_code IS NOT NULL AND stock_code <> ''
        """)
        rows = cursor.fetchall()

    if not rows:
        return pd.DataFrame(columns=["stock_code", "listing_date"]) # db 조회결과 혹시나 없을 때 방어용

    if isinstance(rows[0], dict): # pd 형태로 저장
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(rows, columns=["stock_code", "ipo", "is_active"])

    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6) # stock_code 6자리로 만듦. 혹시나 앞에 0 빠지는 경우 방지

    def to_active_flag(x): # is_active 필터링
        if pd.isna(x): return False # is_active 비면 false
        if isinstance(x, bool): return x # is_active가 bool 형태면 그냥 반환
        return str(x).strip().lower() in ["1", "true", "t", "y", "yes"] # 리스트 안에 해당하는 값이면 True, 아니면 False 반환

    df["is_active"] = df["is_active"].apply(to_active_flag) # is_active 필터링
    df = df[df["is_active"]].copy()

    s = df["ipo"].astype(str).str.strip() # ipo 문자열 변환 및 공백 다듬기
    s = s.replace({"None": "", "nan": "", "NaT": ""}) # 결측치 빈문자열로 변환
    s = s.str.replace("-", "", regex=False) # 2026-03-11을 20260311로 변환
    df["listing_date"] = pd.to_datetime(s, errors="coerce", format="%Y%m%d").dt.date # 시/분/초 정보 없이 년-월-일만 남김 

    df = df.dropna(subset=["listing_date"]).reset_index(drop=True) # 상장일 정보 없는 애들 드롭
    return df[["stock_code", "listing_date"]] # 최종적으로 종목 코드와 정제된 상장일 정보만 담긴 데이터프레임을 반환. 각각 005930, 1975-06-11 형태

def get_latest_kosdaq_trading_date(lookback_days: int = 10) -> str:
    today = pd.Timestamp.today() - pd.Timedelta(days=1) # 오늘 빼고 셈 전날 기준으로 보여줄 예정이라서
    start = (today - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    idx = stock.get_index_ohlcv_by_date(start, end, "2001", name_display=False)
    if idx is None or idx.empty:
        raise ValueError(f"KOSDAQ 지수 데이터 없음: start={start}, end={end}")

    idx.index = pd.to_datetime(idx.index)
    return idx.index.max().strftime("%Y-%m-%d")

# 메인 데이터 구축 함수 (이전 90일, 건너뛰기 처리 반영)
def build_company_stock_base(conn, reference_date: str) -> pd.DataFrame:
    end_dt = pd.to_datetime(reference_date)
    start_dt = end_dt - pd.Timedelta(days=90) # 90일 이전
    
    start_dt_str = start_dt.strftime("%Y-%m-%d")
    end_dt_str = end_dt.strftime("%Y-%m-%d")

    df_list = fetch_listing_from_basic(conn) # is_active 인 종목의 종목코드, 상장일 데이터프레임 반환
    kosdaq_tickers = set(stock.get_market_ticker_list(date=end_dt.strftime("%Y%m%d"), market="KOSDAQ")) # 제일 최근 개장일 기준 pykrx의 kosdaq 종목 목록 가져오기

    base = df_list[df_list["stock_code"].isin(kosdaq_tickers)].copy() # 아까 is_active한 종목들이랑 pykrx의 교집합만 가져옴
    if base.empty:
        raise ValueError("활성 코스닥 종목을 찾지 못했습니다.")

    # DB에 이미 저장되어 있는 내역 가져오기(90일 이전 데이터들 중 이미 저장된 내용들을 (종목코드, 기준일) 쌍으로 가져옴)
    existing_set = get_existing_records(conn, start_dt_str, end_dt_str)
    
    finance_map = fetch_latest_finance_data(conn) # 당기순이익, 자본 총계 가져오기 위한 부분

    # 90일 동안의 KOSDAQ 실제 거래일 목록 확보
    idx_df = stock.get_index_ohlcv_by_date(start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"), "2001") # 시작일~종료일까지의 코스닥 거래일 가져옴
    if idx_df is None or idx_df.empty: # 여기서 90일인데 만약 없으면 API 문제니깐 에러 발생시키고 중단 
        raise ValueError("코스닥 거래일을 가져오지 못함. API 오류일 가능성이 있음.")
    trading_days = idx_df.index.strftime("%Y-%m-%d").tolist() # 데이터프레임의 인덱스를 "2024-05-20" 같은 문자열 형태의 리스트로 변환

    rows = [] # 성공한 결과물들을 모아서 최종 결과표(result)를 만들기 위한 용도 / 성공적으로 수집된 데이터프레임(df_multi)들을 쌓아두는 용도
    fail_list = [] # (종목코드, 에러 메시지) 형태의 튜플로 저장
    total_base = len(base) # 총 수집해야 하는 종목 개수

    for i, stock_code in enumerate(base["stock_code"], start=1): # 0 인덱스를 1로 표기
        if i % 100 == 0: # 진행상황 확인용
            print(f"... processing {i}/{total_base}")
        
        # 이미 DB에 존재하는 일자는 제외하고, 적재해야 할 누락된 일자(missing_dates)만 계산
        missing_dates = [d for d in trading_days if (stock_code, d) not in existing_set] # 거래일만 있는 리스트에서 주식코드, 거래일 쌍이 이미 db에 있는지 확인해서 없는 날짜만 가져옴
        
        if not missing_dates:
            continue # 모두 DB에 존재할 경우 깔끔히 건너뛰기
        
        max_retries = 3 # 최대 재시도 횟수
        df_multi = None
        
        for attempt in range(max_retries):
            try:
                # 기본 0.5초 대기 (재시도할 때는 1초, 1.5초로 늘어남)
                time.sleep(0.5 * (attempt + 1))
                df_multi = assemble_company_stock_rows(stock_code, missing_dates, start_dt, end_dt, finance_map)
                
                if df_multi is not None and not df_multi.empty: # 빈프레임 아니면 for문 끝내기
                    break 
                else:
                    if attempt < max_retries - 1:
                        print(f"[{stock_code}] 데이터 누락 감지. 재시도 중... ({attempt+1}/{max_retries})")
                        
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[{stock_code}] 통신 에러({e}). 재시도 중... ({attempt+1}/{max_retries})")
                else:
                    fail_list.append((stock_code, str(e))) # 최종 실패하면 실패 리스트에 추가
        
        if df_multi is not None and not df_multi.empty: # 빈프레임이 아니면 추가
            rows.append(df_multi)
        else:
            # 3번 다 실패했거나 빈 데이터만 받았다면 최종 실패 처리
            if not any(f[0] == stock_code for f in fail_list): # 중복 추가 방지
                fail_list.append((stock_code, "empty result after retries"))

    print(f"성공 건수: {sum(len(r) for r in rows if r is not None)}")
    print(f"실패/누락 종목 수: {len(fail_list)}")
    
    if fail_list: # 실패 내역 파일 저장
        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fail_log_file = data_dir / f"company_stock_collection_fail_list_{now_str}.csv"
        
        fail_df = pd.DataFrame(fail_list, columns=["stock_code", "error_message"])
        fail_df.to_csv(fail_log_file, index=False, encoding="utf-8-sig")
        print(f"수집 실패 내역이 저장됨 : {fail_log_file}")

    if not rows:
        print("업데이트할 새로운 데이터가 없거나 모두 비어있습니다.")
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)
    return result

# COMPANY_STOCK 업서트
def upload_to_company_stock(conn, df: pd.DataFrame, chunk_size: int = 1000):
    if df is None or df.empty:
        print("업로드할 데이터가 없습니다.")
        return

    sql = """
        INSERT INTO COMPANY_STOCK (
            stock_code, reference_date, open_price, prev_close_price, close_price,
            price_change, high_price, low_price, wk52_high, wk52_low, book_value,
            mktcap, shares_btj, trdvol, acc_trdvol, acc_trdval, bas_trdval,
            foreign_ratio, fluc_rt, dps, eps, dividend_yield, per, pbr
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price), prev_close_price = VALUES(prev_close_price),
            close_price = VALUES(close_price), price_change = VALUES(price_change),
            high_price = VALUES(high_price), low_price = VALUES(low_price),
            wk52_high = VALUES(wk52_high), wk52_low = VALUES(wk52_low),
            book_value = VALUES(book_value), mktcap = VALUES(mktcap),
            shares_btj = VALUES(shares_btj),
            trdvol = VALUES(trdvol), acc_trdvol = VALUES(acc_trdvol),
            acc_trdval = VALUES(acc_trdval), bas_trdval = VALUES(bas_trdval),
            foreign_ratio = VALUES(foreign_ratio), fluc_rt = VALUES(fluc_rt),
            dps = VALUES(dps), eps = VALUES(eps), dividend_yield = VALUES(dividend_yield),
            per = VALUES(per), pbr = VALUES(pbr)
    """ # 일단 이 duplicate key 부분은 처음부터 다 거르고 수행하는거라 없어도 될거 같은데 보험용으로 킵

    df = df.copy()
    df["stock_code"] = df["stock_code"].astype(str).str.strip().str.zfill(6)
    df["reference_date"] = pd.to_datetime(df["reference_date"]).dt.date # YYYY-MM-DD 형태로 변환

    ordered_cols = [
        "stock_code", "reference_date", "open_price", "prev_close_price",
        "close_price", "price_change", "high_price", "low_price",
        "wk52_high", "wk52_low", "book_value", "mktcap",
        "shares_btj",  "trdvol", "acc_trdvol",
        "acc_trdval", "bas_trdval", "foreign_ratio", "fluc_rt",
        "dps", "eps", "dividend_yield", "per", "pbr",
    ] # 열 정렬

    for col in ordered_cols: # 혹시나 모를 열 없을 때를 대비한 안전장치
        if col not in df.columns:
            df[col] = None

    df = df[ordered_cols].copy()
    df = df.replace([np.inf, -np.inf], np.nan).infer_objects(copy=False) # 나누기 오류 등으로 생긴 무한대 값을 NaN으로 바꿈
    df = df.astype(object) # 데이터프레임을 object로 바꿈 (None 변경용)
    df = df.where(pd.notnull(df), None) # Nan 같은 값을 다 None으로 변경

    def to_mysql_value(v): # 파이썬 자료형으로 변환하는 함수
        if v is None: return None
        if pd.isna(v): return None
        if isinstance(v, np.integer): return int(v)
        if isinstance(v, np.floating): return float(v)
        return v

    data = [tuple(to_mysql_value(row[col]) for col in ordered_cols) for _, row in df.iterrows()] # 데이터프레임 순회하며 튜플의 리스트로 최종 변환
    
    success_count = 0
    fail_chunks = []

    with conn.cursor() as cursor:
        for i in range(0, len(data), chunk_size): # chunk_size 만큼씩 db에 적재 
            chunk_data = data[i:i + chunk_size]
            try:
                cursor.executemany(sql, chunk_data)
                conn.commit()  # 에러가 없으면 이 청크(1000개)만 즉시 DB에 저장 확정
                success_count += len(chunk_data)
                
            except Exception as e:
                conn.rollback()  # 에러가 발생하면 이번 청크(1000개)만 취소하고 다음으로 넘어감
                print(f"데이터 삽입 중 오류 발생 (해당 청크 건너뜀): {e}")
                
                # 에러가 나서 못 들어간 청크 데이터만 백업을 위해 따로 모아둠
                fail_chunks.extend(chunk_data)

    print(f"COMPANY_STOCK upsert 완료: 성공 {success_count:,}건 / 실패 {len(fail_chunks):,}건")
    
    if fail_chunks:
        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = data_dir / f"company_stock_fail_{now_str}.csv"
        
        fail_df = pd.DataFrame(fail_chunks, columns=ordered_cols)
        fail_df.to_csv(backup_file, index=False, encoding="utf-8-sig")
        print(f"누락된 {len(fail_chunks)}건의 데이터가 저장됨 : {backup_file}")

def main():
    krx_id = os.getenv("ID")
    krx_pw = os.getenv("PW")

    if not krx_id or not krx_pw:
        raise ValueError("환경변수 ID/PW가 없습니다. .env 확인하세요.")

    if login_krx(krx_id, krx_pw):
        print("KRX 로그인 성공!")
    else:
        raise RuntimeError("KRX 로그인 실패")

    conn = get_connection()

    try:
        create_company_stock(conn) # db 생성

        reference_date = get_latest_kosdaq_trading_date() # 최근 거래일 (2026-03-11 형태) 반환
        print(f"최근 거래일(어제 기준) (reference_date) = {reference_date}") # 액면가는 당일치 안나오고 고가,저가 등 계속 바뀌기 때문에 어제를 기준으로 함

        df = build_company_stock_base(conn, reference_date)

        if df is not None and not df.empty:
            na_rows = df[df.isna().any(axis=1)]
            
            if not na_rows.empty:
                # 종목별로 그룹화하여 어떤 열이 비어있는지 파악
                missing_info = []
                for (stock_code, ref_date), group in na_rows.groupby(['stock_code', 'reference_date']):
                    # 해당 일자의 데이터 중 결측치가 있는 컬럼명만 리스트로 추출
                    missing_cols = group.columns[group.isna().any()].tolist()
                    missing_info.append({
                        'stock_code': stock_code,
                        'reference_date': ref_date,
                        'missing_columns': ", ".join(missing_cols) # 보기 편하게 콤마로 연결
                    })
                missing_df = pd.DataFrame(missing_info)
                
                print(f"일부 열이 비어있는 데이터 개수(종목/일자별): {len(missing_df)}개")
                
                # 결측치가 있는 종목 코드만 리스트로 추출
                unique_stocks = missing_df['stock_code'].unique().tolist()
                
                with conn.cursor() as cursor:
                    if unique_stocks:
                        # IN 절을 만들기 위한 포맷팅 (예: %s, %s, %s)
                        format_strings = ','.join(['%s'] * len(unique_stocks))
                        query = f"SELECT stock_code, corp_name FROM BASIC WHERE stock_code IN ({format_strings})"
                        
                        # 쿼리 실행 시 unique_stocks 리스트를 튜플로 전달
                        cursor.execute(query, tuple(unique_stocks))
                        basic_rows = cursor.fetchall()
                        
                        if basic_rows:
                            if isinstance(basic_rows[0], dict):
                                basic_df = pd.DataFrame(basic_rows)
                            else:
                                basic_df = pd.DataFrame(basic_rows, columns=["stock_code", "corp_name"])
                            basic_df['stock_code'] = basic_df['stock_code'].astype(str).str.zfill(6)
                        else:
                            basic_df = pd.DataFrame(columns=["stock_code", "corp_name"])
                    else:
                        basic_df = pd.DataFrame(columns=["stock_code", "corp_name"])
                
                # 데이터 병합 (결측치 정보 + 회사명)
                merged_missing_df = pd.merge(missing_df, basic_df, on='stock_code', how='left')
                
                # 열 순서를 깔끔하게 정리 (종목코드, 회사명, 기준일, 비어있는 열)
                merged_missing_df = merged_missing_df[['stock_code', 'corp_name', 'reference_date', 'missing_columns']]
                
                # CSV 저장
                now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                save_path = data_dir / f"stocks_with_empty_columns_{now_str}.csv"
                merged_missing_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"일부 열이 비어있는 데이터 리스트를 '{save_path}'에 저장했습니다.")
                
            upload_to_company_stock(conn, df)
        else:
            print("새로 업데이트할 내역이 존재하지 않아 DB 삽입을 생략합니다.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()