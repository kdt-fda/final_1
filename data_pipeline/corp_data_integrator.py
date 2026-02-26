import sys
import pandas as pd
import FinanceDataReader as fdr
from tqdm import tqdm
from pathlib import Path

# 경로 잡기
temp_base = Path(__file__).resolve().parents[1]
if str(temp_base) not in sys.path: # import할 때 이 안에 있는 경로도 탐색하라는 뜻
    sys.path.append(str(temp_base))

from common.setting import init_dart, BASE_DIR

def get_kosdaq_base(dart): # dart, krx 매칭
    dart_all = dart.corp_codes
    
    # krx_kosdaq 코스닥 다 가져오기
    krx_kosdaq = fdr.StockListing('KOSDAQ') 
    
    # SPAC이랑 이름에 '호스팩' 들어간 불필요한 기업 제거
    krx_kosdaq = krx_kosdaq[(krx_kosdaq['Dept'] != 'SPAC(소속부없음)') & (~krx_kosdaq['Name'].str.contains('호스팩'))] 

    # 데이터 전처리 (타입 맞추기 및 결측치 제거)
    dart_all = dart_all[dart_all['stock_code'].notnull()].copy() 
    dart_all['stock_code'] = dart_all['stock_code'].astype(str).str.zfill(6) 
    krx_kosdaq['Code'] = krx_kosdaq['Code'].astype(str).str.zfill(6)

    # 매칭 실패 확인을 위한 outer merge 및 출력
    merge_result = pd.merge(
        dart_all[['corp_name', 'corp_code', 'stock_code']], 
        krx_kosdaq[['Code', 'Name']], 
        right_on='Code', 
        left_on='stock_code', 
        how='outer',
        indicator=True
    )
    only_krx = merge_result[merge_result['_merge'] == 'right_only']
    print(f"KRX에만 있는 기업(DART 매칭 실패): {len(only_krx)}개")
    if not only_krx.empty:
        print(only_krx[['Code', 'Name']].head())

    # 실제 사용 데이터 병합
    kosdaq_df = merge_result[merge_result['_merge'] == 'both'].copy()
    kosdaq_df = kosdaq_df.drop(['Code', '_merge'], axis=1) 
    
    print(f"매칭된 코스닥 기업 수: {len(kosdaq_df)}개")
    return kosdaq_df

def fetch_additional_info(df, dart):
    # 거래소 상장 정보(KRX-DESC) 불러오기
    print("거래소 상장 정보(KRX-DESC) 불러오기")
    try:
        df_krx = fdr.StockListing('KRX-DESC')
        listing_map = df_krx.set_index('Code')['ListingDate'].dt.strftime('%Y-%m-%d').to_dict()
    except Exception as e:
        print(f"거래소 데이터를 가져오는 중 오류 발생: {e}")
        listing_map = {}

    # 통계를 위한 변수 초기화
    counts = {}
    matches = {}
    missing_codes = [] # 조회가 안 된 종목들을 따로 담을 리스트

    print("상장일(ipo), 설립일(est_dt), 업종코드(ind_code) 매칭 및 통계 생성 시작")
    results = []
    
    for _, row in tqdm(df.iterrows(), total=len(df)):
        d_code = str(row['corp_code']).zfill(8)
        s_code = str(row['stock_code']).zfill(6)
        c_name = row['corp_name']
        
        item = {
            'stock_code': s_code,
            'corp_code': d_code,
            'corp_name': c_name,
            'ipo': listing_map.get(s_code, ""), 
            'est_dt': "",
            'ind_code': ""
        }
        
        try:
            # dart의 기업코드로 산업분류 및 기업정보 조회
            company_info = dart.company(d_code)
            if company_info:
                # 업종 코드(2자리로만)
                industry_code = company_info.get('induty_code', '')[:2]
                item['ind_code'] = industry_code
                
                # 딕셔너리 카운팅 및 매칭 리스트 추가
                if industry_code:
                    if industry_code in counts:
                        counts[industry_code] += 1
                        matches[industry_code].append(company_info['stock_name'])
                    else:
                        counts[industry_code] = 1
                        matches[industry_code] = [company_info['stock_name']]
                else:
                    missing_codes.append(company_info['stock_name'])

                # 설립일 날짜 포맷팅
                raw_date = str(company_info.get('est_dt', ''))
                if len(raw_date) == 8:
                    item['est_dt'] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
                else:
                    item['est_dt'] = raw_date
        except Exception as e:
            missing_codes.append(c_name)
            pass
            
        results.append(item)

    print("-" * 30)
    print("최종 결과:", counts)
    print("누락된 종목 총 개수:", len(missing_codes))

    data_dir = BASE_DIR / "data"
    data_dir.mkdir(exist_ok=True)

    # industry_counts.csv 저장
    counts_df = pd.DataFrame(list(counts.items()), columns=['industry_code', 'count'])
    counts_df = counts_df.sort_values(by='industry_code', ascending=True)
    counts_df.to_csv(data_dir / 'industry_counts.csv', index=False, encoding='utf-8-sig')

    # industry_matches.csv 저장
    matches_readable = {k: ", ".join(v) for k, v in matches.items()}
    matches_df = pd.DataFrame(list(matches_readable.items()), columns=['industry_code', 'corp_names'])
    matches_df = matches_df.sort_values(by='industry_code', ascending=True)
    matches_df.to_csv(data_dir / 'industry_matches.csv', index=False, encoding='utf-8-sig')
        
    return pd.DataFrame(results)

def main():
    dart = init_dart()
    
    # 코스닥 기본 맵 생성
    kosdaq_df = get_kosdaq_base(dart)
    
    # 상세 정보 매칭 및 업종별 통계 파일 생성
    final_df = fetch_additional_info(kosdaq_df, dart)
    
    # 최종 결과물 저장
    new_columns_order = ['stock_code', 'corp_code', 'corp_name', 'est_dt', 'ipo', 'ind_code']
    df_final = final_df[new_columns_order]
    
    output_path = BASE_DIR / "data" / "kosdaq_corp_map_final.csv"
    df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n다음 파일들이 'data' 폴더에 생성되었습니다:")
    print(f"1. {output_path.name} (DB 적재용)")
    print(f"2. industry_counts.csv (업종별 통계)")
    print(f"3. industry_matches.csv (업종별 기업 매칭)")

if __name__ == "__main__":
    main()