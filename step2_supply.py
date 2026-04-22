import FinanceDataReader as fdr
import pandas as pd
import requests
import time
from io import StringIO

def get_naver_supply(ticker: str) -> tuple:
    """네이버 금융을 스크래핑하여 최신 기관/외국인 순매수(수량)를 추출합니다."""
    url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        res = requests.get(url, headers=headers)
        res.encoding = 'euc-kr'  # 핵심 수정: 네이버 증권의 한글 인코딩 규격 적용
        
        dfs = pd.read_html(StringIO(res.text))
        
        # 전체 표 중에서 '기관'과 '외국인' 컬럼이 있는 진짜 수급표 탐색
        for df in dfs:
            # 다중 헤더(MultiIndex) 구조 평탄화
            if isinstance(df.columns, pd.MultiIndex):
                cols = [col[-1] for col in df.columns]
            else:
                cols = df.columns.tolist()
            
            # 타겟 데이터 식별
            if '기관' in cols and '외국인' in cols and '날짜' in cols:
                df.columns = cols
                
                # 빈 줄(구분선) 제거 및 최신(첫 번째) 거래일 행 추출
                valid_df = df.dropna(subset=['날짜', '기관', '외국인'])
                if not valid_df.empty:
                    latest_row = valid_df.iloc[0]
                    inst_buy = int(str(latest_row['기관']).replace(',', '').replace('+', ''))
                    fore_buy = int(str(latest_row['외국인']).replace(',', '').replace('+', ''))
                    return inst_buy, fore_buy
                    
    except Exception as e:
        print(f"      [!] {ticker} 파싱 오류: {e}")
        
    return 0, 0

def get_final_hybrid_data(limit: int = 5) -> pd.DataFrame:
    print("[시스템] KOSPI/KOSDAQ 가격(FDR) + 수급(Naver) 결합 파이프라인 가동...")

    # 1. 가격 데이터 추출
    print(" -> (1/3) FDR 가격/시가총액 데이터 수집 완료")
    df_price = fdr.StockListing('KRX')
    df_price = df_price[['Code', 'Name', 'Close', 'ChagesRatio', 'Marcap', 'Volume', 'High', 'Low']].copy()
    df_price.dropna(subset=['Marcap'], inplace=True)
    df_price.sort_values(by='Marcap', ascending=False, inplace=True)
    target_df = df_price.head(limit).copy()

    # 2. 수급 데이터 추출 (Naver 우회)
    print(" -> (2/3) 네이버 증권 수급 데이터 스크래핑 중...")
    inst_list, fore_list = [], []

    for ticker in target_df['Code']:
        inst_buy, fore_buy = get_naver_supply(ticker)
        inst_list.append(inst_buy)
        fore_list.append(fore_buy)
        print(f"    - {ticker} 수급 완료 (기관: {inst_buy}, 외인: {fore_buy})")
        time.sleep(0.5)

    # 3. 데이터 병합
    print(" -> (3/3) 최종 데이터 병합 중...")
    target_df['기관 순매수'] = inst_list
    target_df['외인 순매수'] = fore_list

    target_df.rename(columns={
        'Name': '종목명', 'Close': '현재가', 'ChagesRatio': '등락률',
        'Marcap': '시가총액', 'Volume': '거래량', 'High': '고가', 'Low': '저가'
    }, inplace=True)
    
    return target_df[['종목명', '현재가', '등락률', '시가총액', '거래량', '고가', '저가', '기관 순매수', '외인 순매수']]

if __name__ == "__main__":
    df = get_final_hybrid_data(limit=5)
    print("\n=== [Phase 3] 네이버 우회 파이프라인 병합 완료! ===")
    print(df)