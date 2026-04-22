import FinanceDataReader as fdr
import pandas as pd
import requests
import time
from io import StringIO
from datetime import datetime, timedelta

def get_naver_supply_advanced(ticker: str) -> tuple:
    """네이버 금융에서 당일 수급 및 20일 누적 수급을 동시에 추출합니다."""
    url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        res = requests.get(url, headers=headers)
        res.encoding = 'euc-kr'
        dfs = pd.read_html(StringIO(res.text))
        
        for df in dfs:
            cols = [col[-1] if isinstance(df.columns, pd.MultiIndex) else col for col in df.columns]
            if '기관' in cols and '외국인' in cols and '날짜' in cols:
                df.columns = cols
                valid_df = df.dropna(subset=['날짜', '기관', '외국인']).copy()
                
                if not valid_df.empty:
                    # 문자열 데이터를 정수로 변환 (+, , 제거)
                    valid_df['기관'] = valid_df['기관'].astype(str).str.replace(',', '').str.replace('+', '').astype(int)
                    valid_df['외국인'] = valid_df['외국인'].astype(str).str.replace(',', '').str.replace('+', '').astype(int)
                    
                    # 1. 당일 순매수 (최상단 행)
                    inst_today = valid_df['기관'].iloc[0]
                    fore_today = valid_df['외국인'].iloc[0]
                    
                    # 2. 20일 누적 순매수 (상위 20개 행 합산)
                    inst_20d = valid_df['기관'].head(20).sum()
                    fore_20d = valid_df['외국인'].head(20).sum()
                    
                    return inst_today, fore_today, inst_20d, fore_20d
                    
    except Exception as e:
        print(f"      [!] {ticker} 네이버 파싱 오류: {e}")
        
    return 0, 0, 0, 0

def get_historical_indicators(ticker: str) -> tuple:
    """FDR을 통해 과거 시세를 불러와 이동평균선과 거래량 비율을 계산합니다."""
    # 휴일 감안하여 넉넉하게 40일 전 데이터부터 호출
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
    try:
        df_hist = fdr.DataReader(ticker, start_date)
        if len(df_hist) >= 20:
            # 이동평균선 연산 (마지막 행 기준)
            ma3 = round(df_hist['Close'].rolling(3).mean().iloc[-1])
            ma10 = round(df_hist['Close'].rolling(10).mean().iloc[-1])
            ma20 = round(df_hist['Close'].rolling(20).mean().iloc[-1])
            
            # 거래량 비율 연산 (금일 / 전일 * 100)
            vol_today = df_hist['Volume'].iloc[-1]
            vol_yest = df_hist['Volume'].iloc[-2]
            vol_ratio = round((vol_today / vol_yest) * 100, 2) if vol_yest != 0 else 0.0
            
            return vol_ratio, ma3, ma10, ma20
    except Exception as e:
        print(f"      [!] {ticker} 과거 시세 연산 오류: {e}")
        
    return 0.0, 0, 0, 0

def run_v3_pipeline(limit: int = 5) -> pd.DataFrame:
    print("[시스템] V3.0 스펙 16대 지표 산출 파이프라인 가동...")

    print(" -> (1/3) 대상 종목 추출 및 기본 시세 확보 중...")
    df_price = fdr.StockListing('KRX')
    df_price = df_price[['Code', 'Name', 'Close', 'ChagesRatio', 'Marcap', 'Volume', 'High', 'Low']].copy()
    df_price.dropna(subset=['Marcap'], inplace=True)
    df_price.sort_values(by='Marcap', ascending=False, inplace=True)
    target_df = df_price.head(limit).copy()

    print(" -> (2/3) 시계열 연산 및 수급 스크래핑 중 (WAF 우회)...")
    results = []
    
    for ticker in target_df['Code']:
        # 1. 과거 시세 기반 연산
        vol_ratio, ma3, ma10, ma20 = get_historical_indicators(ticker)
        # 2. 수급 기반 연산
        inst_today, fore_today, inst_20d, fore_20d = get_naver_supply_advanced(ticker)
        
        results.append({
            '거래량%': vol_ratio, '3일 이평': ma3, '10일 이평': ma10, '20일 이평': ma20,
            '기관 순매수': inst_today, '외인 순매수': fore_today,
            '기20 누적': inst_20d, '외20 누적': fore_20d
        })
        print(f"    - {ticker} 모든 지표 연산 완료")
        time.sleep(0.5)

    print(" -> (3/3) 최종 데이터 프레임 병합 및 정렬 중...")
    df_results = pd.DataFrame(results, index=target_df.index)
    final_df = pd.concat([target_df, df_results], axis=1)

    # V3.0 스펙명칭 매핑 및 기관 순매수액(계산) 추가
    final_df.rename(columns={
        'Name': '종목명', 'Close': '현재가', 'ChagesRatio': '등락률',
        'Marcap': '시가총액', 'Volume': '거래량', 'High': '고가', 'Low': '저가'
    }, inplace=True)
    
    # 기관 순매수액 = 당일 기관 순매수 수량 * 현재가
    final_df['기관 순매수액'] = final_df['기관 순매수'] * final_df['현재가']

    # V3.0 스펙 순서대로 컬럼 정렬 (16대 지표)
    v3_columns = [
        '종목명', '현재가', '등락률', '시가총액', '거래량', '거래량%',
        '3일 이평', '10일 이평', '20일 이평', 
        '기관 순매수', '기관 순매수액', '기20 누적', 
        '외인 순매수', '외20 누적', '고가', '저가'
    ]
    
    return final_df[v3_columns]

if __name__ == "__main__":
    df = run_v3_pipeline(limit=5)
    print("\n=== [완성] KRX V3.0 16대 핵심 지표 산출 결과 ===")
    
    # 데이터가 잘리지 않고 보이도록 pandas 출력 옵션 설정
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)