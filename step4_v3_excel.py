import FinanceDataReader as fdr
import pandas as pd
import requests
import time
from io import StringIO
from datetime import datetime, timedelta
import os
import sys

def get_naver_supply_advanced(ticker: str) -> tuple:
    url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'
    }
    
    try:
        res = requests.get(url, headers=headers)
        res.encoding = 'euc-kr'
        dfs = pd.read_html(StringIO(res.text))
        
        for df in dfs:
            if len(df.columns) == 9:
                df.columns = [str(i) for i in range(9)]
                valid_df = df.dropna(subset=['0', '5', '6']).copy()
                valid_df = valid_df[valid_df['0'].astype(str).str.contains(r'\d{4}\.\d{2}\.\d{2}', na=False)]
                
                if not valid_df.empty:
                    valid_df['5'] = valid_df['5'].astype(str).str.replace(',', '').str.replace('+', '')
                    valid_df['6'] = valid_df['6'].astype(str).str.replace(',', '').str.replace('+', '')
                    valid_df['5'] = pd.to_numeric(valid_df['5'], errors='coerce').fillna(0).astype(int)
                    valid_df['6'] = pd.to_numeric(valid_df['6'], errors='coerce').fillna(0).astype(int)
                    
                    inst_today = valid_df['5'].iloc[0]
                    fore_today = valid_df['6'].iloc[0]
                    inst_20d = valid_df['5'].head(20).sum()
                    fore_20d = valid_df['6'].head(20).sum()
                    return inst_today, fore_today, inst_20d, fore_20d
    except:
        pass
    return 0, 0, 0, 0

def get_historical_indicators(ticker: str) -> tuple:
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
    try:
        df_hist = fdr.DataReader(ticker, start_date)
        if len(df_hist) >= 20:
            ma3 = round(df_hist['Close'].rolling(3).mean().iloc[-1])
            ma10 = round(df_hist['Close'].rolling(10).mean().iloc[-1])
            ma20 = round(df_hist['Close'].rolling(20).mean().iloc[-1])
            vol_today = df_hist['Volume'].iloc[-1]
            vol_yest = df_hist['Volume'].iloc[-2]
            vol_ratio = round((vol_today / vol_yest) * 100, 2) if vol_yest != 0 else 0.0
            return vol_ratio, ma3, ma10, ma20
    except:
        pass
    return 0.0, 0, 0, 0

def run_v3_pipeline_extended(limit: int = 500) -> pd.DataFrame:
    print(f"[시스템] KOSPI/KOSDAQ 상위 {limit}개 중소형주 포함 딥 스캔을 시작합니다.")
    print(" -> (1/4) 대상 종목 추출 중...")
    
    df_price = fdr.StockListing('KRX')
    df_price = df_price[['Code', 'Name', 'Close', 'ChagesRatio', 'Marcap', 'Volume', 'High', 'Low']].copy()
    df_price.dropna(subset=['Marcap'], inplace=True)
    df_price.sort_values(by='Marcap', ascending=False, inplace=True)
    target_df = df_price.head(limit).copy()

    print(" -> (2/4) 시계열 연산 및 수급 스크래핑 (WAF 우회 모드)...")
    print(" -> [주의] 약 5~8분 정도 소요됩니다. 커피 한 잔 드시고 오십시오.\n")
    
    results = []
    total = len(target_df)
    
    for idx, ticker in enumerate(target_df['Code'], 1):
        vol_ratio, ma3, ma10, ma20 = get_historical_indicators(ticker)
        inst_today, fore_today, inst_20d, fore_20d = get_naver_supply_advanced(ticker)
        
        results.append({
            '거래량%': vol_ratio, '3일 이평': ma3, '10일 이평': ma10, '20일 이평': ma20,
            '기관 순매수': inst_today, '외인 순매수': fore_today,
            '기20 누적': inst_20d, '외20 누적': fore_20d
        })
        
        # 실시간 진행률 표시 (한 줄 덮어쓰기 기법)
        progress = (idx / total) * 100
        sys.stdout.write(f"\r    진행률: [{idx}/{total}] 종목 스캔 완료 ({progress:.1f}%)")
        sys.stdout.flush()
        
        time.sleep(0.3) # 0.5초에서 0.3초로 WAF 한계치까지 속도 최적화

    print("\n\n -> (3/4) 데이터 프레임 병합 및 정렬 중...")
    df_results = pd.DataFrame(results, index=target_df.index)
    final_df = pd.concat([target_df, df_results], axis=1)

    final_df.rename(columns={
        'Name': '종목명', 'Close': '현재가', 'ChagesRatio': '등락률',
        'Marcap': '시가총액', 'Volume': '거래량', 'High': '고가', 'Low': '저가'
    }, inplace=True)
    
    final_df['기관 순매수액'] = final_df['기관 순매수'] * final_df['현재가']

    v3_columns = [
        '종목명', '현재가', '등락률', '시가총액', '거래량', '거래량%',
        '3일 이평', '10일 이평', '20일 이평', 
        '기관 순매수', '기관 순매수액', '기20 누적', 
        '외인 순매수', '외20 누적', '고가', '저가'
    ]
    return final_df[v3_columns]

def save_to_excel(df: pd.DataFrame):
    save_path = "./output"
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    file_name = f"KRX_V3_REPORT_TOP500_{timestamp}.xlsx"
    full_path = os.path.join(save_path, file_name)
    df.to_excel(full_path, index=False)
    print(f" -> (4/4) 엑셀 저장 완료: {full_path}")
    print(" === 딥 스캔 종료 ===")

if __name__ == "__main__":
    try:
        # 500개 종목으로 타겟 확대
        final_df = run_v3_pipeline_extended(limit=500)
        save_to_excel(final_df)
    except Exception as e:
        print(f"\n최종 오류 발생: {e}")