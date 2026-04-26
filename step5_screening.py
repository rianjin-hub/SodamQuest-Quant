import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import os
import glob
import yfinance as yf  # [추가] 60분봉 전용 엔진

# ==========================================
# 1. 파일 관리 유틸리티
# ==========================================
def get_latest_excel_file(folder_path="./output"):
    """output 폴더에서 가장 최근에 생성된 엑셀 파일을 찾습니다."""
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"{folder_path} 폴더가 없습니다. 먼저 데이터를 수집해주세요.")
    
    list_of_files = glob.glob(f"{folder_path}/*.xlsx")
    if not list_of_files:
        raise FileNotFoundError(f"{folder_path} 폴더에 엑셀 파일이 없습니다.")
    
    # 생성 시간 기준으로 가장 최신 파일 선택
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

# ==========================================
# 2. V3.2 핵심 엔진: 60분봉 초단기 수급 분석 (yfinance 적용)
# ==========================================
def analyze_60m_momentum(ticker, market):
    """yfinance를 이용해 60분봉 데이터를 가져오고 타점을 분석합니다."""
    # 1. 야후 파이낸스용 티커 변환 (KOSPI는 .KS, KOSDAQ은 .KQ)
    ticker_str = str(ticker).zfill(6)
    if market == 'KOSPI':
        yf_ticker = f"{ticker_str}.KS"
    elif market == 'KOSDAQ':
        yf_ticker = f"{ticker_str}.KQ"
    else:
        yf_ticker = f"{ticker_str}.KS" # 기본값

    # 2. 60분봉 데이터 호출 (최근 7일치, 1시간 간격)
    df = yf.download(yf_ticker, period='7d', interval='60m', progress=False)
    
    if len(df) < 20: 
        return "데이터 부족", "대기"

    # yfinance 데이터 구조 호환성 처리
    close_prices = df['Close']
    if isinstance(close_prices, pd.DataFrame):
        close_prices = close_prices.iloc[:, 0]
        
    volume = df['Volume']
    if isinstance(volume, pd.DataFrame):
        volume = volume.iloc[:, 0]

    # 3. 쿨링 타점 계산 (RSI 14)
    delta = close_prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs)).iloc[-1]
    
    # 4. 스마트머니 추적 (OBV)
    obv = (np.sign(delta) * volume).fillna(0).cumsum()
    obv_trend = "상승" if obv.iloc[-1] > obv.iloc[-5] else "하락/횡보"

    # 5. 이격도 (20선 기준)
    ma20 = close_prices.rolling(window=20).mean()
    disparity = (close_prices.iloc[-1] / ma20.iloc[-1]) * 100

    # V3.2 종합 판정
    status = "조건 부합" if (rsi < 45 and obv_trend == "상승" and disparity < 102) else "미흡"
    summary = f"RSI:{rsi:.1f} / OBV:{obv_trend} / 이격:{disparity:.1f}%"
    
    return summary, status

# ==========================================
# 3. 메인 파이프라인: 스크리닝 가동
# ==========================================
def run_quantum_hunter_screening():
    # 1. 최신 데이터 로드
    latest_file = get_latest_excel_file()
    print(f"\n[시스템] 데이터 로드 완료: {os.path.basename(latest_file)}")
    
    df = pd.read_excel(latest_file)

    print("\n=== [1차] V3.5 퀀텀 헌터 일봉/수급 스크리닝 ===")
    
    # 조건 A: 추세 방어 (현재가가 20일 이동평균선 위에 있을 것)
    cond_trend = df['현재가'] > df['20일 이평']
    
    # 조건 B: 스마트 머니 유입 (기관과 외국인의 20일 누적 수급이 모두 양수일 것)
    cond_smart_money = (df['기20 누적'] > 0) & (df['외20 누적'] > 0)
    
    # 조건 C: 당일 수급 강도 (기관 또는 외국인 중 하나라도 당일 순매수액이 0보다 클 것)
    cond_today_buy = (df['기관 순매수'] > 0) | (df['외인 순매수'] > 0)
    
    # 1차 타겟 추출
    target_df = df[cond_trend & cond_smart_money & cond_today_buy].copy()
    
    if target_df.empty:
        print(" -> [!] 1차 조건을 만족하는 종목이 없습니다 (현금 관망 권장).")
        return
    else:
        print(f" -> [!] 총 {len(target_df)}개의 1차 타겟 종목이 포착되었습니다. 60분봉 정밀 분석을 시작합니다...\n")

   # ------------------------------------------
    # 4. V3.2 60분봉 정밀 타점 분석 (1차 통과 종목 대상)
    # ------------------------------------------
    v32_summaries = []
    v32_statuses = []

    print(" -> [시스템] KRX 종목 마스터 데이터를 대조하여 야후 파이낸스용 코드를 추출합니다...")
    krx_list = fdr.StockListing('KRX')
    name_to_info = dict(zip(krx_list['Name'], zip(krx_list['Code'], krx_list['Market'])))

    total_count = len(target_df)
    
    # 진행률 계기판 장착
    for i, stock_name in enumerate(target_df['종목명']): 
        print(f"\r -> [V3.2 엔진 가동 중] {i+1}/{total_count} : {stock_name} 분석 중...   ", end="")
        
        info = name_to_info.get(stock_name)
        if info:
            ticker, market = info
            summary, status = analyze_60m_momentum(ticker, market)
        else:
            summary, status = "코드 매핑 실패", "미흡"
            
        v32_summaries.append(summary)
        v32_statuses.append(status)

    print("\n -> [시스템] 60분봉 정밀 분석이 완료되었습니다.")

    # ------------------------------------------
    # 5. 최종 결과 결합 및 전체 데이터 저장 (핵심 디버깅)
    # ------------------------------------------
    target_df['V3.2 판정'] = v32_statuses
    target_df['V3.2 요약'] = v32_summaries

    print("\n=== [최종] V3.2 초단기 타점 분석 결과 (조건 부합 종목) ===")
    final_passed_df = target_df[target_df['V3.2 판정'] == '조건 부합'].copy()

    if final_passed_df.empty:
        print(" -> [!] 오늘은 V3.2 60분봉 초단기 타점(RSI 과열 해소 & OBV 상승)에 부합하는 종목이 없습니다.")
    else:
        view_cols = ['종목명', '현재가', '등락률', 'V3.2 판정', 'V3.2 요약']
        print(final_passed_df[view_cols].to_string(index=False))

    # [수정] 94개 전체 결과를 엑셀로 저장
    output_filename = latest_file.replace('.xlsx', '_V3.2_Master_Report.xlsx')
    target_df.to_excel(output_filename, index=False)
    print(f"\n[시스템] V3.2 전체 분석 마스터 리포트가 저장되었습니다: {os.path.basename(output_filename)}")
    
# ==========================================
# 4. 실행부
# ==========================================
if __name__ == "__main__":
    try:
        run_quantum_hunter_screening()
    except Exception as e:
        print(f"\n최종 오류 발생: {e}")