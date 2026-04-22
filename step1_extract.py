import FinanceDataReader as fdr
import pandas as pd

def get_base_data_fdr() -> pd.DataFrame:
    """
    FinanceDataReader를 사용하여 최근 거래일 기준 KRX 전 종목 데이터를 수집합니다.
    """
    print("[시스템] FinanceDataReader 엔진으로 KRX 데이터 추출을 시작합니다...")

    # 1. 전 종목 시세 및 시가총액 데이터 호출 (최근 거래일 기준 자동 수집)
    df_krx = fdr.StockListing('KRX')

    # 2. V3.0 Spec에 맞게 필요한 컬럼 추출 및 이름 변경
    # FDR 원본 컬럼: 'Name'(종목명), 'Close'(현재가), 'ChagesRatio'(등락률), 'Marcap'(시가총액), 'Volume'(거래량), 'High'(고가), 'Low'(저가)
    
    result_df = df_krx[['Name', 'Close', 'ChagesRatio', 'Marcap', 'Volume', 'High', 'Low']].copy()
    result_df.rename(columns={
        'Name': '종목명',
        'Close': '현재가',
        'ChagesRatio': '등락률',
        'Marcap': '시가총액',
        'Volume': '거래량',
        'High': '고가',
        'Low': '저가'
    }, inplace=True)

    # 3. 데이터 정제 (결측치 제거 및 시가총액 기준 내림차순 정렬)
    result_df.dropna(inplace=True)
    result_df.sort_values(by='시가총액', ascending=False, inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df

if __name__ == "__main__":
    try:
        final_df = get_base_data_fdr()
        print("\n=== 엔진 교체 성공! 상위 5개 종목 출력 ===")
        print(final_df.head())
    except Exception as e:
        print(f"\n최종 오류 발생: {e}")