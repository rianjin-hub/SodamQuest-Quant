import pandas as pd
import os
import glob

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

def run_quantum_hunter_screening():
    # 1. 최신 데이터 로드 (서버 호출 없이 엑셀 사용)
    latest_file = get_latest_excel_file()
    print(f"[시스템] 데이터 로드 완료: {os.path.basename(latest_file)}")
    
    df = pd.read_excel(latest_file)

    print("\n=== V3.5 퀀텀 헌터 스크리닝 가동 ===")
    
    # 2. 필터링 로직 (조건식)
    # 조건 A: 추세 방어 (현재가가 20일 이동평균선 위에 있을 것)
    cond_trend = df['현재가'] > df['20일 이평']
    
    # 조건 B: 스마트 머니 유입 (기관과 외국인의 20일 누적 수급이 모두 양수일 것)
    cond_smart_money = (df['기20 누적'] > 0) & (df['외20 누적'] > 0)
    
    # 조건 C: 당일 수급 강도 (기관 또는 외국인 중 하나라도 당일 순매수액이 0보다 클 것)
    cond_today_buy = (df['기관 순매수'] > 0) | (df['외인 순매수'] > 0)
    
    # 3. 조건 병합 및 타겟 추출
    target_df = df[cond_trend & cond_smart_money & cond_today_buy].copy()
    
    # 4. 결과 출력
    if target_df.empty:
        print(" -> [!] 현재 시장에서 퀀텀 헌터 조건을 완벽히 만족하는 종목이 없습니다 (현금 관망 권장).")
    else:
        print(f" -> [!] 총 {len(target_df)}개의 타겟 종목이 포착되었습니다.\n")
        # 출력할 핵심 컬럼만 추리기
        view_cols = ['종목명', '현재가', '등락률', '거래량%', '기20 누적', '외20 누적']
        print(target_df[view_cols].to_string(index=False))

if __name__ == "__main__":
    try:
        run_quantum_hunter_screening()
    except Exception as e:
        print(f"\n최종 오류 발생: {e}")