import schedule
import time
import subprocess
import sys

def run_daily_batch():
    """정해진 시간에 Step 4와 Step 5를 순차적으로 자동 실행합니다."""
    print(f"\n{'='*60}")
    print(f"[시스템] {time.strftime('%Y-%m-%d %H:%M')} - V3.5 데일리 배치 작업을 기동합니다.")
    print(f"{'='*60}")
    
    # 1. 데이터 수집 모듈 기동 (Step 4)
    print(" -> 1단계: 딥 스캔 엔진 가동")
    # sys.executable을 사용하여 현재 가상환경의 python으로 안전하게 실행합니다.
    subprocess.run([sys.executable, "step4_v3_excel.py"])
    
    # 2. 타겟 스크리닝 모듈 기동 (Step 5)
    print("\n -> 2단계: 스크리닝 엔진 연계 가동")
    subprocess.run([sys.executable, "step5_screening.py"])
    
    print(f"\n{'='*60}")
    print(f"[시스템] 오늘의 퀀텀 헌터 배치 작업이 성공적으로 종료되었습니다.")
    print(f" -> 다음 실행 시점을 대기합니다...")
    print(f"{'='*60}\n")

# 실행 시간 셋팅 (장 마감 데이터가 완전히 확정되는 오후 3시 40분)
schedule.every().day.at("15:40").do(run_daily_batch)

# 테스트를 원하신다면 아래 줄의 주석(#)을 풀고, 현재 시간보다 1~2분 뒤로 맞춰보세요.
# schedule.every().day.at("12:15").do(run_daily_batch)

if __name__ == "__main__":
    print("[시스템] V3.5 퀀텀 헌터 자동화 데몬(Daemon)이 활성화되었습니다.")
    print(" -> 타겟 실행 시간: 매일 15:40 (오후 3시 40분)")
    print(" -> 주의: 이 터미널 창을 닫지 말고 백그라운드에 최소화해 두십시오. (강제 종료: Ctrl + C)\n")
    
    # 무한 루프: 설정된 시간이 될 때까지 백그라운드에서 감시
    while True:
        schedule.run_pending()
        time.sleep(30) # 30초마다 시간 체크 (CPU 점유율 최소화)