import os
import glob
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 권한 범위 및 이전에 획득한 내 폴더 ID
SCOPES = ['https://www.googleapis.com/auth/drive.file']
PARENT_FOLDER_ID = '1ZOkNtAN8Wne2qGmrlL7UJBK8VvWDl20F' # 주의: 반드시 폴더 ID를 다시 넣어주세요!

def get_latest_excel_file(folder_path="./output"):
    list_of_files = glob.glob(f"{folder_path}/*.xlsx")
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def authenticate_google_drive():
    """OAuth 2.0 기반으로 인증하고 토큰을 관리합니다."""
    creds = None
    # 이전에 한 번 로그인해서 발급받은 토큰이 있다면 그것을 사용
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # 토큰이 없거나 만료되었다면 브라우저를 열어 로그인 진행
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # 획득한 토큰을 파일로 저장하여 다음부터는 자동 로그인
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
    return creds

def upload_to_drive():
    print("\n -> (1/2) 구글 드라이브 업로드 기동 (OAuth 2.0)...")
    
    latest_file = get_latest_excel_file()
    if not latest_file:
        print(" [!] 에러: 업로드할 엑셀 파일이 없습니다.")
        return
    
    file_name = os.path.basename(latest_file)

    try:
        # 인증 및 서비스 객체 생성
        creds = authenticate_google_drive()
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': file_name,
            'parents': [PARENT_FOLDER_ID] 
        }
        
        media = MediaFileUpload(latest_file, 
                                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                resumable=True)

        print(f" -> (2/2) 전송 중: {file_name}")
        file = service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()
        
        print(f" === [성공] 구글 드라이브 업로드 완료! (ID: {file.get('id')}) ===")

    except Exception as e:
        print(f" [!] 업로드 중 에러 발생: {e}")

if __name__ == "__main__":
    upload_to_drive()