import os
import pymysql
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB
from pathlib import Path
import OpenDartReader
from openai import OpenAI
import requests
from pykrx.website.comm import webio


BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

pool = PooledDB(
    creator=pymysql,
    maxconnections=20, # 최대 동시 연결 수
    mincached=5, # 최소 유지 연결 수
    blocking=True, # 연결이 꽉 차면 기다림
    ping=1,  # 커넥션을 풀에서 가져올 때 살아있는지 확인하고, 죽었으면 재연결
    **db_config
)

DJANGO_SECRET_KEY = os.getenv('DJANGO_SECRET_KEY')

def get_connection():
    return pool.connection()

def init_dart(): # API 부분
    api_key = os.getenv('DART_API')
    return OpenDartReader(api_key)

def init_gpt(): # API 부분
    gpt_api = os.getenv('GPT_API')
    return OpenAI(api_key=gpt_api)

def create_batch_task(custom_id, system_prompt, user_content): # OpenAI Batch API용 개별 태스크 객체를 생성하는 함수
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-5.2",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "max_completion_tokens": 10000
        }
    }

# 세션
_krx_session = requests.Session()

def login_krx(login_id: str, login_pw: str) -> bool:
    
    global _krx_session

    def _session_post_read(self, **params):
        return _krx_session.post(self.url, headers=self.headers, data=params)

    def _session_get_read(self, **params):
        return _krx_session.get(self.url, headers=self.headers, params=params)

    webio.Post.read = _session_post_read
    webio.Get.read = _session_get_read
    
    _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
    _LOGIN_JSP  = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
    _LOGIN_URL  = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 초기 세션 발급
    _krx_session.get(_LOGIN_PAGE, headers={"User-Agent": _UA}, timeout=15)
    _krx_session.get(_LOGIN_JSP, headers={"User-Agent": _UA, "Referer": _LOGIN_PAGE}, timeout=15)

    payload = {
        "mbrNm": "", "telNo": "", "di": "", "certType": "",
        "mbrId": login_id, "pw": login_pw,
    }
    headers = {"User-Agent": _UA, "Referer": _LOGIN_PAGE}

    # 로그인 POST
    resp = _krx_session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
    data = resp.json()
    error_code = data.get("_error_code", "")
    
    if error_code != "CD001":
        print(f"\n[디버깅] KRX 로그인 거절 응답 데이터: {data}\n")

    # CD011 중복 로그인 처리
    if error_code == "CD011":
        payload["skipDup"] = "Y"
        resp = _krx_session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")

    return error_code == "CD001"


# krx 안될 경우 대체 코드(krx)
# def login_krx(login_id: str, login_pw: str) -> bool:
    
#     global _krx_session
    
#     # 기존 세션 초기화 (꼬임 방지)
#     _krx_session.cookies.clear()

#     _UA = (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
#     )

#     # 메인 페이지 접속으로 유효한 기본 쿠키(JSESSIONID 등) 먼저 획득
#     main_url = "https://data.krx.co.kr/main/main.jsp"
#     _krx_session.get(main_url, headers={"User-Agent": _UA}, timeout=15)

#     _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
#     _LOGIN_URL  = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"

#     # 로그인 페이지로 리퍼러 설정하여 세션 유지 확인
#     _krx_session.get(_LOGIN_PAGE, headers={"User-Agent": _UA, "Referer": main_url}, timeout=15)

#     payload = {
#         "mbrNm": "", "telNo": "", "di": "", "certType": "",
#         "mbrId": login_id, "pw": login_pw,
#     }
    
#     # 로그인 요청 시 헤더 강화 (Origin 및 X-Requested-With 추가)
#     headers = {
#         "User-Agent": _UA, 
#         "Referer": _LOGIN_PAGE,
#         "Origin": "https://data.krx.co.kr",
#         "X-Requested-With": "XMLHttpRequest"
#     }

#     # 로그인 POST
#     resp = _krx_session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
    
#     try:
#         data = resp.json()
#     except Exception:
#         print(f"JSON 파싱 실패: {resp.text}")
#         return False

#     error_code = data.get("_error_code", "")

#     # CD011 중복 로그인 처리
#     if error_code == "CD011":
#         payload["skipDup"] = "Y"
#         resp = _krx_session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
#         data = resp.json()
#         error_code = data.get("_error_code", "")

#     if error_code != "CD001":
#         print(f"[로그인 실패] 코드: {error_code}, 메시지: {data.get('_error_message', '알 수 없음')}")

#     return error_code == "CD001"