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

def get_connection():
    return pool.connection()

def init_dart(): # API 부분
    api_key = os.getenv('DART_API')
    return OpenDartReader(api_key)

def init_solar(): # API 부분
    solar_api = os.getenv('SOLAR_API')
    return OpenAI(api_key=solar_api, base_url="https://api.upstage.ai/v1", timeout=120, max_retries=1)

def init_gpt(): # API 부분
    gpt_api = os.getenv('GPT_API')
    return OpenAI(api_key=gpt_api)

def get_ai_answer(client, prompt, user_content):
    try:
        response = client.chat.completions.create(
            model="solar-pro3", # 사용하시는 모델명 확인
            messages=[
                {'role': 'system', 'content': prompt},
                {"role": "user", "content": user_content}
            ],
            temperature=0,
            max_tokens=10000,
            reasoning_effort="low"
        )
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"SOLAR 호출 오류: {e}")
        return None

def get_ai_answer_gpt(gpt, prompt, user_content):
    try:
        response = gpt.chat.completions.create(
            model="gpt-5.2",
            messages=[
                {'role': 'system', 'content': prompt},
                {"role": "user", "content": user_content}
            ],
            max_completion_tokens=10000,
            reasoning_effort="low",
        )
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"GPT 호출 오류: {e}")
        return None

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

    # CD011 중복 로그인 처리
    if error_code == "CD011":
        payload["skipDup"] = "Y"
        resp = _krx_session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")

    return error_code == "CD001"