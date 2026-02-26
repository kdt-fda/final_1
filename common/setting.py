import os
import pymysql
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB
from pathlib import Path
import OpenDartReader
from openai import OpenAI

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
    **db_config
)

def get_connection():
    return pool.connection()

def init_dart(): # API 부분
    api_key = os.getenv('DART_API')
    return OpenDartReader(api_key)

def init_solar(): # API 부분
    solar_api = os.getenv('SOLAR_API')
    return OpenAI(api_key=solar_api, base_url="https://api.upstage.ai/v1")

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
            max_tokens=65536,
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
            max_completion_tokens=65536,
            reasoning_effort="low",
        )
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"GPT 호출 오류: {e}")
        return None