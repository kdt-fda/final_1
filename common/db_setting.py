import os
import pymysql
from dotenv import load_dotenv
from dbutils.pooled_db import PooledDB
from pathlib import Path

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
