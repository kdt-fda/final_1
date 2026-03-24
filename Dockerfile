# 1. Base Image
FROM python:3.11-slim

# 2. 환경 변수 설정
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. 작업 디렉토리 생성
WORKDIR /app

# 4. 의존성 설치 (시스템 패키지 포함)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 5. Python 패키지 설치
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install gunicorn

# 6. 프로젝트 복사
COPY ./web_service .
COPY ./common ./common
COPY .env .env

RUN python manage.py collectstatic --noinput --settings=config.settings

# 7. 실행 명령 (Gunicorn 사용)
# 'config.wsgi' 부분은 프로젝트의 wsgi.py 경로에 맞춰 수정하세요.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "config.wsgi:application"]