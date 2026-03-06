# final_1
---

# 간단한 브랜치 관리 명령어 (브랜치 삭제는 순서대로)
- git checkout -b feature/hi (feature/hi 라는 브랜치 생성후 이동)
- git branch (내 브랜치 목록 확인 가능, 현재 브랜치는 *로 표시됨)
- git checkout 브랜치명 (브랜치 이동시 사용)

- git push origin --delete 브랜치명 (원격저장소(github)에서 브랜치를 삭제)
- git fetch origin --prune (원격저장소에서 이미 삭제된 브랜치들을 내 로컬저장소에서도 깔끔하게 정리)
- git branch -d 브랜치명 (내가 만든 브랜치를 로컬저장소에서 삭제, 위의 명령어는 내 브랜치 제외하고 정리함)

---

# 병합 시 할 일
- git commit -m "메시지" (병합하기 전에 작업한 브랜치에서 commit)
- git checkout develop (병합할 브랜치로 이동)
- git pull origin develop (최신 내용 가져오기)
- git merge 브랜치명 (합칠 브랜치 명 입력해서 합치기)
- git push origin develop (원격 저장소에 푸시)

---

# 웹 실행 test
- cd web_service (web_service로 이동)
- python manage.py runserver (웹 실행)