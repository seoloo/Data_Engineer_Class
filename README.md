# Data Engineer Study

데이터 엔지니어 과정 수업 자료를 모아 두고 나중에 다시 보며 공부하기 위한 저장소입니다.

## 과목 목차

| 폴더 | 기간 | 주제 | 주요 내용 |
|---|---|---|---|
| [01_pybo](01_pybo) | 2026-09-10 | Django 기초 | 프로젝트/앱 생성, `urls.py` 라우팅, `HttpResponse`와 템플릿 렌더링 |
| [02_jsonpd](02_jsonpd) | 2026-10-06 | JSON → pandas | 딕셔너리/JSON 문자열·파일, 중첩 JSON 펼치기, CSV, pandas 선택·정리·merge·groupby, 파이프라인(h00~h10) |
| [03_dstd](03_dstd) | 2026-10-07 ~ | 데이터 표준화 · AI Ready Data | 원천 탐색, 표준사전·매핑표, 품질규칙·변환규칙, AS-IS/TO-BE 모델링, MySQL DDL (day01~day05) |
| [04_sparkcamp](04_sparkcamp) | 2026-10-20 ~ | Apache Spark | DataFrame, 스키마, groupBy, 시간, join, Spark SQL, 시간 창, 정제, 파티션, Parquet, Delta, Streaming (s03~s20) |

- `04_sparkcamp/lab` 은 내가 푼 실습, `04_sparkcamp/answer` 는 정답입니다.
- 과목 오리엔테이션 슬라이드 등 강의 자료는 [class_materials](class_materials)에 있습니다.
- 실행 환경 준비는 각 폴더의 `시작하기` 문서와 [setup/hadoop.md](setup/hadoop.md)를 보세요.

## 일별 기록

수업한 날의 요약은 [daily-log](daily-log)에 날짜별로 있습니다 (2026-10-22부터).

## 작업 흐름

수업은 가상환경·경로 문제로 `C:\pybo`, `C:\jsonpd`, `C:\dstd`, `C:\sparkcamp` 에서 진행합니다.
이 저장소에는 [sync.ps1](sync.ps1)로 복사해서 올립니다.

```powershell
powershell -ExecutionPolicy Bypass -File .\sync.ps1
git status
git add -A
git commit -m "2026-10-22 수업 반영"
git push
```

- `sync.ps1`은 원본 폴더를 미러링합니다 (원본에서 지운 파일은 저장소에서도 지워집니다).
- 가상환경(`.venv`), `__pycache__`, Spark 실행 결과(`out`, `tmp`, `spark-warehouse`), `db.sqlite3`, `.env`, `exe/dll`은 복사하지 않습니다.
- 새 과목이 생기면 `sync.ps1`의 `$Map`에 `'C:\새폴더' = '05_새폴더'` 한 줄을 추가합니다.

## AI 에이전트용 안내

에이전트(Claude Code, Codex 등)가 따를 규칙, 구조, 일별 기록 양식은 [AGENTS.md](AGENTS.md)에 있습니다.
