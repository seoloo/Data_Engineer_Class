# Data Engineer Study

데이터 엔지니어 과정 수업 자료를 모아 두고 나중에 다시 보며 공부하기 위한 저장소입니다.

## 과목 목차

| 폴더 | 기간 | 상태 | 주제 | 주요 내용 |
|---|---|---|---|---|
| [01_pybo](01_pybo) | 2026-09-10 | 수업 완료 | Django 기초 | 프로젝트/앱 생성, `urls.py` 라우팅, `HttpResponse`와 템플릿 렌더링 |
| [02_jsonpd](02_jsonpd) | 2026-10-06 | 수업 완료 | JSON → pandas | 딕셔너리/JSON 문자열·파일, 중첩 JSON 펼치기, CSV, pandas 선택·정리·merge·groupby, 파이프라인(h00~h10) |
| [03_dstd](03_dstd) | 2026-10-07 ~ | 수업 완료 | 데이터 표준화 · AI Ready Data | 원천 탐색, 표준사전·매핑표, 품질규칙·변환규칙, AS-IS/TO-BE 모델링, MySQL DDL (day01~day05) |
| [04_sparkcamp](04_sparkcamp) | 2026-10-20 ~ | 수업 중 | Apache Spark | DataFrame, 스키마, groupBy, 시간, join, Spark SQL, 시간 창, 정제, 파티션, Parquet, Delta, Streaming (s03~s20) |

- `04_sparkcamp/lab` 은 내가 푼 실습, `04_sparkcamp/answer` 는 정답입니다.
- 과목 오리엔테이션 슬라이드 등 강의 자료는 [class_materials](class_materials)에 있습니다.
- 실행 환경 준비는 각 폴더의 `시작하기` 문서와 [setup/hadoop.md](setup/hadoop.md)를 보세요.

## 완료한 과목 요약

2026-10-22 기준 Django, JSON/pandas, 데이터 표준화 수업을 마쳤습니다. 아래는 실습 코드와 수업 자료를 바탕으로 정리한 복습용 요약입니다. 이후 과목도 수업이 끝나면 이곳에 요약을 추가합니다.

### 01. Django 기초 — 요청을 받아 응답하기

**학습 흐름:** 프로젝트와 앱 구성 → URL 연결 → 뷰 함수 실행 → 문자열 또는 HTML 응답.

- 프로젝트의 `urls.py`가 요청 주소를 뷰 함수에 연결합니다. `/test/`는 `views.index`, `/tt/`는 `views.tt`로 이어집니다.
- `HttpResponse()`로 문자열을 바로 반환하고, `render()`로 `templates/html/test.html`을 읽어 HTML을 반환하는 두 방식을 실습했습니다.
- **복습 포인트:** 주소를 추가할 때 URL 설정, 뷰 함수, 템플릿 경로가 어떻게 이어지는지 따라가 봅니다.
- **다시 볼 파일:** [URL 설정](01_pybo/main/main/urls.py), [뷰 함수](01_pybo/main/app/views.py), [HTML 템플릿](01_pybo/main/app/templates/html/test.html).

### 02. JSON → pandas — 중첩 데이터를 분석용 표로 만들기

**학습 흐름:** JSON 읽기·쓰기 → 중첩 구조 탐색 → 주문 상품 단위로 펼치기 → CSV와 DataFrame → 정제 → 결합·집계 → 파일과 그래프 저장.

- **JSON과 표의 차이:** `json.dumps/loads`는 문자열, `json.dump/load`는 파일을 다룹니다. 중첩된 주문의 `items`를 펼칠 때는 한 행을 주문 상품 한 개로 정하고, 반복문 또는 `pd.json_normalize(record_path="items", meta=...)`를 사용합니다.
- **선택과 정제:** `loc`, 조건식의 `&`·`|`와 괄호로 행과 열을 고릅니다. `drop_duplicates()`, `pd.to_numeric()`·`pd.to_datetime()`의 `errors="coerce"`로 중복과 변환 실패를 확인하고, 판단하기 어려운 행은 격리합니다.
- **결합과 집계:** 상품 정보는 `merge()`로 붙이고 `validate="many_to_one"`으로 키 중복을 검사합니다. 결제 완료 주문의 수량과 금액을 `groupby().agg()`로 집계한 뒤 CSV와 그래프로 저장합니다.
- **복습 포인트:** 주문 수와 주문 상품 행 수를 구분합니다. 중복 제거 키를 잘못 고르면 정상 행이 사라지고, 결합 대상의 키가 중복되면 행 수와 매출이 늘어납니다. 정제 후에는 `원본 = 중복 + 격리 + 정상` 건수를 확인합니다.
- **다시 볼 파일:** [중첩 펼치기](02_jsonpd/h04_flatten.py), [pandas와 json_normalize](02_jsonpd/h06_pandas_first.py), [정제와 격리](02_jsonpd/h08_clean.py), [merge·groupby](02_jsonpd/h09_merge_group.py), [전체 파이프라인](02_jsonpd/h10_pipeline.py).

### 03. 데이터 표준화 · AI Ready Data — 업무 기준을 규칙과 모델로 옮기기

**학습 흐름:** 회원·주문·상품 원천 탐색 → 표준사전·매핑표 → 품질 진단·변환 규칙 → AS-IS/TO-BE 모델 → MySQL DDL과 적재.

| 일차 | 핵심 내용 | 복습할 실습 |
|---|---|---|
| 1일차 | 인코딩·자료형·빈 값·중복·표기 혼재를 확인하고 원천의 의미 파악 | [원천 탐색](03_dstd/notebooks/day01_탐색.py) |
| 2일차 | 단어·도메인·용어·코드 표준을 정하고 원천 컬럼·코드와 대응 | [표준사전과 매핑표](03_dstd/notebooks/day02_표준사전.py) |
| 3일차 | 품질 문제를 진단하고 변환·제거·제외·격리 기준과 적용 순서 정의 | [품질규칙](03_dstd/notebooks/day03_품질규칙.py) |
| 4일차 | 한 행의 의미, 식별자 후보, 데이터 간 관계를 확인해 모델 작성 | [모델링](03_dstd/notebooks/day04_모델링.py) |
| 5일차 | 원천 스테이징, PK·FK 제약조건, DDL 검사, 정제 데이터 적재와 JOIN 건수 확인 | [DDL과 적재](03_dstd/notebooks/day05_DDL.py) |

- **표준화의 기준:** 컬럼 이름과 값은 [업무설명서](03_dstd/docs/업무설명서.md)와 [원천데이터 정의서](03_dstd/docs/원천데이터_정의서.md)를 근거로 해석합니다. 고객번호 같은 식별자는 문자열로 읽어 앞의 0을 보존합니다.
- **품질 처리:** 완전히 같은 중복은 제거하고, 업무상 분석 대상이 아닌 행은 제외하며, 판단할 수 없는 값은 사유와 함께 격리합니다. 변환 규칙은 [rules.py](03_dstd/src/rules.py)에서 확인하고 원천 건수와 처리 결과의 합계를 맞춰 봅니다.
- **모델링과 집계:** 주문 한 건과 주문 항목 한 줄을 구분합니다. 주문번호만으로 항목을 식별할 수 있는지, 주문 단위 배송비를 항목마다 더하고 있지는 않은지 확인합니다. PK·FK와 JOIN도 이 기준에 맞춰 검토합니다.
- **복습 자료:** [산출물 이름과 검사기 안내](03_dstd/시작하기.md), [표준사전·규칙·모델·DDL 양식](03_dstd/templates). `docs/AS-IS품질보고서.md`는 강사 배포 완성 예시이므로 품질 보고서 작성 방식을 참고할 때 활용합니다.

## 일별 기록

수업한 날의 요약은 [daily-log](daily-log)에 날짜별로 있습니다 (2026-10-22부터).

- [2026-10-22 — Spark SQL · 시간 창 · 정제 · 파티션과 셔플](daily-log/2026-10-22.md): STEP 11~14 실습 내용, 남은 셀과 복습 포인트, 다음 진도(STEP 15).

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
