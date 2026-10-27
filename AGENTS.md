# AGENTS.md

이 저장소에서 일하는 모든 AI 에이전트(Claude Code, Codex, Cursor 등)를 위한 안내서입니다.
작업 전에 끝까지 읽으십시오. 사람용 소개는 [README.md](README.md)에 있습니다.

## 1. 이 저장소는 무엇인가

- 데이터 엔지니어 과정 수강생(사용자 seoloo)의 수업 자료 보관소입니다. 목적은 **나중에 다시 보며 분석·복습**하는 것입니다.
- 애플리케이션이나 라이브러리가 아닙니다. 빌드, 테스트, 배포는 없습니다.
- GitHub: https://github.com/seoloo/Data_Engineer_Class (예전 이름 `Data_Engineer_Study`, 로컬 폴더 이름은 아직 이전 이름)
- 환경: Windows 11, PowerShell 5.1, Python 3.13. 문서와 커밋 메시지는 한국어로 씁니다.

## 2. 가장 중요한 규칙

1. **과목 폴더(`01_*` ~ `05_*`)를 직접 고치지 마십시오.** 이 폴더들은 `C:\` 원본의 복사본(미러)입니다. 여기서 고치거나 파일을 추가해도 다음 `sync.ps1` 실행 때 덮어쓰이거나 지워집니다. 고쳐야 하면 `C:\` 원본에서 고치고, 그것도 사용자가 요청했을 때만 합니다.
2. **커밋과 푸시는 사용자가 합니다.** 사용자가 명시적으로 "커밋해줘", "올려줘"라고 하지 않으면 커밋하거나 푸시하지 마십시오.
3. **커밋에 AI 표시를 넣지 마십시오.** `Co-Authored-By: Claude ...`, `Generated with ...` 같은 줄을 넣지 않습니다. 작성자는 사용자 한 명만 보여야 합니다.
4. **강제 푸시, 히스토리 재작성은 하지 마십시오.** 사용자가 직접 요청한 경우만 예외입니다.
5. **비밀값을 올리지 마십시오.** `.env`, 비밀번호, API 키, 토큰은 커밋하지 않습니다. dstd의 MySQL 비밀번호는 환경변수 `DSTD_DB_PW`로만 다루고, 파일에 적지 않습니다.
6. **추측으로 채우지 마십시오.** 일별 기록과 문서는 실제 파일 내용, 수정 시각, 사용자 설명만 근거로 씁니다.

## 3. 원본 폴더와 동기화

수업은 가상환경과 경로 문제 때문에 `C:\` 바로 아래 폴더에서 진행합니다. 이 저장소에서는 실행하지 않습니다.

| 원본 (실제 작업 위치) | 저장소 폴더 | 비고 |
|---|---|---|
| `C:\pybo\main` | `01_pybo/main` | `C:\pybo\config`는 가상환경이라 제외 |
| `C:\jsonpd` | `02_jsonpd` | |
| `C:\dstd` | `03_dstd` | |
| `C:\sparkcamp` | `04_sparkcamp` | |
| `C:\kafkacamp` | `05_kafkacamp` | Kafka · Spark 연동 실습 |
| `C:\hadoop` | (올리지 않음) | winutils 바이너리. [setup/hadoop.md](setup/hadoop.md)에 설치 방법만 기록 |
| `C:\Users\Playdata\Desktop\프로젝트\수업\` 의 일부 파일 | `class_materials/` | 손으로 복사. `sync.ps1` 대상 아님 |

동기화는 저장소 루트에서 실행합니다.

```powershell
powershell -ExecutionPolicy Bypass -File .\sync.ps1
```

- `robocopy /MIR`로 원본을 미러링합니다. 원본에서 지운 파일은 저장소에서도 지워집니다.
- 제외 대상: 폴더 `.venv*`, `venv`, `__pycache__`, `.ipynb_checkpoints`, `.git`, `tmp`, `spark-warehouse`, `out`, `metastore_db`, `ivy`, `lake`, `jars`, `chk` / 파일 `*.pyc`, `*.crc`, `db.sqlite3`, `.env`, `*.log`, `*.exe`, `*.dll`.
- Spark의 `ivy/`는 다운로드한 라이브러리 캐시, `lake/`는 실행으로 생성한 Delta 테이블입니다. 원본에서 분석 근거로 읽을 수 있지만 저장소에는 복사하지 않습니다.
- Kafka 연동 실습의 `jars/`는 별도로 받은 라이브러리, `chk/`는 스트리밍 체크포인트입니다. 두 폴더도 동기화에서 제외합니다.
- robocopy 종료 코드 0~7은 성공이고, 8 이상이면 실패입니다.
- 새 과목이 생기면 `sync.ps1`의 `$Map`에 `'C:\새폴더' = '06_새폴더'`를 추가하고, 이 문서 3절 표와 README 목차도 같이 고칩니다.
- `sync.ps1`은 ASCII로만 씁니다. PowerShell 5.1은 BOM이 없는 UTF-8 파일의 한글을 깨뜨립니다.
- **그날의 최신 상태는 항상 `C:\` 원본에 있습니다.** 동기화 전이라면 원본을 읽으십시오.

## 4. 디렉터리 구조

```
Data_Engineer_Study/
├─ AGENTS.md           # 이 문서 (에이전트 공용 규칙)
├─ CLAUDE.md           # Claude Code용 진입점 → AGENTS.md를 불러옴
├─ README.md           # 사람용 소개, 과목 목차
├─ sync.ps1            # C:\ 원본 → 저장소 미러링
├─ .gitignore
├─ daily-log/          # 수업일별 기록 (YYYY-MM-DD.md)
├─ class_materials/    # 강의 슬라이드(pptx), 설명 그림
├─ setup/              # 실행 환경 설치 메모
├─ 01_pybo/            # 미러 — 수정 금지
├─ 02_jsonpd/          # 미러 — 수정 금지
├─ 03_dstd/            # 미러 — 수정 금지
├─ 04_sparkcamp/       # 미러 — 수정 금지
└─ 05_kafkacamp/       # 미러 — 수정 금지
```

## 5. 과목별 안내

Python 실습 파일은 VS Code의 `# %%` 셀 형식입니다. 셀 단위로 `Shift+Enter`를 눌러 위에서부터 순서대로 실행합니다(노트북 `.ipynb`가 아닙니다). Kafka의 `.txt` 실습은 각 문제의 `[어디서]`에 지정된 터미널에서 진행합니다.
실행할 때는 `activate`하지 않고 원본 폴더의 `.\.venv\Scripts\python.exe`를 직접 부릅니다.

### 01_pybo — Django 기초 (2026-09-10)
- Django 6.1, SQLite. 프로젝트는 `main/main`(settings, urls), 앱은 `main/app`입니다.
- 라우트: `/test/`(views.index, HttpResponse), `/tt/`(views.tt, `templates/html/test.html` 렌더링).
- `settings.py`의 `SECRET_KEY`는 `django-insecure-`로 시작하는 로컬 개발용 키입니다.

### 02_jsonpd — JSON → pandas (2026-10-06)
- `h00`~`h10`이 1교시~10교시입니다. JSON 문자열/파일 → 중첩 펼치기 → CSV → pandas 선택·정리·merge·groupby → 파이프라인 순서이고, `hA`는 선택 과제(API 호출)입니다.
- `make_data.py`가 `data/`를 만들고, 결과는 `output/`에 저장됩니다.
- 패키지: pandas 3.0, matplotlib, requests.

### 03_dstd — 데이터 표준화와 AI Ready Data (2026-10-07 ~, 5일 과정)
- 일차별 노트북은 `notebooks/day01_탐색` → `day02_표준사전` → `day03_품질규칙` → `day04_모델링` → `day05_DDL` 순서입니다.
- `src/`에는 데이터 생성기(`make_dataset.py`), 검사기(`check_*.py`), DB 도우미(`db.py`)가 있습니다.
- `templates/`는 원본 양식입니다. 학생 산출물은 `docs/`에 정해진 이름으로 저장합니다(검사기가 그 이름으로 찾습니다): `표준사전.xlsx`, `매핑표.xlsx`, `변환규칙정의서.xlsx`, `AS-IS품질보고서.md`, `모델_ASIS.md`, `모델_TOBE.md`, `ddl.sql`.
- MySQL 8.4를 쓰고 접속 정보는 `127.0.0.1:3306`, 사용자 root, DB `dstd_lab`입니다. SQL 파일은 `sql/`에 있습니다.
- 원천 CSV를 엑셀로 열어 저장하면 안 됩니다. `0000123`처럼 앞의 0이 사라집니다.
- 자세한 내용은 [03_dstd/시작하기.md](03_dstd/시작하기.md)에 있습니다.

### 04_sparkcamp — Apache Spark (2026-10-20 ~)
- `lab/sNN_*.py`는 사용자가 푼 실습(빈칸 채우기)이고, `answer/sNN_*.py`는 정답입니다. 정답에는 `# ── 결과 ──` 주석으로 기대 출력이 들어 있습니다.
- 진도: s03 첫 실행 → s04 DataFrame → s05 파일 읽기 → s06 select/filter → s07 스키마 → s08 groupBy → s09 시간 → s10 join → s11 Spark SQL → s12 시간 창 → s13 정제 → s14 파티션 → s15 Parquet → s16·s17 Delta → s18 Streaming → s19 미니 파이프라인 → s20 종합.
- 데이터는 교통카드 태그 로그입니다. `data/tags_day.jsonl`(승차 IN / 하차 OUT, `tag_time`은 이벤트 시각, `recv_time`은 도착 시각)과 `data/stations.csv`, 스트리밍 원천 `data/stream_src/`가 있습니다.
- 실행 환경: Java 17(`JAVA_HOME`), `HADOOP_HOME=C:\hadoop`, pyspark 4.1.3, delta-spark 4.4.1, `local[2]`.
- 자세한 내용은 [04_sparkcamp/시작하기.txt](04_sparkcamp/시작하기.txt)에 있습니다.

### 05_kafkacamp — Apache Kafka (2026-10-26 ~)
- 원본은 `C:\kafkacamp`입니다. `lab/`는 실습 문제, `answer/`는 정답이며, 각각 Python 파일 13개와 터미널용 `.txt` 파일 5개가 있습니다.
- 교재 구성: s02 Docker·연결 → s03 토픽·메시지 → s04 파티션·키·오프셋 → s05 컨슈머 그룹 → s06 프로듀서 → s07 컨슈머 → s08 키 설계 → s09 재생기 → s10 복제·장애 → s11 전달 보장 → s12 Spark 읽기 → s13 시간 창·워터마크 → s14 Kafka 쓰기 → s15 Bronze·Silver 파이프라인 → s16 OPTIMIZE·ZORDER·Gold → s17 성능 측정 → sH 종합 문제. 파일이 있다는 사실과 실제 수업 진도는 구분합니다.
- `kafka1.yml`은 브로커 한 대, `kafka3.yml`은 복제 실습용 세 대의 Docker Compose 구성입니다. 이미지 태그는 두 파일 모두 `apache/kafka:4.3.1`입니다. PC에서는 기본 `localhost:9092`, 세 대 구성에서는 `9092`·`9093`·`9094`로 연결합니다.
- `requirements.txt` 기준 패키지는 pyspark 4.1.3, delta-spark 4.4.1, confluent-kafka 2.16.0, pandas 2.3.3, pyarrow 26.0.0, ipykernel 7.4.0입니다. 설치 여부는 별도로 확인합니다.
- `make_data.py`는 `data/stations.csv`, `tags_mini.jsonl`, `tags_mini_keyed.txt`, `tags_day.jsonl`, `stream_src/part_01.jsonl`~`part_10.jsonl`을 생성합니다. 기존 Spark 수업과 같은 교통카드 태그 로그 구조이며, `tools/replay.py`는 카드 번호를 메시지 키로 사용해 Kafka에 재생하는 도우미입니다.
- 실행 명령과 실습 위치는 [kafka1.yml](05_kafkacamp/kafka1.yml), [lab/s02_docker.txt](05_kafkacamp/lab/s02_docker.txt)를 참고합니다. 초기화 셀에는 토픽·결과·체크포인트 삭제가 포함될 수 있으므로 분석할 때 코드를 통째로 실행하지 않습니다.

## 6. 일별 기록 (daily-log)

2026-10-22부터 수업한 날마다 `daily-log/YYYY-MM-DD.md`를 하나씩 씁니다. 그 이전 수업일은 기록하지 않습니다.

**언제 쓰나:** 사용자가 "수업 끝났어"처럼 하루가 끝났다고 알리면, 아래 순서로 진행합니다.
1. `sync.ps1`을 실행합니다.
2. 그날 기록을 씁니다.
3. 커밋은 사용자가 요청할 때만 합니다.

**그날 한 일을 찾는 법**
- `C:\` 원본에서 그날 수정된 파일을 찾습니다. 예: Git Bash에서 `find /c/sparkcamp -path '*/.venv*' -prune -o -path '*/tmp' -prune -o -newermt 2026-10-22 -type f -print`
- sparkcamp는 `lab` 파일을 같은 번호의 `answer`와 비교해서, 어디까지 풀었는지, 틀린 곳과 빈칸이 어디인지 확인합니다.
- 사용자가 따로 설명한 내용이 있으면 그 설명이 우선입니다.

**양식** ([2026-10-22.md](daily-log/2026-10-22.md) 참고)

```markdown
# YYYY-MM-DD (요일) — 그날 주제 한 줄

- 과목: [폴더](../폴더) (과목명)
- 진도: STEP/일차/교시 범위, 어디까지 했는지
- 실습 파일: 그날 수정된 파일
- 데이터: 사용한 데이터 (필요할 때)

## STEP/주제별 소제목
- 배운 핵심 개념과 함수, 왜 그렇게 하는지

## 복습 포인트 (내 코드에서)
- 정답과 다른 부분, 빈칸으로 남은 셀, 헷갈리기 쉬운 점

## 다음에 할 것
- 남은 셀, 다음 STEP
```

## 7. Git

- 브랜치는 `main` 하나만 씁니다. 원격 주소는 `origin` = `https://github.com/seoloo/Data_Engineer_Class.git`입니다.
- 사용자는 GitHub Desktop으로 커밋합니다. GitHub Desktop은 이 저장소 폴더만 보므로, `C:\`에서 바뀐 내용은 `sync.ps1`을 실행한 뒤에야 변경 사항으로 보입니다.
- 커밋 메시지는 한국어로, 무엇을 반영했는지 씁니다. 예: `2026-10-23 수업 반영 (sparkcamp s13~s15)`. AI 표시 줄은 넣지 않습니다.
- GitHub 파일 크기 제한은 100MB입니다. 50MB가 넘는 파일은 올리기 전에 사용자에게 물어보십시오.
- 줄바꿈 경고(`LF will be replaced by CRLF`)는 무시해도 됩니다.
