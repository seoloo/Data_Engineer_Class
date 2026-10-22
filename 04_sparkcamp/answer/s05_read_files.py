# ==============================================================================
#  STEP 05  파일 읽기 — CSV 와 JSON
#  C:\sparkcamp\answer\s05_read_files.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys
os.chdir(r"C:\sparkcamp")
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder
         .master("local[2]")
         .appName("sparkcamp")
         .config("spark.local.dir", r"C:\sparkcamp\tmp")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "준비 완료")
# ── 결과 ──
# Spark 4.1.3 준비 완료


# %% [05-1] 역 마스터를 옵션 없이 읽어 보여 주십시오.
st = spark.read.csv("data/stations.csv")
st.show()
# ── 결과 ──
# +----------+----------+-----+--------+
# |       _c0|       _c1|  _c2|     _c3|
# +----------+----------+-----+--------+
# |station_id|station_nm| line|      gu|
# |       S01|      강남|2호선|  강남구|
# |       S02|      역삼|2호선|  강남구|
# |       S03|  홍대입구|2호선|  마포구|
# |       S04|      시청|1호선|    중구|
# |       S05|    서울역|1호선|    중구|
# |       S06|      잠실|2호선|  송파구|
# |       S07|    여의도|5호선|영등포구|
# |       S08|    광화문|5호선|  종로구|
# +----------+----------+-----+--------+
# → 칸 이름이 _c0 _c1 _c2 _c3 이고, 머리글 줄이 첫 데이터로 들어갔습니다. 오류가 아닙니다 — 틀린 답이 조용히 나온 것입니다.

# %% [05-1 바꿔 보기 ①] header 를 줄 때와 안 줄 때 줄 수를 나란히 세면?
print(spark.read.csv("data/stations.csv").count(),
      spark.read.csv("data/stations.csv", header=True).count())
# ── 결과 ──
# 9 8
# → 이래서 — 9 와 8. 머리글 한 줄이 데이터로 들어가 역이 하나 늘었습니다. 오류 없이 숫자가 하나 틀리는 — 이 과목에서 계속 나올 "조용한 실수" 의
# 첫 번째입니다.

# %% [05-2] 같은 파일을 머리글을 칸 이름으로 읽으십시오.
st = spark.read.csv("data/stations.csv", header=True)
st.show()
# ── 결과 ──
# +----------+----------+-----+--------+
# |station_id|station_nm| line|      gu|
# +----------+----------+-----+--------+
# |       S01|      강남|2호선|  강남구|
# |       S02|      역삼|2호선|  강남구|
# |       S03|  홍대입구|2호선|  마포구|
# |       S04|      시청|1호선|    중구|
# |       S05|    서울역|1호선|    중구|
# |       S06|      잠실|2호선|  송파구|
# |       S07|    여의도|5호선|영등포구|
# |       S08|    광화문|5호선|  종로구|
# +----------+----------+-----+--------+
# → 8 개 역. 이 표를 역 마스터라고 부릅니다. 오늘 내내 씁니다.

# %% [05-3] 역 마스터의 스키마와 행 수를 보십시오.
st.printSchema()
st.count()
# ── 결과 ──
# root
#  |-- station_id: string (nullable = true)
#  |-- station_nm: string (nullable = true)
#  |-- line: string (nullable = true)
#  |-- gu: string (nullable = true)
#
# 8
# → CSV 는 전부 string 으로 읽힙니다. 숫자 칸이 있다면 inferSchema=True 를 주거나 STEP 07 처럼 직접 정해 줍니다.

# %% [05-4] 작은 로그 tags_mini.jsonl 을 읽어 칸을 자르지 말고 보여 주십시오.
mini = spark.read.json("data/tags_mini.jsonl")
mini.show(truncate=False)
# ── 결과 ──
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# |card_id|event_id|fare|recv_time          |station_id|tag_time           |tag_type|trip_id|
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# |C001   |M01     |0   |2026-10-14 08:01:11|S01       |2026-10-14 08:01:10|IN      |T001   |
# |C002   |M02     |0   |2026-10-14 08:03:06|S06       |2026-10-14 08:03:05|IN      |T002   |
# |C003   |M03     |0   |2026-10-14 08:05:01|S03       |2026-10-14 08:05:00|IN      |T003   |
# |C004   |M04     |0   |2026-10-14 08:07:31|S05       |2026-10-14 08:07:30|IN      |T004   |
# |C005   |M05     |0   |2026-10-14 08:09:46|S01       |2026-10-14 08:09:45|IN      |T005   |
# |C006   |M06     |0   |2026-10-14 08:12:01|S08       |2026-10-14 08:12:00|IN      |T006   |
# |C005   |M07     |1550|2026-10-14 08:12:31|S02       |2026-10-14 08:12:30|OUT     |T005   |
# |C003   |M08     |1550|2026-10-14 08:22:16|S04       |2026-10-14 08:22:15|OUT     |T003   |
# |C002   |M09     |1650|2026-10-14 08:24:51|S01       |2026-10-14 08:24:50|OUT     |T002   |
# |C004   |M10     |1650|2026-10-14 08:29:01|S07       |2026-10-14 08:29:00|OUT     |T004   |
# |C001   |M11     |1650|2026-10-14 08:31:41|S03       |2026-10-14 08:31:40|OUT     |T001   |
# |C005   |M07     |1550|2026-10-14 08:12:58|S02       |2026-10-14 08:12:30|OUT     |T005   |
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# → truncate=False 는 "긴 글자를 ... 로 자르지 말라". 12 줄입니다. M07 이 두 번 있는 것이 보입니까? 맨 아래 줄입니다.

# %% [05-5] 작은 로그의 스키마를 보십시오.
mini.printSchema()
# ── 결과 ──
# root
#  |-- card_id: string (nullable = true)
#  |-- event_id: string (nullable = true)
#  |-- fare: long (nullable = true)
#  |-- recv_time: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- trip_id: string (nullable = true)
# → 칸 순서가 파일과 다르게 알파벳 순입니다. 그리고 tag_time 이 string(글자)입니다. 시각인데 글자로 읽혔습니다 — STEP 07 에서
# 고칩니다.

# %% [05-6] 작은 로그가 몇 줄인지, 이벤트 번호는 몇 가지인지 세십시오.
print(mini.count())
print(mini.select("event_id").distinct().count())
# ── 결과 ──
# 12
# 11
# → 12 와 11. 같은 이벤트가 한 번 더 들어온 것(재전송)입니다. 이 차이를 기억해 두십시오. STEP 13 에서 정리합니다.

# %% [05-7] 하루 로그 tags_day.jsonl 을 day 라는 이름으로 읽고 몇 줄인지 세십시오.
day = spark.read.json("data/tags_day.jsonl")
day.count()
# ── 결과 ──
# 2008

# %% [05-8] 하루 로그의 앞 다섯 줄을 보십시오.
day.show(5)
# ── 결과 ──
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# |card_id|event_id|fare|          recv_time|station_id|           tag_time|tag_type|trip_id|
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# |  C0401|  E00001|   0|2026-10-14 07:00:01|       S03|2026-10-14 07:00:00|      IN|  T0323|
# |  C0066|  E00002|   0|2026-10-14 07:00:03|       S01|2026-10-14 07:00:01|      IN|  T0727|
# |  C0425|  E00003|   0|2026-10-14 07:00:04|       S01|2026-10-14 07:00:02|      IN|  T0943|
# |  C0679|  E00004|   0|2026-10-14 07:00:09|       S07|2026-10-14 07:00:07|      IN|  T0945|
# |  C0529|  E00005|   0|2026-10-14 07:00:23|       S02|2026-10-14 07:00:20|      IN|  T0668|
# +-------+--------+----+-------------------+----------+-------------------+--------+-------+
# only showing top 5 rows
# → 칸이 많아 옆으로 깁니다. 다음 문제처럼 필요한 칸만 골라 보는 습관을 들입니다.

# %% [05-9] 하루 로그에서 event_id · trip_id · tag_type · station_id · tag_time 다섯 칸만 다섯 줄 보십시오.
day.select("event_id", "trip_id", "tag_type", "station_id", "tag_time").show(5)
# ── 결과 ──
# +--------+-------+--------+----------+-------------------+
# |event_id|trip_id|tag_type|station_id|           tag_time|
# +--------+-------+--------+----------+-------------------+
# |  E00001|  T0323|      IN|       S03|2026-10-14 07:00:00|
# |  E00002|  T0727|      IN|       S01|2026-10-14 07:00:01|
# |  E00003|  T0943|      IN|       S01|2026-10-14 07:00:02|
# |  E00004|  T0945|      IN|       S07|2026-10-14 07:00:07|
# |  E00005|  T0668|      IN|       S02|2026-10-14 07:00:20|
# +--------+-------+--------+----------+-------------------+
# only showing top 5 rows
# → select 는 칸을 고르는 일입니다. 칸 순서도 내가 쓴 대로 나옵니다.

# %% [05-10] 일부러 틀려 보십시오. 파일 이름을 tags_dya.jsonl 로 잘못 쳐서 읽어 보십시오.
spark.read.json("data/tags_dya.jsonl")
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/C:/sparkcamp/data/tags_dya.jsonl. SQLSTATE: 42K03
# → PATH_NOT_FOUND — 경로가 없다. 메시지 안에 Spark 가 찾은 전체 경로가 적혀 있습니다. 거기서 오타를 찾으십시오.

# %% [05-11] 폴더째 읽기 — data/stream_src 폴더(파일 10개)를 한 번에 읽어 세십시오.
whole = spark.read.json("data/stream_src")
whole.count()
# ── 결과 ──
# 2008
# → 파일 10개가 한 표가 되었습니다. 숫자가 tags_day 와 같은 2008 — 하루 로그를 10조각 낸 것이기 때문입니다. 큰 데이터는 이렇게 폴더 =
# 표 로 다룹니다.

# %% [05-12] 폴더 안에 파일이 몇 개인지 파이썬으로 세어 보십시오.
files = sorted(os.listdir("data/stream_src"))
print(len(files), files[:3])
# ── 결과 ──
# 10 ['part_01.jsonl', 'part_02.jsonl', 'part_03.jsonl']
# → 이 조각들이 STEP 18 에서 하나씩 흘러 들어오는 실시간 데이터 역할을 합니다.
