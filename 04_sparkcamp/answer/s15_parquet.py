# ==============================================================================
#  STEP 15  파일로 남기기 — CSV 와 Parquet, 그리고 폴더 나누기
#  C:\sparkcamp\answer\s15_parquet.py
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

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())
import shutil
def ls(path):
    """폴더 안의 파일 이름을 보여 준다 (점으로 시작하는 검사용 파일은 뺀다)"""
    return [f for f in sorted(os.listdir(path)) if not f.startswith(".")]
def du(path):
    """폴더 전체 크기 (KB)"""
    total = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)
    return round(total / 1024, 1)
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 1978


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
shutil.rmtree("out/s15", ignore_errors=True)
print("out/s15 비움")
# ── 결과 ──
# out/s15 비움


# %% [15-1] 정상 이벤트(clean)를 CSV 로 저장하고 폴더 안을 보십시오. 머리글도 넣으십시오.
clean.write.csv("out/s15/clean_csv", header=True)
ls("out/s15/clean_csv")
# ── 결과 ──
# ['_SUCCESS', 'part-00000-6c04958c-cdfc-4072-9ea5-5efef78554d5-c000.csv']
# → 폴더가 생겼고 안에 part 파일과 _SUCCESS. part 파일 수는 조각 수만큼입니다. clean 은 중복 제거(셔플) 뒤에 AQE(STEP 14)
# 가 작은 조각들을 하나로 합쳐 두어서 파일이 하나입니다.

# %% [15-2] 저장한 CSV 를 다시 읽어 스키마를 보십시오.
spark.read.csv("out/s15/clean_csv", header=True).printSchema()
# ── 결과 ──
# root
#  |-- event_id: string (nullable = true)
#  |-- card_id: string (nullable = true)
#  |-- trip_id: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: string (nullable = true)
#  |-- fare: string (nullable = true)
#  |-- recv_time: string (nullable = true)
# → 전부 string. 시각도 요금도 글자가 되었습니다. CSV 는 자료형을 기억하지 못합니다.

# %% [15-3] 같은 clean 을 Parquet 으로 저장하고 폴더 안을 보십시오.
clean.write.parquet("out/s15/clean_parquet")
ls("out/s15/clean_parquet")
# ── 결과 ──
# ['_SUCCESS', 'part-00000-9bf14480-b4ff-40e6-b889-3b3b98c93d1a-c000.snappy.parquet']
# → 파일 이름 끝이 .snappy.parquet — snappy 라는 방식으로 압축되어 있습니다.

# %% [15-4] Parquet 을 다시 읽어 스키마와 줄 수를 보십시오.
pq = spark.read.parquet("out/s15/clean_parquet")
pq.printSchema()
pq.count()
# ── 결과 ──
# root
#  |-- event_id: string (nullable = true)
#  |-- card_id: string (nullable = true)
#  |-- trip_id: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: timestamp (nullable = true)
#  |-- fare: integer (nullable = true)
#  |-- recv_time: timestamp (nullable = true)
#
# 1978
# → timestamp · integer 그대로. 스키마를 따로 주지 않아도 파일 안에 적혀 있습니다. 1978 줄.

# %% [15-5] 두 폴더의 크기(KB) 를 비교하십시오. (준비 셀의 du 함수)
print("CSV    :", du("out/s15/clean_csv"), "KB")
print("Parquet:", du("out/s15/clean_parquet"), "KB")
# ── 결과 (작성 환경 — 조각 수에 따라 조금 다름) ──
# CSV    : 175.3 KB
# Parquet: 52.8 KB
# → Parquet 이 훨씬 작습니다. 같은 값이 많은 칸(tag_type · station_id)을 꾹꾹 눌러 담았기 때문입니다. 데이터가 커질수록 차이가
# 커집니다.

# %% [15-6] 일부러 틀려 보십시오. 같은 경로에 Parquet 을 한 번 더 쓰십시오.
clean.write.parquet("out/s15/clean_parquet")
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [PATH_ALREADY_EXISTS] Path file:/C:/sparkcamp/out/s15/clean_parquet already exists. Set mode as "overwrite" to overwrite the existing path. SQLSTATE: 42K04
# → PATH_ALREADY_EXISTS. 기본 mode 는 "있으면 멈춘다" 입니다. 실수로 덮어쓰는 사고를 막아 줍니다. 메시지가 mode 를
# overwrite 로 하라고 알려 줍니다.

# %% [15-7] mode 를 append 로 해서 한 번 더 쓰고, 줄 수를 세십시오.
clean.write.mode("append").parquet("out/s15/clean_parquet")
spark.read.parquet("out/s15/clean_parquet").count()
# ── 결과 ──
# 3956
# → 3956 = 1978 × 2. 그대로 한 벌 더 붙었습니다. append 는 "이미 있는 것과 겹치는지" 를 보지 않습니다. 같은 데이터를 두 번 넣으면
# 두 배가 됩니다 — 스트리밍에서 이 일이 생기면 중복입니다. 기억해 두십시오(STEP 17 · 18).

# %% [15-8] mode 를 overwrite 로 써서 원래대로 되돌리고 세십시오.
clean.write.mode("overwrite").parquet("out/s15/clean_parquet")
spark.read.parquet("out/s15/clean_parquet").count()
# ── 결과 ──
# 1978
# → 1978. overwrite 는 지우고 새로 씁니다.

# %% [15-9] partitionBy — tag_type 별로 폴더를 나눠 저장하고, 폴더 모양을 보십시오.
clean.write.partitionBy("tag_type").parquet("out/s15/by_type")
print(ls("out/s15/by_type"))
print(ls("out/s15/by_type/tag_type=IN")[:2])
# ── 결과 ──
# ['_SUCCESS', 'tag_type=IN', 'tag_type=OUT']
# ['part-00000-bb718169-b58d-4204-accc-1b64a0af367a.c000.snappy.parquet']
# → tag_type=IN · tag_type=OUT 두 폴더. 칸 이름=값 이 폴더 이름입니다. 파일 안에서는 tag_type 칸이 빠지고 폴더 이름이 그
# 역할을 합니다.

# %% [15-9 바꿔 보기 ①] 두 칸으로 나누면? — tag_type 아래에 시(hh) 폴더
(clean.withColumn("hh", F.hour("tag_time"))
      .write.mode("overwrite").partitionBy("tag_type", "hh").parquet("out/s15/by_two"))
print(ls("out/s15/by_two"))
print(ls("out/s15/by_two/tag_type=IN"))
# ── 결과 ──
# ['_SUCCESS', 'tag_type=IN', 'tag_type=OUT']
# ['hh=7', 'hh=8', 'hh=9']
# → 이래서 — 폴더가 두 층이 됩니다 — tag_type=IN 안에 hh=7 · hh=8 · … 날짜/시간 데이터는 보통 이렇게 연=…/월=…/일=… 로 나눠
# 둡니다. 너무 잘게 나누면 폴더마다 작은 파일이 생겨 오히려 느려지니, 자주 거르는 칸 한두 개만 씁니다.

# %% [15-10] 나눠 저장한 것을 읽으면서 하차만 거르고, 실행 계획에서 PartitionFilters 를 찾으십시오.
outs = spark.read.parquet("out/s15/by_type").filter(F.col("tag_type") == "OUT")
print(outs.count())
outs.explain()
# ── 결과 ──
# 986
# == Physical Plan ==
# *(1) ColumnarToRow
# +- FileScan parquet [event_id#633,card_id#634,trip_id#635,station_id#636,tag_time#637,fare#638,recv_time#639,tag_type#640] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/C:/sparkcamp/out/s15/by_type], PartitionFilters: [isnotnull(tag_type#640), (tag_type#640 = OUT)], PushedFilters: [], ReadSchema: struct<event_id:string,card_id:string,trip_id:string,station_id:string,tag_time:timestamp,fare:in...
# → PartitionFilters 에 tag_type = OUT 이 들어 있습니다 — IN 폴더는 아예 열지 않았다는 뜻입니다. 날짜로 나눠 두면 "오늘
# 것만" 읽을 때 엄청나게 빨라집니다.

# %% [15-11] 시(hour) 칸을 만들어 시간별 폴더로 저장하고, 폴더 목록을 보십시오. 파일은 폴더마다 하나씩만 생기게 하십시오.
(clean.withColumn("hh", F.hour("tag_time"))
      .coalesce(1)
      .write.partitionBy("hh").parquet("out/s15/by_hour"))
ls("out/s15/by_hour")
# ── 결과 ──
# ['_SUCCESS', 'hh=10', 'hh=7', 'hh=8', 'hh=9']
# → hh=7 · hh=8 · hh=9 · hh=10. coalesce(1) 을 앞에 두면 폴더마다 파일이 하나씩만 생깁니다(STEP 14). 작은 데이터에서
# 파일이 잘게 쪼개지는 것을 막는 흔한 방법입니다.
