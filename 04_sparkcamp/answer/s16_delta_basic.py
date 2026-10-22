# ==============================================================================
#  STEP 16  Delta Lake ① — 장부가 붙은 Parquet
#  C:\sparkcamp\answer\s16_delta_basic.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys
os.chdir(r"C:\sparkcamp")
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
builder = (SparkSession.builder
           .master("local[2]")
           .appName("sparkcamp")
           .config("spark.local.dir", r"C:\sparkcamp\tmp")
           .config("spark.jars.ivy", r"C:\sparkcamp\ivy")
           .config("spark.ui.showConsoleProgress", "false")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "+ Delta 준비 완료")

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
RAW_SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time STRING, fare INT, recv_time STRING
"""   # Bronze 용 — 시각 칸을 글자 그대로 (깨진 원문을 남긴다)
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())

import shutil, json
raw_text = spark.read.schema(RAW_SCHEMA).json("data/tags_day.jsonl")   # Bronze 용 — 시각을 글자 그대로
def ls(path):
    return [f for f in sorted(os.listdir(path)) if not f.startswith(".")]
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 1978


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for p in ["lake/bronze/tags", "lake/ref/stations"]:
    shutil.rmtree(p, ignore_errors=True)
print("lake/bronze/tags · lake/ref/stations 비움")
# ── 결과 ──
# lake/bronze/tags · lake/ref/stations 비움


# %% [16-1] 시각을 글자 그대로 읽은 원본(raw_text — 준비 셀)을 Delta 형식으로 lake/bronze/tags 에 저장하고, 폴더 안을 보십시오.
raw_text.write.format("delta").mode("overwrite").save("lake/bronze/tags")
ls("lake/bronze/tags")
# ── 결과 (Delta 화면) ──
# ['_delta_log', 'part-00000-1f6c…(긴 이름)…-c000.snappy.parquet']
# → Parquet 파일 하나와 _delta_log 폴더. 이것이 Delta 테이블의 전부입니다. 원문을 고치지 않고 넣었으니 Bronze 입니다. STEP
# 07 의 SCHEMA(시각 = TIMESTAMP)로 읽은 raw 를 넣으면 깨진 시각이 null 로 바뀐 채 저장되어 원문이 사라집니다. 그래서 Bronze
# 는 RAW_SCHEMA(시각 = STRING) 로 읽은 것을 넣고, 시각 바꾸기는 Silver 에서 합니다.

# %% [16-2] 장부 폴더(_delta_log) 안의 .json 파일 이름을 보십시오.
[f for f in ls("lake/bronze/tags/_delta_log") if f.endswith(".json")]
# ── 결과 (Delta 화면) ──
# ['00000000000000000000.json']
# → 숫자 20자리 = 버전 0. 앞으로 쓰기·고치기를 할 때마다 00…01.json, 00…02.json 이 하나씩 늘어납니다.

# %% [16-3] 장부 첫 파일 안에 어떤 종류의 기록이 들어 있는지 보십시오. (한 줄 = 기록 하나)
with open("lake/bronze/tags/_delta_log/00000000000000000000.json", encoding="utf-8") as f:
    kinds = sorted({list(json.loads(line))[0] for line in f})
kinds
# ── 결과 (Delta 화면) ──
# ['add', 'commitInfo', 'metaData', 'protocol']
# → commitInfo(누가 언제 무엇을) · metaData(스키마) · protocol(Delta 규칙 버전) · add(이 파일을 더했다). "파일을
# 더했다" 는 기록이 있어야 그 파일이 보입니다. 기록 없는 파일은 폴더에 있어도 없는 것입니다.

# %% [16-4] Delta 로 다시 읽어 줄 수와 스키마를 보고, 깨진 시각(분이 67) 이 원문 그대로 남아 있는지 찾으십시오.
bronze = spark.read.format("delta").load("lake/bronze/tags")
bronze.printSchema()
print(bronze.count())
bronze.filter(F.col("tag_time").contains(":67:")).select("event_id", "tag_time").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# root
#  |-- event_id: string (nullable = true)
#  |-- card_id: string (nullable = true)
#  |-- trip_id: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: string (nullable = true)
#  |-- fare: integer (nullable = true)
#  |-- recv_time: string (nullable = true)
#
# 2008
# +--------+-------------------+
# |event_id|           tag_time|
# +--------+-------------------+
# |  E00192|2026-10-14 07:67:00|
# +--------+-------------------+
# → 2008. tag_time 이 string 이라 07:67:00 이 그대로 남아 있습니다. 나중에 "이 줄은 왜 빠졌나" 를 물으면 Bronze 를 열어
# 보여 줄 수 있습니다. Bronze = 증거 보관함.

# %% [16-5] 장부 보기 — 이 테이블의 역사(history)를 보십시오.
dt = DeltaTable.forPath(spark, "lake/bronze/tags")
dt.history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      0|    WRITE|
# +-------+---------+
# → 버전 0, WRITE. history() 에는 칸이 많습니다(시각·사용자·건수…). 오늘은 두 칸만 봅니다.

# %% [16-6] 실수로 연습용 작은 로그(tags_mini, 12줄)를 Bronze 에 append 하고, 줄 수와 역사를 보십시오.
mini = spark.read.schema(RAW_SCHEMA).json("data/tags_mini.jsonl")
mini.write.format("delta").mode("append").save("lake/bronze/tags")
print(spark.read.format("delta").load("lake/bronze/tags").count())
dt.history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 2020
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      1|    WRITE|
# |      0|    WRITE|
# +-------+---------+
# → 2020 = 2008 + 12, 버전 1 이 생겼습니다. 역사는 최신이 위입니다.

# %% [16-7] 시간 여행 — 버전 0 과 버전 1 의 줄 수를 각각 세십시오.
v0 = spark.read.format("delta").option("versionAsOf", 0).load("lake/bronze/tags")
v1 = spark.read.format("delta").option("versionAsOf", 1).load("lake/bronze/tags")
print(v0.count(), v1.count())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 2008 2020
# → 2008 · 2020. 버전 0 은 "실수하기 전" 입니다. 통장의 지난 잔고를 보는 것과 같습니다.

# %% [16-8] 실수로 넣은 12줄(event_id 가 M 으로 시작)을 지우고, 줄 수와 역사를 보십시오.
dt.delete("event_id LIKE 'M%'")
print(spark.read.format("delta").load("lake/bronze/tags").count())
dt.history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 2008
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      2|   DELETE|
# |      1|    WRITE|
# |      0|    WRITE|
# +-------+---------+
# → 다시 2008, 버전 2 DELETE. Parquet 이었다면 전부 다시 써야 했을 일을 한 줄로 했습니다.

# %% [16-9] 지운 뒤에도 버전 1 은 여전히 읽히는지 보십시오.
spark.read.format("delta").option("versionAsOf", 1).load("lake/bronze/tags").count()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 2020
# → 2020 — 지운 것도 장부에 남아 있습니다. 그래서 실수해도 되돌릴 수 있습니다(dt.restoreToVersion(0)). 오래된 파일을 정말로 치우는
# 명령은 VACUUM 인데, 기본 7일 지난 것만 지웁니다.

# %% [16-10] 역 마스터를 Delta 로 lake/ref/stations 에 저장하고, 서울역(S05)의 이름을 서울역(KTX) 로 고친(UPDATE) 뒤 확인하십시오.
st.write.format("delta").mode("overwrite").save("lake/ref/stations")
ref = DeltaTable.forPath(spark, "lake/ref/stations")
ref.update(condition="station_id = 'S05'", set={"station_nm": "'서울역(KTX)'"})
spark.read.format("delta").load("lake/ref/stations").filter("station_id IN ('S04','S05')").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +----------+-----------+-----+----+
# |station_id| station_nm| line|  gu|
# +----------+-----------+-----+----+
# |       S04|       시청|1호선|중구|
# |       S05|서울역(KTX)|1호선|중구|
# +----------+-----------+-----+----+
# → set 의 새 값은 SQL 식이라 글자는 작은따옴표를 한 번 더 씁니다("'서울역(KTX)'"). 빼먹으면 서울역(KTX) 라는 칸을 찾다가 오류가
# 납니다.

# %% [16-11] 일부러 틀려 보십시오. Bronze 에 칸이 하나 더 있는(hh) 데이터를 append 하십시오.
raw_text.withColumn("hh", F.substring("tag_time", 12, 2)) \
   .write.format("delta").mode("append").save("lake/bronze/tags")
# ── 결과 (Delta 화면 — 공식 문서 기준) ──
# AnalysisException: [DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table (Table ID: 3c1e…).
# To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
# '.option("mergeSchema", "true")'.
# → 스키마가 다르다며 막습니다(schema enforcement). Parquet 폴더였다면 그냥 들어가서 나중에 읽을 때 엉망이 됩니다. 정말 칸을 늘려야
# 할 때만 .option("mergeSchema", "true") 를 붙입니다. (메시지 앞의 [ ] 이름은 Delta 버전에 따라 조금 다를 수 있습니다)

# %% [16-12] 어떤 폴더가 Delta 테이블인지 물어보십시오 — Delta 로 쓴 것과 STEP 15 의 Parquet 폴더.
print(DeltaTable.isDeltaTable(spark, "lake/bronze/tags"))
print(DeltaTable.isDeltaTable(spark, "out/s15/clean_parquet"))
# ── 결과 (Delta 화면) ──
# True
# False
# → True · False. 장부(_delta_log)가 있어야 Delta 입니다. 이 질문을 STEP 19 에서 "처음이면 새로 만들고, 있으면 MERGE"
# 를 가를 때 씁니다.
