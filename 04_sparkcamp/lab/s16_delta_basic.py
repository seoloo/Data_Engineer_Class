# ==============================================================================
#  STEP 16  Delta Lake ① — 장부가 붙은 Parquet
#  C:\sparkcamp\lab\s16_delta_basic.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
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


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for p in ["lake/bronze/tags", "lake/ref/stations"]:
    shutil.rmtree(p, ignore_errors=True)
print("lake/bronze/tags · lake/ref/stations 비움")


# %% [16-1] 시각을 글자 그대로 읽은 원본(raw_text — 준비 셀)을 Delta 형식으로 lake/bronze/tags 에 저장하고, 폴더 안을 보십시오.
raw_text.write.format("delta").mode("overwrite").save("lake/bronze/tags")
ls("lake/bronze/tags")



# %% [16-2] 장부 폴더(_delta_log) 안의 .json 파일 이름을 보십시오.
[f for f in ls("lake/bronze/tags/_delta_log") if f.endswith(".json")]


# %% [16-3] 장부 첫 파일 안에 어떤 종류의 기록이 들어 있는지 보십시오. (한 줄 = 기록 하나)
with open("lake/bronze/tags/_delta_log/00000000000000000000.json") as f:
    kinds = sorted({list(json.loads(line))[0] for line in f})
kinds

# commitInfo - 누가 언제 무엇을 / metaData - 스키마
# Protocol - Delta 규칙 - add - 추가했다 더했다



# %% [16-4] Delta 로 다시 읽어 줄 수와 스키마를 보고, 깨진 시각(분이 67) 이 원문 그대로 남아 있는지 찾으십시오.
bronze = spark.read.format("delta").load("lake/bronze/tags")
bronze.printSchema()
print(bronze.count())
bronze.filter(F.col("tag_time").contains(":67:"))       \
    .select("event_id", "tag_time").show()
    



# %% [16-5] 장부 보기 — 이 테이블의 역사(history)를 보십시오.
dt = DeltaTable.forPath(spark, "lake/bronze/tags")
dt.history().select("version", "operation").show()



# %% [16-6] 실수로 연습용 작은 로그(tags_mini, 12줄)를 Bronze 에 append 하고, 줄 수와 역사를 보십시오.
mini = spark.read.schema(RAW_SCHEMA).json("data/tags_mini.jsonl")
mini.write.format("delta").mode("append").save("lake/bronze/tags")
print(spark.read.format("delta").load("lake/bronze/tags").count())
dt.history().select("version", "operation").show()




# %% [16-7] 시간 여행 — 버전 0 과 버전 1 의 줄 수를 각각 세십시오.
v0 = spark.read.format("delta").option("versionAsOf", 0).load("lake/bronze/tags")
v1 = spark.read.format("delta").option("versionAsOf", 1).load("lake/bronze/tags")
print(v0.count(), v1.count())




# %% [16-8] 실수로 넣은 12줄(event_id 가 M 으로 시작)을 지우고, 줄 수와 역사를 보십시오.
dt.delete("event_id LIKE 'M%'")
print(spark.read.format("delta").load("lake/bronze/tags"))
dt.history().select("version", "operation").show()



# %% [16-9] 지운 뒤에도 버전 1 은 여전히 읽히는지 보십시오.
spark.read.format("delta").option("versionAsOf", 1).load("lake/bronze/tags").count()





# %% [16-10] 역 마스터를 Delta 로 lake/ref/stations 에 저장하고, 서울역(S05)의 이름을 서울역(KTX) 로 고친(UPDATE) 뒤 확인하십시오.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
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
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
raw_text.withColumn("hh", F.substring("tag_time", 12, 2)) \
   .write.format("delta").mode("append").save("lake/bronze/tags")
# ── 결과 (Delta 화면 — 공식 문서 기준) ──
# AnalysisException: [DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table (Table ID: 3c1e…).
# To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
# '.option("mergeSchema", "true")'.
# → 스키마가 다르다며 막습니다(schema enforcement). Parquet 폴더였다면 그냥 들어가서 나중에 읽을 때 엉망이 됩니다. 정말 칸을 늘려야
# 할 때만 .option("mergeSchema", "true") 를 붙입니다. (메시지 앞의 [ ] 이름은 Delta 버전에 따라 조금 다를 수 있습니다)




# %% [16-12] 어떤 폴더가 Delta 테이블인지 물어보십시오 — Delta 로 쓴 것과 STEP 15 의 Parquet 폴더.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
print(DeltaTable.isDeltaTable(spark, "lake/bronze/tags"))
print(DeltaTable.isDeltaTable(spark, "out/s15/clean_parquet"))
# ── 결과 (Delta 화면) ──
# True
# False
# → True · False. 장부(_delta_log)가 있어야 Delta 입니다. 이 질문을 STEP 19 에서 "처음이면 새로 만들고, 있으면 MERGE"
# 를 가를 때 씁니다.

