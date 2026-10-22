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




# %% [16-2] 장부 폴더(_delta_log) 안의 .json 파일 이름을 보십시오.




# %% [16-3] 장부 첫 파일 안에 어떤 종류의 기록이 들어 있는지 보십시오. (한 줄 = 기록 하나)




# %% [16-4] Delta 로 다시 읽어 줄 수와 스키마를 보고, 깨진 시각(분이 67) 이 원문 그대로 남아 있는지 찾으십시오.




# %% [16-5] 장부 보기 — 이 테이블의 역사(history)를 보십시오.




# %% [16-6] 실수로 연습용 작은 로그(tags_mini, 12줄)를 Bronze 에 append 하고, 줄 수와 역사를 보십시오.




# %% [16-7] 시간 여행 — 버전 0 과 버전 1 의 줄 수를 각각 세십시오.




# %% [16-8] 실수로 넣은 12줄(event_id 가 M 으로 시작)을 지우고, 줄 수와 역사를 보십시오.




# %% [16-9] 지운 뒤에도 버전 1 은 여전히 읽히는지 보십시오.




# %% [16-10] 역 마스터를 Delta 로 lake/ref/stations 에 저장하고, 서울역(S05)의 이름을 서울역(KTX) 로 고친(UPDATE) 뒤 확인하십시오.




# %% [16-11] 일부러 틀려 보십시오. Bronze 에 칸이 하나 더 있는(hh) 데이터를 append 하십시오.




# %% [16-12] 어떤 폴더가 Delta 테이블인지 물어보십시오 — Delta 로 쓴 것과 STEP 15 의 Parquet 폴더.
