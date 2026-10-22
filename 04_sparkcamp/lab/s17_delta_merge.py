# ==============================================================================
#  STEP 17  Delta Lake ② — MERGE 로 고치고, OPTIMIZE 로 정리한다
#  C:\sparkcamp\lab\s17_delta_merge.py
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
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())

import shutil


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for p in ["lake/silver/trips", "lake/silver/trips_rev", "lake/bronze/tags_small"]:
    shutil.rmtree(p, ignore_errors=True)
print("lake/silver/trips · trips_rev · lake/bronze/tags_small 비움")


# %% [17-1] 이벤트를 이동 한 건 = 한 줄로 접는 함수 to_trips 를 만드십시오.




# %% [17-2] 첫 번째 묶음 — 08:30 전에 도착한 정상 이벤트(b1)로 이동 표 t1 을 만들고, Silver 로 저장하십시오.




# %% [17-3] Silver 를 읽어 이동이 몇 건인지, 그중 아직 하차가 없는 이동이 몇 건인지 세십시오.




# %% [17-4] 두 번째 묶음 — 나머지 이벤트(b2)로 t2 를 만들고, 그중 Silver 에 이미 있는 이동과 새로운 이동이 각각 몇 건인지 세십시오.




# %% [17-5] MERGE — t2 를 Silver 에 Upsert 하고, 결과 숫자를 보십시오.




# %% [17-6] MERGE 뒤 Silver 의 이동 수와 아직 하차가 없는 이동 수를 세십시오. 그리고 STEP 13 의 숫자와 비교하십시오.




# %% [17-7] 같은 t2 로 MERGE 를 한 번 더 하고, 결과 숫자와 이동 수를 보십시오. (장애로 같은 데이터가 다시 들어온 상황)




# %% [17-8] 비교 — MERGE 대신 append 로 쌓았다면 어떻게 되었을지, t1 과 t2 를 그냥 이어 붙여 세어 보십시오.




# %% [17-9] Silver 의 역사를 보십시오.




# %% [17-10] 작은 파일 만들기 — 원본을 일부러 20 조각으로 나눠 Delta 로 저장하고, 파일이 몇 개인지 보십시오.




# %% [17-11] OPTIMIZE + ZORDER — 작은 파일을 합치면서 역 코드로 정렬하고, 몇 개를 빼고 몇 개를 더했는지 보십시오.




# %% [17-12] OPTIMIZE 뒤에 파일 수와 줄 수, 역사를 보십시오.




# %% [17-13] 순서를 뒤집어 보기 — 나중 묶음(t2)으로 먼저 새 테이블 trips_rev 를 만들고, 앞 묶음(t1)을 나중에 MERGE 하십시오. 그리고 17-6 과
# 같은 세 숫자를 세십시오.
