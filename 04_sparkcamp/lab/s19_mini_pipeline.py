# ==============================================================================
#  STEP 19  미니 파이프라인 — 폴더 → Bronze → Silver, 그리고 장애 실험
#  C:\sparkcamp\lab\s19_mini_pipeline.py
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

RAW_SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time STRING, fare INT, recv_time STRING
"""   # Bronze 용 — 시각 칸을 글자 그대로 (깨진 원문을 남긴다)

import shutil

def drop(n):
    """stream_src 의 n 번째 조각을 s19_in 으로 옮겨 놓는다 = 새 데이터 도착"""
    name = f"part_{n:02d}.jsonl"
    shutil.copy(f"data/stream_src/{name}", f"data/s19_in/_{name}")
    os.replace(f"data/s19_in/_{name}", f"data/s19_in/{name}")

st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]

def to_trips(ev):
    """이벤트(승차·하차 줄)를 이동 한 건 = 한 줄로 접는다"""
    is_in, is_out = F.col("tag_type") == "IN", F.col("tag_type") == "OUT"
    return ev.groupBy("trip_id").agg(
        F.first("card_id").alias("card_id"),
        F.max(F.when(is_in,  F.col("station_id"))).alias("board_st"),
        F.max(F.when(is_in,  F.col("tag_time"))).alias("board_time"),
        F.max(F.when(is_out, F.col("station_id"))).alias("alight_st"),
        F.max(F.when(is_out, F.col("tag_time"))).alias("alight_time"),
        F.max("fare").alias("fare"))

rules = {                                   # MERGE 규칙 (STEP 17)
    "card_id":     "coalesce(s.card_id, n.card_id)",
    "board_st":    "coalesce(n.board_st, s.board_st)",
    "board_time":  "coalesce(n.board_time, s.board_time)",
    "alight_st":   "coalesce(n.alight_st, s.alight_st)",
    "alight_time": "coalesce(n.alight_time, s.alight_time)",
    "fare":        "greatest(s.fare, n.fare)"}


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for q in spark.streams.active:
    q.stop()
for p in ["data/s19_in", "lake/s19", "chk/s19"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs("data/s19_in")
print("s19_in · lake/s19 · chk/s19 비움")


# %% [19-1] 조각 1~3 을 도착시키십시오.




# %% [19-2] ① Bronze — RAW_SCHEMA(시각 = 글자) 로 읽어 들어온 그대로에 받은 시각만 붙여 Delta 로 쌓는 함수를 만들고 한 번 실행하십시오.




# %% [19-3] ② Silver 의 일꾼 함수 — 새 묶음을 받아 정리하고(STEP 13) 이동으로 접어(to_trips) MERGE 하는 upsert_trips 를
# 만드십시오. 처음이면 테이블을 새로 만듭니다.




# %% [19-4] ② Silver 스트리밍 — Bronze 를 스트리밍으로 읽어 묶음마다 upsert_trips 를 부르는 함수를 만들고 실행하십시오. Silver 의 이동
# 수와 하차 없음 수를 돌려주게 하십시오.




# %% [19-5] 나머지 조각 4~10 을 한꺼번에 도착시키고, Bronze → Silver 를 차례로 다시 돌리십시오.




# %% [19-6] 배치와 같은 답인가? — Silver 로 STEP 13 · 17 의 기준 숫자를 다시 구하십시오. 이동 수 · 하차 없음 · 승차 없음 · 요금 합계.




# %% [19-7] 새 조각 없이 Bronze → Silver 를 한 번 더 돌려 보십시오.




# %% [19-8] 장애 실험 — Silver 의 체크포인트를 지우고 다시 돌리십시오. 숫자가 두 배가 될까요?




# %% [19-9] Silver 의 역사를 보십시오.




# %% [19-10] ③ Gold — Silver 로 역 이름별 승차 인원을 만들어 lake/s19/gold/board_by_station 에 저장하고 보십시오.
