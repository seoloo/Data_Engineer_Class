# ==============================================================================
#  STEP 15  Kafka → Bronze → Silver — 정확히 한 번의 파이프라인
#  C:\kafkacamp\lab\s15_pipeline.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys, glob, json, time
os.chdir(r"C:\kafkacamp")
sys.path.insert(0, os.getcwd())          # tools\replay.py 를 import 하려고
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
BOOT = "localhost:9092"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
b = (SparkSession.builder.master("local[2]").appName("kafkacamp")
     .config("spark.local.dir", r"C:\kafkacamp\tmp")
     .config("spark.jars.ivy", r"C:\kafkacamp\ivy")
     .config("spark.ui.showConsoleProgress", "false")
     .config("spark.sql.shuffle.partitions", "4")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
jars = (glob.glob(r"C:\kafkacamp\jars\*.jar")           # USB 로 받은 jar
        or glob.glob(r"C:\kafkacamp\ivy\jars\*.jar"))     # 처음 한 번 받아 둔 jar
if jars:      # 있으면 인터넷을 거치지 않고 바로 쓴다
    spark = b.config("spark.jars", ",".join(jars)).getOrCreate()
else:         # 없으면 인터넷에서 받는다 (처음 한 번 1~2분)
    spark = configure_spark_with_delta_pip(b, extra_packages=[KAFKA_PKG]).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "+ Kafka + Delta 준비 완료")

RAW_SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time STRING, fare INT, recv_time STRING
"""   # Bronze 용 — 시각 칸을 글자 그대로 (깨진 원문을 남긴다)

import shutil
from tools.replay import send_part, counts
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
from confluent_kafka.admin import AdminClient, NewTopic
adm = AdminClient({"bootstrap.servers": BOOT})
if "tags_live" in adm.list_topics(timeout=5).topics:
    adm.delete_topics(["tags_live"])["tags_live"].result()
    time.sleep(3)
adm.create_topics([NewTopic("tags_live", num_partitions=3, replication_factor=1)])["tags_live"].result()
for p in ["lake/s15", "chk/s15"]:
    shutil.rmtree(p, ignore_errors=True)
print("tags_live 비움 · lake/s15 · chk/s15 비움:", counts("tags_live"))


# %% [15-1] 조각 1~3 을 tags_live 로 보내십시오.




# %% [15-2] ① Bronze — tags_live 를 읽어 value 를 RAW_SCHEMA 로 펼치고, partition · offset · Kafka
# timestamp(kafka_ts) · 받은 시각(ingest_time) 을 붙여 Delta 로 쌓는 함수 run_bronze() 를 만들고 실행하십시오.




# %% [15-3] ② Silver 의 일꾼 함수 upsert_trips 를 만드십시오. (1과목 19-3 과 같고, 경로만 s15)




# %% [15-4] ② Silver 스트리밍 run_silver() 를 만들고 실행하십시오. (이동 수, 하차 없음) 을 돌려줍니다.




# %% [15-5] 나머지 조각 4~10 을 보내고, Bronze → Silver 를 다시 돌리십시오.




# %% [15-6] Silver 로 기준 숫자 넷을 구하십시오 — 이동 수 · 하차 없음 · 승차 없음 · 요금 합계.




# %% [15-7] 장애 실험 ① — Bronze 의 체크포인트를 지우고 run_bronze() 를 다시 돌리십시오. Bronze 는 몇 줄이 될까요?




# %% [15-8] 장애 실험 ② — 이 상태로 run_silver() 를 돌리십시오. Silver 도 두 배가 될까요?




# %% [15-8 바꿔 보기 ①] Bronze 도 멱등으로 만들려면? — (partition, offset) 을 키로 MERGE 하는 Bronze 를 새로(bronze2) 만들어 두 번 돌리면
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [15-9] Bronze 에서 같은 (partition, offset) 이 몇 번씩 들어 있는지 세고, 송장으로 중복을 걸러 내면 몇 줄인지 보십시오.




# %% [15-10] Bronze 체크포인트에 무엇이 적혀 있는지 보십시오. offsets 폴더의 마지막 파일 맨 끝 줄입니다.




# %% [15-11] Silver 의 역사를 보십시오.
