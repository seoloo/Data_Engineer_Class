# ==============================================================================
#  마무리  마무리 — 혼자 푸는 열 문제
#  C:\kafkacamp\lab\sH_practice.py
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

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
from confluent_kafka import Producer, Consumer, TopicPartition
from tools.replay import counts
import shutil
st = spark.read.csv("data/stations.csv", header=True)

def read_topic(topic):                       # 토픽 전체를 배치로 읽어 key · value 글자로
    return (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", BOOT)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.col("key").cast("string").alias("key"),
                    F.col("value").cast("string").alias("value"),
                    "partition", "offset", "timestamp"))


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
shutil.rmtree("chk/sH", ignore_errors=True)
from confluent_kafka.admin import AdminClient
adm = AdminClient({"bootstrap.servers": BOOT})
if "alert_s99" in adm.list_topics(timeout=5).topics:
    adm.delete_topics(["alert_s99"])["alert_s99"].result()
    time.sleep(3)
print("chk/sH · alert_s99 비움")


# %% [H-1] tags 에서 하차(OUT)의 요금 합계를 구하십시오. 중복(event_id)을 빼기 전과 뺀 뒤 두 숫자. (STEP 12)




# %% [H-2] tags 의 Kafka 파티션마다 카드가 몇 장인지, 그리고 세 파티션에 겹치는 카드가 있는지 확인하십시오. (STEP 08 · 12)




# %% [H-3] tags 로 하차가 가장 많은 역 세 곳을 역 이름과 함께 구하십시오. 중복 제거. (STEP 12 · 1과목 STEP 10)




# %% [H-4] station_cnt 토픽의 JSON 을 풀어 boards 의 합계를 구하십시오. (STEP 14)




# %% [H-5] win5 토픽에서 승차가 가장 많았던 5분 창 세 개를 구하십시오. (STEP 14)




# %% [H-6] 파이썬 컨슈머로 gold_board 를 처음부터 읽어 pandas 표로 만들고, 노선별 승차 합계를 구하십시오. (STEP 07)




# %% [H-7] 스트리밍 경보 — tags_live 에서 역 코드가 S99 인 태그만 골라 alert_s99 토픽에 쓰는 쿼리를 availableNow 로 돌리고, 몇 건이
# 갔는지 세십시오. (STEP 14)




# %% [H-8] Bronze(lake/s15/bronze)에서 Kafka 파티션별 가장 큰 오프셋을 구하고, counts("tags_live") 와 비교하십시오. (STEP
# 15)




# %% [H-9] tags 로 1시간 창별 승차 수(중복 제거)를 구하십시오. (STEP 13 · 1과목 12-8)




# %% [H-10] 파이썬 프로듀서(murmur2)로 키 C0401 의 시험 메시지를 alert_s99 에 하나 보내고, 몇 번 파티션에 갔는지와 tags 에서 C0401 이
# 있는 파티션이 같은지 보십시오. (STEP 08)
