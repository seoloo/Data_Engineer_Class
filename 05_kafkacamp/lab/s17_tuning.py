# ==============================================================================
#  STEP 17  지연과 처리량 — 재고, 바꾸고, 다시 잰다
#  C:\kafkacamp\lab\s17_tuning.py
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
from tools.replay import send_part, counts
import shutil


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for q in spark.streams.active:
    q.stop()
shutil.rmtree("chk/s17", ignore_errors=True)
from confluent_kafka.admin import AdminClient, NewTopic
adm = AdminClient({"bootstrap.servers": BOOT})
for t in ["lat17", "perf17"]:
    if t in adm.list_topics(timeout=5).topics:
        adm.delete_topics([t])[t].result()
time.sleep(3)
adm.create_topics([NewTopic("lat17", num_partitions=3, replication_factor=1)])["lat17"].result()
print("chk/s17 비움 · 측정용 토픽 lat17 새로 만듦")


# %% [17-1] tags(2,008줄)를 배치당 최대 500줄(maxOffsetsPerTrigger)로 읽는 스트리밍을 availableNow 로 돌리고, 배치마다 읽은 줄
# 수를 보십시오.




# %% [17-1 바꿔 보기 ①] maxOffsetsPerTrigger 를 100 으로 줄이면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [17-2] 17-1 의 첫 배치 진행 기록에서 다섯 가지만 꺼내 보십시오 — 배치 번호 · 읽은 줄 · 들어온 속도 · 처리 속도 · 걸린 시간(ms).




# %% [17-3] 끝에서 끝까지 지연 재기 — 측정용 토픽 lat17 을 지금부터(latest) 1초 간격으로 읽으면서, 배치마다 처리 시각 − Kafka timestamp 의
# 평균을 기록하는 쿼리를 켜고, part_01 을 speed=600 으로 lat17 에 흘려 보내십시오. ([초기화] 셀이 lat17 을 만들어 두었습니다)




# %% [17-4] trigger 간격을 5초로 바꿔 같은 실험을 하십시오. (체크포인트 이름을 lat5 로)




# %% [17-5] 집계 조각 수의 효과 — 5분 창 집계를 shuffle.partitions 4 와 50 으로 각각 돌려 걸린 시간을 비교하십시오.




# %% [17-6] 프로듀서 쪽 — 20,000개를 linger.ms 0 과 50 으로 각각 보내 초당 개수를 재 보십시오.




# %% [17-7] 내 PC 의 측정표를 만드십시오. 17-1 ~ 17-6 의 숫자를 아래 사전에 직접 옮겨 적고 표로 찍습니다.




# %% [17-8] 일부러 틀려 보십시오. maxOffsetsPerTrigger 에 0 을 주고 켠 뒤, 5초 뒤에 돌고 있는지 · 오류가 있는지 · 몇 줄 읽었는지 보십시오.
