# ==============================================================================
#  STEP 14  결과를 다시 Kafka 로 — 계산한 것을 다른 프로그램에 넘기기
#  C:\kafkacamp\lab\s14_spark_write.py
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
from tools.replay import counts
from confluent_kafka.admin import AdminClient
adm = AdminClient({"bootstrap.servers": BOOT})


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
import shutil
old = [t for t in ["station_cnt", "win5"] if t in adm.list_topics(timeout=5).topics]
for t in old:
    adm.delete_topics([t])[t].result()
time.sleep(3)
shutil.rmtree("chk/s14", ignore_errors=True)
print("지운 토픽:", old)


# %% [14-1] tags 를 배치로 읽어 역별 승차 수(중복 제거 · S99 포함)를 구한 DataFrame boards 를 만드십시오.




# %% [14-2] boards 를 key = 역 코드, value = JSON 모양으로 바꿔 보십시오. (아직 보내지 않습니다)




# %% [14-2 바꿔 보기 ①] struct 안에 역 이름(station_nm) 까지 넣으려면? — 역 마스터를 붙여서
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [14-3] out 을 station_cnt 토픽으로 보내고, 파티션별 개수를 보십시오.




# %% [14-4] station_cnt 를 Spark 로 다시 읽어 key · value 를 글자로 보십시오.




# %% [14-5] 스트리밍으로 — tags_live(지금 10조각이 다 있음)를 읽어 5분 창 승차 수(워터마크 10분)를 win5 토픽에 쓰는 쿼리를 availableNow
# 로 돌리고, win5 에 몇 개가 들어갔는지 보십시오.




# %% [14-6] 일부러 틀려 보십시오. 14-5 의 쿼리를 checkpointLocation 없이 켜 보십시오.




# %% [14-7] 일부러 틀려 보십시오. value 칸 없이(이름이 v) 보내 보십시오.
