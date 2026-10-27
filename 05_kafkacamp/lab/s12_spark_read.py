# ==============================================================================
#  STEP 12  Spark 로 Kafka 읽기 — 토픽을 표로
#  C:\kafkacamp\lab\s12_spark_read.py
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


# %% [12-1] tags 토픽을 배치로 읽어 raw 라는 이름에 담고, 스키마를 보십시오.




# %% [12-2] raw 의 줄 수와 Spark 조각(파티션) 수를 보십시오.




# %% [12-3] key · value 를 글자로 바꿔서 partition · offset 과 함께 세 줄 보십시오.




# %% [12-3 바꿔 보기 ①] cast 를 빼고 value 를 그대로 보면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [12-4] value 를 SCHEMA 로 풀어 칸으로 펼친 DataFrame ev 를 만들고, 다섯 칸만 세 줄 보십시오. partition · offset 은 남겨
# 둡니다.




# %% [12-5] ev 로 승하차별 줄 수와 이벤트 가짓수를 구하십시오.




# %% [12-6] Kafka 파티션별 줄 수를 세십시오.




# %% [12-7] tag_time 이 비어 있는(null) 줄을 보십시오. (깨진 시각 07:67 같은 것)




# %% [12-8] Spark SQL 로 — ev 를 임시 뷰 ev 로 등록하고, 역별 승차 인원을 많은 순으로 구하십시오.




# %% [12-9] 0번 파티션의 0~9번 오프셋만 읽으십시오. 장부를 직접 고르는 assign 과 오프셋 JSON 을 씁니다.




# %% [12-10] 일부러 틀려 보십시오. subscribe 를 빼고 읽어 보십시오.
