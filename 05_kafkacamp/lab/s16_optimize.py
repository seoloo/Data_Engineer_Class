# ==============================================================================
#  STEP 16  정리하고 내보내기 — OPTIMIZE · ZORDER · Gold 를 Kafka 로
#  C:\kafkacamp\lab\s16_optimize.py
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
from tools.replay import counts
st = spark.read.csv("data/stations.csv", header=True)
BRONZE, SILVER = "lake/s15/bronze", "lake/s15/silver"


# %% [16-1] Bronze 의 파일 수와 크기를 보십시오.




# %% [16-2] Bronze 를 OPTIMIZE 하고, 몇 개를 빼고 몇 개를 더했는지 보십시오.




# %% [16-3] 다시 파일 수와 줄 수를 보십시오.




# %% [16-4] Silver 를 탄 역(board_st) 기준으로 ZORDER 하십시오.




# %% [16-5] Bronze 의 역사를 보십시오.




# %% [16-6] Gold — Silver 로 역 이름별 승차 인원을 만들어 lake/s16/gold_board 에 저장하고 보십시오.




# %% [16-6 바꿔 보기 ①] Gold 를 노선(line)별로 바꾸면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [16-7] Gold 를 gold_board 토픽으로 보내십시오. key = 역 코드, value = 네 칸의 JSON.




# %% [16-8] 일부러 틀려 보십시오. Delta 가 아닌 폴더(data/stream_src)를 OPTIMIZE 해 보십시오.
