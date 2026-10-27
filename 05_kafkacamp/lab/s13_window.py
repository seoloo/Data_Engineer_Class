# ==============================================================================
#  STEP 13  스트리밍 창과 워터마크 — 늦게 온 태그를 언제까지 기다릴까
#  C:\kafkacamp\lab\s13_window.py
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


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
import shutil
from confluent_kafka.admin import AdminClient, NewTopic
adm = AdminClient({"bootstrap.servers": BOOT})
if "tags_live" in adm.list_topics(timeout=5).topics:
    adm.delete_topics(["tags_live"])["tags_live"].result()
    time.sleep(3)
adm.create_topics([NewTopic("tags_live", num_partitions=3, replication_factor=1)])["tags_live"].result()
shutil.rmtree("chk/s13", ignore_errors=True)
shutil.rmtree("out/s13", ignore_errors=True)
print("tags_live 를 비우고 다시 만들었습니다:", counts("tags_live"))


# %% [13-1] part_01 을 tags_live 로 보내고, tags_live 를 readStream 으로 읽어 칸으로 펼친 live 를 만드십시오.
print(send_part(1))
live = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOT)
        .option("subscribe", "tags_live")
        .option("startingOffsets", "ealiest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*"))

live.isStreaming



# %% [13-2] 역별 태그 수를 세는 스트리밍 쿼리를 메모리 표 st13 로 켜고, 지금까지 온 것을 다 처리한 뒤 보십시오.




# %% [13-3] part_02 를 보내고, 다시 처리를 기다린 뒤 같은 표를 보십시오. (쿼리는 다시 만들지 않습니다)




# %% [13-4] q1 의 진행 기록에서 배치마다 몇 줄을 읽었는지 보고, 쿼리를 멈추십시오.




# %% [13-5] 5분 창 + 워터마크 10분 으로 태그 수를 세어 파일(out/s13/win)에 append 하는 함수 run_win() 을 만드십시오. 한 번 부르면 지금
# 있는 것만 처리하고 멈춥니다(availableNow).
def run_win(delay="10 minutes", name="win"):
    q = (live.withWatermark("tag_time", delay)
         .groupBy(F.window("tag_time", "5 minutes").alias("w"))
         .count()
         .select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count")
         .writeStream.format("parquet")
         .option("path", f"out/s13/{name}")
         .option("checkpointLocation", f"chk/s13/{name}")
         .outputMode("append")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    last = [p for p in q.recentProgress if p["numInputRows"] > 0][-1]
    print("읽은 줄:", sum(p["numInputRows"] for p in q.recentProgress),
          "/ 워터마크:", q.lastProgress["eventTime"].get("watermark"),
          "/ 버린 줄:", sum(p["stateOperators"][0]["numRowsDroppedByWatermark"] for p in q.recentProgress))
 
def show_win(name="win"):
    w = spark.read.parquet(f"out/s13/{name}")
    print("닫힌 창:", w.count(), "/ 그 안의 태그:", w.agg(F.sum("count")).first()[0])


# %% [13-6] run_win() 을 한 번 부르고, 닫힌 창을 보십시오. (지금 tags_live 에는 part_01 · 02 = 402줄)




# %% [13-7] part_03 · part_04 를 보내고, 그때마다 run_win() 을 부르십시오.




# %% [13-8] 나머지 part_05 ~ 10 을 모두 보내며 run_win() 을 부르고, 마지막으로 배치(전부 다 있는 파일)로 센 숫자와 비교하십시오.




# %% [13-8 바꿔 보기 ①] 새 이름(win_all)으로 10조각을 한 번에 읽히면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [13-9] 일부러 틀려 보십시오. 워터마크 없이 append 로 창 집계를 켜 보십시오.
