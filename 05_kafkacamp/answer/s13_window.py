# ==============================================================================
#  STEP 13  스트리밍 창과 워터마크 — 늦게 온 태그를 언제까지 기다릴까
#  C:\kafkacamp\answer\s13_window.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
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
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


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
# ── 결과 ──
# tags_live 를 비우고 다시 만들었습니다: [0, 0, 0]


# %% [13-1] part_01 을 tags_live 로 보내고, tags_live 를 readStream 으로 읽어 칸으로 펼친 live 를 만드십시오.
print(send_part(1))
live = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOT)
        .option("subscribe", "tags_live")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*"))
live.isStreaming
# ── 결과 ──
# 201
# True
# → 201 줄을 보냈고, live 는 True — 끝나지 않는 표. read → readStream 한 단어만 바뀌었습니다. 스트리밍의
# startingOffsets 는 처음 시작할 때만 쓰이고, 다시 켜면 체크포인트의 위치부터 읽습니다.

# %% [13-2] 역별 태그 수를 세는 스트리밍 쿼리를 메모리 표 st13 로 켜고, 지금까지 온 것을 다 처리한 뒤 보십시오.
q1 = (live.groupBy("station_id").count()
      .writeStream.format("memory").queryName("st13")
      .outputMode("complete")
      .option("checkpointLocation", "chk/s13/st")
      .start())
q1.processAllAvailable()
spark.table("st13").orderBy("station_id").show()
# ── 결과 ──
# +----------+-----+
# |station_id|count|
# +----------+-----+
# |       S01|   20|
# |       S02|   25|
# |       S03|   23|
# |       S04|   20|
# |       S05|   25|
# |       S06|   23|
# |       S07|   30|
# |       S08|   34|
# |       S99|    1|
# +----------+-----+
# → part_01 의 201줄이 역별로 세어졌습니다. processAllAvailable() = "지금 Kafka 에 있는 것을 다 처리할 때까지 기다려라"
# — 수업에서 결과를 확인할 때만 쓰는 함수입니다. 쿼리는 뒤에서 계속 돌고 있습니다.

# %% [13-3] part_02 를 보내고, 다시 처리를 기다린 뒤 같은 표를 보십시오. (쿼리는 다시 만들지 않습니다)
send_part(2)
q1.processAllAvailable()
spark.table("st13").orderBy("station_id").show()
# ── 결과 ──
# +----------+-----+
# |station_id|count|
# +----------+-----+
# |       S01|   50|
# |       S02|   43|
# |       S03|   50|
# |       S04|   47|
# |       S05|   52|
# |       S06|   44|
# |       S07|   56|
# |       S08|   59|
# |       S99|    1|
# +----------+-----+
# → 역마다 숫자가 올라갔습니다. 우리는 쿼리를 다시 실행하지 않았습니다 — Kafka 에 보내기만 했는데 표가 바뀌었습니다. 이것이 Kafka + Spark
# 스트리밍입니다. 합계는 402.

# %% [13-4] q1 의 진행 기록에서 배치마다 몇 줄을 읽었는지 보고, 쿼리를 멈추십시오.
print([(p["batchId"], p["numInputRows"]) for p in q1.recentProgress if p["numInputRows"] > 0])
q1.stop()
# ── 결과 (배치가 나뉜 모양은 PC 마다 다름 — 합계 402 는 같음) ──
# [(0, 201), (1, 201)]
# → 배치마다 읽은 줄 수 — 합이 402. 몇 개의 배치로 나뉘었는지는 보내는 속도와 처리 속도에 따라 PC 마다 다릅니다. 다 쓴 쿼리는 stop()(1과목
# 18-8).

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
# ── 결과 ──
# (출력 없음)
# → 정의만 했으니 출력이 없습니다. withWatermark 는 groupBy 보다 먼저 붙입니다. 진행 기록(recentProgress)의
# eventTime.watermark 가 지금 마감선, numRowsDroppedByWatermark 가 마감 뒤에 와서 버린 줄 수입니다. (워터마크 시각은
# 세계 표준시(UTC) 로 찍힙니다 — 한국 시각 = UTC + 9시간)

# %% [13-6] run_win() 을 한 번 부르고, 닫힌 창을 보십시오. (지금 tags_live 에는 part_01 · 02 = 402줄)
run_win()
show_win()
# ── 결과 ──
# 읽은 줄: 402 / 워터마크: 1970-01-01T00:00:00.000Z / 버린 줄: 0
# 닫힌 창: 0 / 그 안의 태그: None
# → 402줄을 읽었는데 닫힌 창이 0개, 워터마크는 1970-01-01(아직 마감선이 없음). 워터마크는 배치가 끝난 뒤에 "가장 늦은 tag_time −
# 10분" 으로 정해지고, 창은 그 다음 배치에서 닫힙니다. 지금은 첫 배치였으니 모든 창이 아직 열려 있습니다(늦은 태그를 기다리는 중). 그래서 파일에
# 아무것도 없습니다(None).

# %% [13-7] part_03 · part_04 를 보내고, 그때마다 run_win() 을 부르십시오.
for k in [3, 4]:
    send_part(k)
    run_win()
show_win()
# ── 결과 ──
# 읽은 줄: 201 / 워터마크: 2026-10-13T22:43:34.000Z / 버린 줄: 0
# 읽은 줄: 201 / 워터마크: 2026-10-13T23:00:43.000Z / 버린 줄: 1
# 닫힌 창: 12 / 그 안의 태그: 462
# → 워터마크가 2026-10-13T22:43:34Z(= 한국 07:43:34) → 23:00:43Z(= 08:00:43) 로 앞으로 가면서 닫힌 창
# 12개(07:00 ~ 08:00), 태그 462개가 파일에 나왔습니다. 그리고 part_04 에서 버린 줄 1 — 20~40분 늦게 도착한 하차 태그가 이미
# 닫힌 창에 들어가려다 버려졌습니다.

# %% [13-8] 나머지 part_05 ~ 10 을 모두 보내며 run_win() 을 부르고, 마지막으로 배치(전부 다 있는 파일)로 센 숫자와 비교하십시오.
for k in range(5, 11):
    send_part(k)
    run_win()
show_win()
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl").filter(F.col("tag_time").isNotNull())
print("배치로 센 태그:", day.count())
# ── 결과 ──
# 읽은 줄: 201 / 워터마크: 2026-10-13T23:15:08.000Z / 버린 줄: 0
# 읽은 줄: 201 / 워터마크: 2026-10-13T23:29:29.000Z / 버린 줄: 1
# 읽은 줄: 201 / 워터마크: 2026-10-13T23:42:56.000Z / 버린 줄: 0
# 읽은 줄: 201 / 워터마크: 2026-10-13T23:58:53.000Z / 버린 줄: 1
# 읽은 줄: 201 / 워터마크: 2026-10-14T00:17:00.000Z / 버린 줄: 0
# 읽은 줄: 199 / 워터마크: 2026-10-14T00:38:34.000Z / 버린 줄: 0
# 닫힌 창: 31 / 그 안의 태그: 1689
# 배치로 센 태그: 2003
# → 닫힌 창 31개(07:00 ~ 09:35), 태그 1,689 — 배치로 센 2,003 보다 314 적습니다. 차이 = ① 워터마크 때문에 버린 줄
# 3(part_04 · 06 · 08 에서 하나씩) + ② 마지막 워터마크(한국 09:38:34) 뒤라 아직 안 닫힌 창의 311줄. 데이터가 더 오면(다음 날
# 아침) 그 창들도 닫힙니다. 실시간 집계는 빠른 대신 조금 덜 정확하고, 그 정도를 숫자로 말할 수 있어야 합니다(버린 줄 수). 정확한 숫자가 필요한 정산은
# STEP 15 의 Bronze → Silver 처럼 나중에 다시 맞추는 길을 함께 둡니다.

# %% [13-8 바꿔 보기 ①] 새 이름(win_all)으로 10조각을 한 번에 읽히면?
run_win("10 minutes", "win_all")
show_win("win_all")
# ── 결과 ──
# 읽은 줄: 2008 / 워터마크: 1970-01-01T00:00:00.000Z / 버린 줄: 0
# 닫힌 창: 0 / 그 안의 태그: None
# → 이래서 — 읽은 줄 2008, 버린 줄 0, 닫힌 창 0. 한 번에 읽으면 그 배치 안에서는 아무것도 버려지지 않고(워터마크는 배치가 끝난 뒤에
# 움직이니까), 같은 이유로 아직 아무 창도 닫히지 않습니다. 13-7 · 13-8 처럼 조각조각 와야 워터마크가 움직이고 창이 닫히고, 늦은 태그가
# 버려집니다. 워터마크의 효과는 데이터가 어떤 간격으로 오는지에 달려 있습니다 — 프로젝트에서 재생기의 speed 를 바꿔 가며 버린 줄 수를 재 보는 것이
# 좋은 실험입니다.

# %% [13-9] 일부러 틀려 보십시오. 워터마크 없이 append 로 창 집계를 켜 보십시오.
(live.groupBy(F.window("tag_time", "5 minutes")).count()
     .writeStream.format("parquet")
     .option("path", "out/s13/bad").option("checkpointLocation", "chk/s13/bad")
     .outputMode("append").start())
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append. This output mode is not supported for streaming aggregations without watermark on streaming DataFrames/DataSets. SQLSTATE: 42KDE
# → append output mode not supported ... without watermark — 워터마크가 없으면 창이 언제 닫히는지 모르니 "한
# 번만 내보낸다(append)" 를 약속할 수 없습니다. 1과목 18-11 과 같은 오류입니다. 바이브 코딩으로 받은 스트리밍 코드에서 withWatermark
# 가 빠지면 이 오류가 납니다.
