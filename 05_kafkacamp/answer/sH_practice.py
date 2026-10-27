# ==============================================================================
#  마무리  마무리 — 혼자 푸는 열 문제
#  C:\kafkacamp\answer\sH_practice.py
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
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
shutil.rmtree("chk/sH", ignore_errors=True)
from confluent_kafka.admin import AdminClient
adm = AdminClient({"bootstrap.servers": BOOT})
if "alert_s99" in adm.list_topics(timeout=5).topics:
    adm.delete_topics(["alert_s99"])["alert_s99"].result()
    time.sleep(3)
print("chk/sH · alert_s99 비움")
# ── 결과 ──
# chk/sH · alert_s99 비움


# %% [H-1] tags 에서 하차(OUT)의 요금 합계를 구하십시오. 중복(event_id)을 빼기 전과 뺀 뒤 두 숫자. (STEP 12)
ev = (read_topic("tags").select(F.from_json("value", SCHEMA).alias("e")).select("e.*"))
outs = ev.filter(F.col("tag_type") == "OUT")
print(outs.agg(F.sum("fare")).first()[0])
print(outs.dropDuplicates(["event_id"]).agg(F.sum("fare")).first()[0])
# ── 결과 ──
# 1664800
# 1651400
# → 1,664,800 → 1,651,400. 재전송된 하차의 요금이 두 번 더해져 있었습니다. 뺀 뒤의 숫자에서 격리(S99 · 깨진 시각)까지 빼면
# Silver 의 1,647,800 이 됩니다.

# %% [H-2] tags 의 Kafka 파티션마다 카드가 몇 장인지, 그리고 세 파티션에 겹치는 카드가 있는지 확인하십시오. (STEP 08 · 12)
cards = read_topic("tags").select("partition", F.col("key").alias("card_id")).distinct()
cards.groupBy("partition").count().orderBy("partition").show()
print("두 파티션 이상에 나온 카드:", cards.groupBy("card_id").count().filter("count > 1").count())
# ── 결과 ──
# +---------+-----+
# |partition|count|
# +---------+-----+
# |        0|  174|
# |        1|  193|
# |        2|  165|
# +---------+-----+
#
# 두 파티션 이상에 나온 카드: 0
# → 겹치는 카드 0 — 같은 키(카드)는 언제나 한 파티션에만 갑니다. 이 사실 덕분에 "카드별 순서" 를 믿을 수 있습니다.

# %% [H-3] tags 로 하차가 가장 많은 역 세 곳을 역 이름과 함께 구하십시오. 중복 제거. (STEP 12 · 1과목 STEP 10)
(ev.dropDuplicates(["event_id"]).filter(F.col("tag_type") == "OUT")
   .join(st, "station_id")
   .groupBy("station_nm").count()
   .orderBy(F.col("count").desc(), "station_nm")
   .show(3))
# ── 결과 ──
# +----------+-----+
# |station_nm|count|
# +----------+-----+
# |  홍대입구|  146|
# |      시청|  132|
# |    여의도|  132|
# +----------+-----+
# only showing top 3 rows
# → 홍대입구 146 · 시청 132 · 여의도 132. 1과목 마무리 H-1(홍대입구 145 · 시청 132 · 여의도 131)과 1씩 다른 곳이 있는데,
# 1과목은 격리(깨진 시각 · S99)까지 한 뒤에 셌고 여기서는 중복만 뺐기 때문입니다. 숫자가 다를 때 "어느 단계의 표로 셌나" 를 먼저 묻는 습관 —
# 실무에서 가장 많이 쓰는 질문입니다.

# %% [H-4] station_cnt 토픽의 JSON 을 풀어 boards 의 합계를 구하십시오. (STEP 14)
sc = (read_topic("station_cnt")
      .select(F.from_json("value", "station_id STRING, boards LONG").alias("j"))
      .select("j.*"))
sc.agg(F.count("*").alias("rows"), F.sum("boards").alias("boards")).show()
# ── 결과 ──
# +----+------+
# |rows|boards|
# +----+------+
# |   9|  1000|
# +----+------+
# → 아홉 줄, 합계 1000 = 중복을 뺀 승차 이벤트 수(S99 · 깨진 시각 포함). from_json 의 스키마는 DDL 글자로 짧게 줄 수도
# 있습니다(1과목 STEP 07).

# %% [H-5] win5 토픽에서 승차가 가장 많았던 5분 창 세 개를 구하십시오. (STEP 14)
w5 = (read_topic("win5")
      .select(F.from_json("value", "start STRING, count LONG").alias("j"))
      .select("j.*"))
w5.orderBy(F.col("count").desc(), "start").show(3, truncate=False)
# ── 결과 ──
# +-------------------+-----+
# |start              |count|
# +-------------------+-----+
# |2026-10-14 08:05:00|43   |
# |2026-10-14 08:25:00|42   |
# |2026-10-14 08:30:00|42   |
# +-------------------+-----+
# only showing top 3 rows
# → 8시대 초반 — 1과목 12-4 의 "피크 5분" 과 같은 창입니다(중복 포함 여부에 따라 숫자가 1~2 다를 수 있음 — win5 는 중복을 빼지
# 않았습니다).

# %% [H-6] 파이썬 컨슈머로 gold_board 를 처음부터 읽어 pandas 표로 만들고, 노선별 승차 합계를 구하십시오. (STEP 07)
import pandas as pd
c = Consumer({"bootstrap.servers": BOOT, "group.id": "gH6", "auto.offset.reset": "earliest"})
c.subscribe(["gold_board"])
rows, empty = [], 0
while empty < 5:
    m = c.poll(1.0)
    if m is None:
        empty += 1
        continue
    empty = 0
    rows.append(json.loads(m.value()))
c.close()
g = pd.DataFrame(rows)
print(g.groupby("line")["board_cnt"].sum().sort_values(ascending=False))
# ── 결과 ──
# line
# 2호선    484
# 5호선    261
# 1호선    247
# Name: board_cnt, dtype: int64
# → 2호선 484 · 5호선 261 · 1호선 247 — 16-6 바꿔 보기와 같습니다. 같은 토픽을 Spark 로도, 파이썬으로도 읽을 수 있습니다.
# 3과목에서는 이 파이썬 컨슈머가 MongoDB 에 넣습니다.

# %% [H-7] 스트리밍 경보 — tags_live 에서 역 코드가 S99 인 태그만 골라 alert_s99 토픽에 쓰는 쿼리를 availableNow 로 돌리고, 몇 건이
# 갔는지 세십시오. (STEP 14)
alerts = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", BOOT)
          .option("subscribe", "tags_live")
          .option("startingOffsets", "earliest")
          .load()
          .select("key", F.col("value").cast("string").alias("value"))
          .filter(F.from_json("value", SCHEMA)["station_id"] == "S99"))
q = (alerts.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("topic", "alert_s99")
     .option("checkpointLocation", "chk/sH/alert")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
sum(counts("alert_s99"))
# ── 결과 ──
# 6
# → 6 — 1과목의 S99 여섯 건. 원본 메시지를 그대로(key · value) 넘겼습니다. 실제 서비스라면 이 토픽을 알림 프로그램(문자 · 슬랙)이
# 읽습니다. 걸러서 다른 토픽으로 넘기기 — 프로젝트 주제 2(이상 결제 감지)의 뼈대입니다.

# %% [H-8] Bronze(lake/s15/bronze)에서 Kafka 파티션별 가장 큰 오프셋을 구하고, counts("tags_live") 와 비교하십시오. (STEP
# 15)
(spark.read.format("delta").load("lake/s15/bronze")
   .groupBy("partition").agg(F.max("offset").alias("max_offset"))
   .orderBy("partition").show())
print(counts("tags_live"))
# ── 결과 ──
# +---------+----------+
# |partition|max_offset|
# +---------+----------+
# |        0|       685|
# |        1|       697|
# |        2|       623|
# +---------+----------+
#
# [686, 698, 624]
# → 가장 큰 오프셋 + 1 = 그 파티션의 메시지 수. Bronze 가 Kafka 의 마지막 메시지까지 받았다는 것을 송장으로 확인했습니다. 실무의 "데이터
# 다 들어왔나?" 점검이 이 모양입니다.

# %% [H-9] tags 로 1시간 창별 승차 수(중복 제거)를 구하십시오. (STEP 13 · 1과목 12-8)
(ev.dropDuplicates(["event_id"]).filter(F.col("tag_type") == "IN")
   .groupBy(F.window("tag_time", "1 hour").alias("w")).count()
   .select(F.col("w.start").alias("start"), "count")
   .orderBy("start").show())
# ── 결과 ──
# +-------------------+-----+
# |              start|count|
# +-------------------+-----+
# |2026-10-14 07:00:00|  299|
# |2026-10-14 08:00:00|  450|
# |2026-10-14 09:00:00|  249|
# +-------------------+-----+
# → 299 · 450 · 249 = 998. 중복을 뺀 승차 1,000 중 깨진 시각 2줄은 창을 만들 수 없어서 window 가 저절로 뺐습니다. 배치에서는
# 워터마크가 필요 없습니다 — 데이터가 다 있으니 창을 언제 닫을지 고민할 필요가 없기 때문입니다.

# %% [H-10] 파이썬 프로듀서(murmur2)로 키 C0401 의 시험 메시지를 alert_s99 에 하나 보내고, 몇 번 파티션에 갔는지와 tags 에서 C0401 이
# 있는 파티션이 같은지 보십시오. (STEP 08)
p = Producer({"bootstrap.servers": BOOT, "partitioner": "murmur2_random"})
got = []
p.produce("alert_s99", key="C0401", value="test", on_delivery=lambda e, m: got.append(m.partition()))
p.flush()
print("alert_s99 :", got[0], "/ 파티션 수", len(counts("alert_s99")))
print("tags      :", read_topic("tags").filter("key = 'C0401'").select("partition").distinct().first()[0], "/ 파티션 수", len(counts("tags")))
# ── 결과 ──
# alert_s99 : 0 / 파티션 수 1
# tags      : 1 / 파티션 수 3
# → alert_s99 는 Spark 가 쓰면서 자동으로 만든 토픽이라 파티션 1개 — 그래서 무조건 0 번입니다. tags 는 3개라 C0401 은 다른
# 번호일 수 있습니다. 같은 키라도 파티션 수가 다르면 번호가 다릅니다(04-11). 결과 토픽도 미리 같은 파티션 수로 만들어 두면 "같은 카드는 같은 번호"
# 가 토픽을 넘어서도 지켜집니다.
