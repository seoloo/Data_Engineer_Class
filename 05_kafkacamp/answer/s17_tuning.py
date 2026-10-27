# ==============================================================================
#  STEP 17  지연과 처리량 — 재고, 바꾸고, 다시 잰다
#  C:\kafkacamp\answer\s17_tuning.py
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
import shutil
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


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
# ── 결과 ──
# chk/s17 비움 · 측정용 토픽 lat17 새로 만듦


# %% [17-1] tags(2,008줄)를 배치당 최대 500줄(maxOffsetsPerTrigger)로 읽는 스트리밍을 availableNow 로 돌리고, 배치마다 읽은 줄
# 수를 보십시오.
q = (spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("subscribe", "tags")
     .option("startingOffsets", "earliest")
     .option("maxOffsetsPerTrigger", 500)
     .load()
     .writeStream.format("noop")
     .option("checkpointLocation", "chk/s17/limit")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
rows = [p["numInputRows"] for p in q.recentProgress if p["numInputRows"] > 0]
print(len(rows), "배치:", rows, "합계", sum(rows))
# ── 결과 ──
# 5 배치: [498, 498, 498, 499, 15] 합계 2008
# → 배치마다 500 이하로 나눠 읽어 합계 2008. 500 이 아니라 498 처럼 조금 모자란 것은, 500 을 세 파티션에 쌓인 양의 비율대로 나누고
# 소수점을 버리기 때문입니다. format("noop") 은 "아무 데도 쓰지 말고 읽기만" — 속도를 잴 때 쓰는 싱크입니다.

# %% [17-1 바꿔 보기 ①] maxOffsetsPerTrigger 를 100 으로 줄이면?
q2 = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "tags")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100)
      .load()
      .writeStream.format("noop")
      .option("checkpointLocation", "chk/s17/limit100")
      .trigger(availableNow=True)
      .start())
q2.awaitTermination()
rows = [p["numInputRows"] for p in q2.recentProgress if p["numInputRows"] > 0]
print(len(rows), "배치, 합계", sum(rows))
# ── 결과 ──
# 21 배치, 합계 2008
# → 이래서 — 배치가 약 20개로 늘어납니다(2,008 ÷ 99 정도). 배치마다 준비 · 마무리 시간이 붙으니 전체 시간은 늘어나지만, 배치 하나는 짧아서
# 한 배치가 오래 걸려 다음 배치를 못 시작하는 일은 줄어듭니다. 실무에서는 "평소 들어오는 양의 몇 배" 정도로 잡아, 장애 뒤 밀린 LAG 를 조금씩 나눠
# 따라잡게 합니다.

# %% [17-2] 17-1 의 첫 배치 진행 기록에서 다섯 가지만 꺼내 보십시오 — 배치 번호 · 읽은 줄 · 들어온 속도 · 처리 속도 · 걸린 시간(ms).
p0 = [p for p in q.recentProgress if p["numInputRows"] > 0][0]
print("batchId               :", p0["batchId"])
print("numInputRows          :", p0["numInputRows"])
print("inputRowsPerSecond    :", round(p0["inputRowsPerSecond"], 1))
print("processedRowsPerSecond:", round(p0["processedRowsPerSecond"], 1))
print("triggerExecution(ms)  :", p0["durationMs"]["triggerExecution"])
# ── 결과 (작성 환경에서 잰 숫자 — 시간 · 속도는 PC 마다 다름. 모양과 크고 작음만 비교) ──
# batchId               : 0
# numInputRows          : 498
# inputRowsPerSecond    : 0.0
# processedRowsPerSecond: 207.8
# triggerExecution(ms)  : 2396
# → processedRowsPerSecond = 처리량, triggerExecution = 배치 하나 걸린 시간. inputRowsPerSecond(들어온
# 속도)가 processedRowsPerSecond(처리 속도)보다 계속 크면 밀리고 있다는 뜻입니다 — Kafka 쪽에서는 LAG 가 커집니다(STEP
# 05). 숫자는 PC 마다 다릅니다.

# %% [17-3] 끝에서 끝까지 지연 재기 — 측정용 토픽 lat17 을 지금부터(latest) 1초 간격으로 읽으면서, 배치마다 처리 시각 − Kafka timestamp 의
# 평균을 기록하는 쿼리를 켜고, part_01 을 speed=600 으로 lat17 에 흘려 보내십시오. ([초기화] 셀이 lat17 을 만들어 두었습니다)
lat = []
def measure(df, bid):
    r = df.select(F.count("*").alias("n"),
                  F.avg(F.current_timestamp().cast("double") - F.col("timestamp").cast("double")).alias("sec")).first()
    if r["n"]:
        lat.append((bid, r["n"], round(r["sec"], 1)))

ql = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "lat17")
      .option("startingOffsets", "latest")
      .load()
      .writeStream.foreachBatch(measure)
      .option("checkpointLocation", "chk/s17/lat1")
      .trigger(processingTime="1 second")
      .start())
time.sleep(5)                                # 쿼리가 준비될 시간
send_part(1, topic="lat17", speed=600)       # 10초 남짓 흘려 보냄 (09-4 와 같음, 따로 쓰는 토픽 lat17)
time.sleep(5)
ql.stop()
print(lat)
print("평균 지연(초):", round(sum(n * s for _, n, s in lat) / sum(n for _, n, s in lat), 2))
# ── 결과 (작성 환경에서 잰 숫자 — 시간 · 속도는 PC 마다 다름. 모양과 크고 작음만 비교) ──
# [(1, 5, 0.4), (2, 15, 0.8), (3, 16, 0.8), (4, 15, 0.8), (5, 16, 0.8), (6, 18, 0.7), (7, 16, 0.8), (8, 18, 0.8), (9, 18, 0.8), (10, 19, 0.8), (11, 18, 0.8), (12, 19, 0.7), (13, 8, 1.2)]
# 평균 지연(초): 0.79
# → 배치마다 (번호, 줄 수, 평균 지연 초). 1초마다 배치가 돌았으니 지연은 대략 0.5 ~ 2초. Kafka timestamp(보낸 시각, 09-5) 를
# 쓰는 이유가 이것입니다 — tag_time 은 2026-10-14 라 지연을 잴 수 없습니다. 이 숫자가 프로젝트 5단계 측정표의 첫 줄입니다.

# %% [17-4] trigger 간격을 5초로 바꿔 같은 실험을 하십시오. (체크포인트 이름을 lat5 로)
lat = []
ql = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "lat17")
      .option("startingOffsets", "latest")
      .load()
      .writeStream.foreachBatch(measure)
      .option("checkpointLocation", "chk/s17/lat5")
      .trigger(processingTime="5 seconds")
      .start())
time.sleep(5)
send_part(1, topic="lat17", speed=600)
time.sleep(10)
ql.stop()
print(lat)
print("평균 지연(초):", round(sum(n * s for _, n, s in lat) / sum(n for _, n, s in lat), 2))
# ── 결과 (작성 환경에서 잰 숫자 — 시간 · 속도는 PC 마다 다름. 모양과 크고 작음만 비교) ──
# [(1, 59, 0.9), (2, 142, 4.0)]
# 평균 지연(초): 3.09
# → 배치 수는 줄고(한 배치에 더 많이), 평균 지연은 늘었습니다(버스를 오래 기다림). 배차 간격 그대로의 맞바꿈입니다.

# %% [17-5] 집계 조각 수의 효과 — 5분 창 집계를 shuffle.partitions 4 와 50 으로 각각 돌려 걸린 시간을 비교하십시오.
def timed(nparts, name):
    spark.conf.set("spark.sql.shuffle.partitions", nparts)
    t0 = time.time()
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", "tags")
         .option("startingOffsets", "earliest")
         .load()
         .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
         .withWatermark("tag_time", "10 minutes")
         .groupBy(F.window("tag_time", "5 minutes"), "station_id").count()
         .writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", f"chk/s17/{name}")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return round(time.time() - t0, 1)

print("shuffle 4 :", timed(4, "sp4"), "초")
print("shuffle 50:", timed(50, "sp50"), "초")
spark.conf.set("spark.sql.shuffle.partitions", 4)
# ── 결과 (작성 환경에서 잰 숫자 — 시간 · 속도는 PC 마다 다름. 모양과 크고 작음만 비교) ──
# shuffle 4 : 5.1 초
# shuffle 50: 19.8 초
# → 50 쪽이 더 느립니다. 창 집계는 조각마다 상태(state)를 저장하는데, 데이터는 2,008줄뿐인데 조각이 50개면 빈 조각을 열고 닫는 일만
# 늘어납니다. 기본값은 200 입니다 — 노트북 실습에서 준비 셀에 4 를 넣어 둔 이유입니다. 그리고 이 값은 체크포인트를 새로 만들 때만 바뀝니다(그래서
# 이름을 sp4 · sp50 으로 나눴습니다).

# %% [17-6] 프로듀서 쪽 — 20,000개를 linger.ms 0 과 50 으로 각각 보내 초당 개수를 재 보십시오.
from confluent_kafka import Producer
def send_bulk(linger):
    p = Producer({"bootstrap.servers": BOOT, "linger.ms": linger})
    t0 = time.time()
    for i in range(20000):
        p.produce("perf17", value=f"m{i}")
        if i % 5000 == 0:
            p.poll(0)
    p.flush()
    return round(20000 / (time.time() - t0))

print("linger 0 :", send_bulk(0), "개/초")
print("linger 50:", send_bulk(50), "개/초")
# ── 결과 (작성 환경에서 잰 숫자 — 시간 · 속도는 PC 마다 다름. 모양과 크고 작음만 비교) ──
# linger 0 : 158628 개/초
# linger 50: 907092 개/초
# → 숫자는 PC 마다 크게 다릅니다. 보통 linger 를 주면 한 번에 모아 보내서 초당 개수가 같거나 늘고, 메시지 하나하나는 그만큼(최대 50ms) 늦게
# 출발합니다. 내 PC 에서는 어느 쪽이 빨랐는지 측정표에 적으십시오 — 방향이 반대로 나와도 틀린 것이 아닙니다. 데이터가 작고 Kafka 가 같은 PC 에
# 있으면 차이가 거의 안 날 수도 있습니다.

# %% [17-7] 내 PC 의 측정표를 만드십시오. 17-1 ~ 17-6 의 숫자를 아래 사전에 직접 옮겨 적고 표로 찍습니다.
import pandas as pd
table = pd.DataFrame([
    {"손잡이": "maxOffsetsPerTrigger 500", "숫자": "배치 5개 · 498/배치",     "느낀 점": "밀렸을 때 나눠 먹는다"},
    {"손잡이": "trigger 1초",              "숫자": "지연 약 ?초",             "느낀 점": ""},
    {"손잡이": "trigger 5초",              "숫자": "지연 약 ?초",             "느낀 점": "배치 적고 지연 큼"},
    {"손잡이": "shuffle 4 → 50",           "숫자": "?초 → ?초",               "느낀 점": "작은 데이터엔 4"},
    {"손잡이": "linger 0 → 50",            "숫자": "?개/초 → ?개/초",          "느낀 점": ""},
])
print(table.to_string(index=False))
# ── 결과 (? 는 학생이 채울 칸) ──
#                      손잡이             숫자         느낀 점
# maxOffsetsPerTrigger 500 배치 5개 · 498/배치 밀렸을 때 나눠 먹는다
#               trigger 1초        지연 약 ?초
#               trigger 5초        지연 약 ?초   배치 적고 지연 큼
#           shuffle 4 → 50        ?초 → ?초    작은 데이터엔 4
#            linger 0 → 50    ?개/초 → ?개/초
# → ? 자리에 내 PC 의 숫자를 넣으십시오. 이 표가 프로젝트 5단계 발표의 뼈대입니다. 팀 프로젝트에서는 자기 주제의 데이터로, 한 번에 손잡이 하나씩
# 바꿔 같은 표를 만듭니다.

# %% [17-8] 일부러 틀려 보십시오. maxOffsetsPerTrigger 에 0 을 주고 켠 뒤, 5초 뒤에 돌고 있는지 · 오류가 있는지 · 몇 줄 읽었는지 보십시오.
q0 = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "tags")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 0)
      .load()
      .writeStream.format("noop")
      .option("checkpointLocation", "chk/s17/zero")
      .trigger(processingTime="1 second")
      .start())
time.sleep(5)
print("돌고 있나:", q0.isActive, "/ 오류:", q0.exception())
print("읽은 줄:", sum(p["numInputRows"] for p in q0.recentProgress))
q0.stop()
# ── 결과 (Spark 4.1.3 — 오류 없이 조용히 0줄) ──
# 돌고 있나: True / 오류: None
# 읽은 줄: 0
# → 오류가 나지 않습니다. 쿼리는 멀쩡히 돌고 있는데 읽은 줄이 0 — 배치당 0줄이라 영원히 아무것도 못 읽습니다. Spark 는 이 값을 막아 주지
# 않습니다. 빨간 오류보다 무서운 조용한 실수입니다(1과목에서 본 그것). 바이브 코딩으로 받은 설정 값은 공식 문서(Structured Streaming +
# Kafka Integration Guide)에서 이름과 범위를 확인하고, 켠 뒤에는 numInputRows(17-2)를 꼭 봅니다.
