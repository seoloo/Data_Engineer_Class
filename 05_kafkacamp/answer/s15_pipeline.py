# ==============================================================================
#  STEP 15  Kafka → Bronze → Silver — 정확히 한 번의 파이프라인
#  C:\kafkacamp\answer\s15_pipeline.py
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
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


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
# ── 결과 ──
# tags_live 비움 · lake/s15 · chk/s15 비움: [0, 0, 0]


# %% [15-1] 조각 1~3 을 tags_live 로 보내십시오.
for k in [1, 2, 3]:
    send_part(k)
counts("tags_live")
# ── 결과 ──
# [232, 215, 156]
# → 합계 603 (201 × 3). 1과목 19-1 의 "폴더에 조각 셋" 과 같은 상태입니다.

# %% [15-2] ① Bronze — tags_live 를 읽어 value 를 RAW_SCHEMA 로 펼치고, partition · offset · Kafka
# timestamp(kafka_ts) · 받은 시각(ingest_time) 을 붙여 Delta 로 쌓는 함수 run_bronze() 를 만들고 실행하십시오.
def run_bronze():
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", "tags_live")
         .option("startingOffsets", "earliest")
         .load()
         .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e"),
                 "partition", "offset", F.col("timestamp").alias("kafka_ts"))
         .select("e.*", "partition", "offset", "kafka_ts")
         .withColumn("ingest_time", F.current_timestamp())
         .writeStream
         .format("delta")
         .option("checkpointLocation", "chk/s15/bronze")
         .trigger(availableNow=True)
         .start("lake/s15/bronze"))
    q.awaitTermination()
    return spark.read.format("delta").load("lake/s15/bronze").count()

run_bronze()
# ── 결과 ──
# 603
# → 603. 1과목 19-2 와 같은 숫자인데, 이번에는 줄마다 어느 장부의 몇 번(partition · offset) 에서 왔는지가 같이 적혀 있습니다.
# 시각은 글자 그대로(RAW_SCHEMA) — 깨진 07:67:00 도 원문대로 남습니다.

# %% [15-3] ② Silver 의 일꾼 함수 upsert_trips 를 만드십시오. (1과목 19-3 과 같고, 경로만 s15)
def upsert_trips(batch_df, batch_id):
    ev = (batch_df.dropDuplicates(["event_id"])
                  .withColumn("tag_time", F.try_to_timestamp("tag_time"))
                  .filter(F.col("station_id").isNotNull() & F.col("station_id").isin(valid_ids)
                          & F.col("tag_time").isNotNull()))
    new = to_trips(ev)
    if not DeltaTable.isDeltaTable(spark, "lake/s15/silver"):
        new.write.format("delta").save("lake/s15/silver")
        print(f"  배치 {batch_id}: 새 테이블 {new.count()} 건")
        return
    m = (DeltaTable.forPath(spark, "lake/s15/silver").alias("s")
           .merge(new.alias("n"), "s.trip_id = n.trip_id")
           .whenMatchedUpdate(set=rules)
           .whenNotMatchedInsertAll()
           .execute()
           .first())
    print(f"  배치 {batch_id}: 고침 {m['num_updated_rows']} · 넣음 {m['num_inserted_rows']}")
# ── 결과 ──
# (출력 없음)
# → 정의만 했으니 출력이 없습니다. 1과목에서 쓴 것과 똑같습니다 — Silver 는 데이터가 어디서 왔는지 모릅니다. Bronze 만 보면 되도록 나눠 둔
# 덕분입니다.

# %% [15-4] ② Silver 스트리밍 run_silver() 를 만들고 실행하십시오. (이동 수, 하차 없음) 을 돌려줍니다.
def run_silver():
    q = (spark.readStream.format("delta").load("lake/s15/bronze")
         .writeStream
         .foreachBatch(upsert_trips)
         .option("checkpointLocation", "chk/s15/silver")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    s = spark.read.format("delta").load("lake/s15/silver")
    return s.count(), s.filter(F.col("alight_st").isNull()).count()

run_silver()
# ── 결과 ──
#   배치 0: 새 테이블 381 건
# (381, 167)
# → 배치 0: 새 테이블 381 건 · (381, 167) — 1과목 19-4 와 같은 숫자입니다. 입구가 폴더에서 Kafka 로 바뀌었어도 같은 데이터면
# 같은 답.

# %% [15-5] 나머지 조각 4~10 을 보내고, Bronze → Silver 를 다시 돌리십시오.
for k in range(4, 11):
    send_part(k)
print("Bronze:", run_bronze())
print("Silver:", run_silver())
# ── 결과 ──
# Bronze: 2008
#   배치 1: 고침 161 · 넣음 619
# Silver: (1000, 14)
# → Bronze 2008, Silver 배치 1: 고침 161 · 넣음 619 → (1000, 14). 1과목 19-5 와 같습니다.

# %% [15-6] Silver 로 기준 숫자 넷을 구하십시오 — 이동 수 · 하차 없음 · 승차 없음 · 요금 합계.
s = spark.read.format("delta").load("lake/s15/silver")
s.agg(F.count("*").alias("trips"),
      F.sum(F.when(F.col("alight_st").isNull(), 1).otherwise(0)).alias("no_alight"),
      F.sum(F.when(F.col("board_st").isNull(), 1).otherwise(0)).alias("no_board"),
      F.sum("fare").alias("fare_sum")).show()
# ── 결과 ──
# +-----+---------+--------+--------+
# |trips|no_alight|no_board|fare_sum|
# +-----+---------+--------+--------+
# | 1000|       14|       8| 1647800|
# +-----+---------+--------+--------+
# → 1000 · 14 · 8 · 1,647,800. 파일(1과목 STEP 13) · 폴더 스트리밍(1과목 STEP 19) · Kafka 스트리밍(지금) — 세
# 길이 모두 같은 답입니다.

# %% [15-7] 장애 실험 ① — Bronze 의 체크포인트를 지우고 run_bronze() 를 다시 돌리십시오. Bronze 는 몇 줄이 될까요?
shutil.rmtree("chk/s15/bronze")
print("Bronze:", run_bronze())
# ── 결과 ──
# Bronze: 4016
# → 4016 = 2008 × 2. 체크포인트(= Kafka 오프셋 메모)를 잃었으니 startingOffsets=earliest 로 tags_live 를
# 처음부터 다시 읽었고, Bronze 는 append(덧붙이기) 라 그대로 두 번 쌓였습니다. 1과목 18-16 과 같은 "이중 청구" 입니다.

# %% [15-8] 장애 실험 ② — 이 상태로 run_silver() 를 돌리십시오. Silver 도 두 배가 될까요?
print("Silver:", run_silver())
# ── 결과 ──
#   배치 2: 고침 1000 · 넣음 0
# Silver: (1000, 14)
# → 배치 2: 고침 1000 · 넣음 0 → (1000, 14) 그대로. Bronze 에 새로 붙은 2,008줄을 Silver 가 받았지만, MERGE(멱등)
# 라서 같은 이동을 덮어썼을 뿐입니다. 체크포인트 + 멱등 쓰기 = 정확히 한 번의 결과 — 프로젝트 3단계의 한 문장이 이 두 숫자(4016 ·
# 1000)입니다.

# %% [15-8 바꿔 보기 ①] Bronze 도 멱등으로 만들려면? — (partition, offset) 을 키로 MERGE 하는 Bronze 를 새로(bronze2) 만들어 두 번 돌리면
def bronze_merge(batch_df, batch_id):
    path = "lake/s15/bronze2"
    if not DeltaTable.isDeltaTable(spark, path):
        batch_df.write.format("delta").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("s")
        .merge(batch_df.alias("n"), "s.partition = n.partition AND s.offset = n.offset")
        .whenNotMatchedInsertAll()
        .execute())

def run_bronze2():
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", "tags_live")
         .option("startingOffsets", "earliest")
         .load()
         .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e"),
                 "partition", "offset", F.col("timestamp").alias("kafka_ts"))
         .select("e.*", "partition", "offset", "kafka_ts")
         .writeStream.foreachBatch(bronze_merge)
         .option("checkpointLocation", "chk/s15/bronze2")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return spark.read.format("delta").load("lake/s15/bronze2").count()

print("처음:", run_bronze2())
shutil.rmtree("chk/s15/bronze2")
print("체크포인트 지우고 다시:", run_bronze2())
# ── 결과 ──
# 처음: 2008
# 체크포인트 지우고 다시: 2008
# → 이래서 — 2008 → 2008. 송장(partition, offset)을 키로 없을 때만 넣기(whenNotMatchedInsertAll)를 하면,
# Kafka 를 처음부터 다시 읽어도 Bronze 가 늘지 않습니다. 대신 MERGE 는 append 보다 느립니다(매번 기존 테이블과 맞춰 봐야 하니까).
# 그래서 보통은 Bronze 는 빠르게 append, 중복은 Silver 에서 거르는 쪽을 고릅니다 — 어느 쪽을 골랐는지와 이유를 프로젝트 발표에 쓰십시오.

# %% [15-9] Bronze 에서 같은 (partition, offset) 이 몇 번씩 들어 있는지 세고, 송장으로 중복을 걸러 내면 몇 줄인지 보십시오.
b = spark.read.format("delta").load("lake/s15/bronze")
b.groupBy("partition", "offset").count().groupBy("count").count().show()
print("송장 기준으로 거르면:", b.dropDuplicates(["partition", "offset"]).count())
# ── 결과 ──
# +-----+-----+
# |count|count|
# +-----+-----+
# |    2| 2008|
# +-----+-----+
#
# 송장 기준으로 거르면: 2008
# → (partition, offset) 이 두 번씩 들어간 것이 2,008개. 송장으로 거르면 2008 — 원래 Kafka 에 있던 그대로입니다. 같은 장부의
# 같은 번호는 같은 메시지이기 때문입니다. Bronze 에 송장 칸을 남겨 둔 이유입니다. (event_id 로 거르면 1988 — 그건 개찰구의 재전송까지
# 거른 것)

# %% [15-10] Bronze 체크포인트에 무엇이 적혀 있는지 보십시오. offsets 폴더의 마지막 파일 맨 끝 줄입니다.
last = sorted((f for f in os.listdir("chk/s15/bronze/offsets") if f.isdigit()), key=int)[-1]
with open(f"chk/s15/bronze/offsets/{last}", encoding="utf-8") as f:
    lines = f.read().splitlines()
print("파일:", last)
print(json.dumps(json.loads(lines[-1]), sort_keys=True))
print(counts("tags_live"))
# ── 결과 ──
# 파일: 0
# {"tags_live": {"0": 686, "1": 698, "2": 624}}
# [686, 698, 624]
# → offsets 폴더에는 0 · 1 · 2 … 같은 숫자 파일(배치마다 하나)과 .0.crc 같은 검사용 파일이 섞여 있어서 숫자 이름만
# 골랐습니다(f.isdigit()). 맨 끝 줄은 {"tags_live": {"0": …, "1": …, "2": …}} — 파티션마다 다음에 읽을 오프셋. 그
# 숫자가 counts("tags_live") 와 같습니다(끝까지 읽었으니). 파일 이름이 0 하나뿐인 것은 15-7 에서 체크포인트를 지우고 새로 시작했기
# 때문입니다. Spark 의 체크포인트는 Kafka 컨슈머 그룹의 책갈피(STEP 05)를 자기 폴더에 따로 적어 두는 것입니다. 그래서 Spark 는
# Kafka 의 그룹 커밋에 기대지 않습니다.

# %% [15-11] Silver 의 역사를 보십시오.
DeltaTable.forPath(spark, "lake/s15/silver").history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 작성 환경은 Delta 를 같은 규칙으로 흉내 내어 검산, 0.6 의 점검 파일로 확인) ──
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      2|    MERGE|
# |      1|    MERGE|
# |      0|    WRITE|
# +-------+---------+
# → 0 WRITE(조각 1~3) · 1 MERGE(조각 4~10) · 2 MERGE(장애 뒤 Bronze 의 중복분). 장애가 장부에 남습니다.
