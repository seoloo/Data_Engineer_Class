# ==============================================================================
#  STEP 14  결과를 다시 Kafka 로 — 계산한 것을 다른 프로그램에 넘기기
#  C:\kafkacamp\answer\s14_spark_write.py
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
from tools.replay import counts
from confluent_kafka.admin import AdminClient
adm = AdminClient({"bootstrap.servers": BOOT})
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
import shutil
old = [t for t in ["station_cnt", "win5"] if t in adm.list_topics(timeout=5).topics]
for t in old:
    adm.delete_topics([t])[t].result()
time.sleep(3)
shutil.rmtree("chk/s14", ignore_errors=True)
print("지운 토픽:", old)
# ── 결과 ──
# 지운 토픽: []


# %% [14-1] tags 를 배치로 읽어 역별 승차 수(중복 제거 · S99 포함)를 구한 DataFrame boards 를 만드십시오.
ev = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "tags")
      .option("startingOffsets", "earliest")
      .load()
      .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
      .select("e.*")
      .dropDuplicates(["event_id"]))
boards = ev.filter(F.col("tag_type") == "IN").groupBy("station_id").agg(F.count("*").alias("boards"))
boards.orderBy("station_id").show()
# ── 결과 ──
# +----------+------+
# |station_id|boards|
# +----------+------+
# |       S01|   121|
# |       S02|   128|
# |       S03|   119|
# |       S04|   121|
# |       S05|   126|
# |       S06|   118|
# |       S07|   114|
# |       S08|   147|
# |       S99|     6|
# +----------+------+
# → 중복을 빼고 센 역별 승차 수입니다. 1과목 13-11 의 clean_cnt 와 비교하면 S99 의 6 과 깨진 시각 줄만큼 다릅니다(여기서는 격리를 안
# 했으니).

# %% [14-2] boards 를 key = 역 코드, value = JSON 모양으로 바꿔 보십시오. (아직 보내지 않습니다)
out = boards.select(F.col("station_id").alias("key"),
                    F.to_json(F.struct("station_id", "boards")).alias("value"))
out.show(3, truncate=False)
# ── 결과 ──
# +---+---------------------------------+
# |key|value                            |
# +---+---------------------------------+
# |S01|{"station_id":"S01","boards":121}|
# |S07|{"station_id":"S07","boards":114}|
# |S05|{"station_id":"S05","boards":126}|
# +---+---------------------------------+
# only showing top 3 rows
# → value 칸에 {"station_id":"S01","boards":121} 같은 JSON 글자. struct 는 여러 칸을 하나로 묶고, to_json
# 은 그 묶음을 JSON 글자로 바꿉니다. from_json 의 반대입니다.

# %% [14-2 바꿔 보기 ①] struct 안에 역 이름(station_nm) 까지 넣으려면? — 역 마스터를 붙여서
st = spark.read.csv("data/stations.csv", header=True)
(boards.join(st, "station_id", "left")
    .select(F.col("station_id").alias("key"),
            F.to_json(F.struct("station_id", "station_nm", "boards")).alias("value"))
    .orderBy("key")
    .show(3, truncate=False))
# ── 결과 ──
# +---+---------------------------------------------------------+
# |key|value                                                    |
# +---+---------------------------------------------------------+
# |S01|{"station_id":"S01","station_nm":"강남","boards":121}    |
# |S02|{"station_id":"S02","station_nm":"역삼","boards":128}    |
# |S03|{"station_id":"S03","station_nm":"홍대입구","boards":119}|
# +---+---------------------------------------------------------+
# only showing top 3 rows
# → 이래서 — struct 에 칸을 더 넣기만 하면 JSON 에 그 칸이 늘어납니다. 한글(광화문 · 강남)도 그대로 들어갑니다(JSON 은 UTF-8).
# left 로 붙였으니 S99 는 station_nm 이 없어서 JSON 에서 그 칸이 빠집니다(null 인 칸은 to_json 이 생략). 받는 쪽은 "칸이
# 없을 수도 있다" 를 알고 있어야 합니다 — 3과목 MongoDB 의 "칸이 문서마다 달라도 된다" 로 이어집니다.

# %% [14-3] out 을 station_cnt 토픽으로 보내고, 파티션별 개수를 보십시오.
(out.write.format("kafka")
    .option("kafka.bootstrap.servers", BOOT)
    .option("topic", "station_cnt")
    .save())
counts("station_cnt")
# ── 결과 ──
# [9]
# → 합계 9(역 8 + S99). station_cnt 토픽은 자동으로 생겼습니다(03-10) — 그래서 파티션이 1개입니다. 실무에서는 결과 토픽도 미리
# 만들어 파티션 수를 정합니다.

# %% [14-4] station_cnt 를 Spark 로 다시 읽어 key · value 를 글자로 보십시오.
(spark.read.format("kafka")
      .option("kafka.bootstrap.servers", BOOT)
      .option("subscribe", "station_cnt")
      .option("startingOffsets", "earliest")
      .load()
      .select(F.col("key").cast("string"), F.col("value").cast("string"))
      .show(truncate=False))
# ── 결과 ──
# +---+---------------------------------+
# |key|value                            |
# +---+---------------------------------+
# |S01|{"station_id":"S01","boards":121}|
# |S07|{"station_id":"S07","boards":114}|
# |S05|{"station_id":"S05","boards":126}|
# |S99|{"station_id":"S99","boards":6}  |
# |S03|{"station_id":"S03","boards":119}|
# |S02|{"station_id":"S02","boards":128}|
# |S08|{"station_id":"S08","boards":147}|
# |S04|{"station_id":"S04","boards":121}|
# |S06|{"station_id":"S06","boards":118}|
# +---+---------------------------------+
# → 보낸 그대로 아홉 줄. 이 토픽을 3과목에서 파이썬 컨슈머가 읽어 MongoDB 에 넣습니다.

# %% [14-5] 스트리밍으로 — tags_live(지금 10조각이 다 있음)를 읽어 5분 창 승차 수(워터마크 10분)를 win5 토픽에 쓰는 쿼리를 availableNow
# 로 돌리고, win5 에 몇 개가 들어갔는지 보십시오.
live = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOT)
        .option("subscribe", "tags_live")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("tag_type") == "IN"))
win = (live.withWatermark("tag_time", "10 minutes")
       .groupBy(F.window("tag_time", "5 minutes").alias("w"))
       .count()
       .select(F.date_format("w.start", "HH:mm").alias("key"),
               F.to_json(F.struct(F.col("w.start").cast("string").alias("start"),
                                  "count")).alias("value")))
q = (win.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("topic", "win5")
     .option("checkpointLocation", "chk/s14/win5")
     .outputMode("update")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
counts("win5")
# ── 결과 ──
# [36]
# → win5 에 36줄 — 07:00 ~ 09:55 의 5분 창 36개마다 한 줄씩(1과목 12-2 의 창 수와 같음). 이번에는 update — "이번
# 배치에서 바뀐 창" 을 아직 닫히지 않았어도 보냅니다. 한 번에 다 읽었으니 창마다 한 번씩입니다. 조각조각 읽으면(13-7) 같은 창이 숫자가 바뀔 때마다
# 또 보내집니다 — 받는 쪽이 키(창 시작 시각)로 덮어쓰면 언제나 최신 숫자가 남습니다(멱등).

# %% [14-6] 일부러 틀려 보십시오. 14-5 의 쿼리를 checkpointLocation 없이 켜 보십시오.
(win.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("topic", "win5")
     .outputMode("update")
     .start())
# ── 결과 (오류 — 일부러 틀린 것. Spark 4.1.3 의 오류 문장) ──
# AnalysisException: checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...).
# → checkpointLocation must be specified — 체크포인트(어디까지 했나 메모) 없이 Kafka 로 쓰는 스트리밍은 켤 수 없다.
# 1과목 STEP 18 의 "계산서 옆 메모" 입니다.

# %% [14-7] 일부러 틀려 보십시오. value 칸 없이(이름이 v) 보내 보십시오.
(boards.select(F.col("station_id").alias("key"), F.col("boards").cast("string").alias("v"))
    .write.format("kafka")
    .option("kafka.bootstrap.servers", BOOT)
    .option("topic", "station_cnt")
    .save())
# ── 결과 (오류 — 일부러 틀린 것. Spark 4.1.3 의 오류 문장) ──
# AnalysisException: Required attribute 'value' not found
# → Required attribute 'value' not found — Kafka 로 보낼 내용(value)이 없다. 칸 이름을 정확히 value 로 붙여야
# 합니다(alias("value")).
