# ==============================================================================
#  STEP 12  Spark 로 Kafka 읽기 — 토픽을 표로
#  C:\kafkacamp\answer\s12_spark_read.py
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
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


# %% [12-1] tags 토픽을 배치로 읽어 raw 라는 이름에 담고, 스키마를 보십시오.
raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", "tags")
       .option("startingOffsets", "earliest")
       .load())
raw.printSchema()
# ── 결과 ──
# root
#  |-- key: binary (nullable = true)
#  |-- value: binary (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestampType: integer (nullable = true)
# → 칸 일곱 개. key · value 가 binary(바이트) 입니다. 어떤 토픽을 읽어도 이 일곱 칸은 같습니다.

# %% [12-2] raw 의 줄 수와 Spark 조각(파티션) 수를 보십시오.
print(raw.count())
print(raw.rdd.getNumPartitions())
# ── 결과 ──
# 2008
# 3
# → 2008 · 3. Kafka 의 장부 3개가 Spark 의 조각 3개가 됐습니다 — Kafka 파티션 하나를 Spark 일꾼 하나가 맡습니다. 그래서
# Kafka 파티션 수 = Spark 가 동시에 읽는 최대 수입니다(프로젝트 1단계와 5단계가 여기서 만납니다).

# %% [12-3] key · value 를 글자로 바꿔서 partition · offset 과 함께 세 줄 보십시오.
raw.select(F.col("key").cast("string").alias("key"),
           F.col("value").cast("string").alias("value"),
           "partition", "offset").show(3, truncate=60)
# ── 결과 ──
# +-----+------------------------------------------------------------+---------+------+
# |  key|                                                       value|partition|offset|
# +-----+------------------------------------------------------------+---------+------+
# |C0066|{"event_id": "E00002", "card_id": "C0066", "trip_id": "T0...|        0|     0|
# |C0286|{"event_id": "E00006", "card_id": "C0286", "trip_id": "T0...|        0|     1|
# |C0023|{"event_id": "E00008", "card_id": "C0023", "trip_id": "T0...|        0|     2|
# +-----+------------------------------------------------------------+---------+------+
# only showing top 3 rows
# → value 가 {"event_id": "E00… 로 시작하는 JSON 글자입니다. key 는 카드 번호. 각 장부의 offset 0 부터 나옵니다.

# %% [12-3 바꿔 보기 ①] cast 를 빼고 value 를 그대로 보면?
raw.select("key", "value").show(2, truncate=40)
# ── 결과 ──
# +----------------+----------------------------------------+
# |             key|                                   value|
# +----------------+----------------------------------------+
# |[43 30 30 36 36]|[7B 22 65 76 65 6E 74 5F 69 64 22 3A ...|
# |[43 30 32 38 36]|[7B 22 65 76 65 6E 74 5F 69 64 22 3A ...|
# +----------------+----------------------------------------+
# only showing top 2 rows
# → 이래서 — [7B 22 65 76 65 …] 처럼 16진수 바이트로 보입니다. 7B 는 { , 22 는 " 의 바이트 번호입니다. Kafka 는 내용을
# 모르고 바이트만 옮긴다(03-8)는 것을 Spark 에서 다시 확인한 것입니다. 그래서 cast("string") 이 언제나 첫 줄입니다.

# %% [12-4] value 를 SCHEMA 로 풀어 칸으로 펼친 DataFrame ev 를 만들고, 다섯 칸만 세 줄 보십시오. partition · offset 은 남겨
# 둡니다.
ev = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                 "partition", "offset")
         .select("e.*", "partition", "offset"))
ev.select("event_id", "card_id", "tag_type", "station_id", "tag_time", "partition", "offset").show(3)
# ── 결과 ──
# +--------+-------+--------+----------+-------------------+---------+------+
# |event_id|card_id|tag_type|station_id|           tag_time|partition|offset|
# +--------+-------+--------+----------+-------------------+---------+------+
# |  E00002|  C0066|      IN|       S01|2026-10-14 07:00:01|        0|     0|
# |  E00006|  C0286|      IN|       S03|2026-10-14 07:00:26|        0|     1|
# |  E00008|  C0023|      IN|       S08|2026-10-14 07:00:36|        0|     2|
# +--------+-------+--------+----------+-------------------+---------+------+
# only showing top 3 rows
# → 1과목에서 파일로 읽은 표와 같은 칸이 됐습니다. tag_time 이 timestamp 로 바뀐 것은 SCHEMA 덕분입니다. 여기서부터는 1과목의 코드를
# 그대로 씁니다.

# %% [12-5] ev 로 승하차별 줄 수와 이벤트 가짓수를 구하십시오.
ev.groupBy("tag_type").count().orderBy("tag_type").show()
print(ev.select("event_id").distinct().count())
# ── 결과 ──
# +--------+-----+
# |tag_type|count|
# +--------+-----+
# |      IN| 1012|
# |     OUT|  996|
# +--------+-----+
#
# 1988
# → IN 1012 · OUT 996 · 1988 — 1과목 08-1 · 08-10, STEP 09-7 과 같은 숫자. 파일 → Kafka → Spark 를
# 거쳐도 한 줄도 변하지 않았습니다.

# %% [12-6] Kafka 파티션별 줄 수를 세십시오.
ev.groupBy("partition").count().orderBy("partition").show()
# ── 결과 ──
# +---------+-----+
# |partition|count|
# +---------+-----+
# |        0|  686|
# |        1|  698|
# |        2|  624|
# +---------+-----+
# → STEP 09 의 counts("tags") 와 같은 세 숫자. 카드 번호로 나눴으니 한 카드의 기록은 한 칸(파티션)에만 있습니다.

# %% [12-7] tag_time 이 비어 있는(null) 줄을 보십시오. (깨진 시각 07:67 같은 것)
ev.filter(F.col("tag_time").isNull()).select("event_id", "station_id", "tag_time").show()
# ── 결과 ──
# +--------+----------+--------+
# |event_id|station_id|tag_time|
# +--------+----------+--------+
# |  E00192|       S02|    NULL|
# |  E01271|       S03|    NULL|
# |  E01388|       S07|    NULL|
# |  E01388|       S07|    NULL|
# |  E01716|       S06|    NULL|
# +--------+----------+--------+
# → 1과목 STEP 07 의 그 깨진 시각들입니다. from_json 은 설계도에 안 맞는 값을 오류 없이 null 로 만듭니다 — 조용한 실수. 그래서
# STEP 15 의 Bronze 는 시각을 글자 그대로 받아 둡니다.

# %% [12-8] Spark SQL 로 — ev 를 임시 뷰 ev 로 등록하고, 역별 승차 인원을 많은 순으로 구하십시오.
ev.createOrReplaceTempView("ev")
spark.sql("""
    SELECT station_id, COUNT(*) AS boards
    FROM ev
    WHERE tag_type = 'IN'
    GROUP BY station_id
    ORDER BY boards DESC
""").show()
# ── 결과 ──
# +----------+------+
# |station_id|boards|
# +----------+------+
# |       S08|   147|
# |       S02|   130|
# |       S05|   129|
# |       S01|   122|
# |       S04|   122|
# |       S03|   120|
# |       S06|   120|
# |       S07|   116|
# |       S99|     6|
# +----------+------+
# → 1과목 08-2 와 같은 표입니다(중복 포함, S99 포함). 데이터가 어디서 왔든(파일 · Kafka) 표가 되면 같은 SQL 을 씁니다.

# %% [12-9] 0번 파티션의 0~9번 오프셋만 읽으십시오. 장부를 직접 고르는 assign 과 오프셋 JSON 을 씁니다.
part0 = (spark.read.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("assign", """{"tags":[0]}""")
         .option("startingOffsets", """{"tags":{"0":0}}""")
         .option("endingOffsets", """{"tags":{"0":10}}""")
         .load())
print(part0.count())
part0.select("partition", "offset").agg(F.min("offset"), F.max("offset")).show()
# ── 결과 ──
# 10
# +-----------+-----------+
# |min(offset)|max(offset)|
# +-----------+-----------+
# |          0|          9|
# +-----------+-----------+
# → 10줄, offset 0 ~ 9. endingOffsets 는 그 번호 바로 앞까지(10 은 안 들어감)입니다. subscribe 대신 assign 으로
# 장부를 골랐습니다(07-9 의 파이썬 assign 과 같음). subscribe 로 하면서 오프셋 JSON 에 장부 하나만 적으면 "모든 파티션을
# 적어라"(you must specify all TopicPartitions) 오류가 납니다. "어제 장애 시각의 오프셋 구간만 다시 꺼내 보기" 같은 조사를
# 이렇게 합니다.

# %% [12-10] 일부러 틀려 보십시오. subscribe 를 빼고 읽어 보십시오.
spark.read.format("kafka").option("kafka.bootstrap.servers", BOOT).load()
# ── 결과 (오류 — 일부러 틀린 것. Spark 4.1.3 의 오류 문장) ──
# IllegalArgumentException: One of the following options must be specified for Kafka source: subscribe, subscribepattern, assign. See the docs for more details.
# → One of the following options must be specified for Kafka source: subscribe,
# subscribepattern, assign — 무엇을 읽을지 안 정했다. 바이브 코딩으로 받은 Spark-Kafka 코드에서 옵션 이름
# 오타(subcribe)가 나면 이 오류가 납니다.
