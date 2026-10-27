# ==============================================================================
#  STEP 16  정리하고 내보내기 — OPTIMIZE · ZORDER · Gold 를 Kafka 로
#  C:\kafkacamp\answer\s16_optimize.py
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
from tools.replay import counts
st = spark.read.csv("data/stations.csv", header=True)
BRONZE, SILVER = "lake/s15/bronze", "lake/s15/silver"
# ── 결과 ──
# Spark 4.1.3 + Kafka + Delta 준비 완료


# %% [16-1] Bronze 의 파일 수와 크기를 보십시오.
DeltaTable.forPath(spark, BRONZE).detail().select("numFiles", "sizeInBytes").show()
# ── 결과 (Delta 화면 — 작성 환경은 Delta 를 같은 규칙으로 흉내 내어 검산, 0.6 의 점검 파일로 확인) ──
# +--------+-----------+
# |numFiles|sizeInBytes|
# +--------+-----------+
# |       9|     141714|
# +--------+-----------+
# → numFiles 9 — STEP 15 에서 Bronze 에 쓴 배치가 셋(15-2 · 15-5 · 15-7)이고, 배치마다 Kafka 파티션 수(3)만큼
# 파일을 썼습니다. 크기 숫자는 PC 마다 조금 다릅니다.

# %% [16-2] Bronze 를 OPTIMIZE 하고, 몇 개를 빼고 몇 개를 더했는지 보십시오.
m = DeltaTable.forPath(spark, BRONZE).optimize().executeCompaction()
m.select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
# ── 결과 (Delta 화면 — 작성 환경은 Delta 를 같은 규칙으로 흉내 내어 검산, 0.6 의 점검 파일로 확인) ──
# +---------------+-------------+
# |numFilesRemoved|numFilesAdded|
# +---------------+-------------+
# |              9|            1|
# +---------------+-------------+
# → 9 개를 빼고 1 개를 더했습니다. 1과목 17-11 의 20 → 1 과 같은 모양입니다.

# %% [16-3] 다시 파일 수와 줄 수를 보십시오.
DeltaTable.forPath(spark, BRONZE).detail().select("numFiles").show()
spark.read.format("delta").load(BRONZE).count()
# ── 결과 (Delta 화면 — 작성 환경은 Delta 를 같은 규칙으로 흉내 내어 검산, 0.6 의 점검 파일로 확인) ──
# +--------+
# |numFiles|
# +--------+
# |       1|
# +--------+
#
# 4016
# → 파일 1 개, 줄은 그대로 4016(15-7 의 중복까지 그대로). OPTIMIZE 는 그릇만 바꾸고 내용은 안 건드립니다 — 중복 정리는 Silver 의
# 일입니다.

# %% [16-4] Silver 를 탄 역(board_st) 기준으로 ZORDER 하십시오.
m = DeltaTable.forPath(spark, SILVER).optimize().executeZOrderBy("board_st")
m.select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
# ── 결과 (Delta 화면 — numFilesRemoved 는 PC 마다 다를 수 있음) ──
# +---------------+-------------+
# |numFilesRemoved|numFilesAdded|
# +---------------+-------------+
# |              1|            1|
# +---------------+-------------+
# → 파일이 하나로 합쳐지면서 board_st 가 비슷한 줄끼리 모였습니다. 지금은 데이터가 작아서 효과를 재기 어렵습니다 — 수억 줄에서 "광화문 승차만" 을
# 물을 때 읽는 파일 수가 줄어드는 것이 ZORDER 의 쓸모입니다. 뺀 파일 수는 PC 마다 다를 수 있습니다(MERGE 가 파일을 어떻게 나눴는지에 따라).

# %% [16-5] Bronze 의 역사를 보십시오.
DeltaTable.forPath(spark, BRONZE).history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 작성 환경은 Delta 를 같은 규칙으로 흉내 내어 검산, 0.6 의 점검 파일로 확인) ──
# +-------+----------------+
# |version|       operation|
# +-------+----------------+
# |      3|        OPTIMIZE|
# |      2|STREAMING UPDATE|
# |      1|STREAMING UPDATE|
# |      0|STREAMING UPDATE|
# +-------+----------------+
# → STREAMING UPDATE 세 번(스트리밍이 쓴 배치) 다음에 OPTIMIZE. 스트리밍으로 쓴 것도 배치마다 장부에 한 줄씩 남습니다.

# %% [16-6] Gold — Silver 로 역 이름별 승차 인원을 만들어 lake/s16/gold_board 에 저장하고 보십시오.
gold = (spark.read.format("delta").load(SILVER)
        .filter(F.col("board_st").isNotNull())
        .groupBy("board_st").agg(F.count("*").alias("board_cnt"))
        .join(st, F.col("board_st") == F.col("station_id"))
        .select("station_id", "station_nm", "line", "board_cnt")
        .orderBy(F.col("board_cnt").desc()))
gold.write.format("delta").mode("overwrite").save("lake/s16/gold_board")
gold.show()
# ── 결과 ──
# +----------+----------+-----+---------+
# |station_id|station_nm| line|board_cnt|
# +----------+----------+-----+---------+
# |       S08|    광화문|5호선|      147|
# |       S02|      역삼|2호선|      127|
# |       S05|    서울역|1호선|      126|
# |       S01|      강남|2호선|      121|
# |       S04|      시청|1호선|      121|
# |       S03|  홍대입구|2호선|      119|
# |       S06|      잠실|2호선|      117|
# |       S07|    여의도|5호선|      114|
# +----------+----------+-----+---------+
# → 1과목 19-10 과 같은 숫자(광화문 147 · 역삼 127 · 서울역 126 …). Kafka 로 받아 만든 Gold 가 파일로 만든 Gold 와
# 같습니다.

# %% [16-6 바꿔 보기 ①] Gold 를 노선(line)별로 바꾸면?
(gold.groupBy("line").agg(F.sum("board_cnt").alias("board_cnt"))
     .orderBy(F.col("board_cnt").desc()).show())
# ── 결과 ──
# +-----+---------+
# | line|board_cnt|
# +-----+---------+
# |2호선|      484|
# |5호선|      261|
# |1호선|      247|
# +-----+---------+
# → 이래서 — 2호선 484 · 5호선 261 · 1호선 247 — 1과목 마무리 H-3 의 답과 같습니다. Gold 는 묻는 질문마다 하나씩 만드는 작은
# 표입니다. 역별 · 노선별 · 시간대별 — 앱 화면 하나에 Gold 하나가 붙는다고 생각하면 됩니다.

# %% [16-7] Gold 를 gold_board 토픽으로 보내십시오. key = 역 코드, value = 네 칸의 JSON.
(gold.select(F.col("station_id").alias("key"),
             F.to_json(F.struct("station_id", "station_nm", "line", "board_cnt")).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("topic", "gold_board")
     .save())
counts("gold_board")
# ── 결과 ──
# [8]
# → 8 줄 — 역 여덟 곳. 이 토픽이 3과목의 입구입니다. 다시 보내면(재실행) 같은 역이 한 줄 더 쌓이지만, 받는 쪽(MongoDB)이 역 코드를 _id
# 로 덮어쓰면 결과는 같습니다(멱등).

# %% [16-8] 일부러 틀려 보십시오. Delta 가 아닌 폴더(data/stream_src)를 OPTIMIZE 해 보십시오.
DeltaTable.forPath(spark, "data/stream_src").optimize().executeCompaction()
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [DELTA_MISSING_DELTA_TABLE] `data/stream_src` is not a Delta table.
# → is not a Delta table — 장부(_delta_log)가 없는 폴더는 Delta 가 아니다(1과목 16-12). OPTIMIZE · MERGE
# · history 는 Delta 테이블에만 됩니다.
