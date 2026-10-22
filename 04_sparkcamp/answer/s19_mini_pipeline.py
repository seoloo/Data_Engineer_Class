# ==============================================================================
#  STEP 19  미니 파이프라인 — 폴더 → Bronze → Silver, 그리고 장애 실험
#  C:\sparkcamp\answer\s19_mini_pipeline.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys
os.chdir(r"C:\sparkcamp")
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
builder = (SparkSession.builder
           .master("local[2]")
           .appName("sparkcamp")
           .config("spark.local.dir", r"C:\sparkcamp\tmp")
           .config("spark.jars.ivy", r"C:\sparkcamp\ivy")
           .config("spark.ui.showConsoleProgress", "false")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "+ Delta 준비 완료")

RAW_SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time STRING, fare INT, recv_time STRING
"""   # Bronze 용 — 시각 칸을 글자 그대로 (깨진 원문을 남긴다)

import shutil

def drop(n):
    """stream_src 의 n 번째 조각을 s19_in 으로 옮겨 놓는다 = 새 데이터 도착"""
    name = f"part_{n:02d}.jsonl"
    shutil.copy(f"data/stream_src/{name}", f"data/s19_in/_{name}")
    os.replace(f"data/s19_in/_{name}", f"data/s19_in/{name}")

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
# Spark 4.1.3 준비 완료


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for q in spark.streams.active:
    q.stop()
for p in ["data/s19_in", "lake/s19", "chk/s19"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs("data/s19_in")
print("s19_in · lake/s19 · chk/s19 비움")
# ── 결과 ──
# s19_in · lake/s19 · chk/s19 비움


# %% [19-1] 조각 1~3 을 도착시키십시오.
for n in [1, 2, 3]:
    drop(n)
sorted(os.listdir("data/s19_in"))
# ── 결과 ──
# ['part_01.jsonl', 'part_02.jsonl', 'part_03.jsonl']

# %% [19-2] ① Bronze — RAW_SCHEMA(시각 = 글자) 로 읽어 들어온 그대로에 받은 시각만 붙여 Delta 로 쌓는 함수를 만들고 한 번 실행하십시오.
def run_bronze():
    q = (spark.readStream.schema(RAW_SCHEMA).json("data/s19_in")
         .withColumn("ingest_time", F.current_timestamp())
         .writeStream
         .format("delta")
         .option("checkpointLocation", "chk/s19/bronze")
         .trigger(availableNow=True)
         .start("lake/s19/bronze"))
    q.awaitTermination()
    return spark.read.format("delta").load("lake/s19/bronze").count()

run_bronze()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 603
# → 603. Bronze 는 고치지 않고 그대로 — 중복도 S99 도 깨진 시각도 원문 글자 그대로(07:67:00) 들어 있습니다(STEP 16-4). 시각
# 바꾸기는 Silver 에서 합니다. 스트리밍 싱크를 Delta 로 하면 배치마다 장부에 한 줄씩 적히고, 반쯤 쓰다 멈춘 파일은 보이지 않습니다(ACID).

# %% [19-3] ② Silver 의 일꾼 함수 — 새 묶음을 받아 정리하고(STEP 13) 이동으로 접어(to_trips) MERGE 하는 upsert_trips 를
# 만드십시오. 처음이면 테이블을 새로 만듭니다.
def upsert_trips(batch_df, batch_id):
    ev = (batch_df.dropDuplicates(["event_id"])
                  .withColumn("tag_time", F.try_to_timestamp("tag_time"))      # 글자 → 시각 (깨진 것은 null)
                  .filter(F.col("station_id").isNotNull() & F.col("station_id").isin(valid_ids)
                          & F.col("tag_time").isNotNull()))                      # 격리 규칙 (STEP 13)
    new = to_trips(ev)
    if not DeltaTable.isDeltaTable(spark, "lake/s19/silver"):        # 처음이면
        new.write.format("delta").save("lake/s19/silver")
        print(f"  배치 {batch_id}: 새 테이블 {new.count()} 건")
        return
    m = (DeltaTable.forPath(spark, "lake/s19/silver").alias("s")      # 있으면 MERGE
           .merge(new.alias("n"), "s.trip_id = n.trip_id")
           .whenMatchedUpdate(set=rules)                               # 준비 셀의 rules (STEP 17)
           .whenNotMatchedInsertAll()
           .execute()
           .first())
    print(f"  배치 {batch_id}: 고침 {m['num_updated_rows']} · 넣음 {m['num_inserted_rows']}")
# ── 결과 ──
# (출력 없음)
# → 정의만 했으니 아무것도 안 나옵니다. 줄마다 어디서 배운 것인지 짚어 주십시오 — try_to_timestamp(STEP 09) ·
# dropDuplicates·격리 규칙(STEP 13) · to_trips·rules·MERGE(STEP 17) · isDeltaTable(16-12) ·
# foreachBatch 함수 모양(18-13).

# %% [19-4] ② Silver 스트리밍 — Bronze 를 스트리밍으로 읽어 묶음마다 upsert_trips 를 부르는 함수를 만들고 실행하십시오. Silver 의 이동
# 수와 하차 없음 수를 돌려주게 하십시오.
def run_silver():
    q = (spark.readStream.format("delta").load("lake/s19/bronze")
         .writeStream
         .foreachBatch(upsert_trips)
         .option("checkpointLocation", "chk/s19/silver")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    s = spark.read.format("delta").load("lake/s19/silver")
    return s.count(), s.filter(F.col("alight_st").isNull()).count()

run_silver()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
#   배치 0: 새 테이블 381 건
# (381, 167)
# → 배치 0 에서 새 테이블이 만들어졌습니다. (이동 수, 하차 없음) 짝으로 나옵니다. 아직 하차가 안 들어온 이동이 많습니다.

# %% [19-5] 나머지 조각 4~10 을 한꺼번에 도착시키고, Bronze → Silver 를 차례로 다시 돌리십시오.
for n in range(4, 11):
    drop(n)
print("Bronze:", run_bronze())
print("Silver:", run_silver())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# Bronze: 2008
#   배치 1: 고침 161 · 넣음 619
# Silver: (1000, 14)
# → Bronze 2008 (원본 전부). Silver 는 배치 1 에서 MERGE — 기다리던 하차들이 고침, 새 이동이 넣음으로 들어왔습니다. (1000,
# 14) 가 나와야 합니다.

# %% [19-6] 배치와 같은 답인가? — Silver 로 STEP 13 · 17 의 기준 숫자를 다시 구하십시오. 이동 수 · 하차 없음 · 승차 없음 · 요금 합계.
s = spark.read.format("delta").load("lake/s19/silver")
s.agg(F.count("*").alias("trips"),
      F.sum(F.when(F.col("alight_st").isNull(), 1).otherwise(0)).alias("no_alight"),
      F.sum(F.when(F.col("board_st").isNull(), 1).otherwise(0)).alias("no_board"),
      F.sum("fare").alias("fare_sum")).show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-----+---------+--------+--------+
# |trips|no_alight|no_board|fare_sum|
# +-----+---------+--------+--------+
# | 1000|       14|       8| 1647800|
# +-----+---------+--------+--------+
# → 1000 · 14 · 8 · 1,647,800 — STEP 13-12 의 요금 합계, STEP 17-6 의 세 숫자와 정확히 같습니다. 데이터를 한 번에
# 받든(배치) 열 조각으로 나눠 받든(스트리밍) 같은 답. 파이프라인을 검증하는 가장 확실한 방법입니다.

# %% [19-7] 새 조각 없이 Bronze → Silver 를 한 번 더 돌려 보십시오.
print("Bronze:", run_bronze())
print("Silver:", run_silver())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# Bronze: 2008
# Silver: (1000, 14)
# → 배치가 안 생기고 숫자 그대로. 체크포인트가 "이미 다 읽었다" 고 기억합니다.

# %% [19-8] 장애 실험 — Silver 의 체크포인트를 지우고 다시 돌리십시오. 숫자가 두 배가 될까요?
shutil.rmtree("chk/s19/silver")
print("Silver:", run_silver())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
#   배치 0: 고침 1000 · 넣음 0
# Silver: (1000, 14)
# → 메모를 잃었으니 Bronze 2008 줄을 처음부터 다시 읽었습니다(배치 0). 그런데 결과는 (1000, 14) 그대로 — 전부 "고침", 넣음 0.
# STEP 18-16 에서는 두 배가 됐습니다. 차이는 쓰는 방법입니다 — append 는 두 번 쓰면 두 배, MERGE 는 두 번 써도 한 번(멱등).
# 체크포인트가 깨져도 결과가 흔들리지 않는 것, 이것이 프로젝트 3단계의 Exactly-once 입니다.

# %% [19-9] Silver 의 역사를 보십시오.
DeltaTable.forPath(spark, "lake/s19/silver").history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      2|    MERGE|
# |      1|    MERGE|
# |      0|    WRITE|
# +-------+---------+
# → 0 WRITE(배치 0 에서 새로 만듦) · 1 MERGE(조각 4~10) · 2 MERGE(장애 뒤 다시 읽기). 장애가 장부에 그대로 남습니다. 무슨
# 일이 있었는지 나중에 설명할 수 있습니다.

# %% [19-10] ③ Gold — Silver 로 역 이름별 승차 인원을 만들어 lake/s19/gold/board_by_station 에 저장하고 보십시오.
gold = (spark.read.format("delta").load("lake/s19/silver")
        .filter(F.col("board_st").isNotNull())
        .groupBy("board_st").agg(F.count("*").alias("board_cnt"))
        .join(st, F.col("board_st") == F.col("station_id"))
        .select("station_nm", "line", "board_cnt")
        .orderBy(F.col("board_cnt").desc()))
gold.write.format("delta").mode("overwrite").save("lake/s19/gold/board_by_station")
gold.show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +----------+-----+---------+
# |station_nm| line|board_cnt|
# +----------+-----+---------+
# |    광화문|5호선|      147|
# |      역삼|2호선|      127|
# |    서울역|1호선|      126|
# |      강남|2호선|      121|
# |      시청|1호선|      121|
# |  홍대입구|2호선|      119|
# |      잠실|2호선|      117|
# |    여의도|5호선|      114|
# +----------+-----+---------+
# → STEP 13-11 의 clean_cnt 와 같은 숫자가 이름과 함께 나옵니다. 이 작은 표가 사람이 보는 답입니다. 3과목에서 이 표를 MongoDB 로
# 옮겨 앱 화면에 띄웁니다.
