# ==============================================================================
#  STEP 17  Delta Lake ② — MERGE 로 고치고, OPTIMIZE 로 정리한다
#  C:\sparkcamp\answer\s17_delta_merge.py
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

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())

import shutil
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 1978


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for p in ["lake/silver/trips", "lake/silver/trips_rev", "lake/bronze/tags_small"]:
    shutil.rmtree(p, ignore_errors=True)
print("lake/silver/trips · trips_rev · lake/bronze/tags_small 비움")
# ── 결과 ──
# lake/silver/trips · trips_rev · lake/bronze/tags_small 비움


# %% [17-1] 이벤트를 이동 한 건 = 한 줄로 접는 함수 to_trips 를 만드십시오.
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
# ── 결과 ──
# (출력 없음)
# → STEP 13 에서는 승차 표·하차 표를 join 했는데, 여기서는 trip_id 로 묶어서(groupBy) 승차 칸·하차 칸을 골라 냅니다. 같은
# 결과지만 한 묶음 안에 승차만 있든 하차만 있든 다 받을 수 있어서 MERGE 와 잘 맞습니다. F.when(조건, 값) 은 조건이 아니면 null — max
# 는 null 을 건너뜁니다.

# %% [17-2] 첫 번째 묶음 — 08:30 전에 도착한 정상 이벤트(b1)로 이동 표 t1 을 만들고, Silver 로 저장하십시오.
b1 = clean.filter(F.col("recv_time") < "2026-10-14 08:30:00")
t1 = to_trips(b1)
print(b1.count(), t1.count())
t1.write.format("delta").mode("overwrite").save("lake/silver/trips")
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 857 527
# 527
# → 실시간이라면 "08:30 까지 들어온 것" 입니다. 도착 시각(recv_time) 으로 자른 이유가 그것입니다.

# %% [17-3] Silver 를 읽어 이동이 몇 건인지, 그중 아직 하차가 없는 이동이 몇 건인지 세십시오.
silver_df = spark.read.format("delta").load("lake/silver/trips")
print(silver_df.count())
silver_df.filter(F.col("alight_st").isNull()).count()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 527
# 195
# → 08:30 기준으로는 아직 안 내린 사람이 많습니다. 이 빈칸들이 다음 묶음에서 채워져야 합니다.

# %% [17-4] 두 번째 묶음 — 나머지 이벤트(b2)로 t2 를 만들고, 그중 Silver 에 이미 있는 이동과 새로운 이동이 각각 몇 건인지 세십시오.
b2 = clean.filter(F.col("recv_time") >= "2026-10-14 08:30:00")
t2 = to_trips(b2)
already = t2.join(silver_df, "trip_id").count()
print("t2 전체:", t2.count(), "/ 이미 있음:", already, "/ 새로움:", t2.count() - already)
# ── 결과 ──
# t2 전체: 661 / 이미 있음: 188 / 새로움: 473
# → "이미 있음" = 08:30 전에 탔고 그 뒤에 내린 사람. 이 줄들은 고쳐야 하고, "새로움" 은 넣어야 합니다. 이 두 숫자를 칠판에 쓰십시오.

# %% [17-5] MERGE — t2 를 Silver 에 Upsert 하고, 결과 숫자를 보십시오.
rules = {                                   # 있으면 이렇게 고친다 — 칸마다 "새 값이 있으면 새 값"
    "card_id":     "coalesce(s.card_id, n.card_id)",
    "board_st":    "coalesce(n.board_st, s.board_st)",
    "board_time":  "coalesce(n.board_time, s.board_time)",
    "alight_st":   "coalesce(n.alight_st, s.alight_st)",
    "alight_time": "coalesce(n.alight_time, s.alight_time)",
    "fare":        "greatest(s.fare, n.fare)"}

silver = DeltaTable.forPath(spark, "lake/silver/trips")
(silver.alias("s")
   .merge(t2.alias("n"), "s.trip_id = n.trip_id")
   .whenMatchedUpdate(set=rules)
   .whenNotMatchedInsertAll()
   .execute()
   .show())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-----------------+----------------+----------------+-----------------+
# |num_affected_rows|num_updated_rows|num_deleted_rows|num_inserted_rows|
# +-----------------+----------------+----------------+-----------------+
# |              661|             188|               0|              473|
# +-----------------+----------------+----------------+-----------------+
# → rules 는 한 번 만들어 두고 계속 씁니다(17-7 · 17-13 · STEP 19). num_updated_rows = 17-4 의 "이미 있음",
# num_inserted_rows = "새로움". 지운 것(deleted)은 0. 한 번의 명령으로 고치기와 넣기를 함께 했습니다.

# %% [17-6] MERGE 뒤 Silver 의 이동 수와 아직 하차가 없는 이동 수를 세십시오. 그리고 STEP 13 의 숫자와 비교하십시오.
after = spark.read.format("delta").load("lake/silver/trips")
print("이동:", after.count())
print("하차 없음:", after.filter(F.col("alight_st").isNull()).count())
print("승차 없음:", after.filter(F.col("board_st").isNull()).count())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# 이동: 1000
# 하차 없음: 14
# 승차 없음: 8
# → 이동 1,000 = STEP 13 의 trips 992 + 승차를 잃은 하차 8 (13-10). 이번에는 groupBy 로 접었기 때문에 짝 잃은 하차도
# 한 줄로 들어왔습니다. 하차 없음 14 는 STEP 13 과 같습니다. 숫자가 맞으면 MERGE 가 제대로 된 것입니다.

# %% [17-7] 같은 t2 로 MERGE 를 한 번 더 하고, 결과 숫자와 이동 수를 보십시오. (장애로 같은 데이터가 다시 들어온 상황)
(silver.alias("s")
   .merge(t2.alias("n"), "s.trip_id = n.trip_id")
   .whenMatchedUpdate(set=rules)
   .whenNotMatchedInsertAll()
   .execute()
   .show())
spark.read.format("delta").load("lake/silver/trips").count()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-----------------+----------------+----------------+-----------------+
# |num_affected_rows|num_updated_rows|num_deleted_rows|num_inserted_rows|
# +-----------------+----------------+----------------+-----------------+
# |              661|             661|               0|                0|
# +-----------------+----------------+----------------+-----------------+
#
# 1000
# → 이번에는 전부 matched(updated), inserted 0. 그리고 이동 수는 그대로 1,000. 값도 같은 값으로 다시 채웠을 뿐입니다. 두 번
# 해도 한 번 한 것과 같다 — 멱등. 다음 과목의 Exactly-once 가 이 성질 위에 서 있습니다.

# %% [17-8] 비교 — MERGE 대신 append 로 쌓았다면 어떻게 되었을지, t1 과 t2 를 그냥 이어 붙여 세어 보십시오.
appended = t1.unionByName(t2)
print("줄 수:", appended.count())
print("이동 번호 가짓수:", appended.select("trip_id").distinct().count())
# ── 결과 ──
# 줄 수: 1188
# 이동 번호 가짓수: 1000
# → 줄 수가 이동 수보다 많습니다. 같은 이동이 두 줄(승차만 있는 줄 + 하차가 붙은 줄)이 된 것입니다. STEP 15-7 에서 본 append 의 함정이
# 그대로 나타납니다. 그래서 Silver 는 MERGE 로 만듭니다.

# %% [17-9] Silver 의 역사를 보십시오.
silver.history().select("version", "operation").show()
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      2|    MERGE|
# |      1|    MERGE|
# |      0|    WRITE|
# +-------+---------+
# → 0 WRITE · 1 MERGE · 2 MERGE. 두 번째 MERGE 도 기록은 남습니다(내용은 같아도). 언제 무엇이 들어왔는지 장부로 추적할 수
# 있습니다.

# %% [17-10] 작은 파일 만들기 — 원본을 일부러 20 조각으로 나눠 Delta 로 저장하고, 파일이 몇 개인지 보십시오.
raw.repartition(20).write.format("delta").mode("overwrite").save("lake/bronze/tags_small")
small = DeltaTable.forPath(spark, "lake/bronze/tags_small")
small.detail().select("numFiles", "sizeInBytes").show()
# ── 결과 (Delta 화면 — sizeInBytes 는 예시) ──
# +--------+-----------+
# |numFiles|sizeInBytes|
# +--------+-----------+
# |      20|      78316|
# +--------+-----------+
# → numFiles 20. 스트리밍이 몇 초마다 조금씩 쓰면 이런 모양이 됩니다. detail() 은 테이블의 현재 요약입니다(크기 숫자는 PC 마다 조금
# 다를 수 있음).

# %% [17-11] OPTIMIZE + ZORDER — 작은 파일을 합치면서 역 코드로 정렬하고, 몇 개를 빼고 몇 개를 더했는지 보십시오.
small.optimize().executeZOrderBy("station_id") \
     .select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
# ── 결과 (Delta 화면 — 공식 문서 기준) ──
# +---------------+-------------+
# |numFilesRemoved|numFilesAdded|
# +---------------+-------------+
# |             20|            1|
# +---------------+-------------+
# → 20 개를 빼고 1 개를 더했습니다. 데이터가 작아서 하나로 합쳐졌습니다(기본 목표 파일 크기는 1GB). 실제 수백 GB 테이블에서는 수천 개가 수백
# 개가 됩니다.

# %% [17-12] OPTIMIZE 뒤에 파일 수와 줄 수, 역사를 보십시오.
small.detail().select("numFiles").show()
print(spark.read.format("delta").load("lake/bronze/tags_small").count())
small.history().select("version", "operation").show()
# ── 결과 (Delta 화면 — Delta 테이블로 검산) ──
# +--------+
# |numFiles|
# +--------+
# |       1|
# +--------+
#
# 2008
# +-------+---------+
# |version|operation|
# +-------+---------+
# |      1| OPTIMIZE|
# |      0|    WRITE|
# +-------+---------+
# → 파일 1 개, 줄은 그대로 2008, 역사에 OPTIMIZE. 내용은 하나도 안 바뀌고 담는 그릇만 정리했습니다. 지난 버전의 작은 파일 20개는
# VACUUM 전까지 폴더에 남아 있습니다(시간 여행용).

# %% [17-13] 순서를 뒤집어 보기 — 나중 묶음(t2)으로 먼저 새 테이블 trips_rev 를 만들고, 앞 묶음(t1)을 나중에 MERGE 하십시오. 그리고 17-6 과
# 같은 세 숫자를 세십시오.
t2.write.format("delta").mode("overwrite").save("lake/silver/trips_rev")
rev = DeltaTable.forPath(spark, "lake/silver/trips_rev")
(rev.alias("s")
   .merge(t1.alias("n"), "s.trip_id = n.trip_id")
   .whenMatchedUpdate(set=rules)
   .whenNotMatchedInsertAll()
   .execute()
   .show())
r = spark.read.format("delta").load("lake/silver/trips_rev")
print("이동:", r.count())
print("하차 없음:", r.filter(F.col("alight_st").isNull()).count())
print("승차 없음:", r.filter(F.col("board_st").isNull()).count())
# ── 결과 (Delta 화면 — 숫자는 Delta 테이블로 검산) ──
# +-----------------+----------------+----------------+-----------------+
# |num_affected_rows|num_updated_rows|num_deleted_rows|num_inserted_rows|
# +-----------------+----------------+----------------+-----------------+
# |              527|             188|               0|              339|
# +-----------------+----------------+----------------+-----------------+
#
# 이동: 1000
# 하차 없음: 14
# 승차 없음: 8
# → 1000 · 14 · 8 — 17-6 과 똑같습니다. 하차가 먼저 들어와 승차 칸이 빈 줄들이, 나중에 온 승차로 채워졌습니다(updated). rules
# 를 하차 칸만 채우게 만들었다면 같은 순서에서 "승차 없음" 이 196 건 남습니다(8 이어야 맞습니다). 실시간에서는 도착 순서가 뒤바뀌는 일이 늘 있습니다
# — 주제 6 배달 앱의 "배달 완료가 조리 중보다 먼저 도착" 이 같은 문제입니다.
