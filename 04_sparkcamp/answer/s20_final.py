# ==============================================================================
#  마무리  마무리 — 혼자 푸는 열 문제
#  C:\sparkcamp\answer\s20_final.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys
os.chdir(r"C:\sparkcamp")
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder
         .master("local[2]")
         .appName("sparkcamp")
         .config("spark.local.dir", r"C:\sparkcamp\tmp")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "준비 완료")

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

trips = to_trips(clean)
print("trips:", trips.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 1978
# trips: 1000


# %% [H-1] 하차 인원이 가장 많은 역 세 곳을 역 이름으로 보이십시오. (clean 기준)
(clean.filter(F.col("tag_type") == "OUT")
      .join(st, "station_id")
      .groupBy("station_nm").count()
      .orderBy(F.col("count").desc())
      .show(3))
# ── 결과 ──
# +----------+-----+
# |station_nm|count|
# +----------+-----+
# |  홍대입구|  145|
# |      시청|  132|
# |    여의도|  131|
# +----------+-----+
# only showing top 3 rows
# → 승차 1등(광화문)과 하차 1등이 다릅니다. 08-7 의 피벗 표에서도 볼 수 있었습니다.

# %% [H-2] 07시대(07:00:00 이상 08:00:00 미만)에 탄 사람(승차)은 몇 명입니까?
clean.filter((F.col("tag_type") == "IN") &
             (F.col("tag_time") >= "2026-10-14 07:00:00") &
             (F.col("tag_time") <  "2026-10-14 08:00:00")).count()
# ── 결과 ──
# 298
# → 범위는 이상 · 미만. 조건이 셋이면 괄호도 셋.

# %% [H-3] 노선별 승차 인원을 많은 순으로 보이십시오.
(clean.filter(F.col("tag_type") == "IN")
      .join(st, "station_id")
      .groupBy("line").count()
      .orderBy(F.col("count").desc())
      .show())
# ── 결과 ──
# +-----+-----+
# | line|count|
# +-----+-----+
# |2호선|  484|
# |5호선|  261|
# |1호선|  247|
# +-----+-----+

# %% [H-4] 이동 시간이 30분을 넘는 이동은 몇 건입니까? (승차·하차 시각이 둘 다 있는 이동만)
(trips.filter(F.col("board_time").isNotNull() & F.col("alight_time").isNotNull())
       .withColumn("travel_min",
                   (F.unix_timestamp("alight_time") - F.unix_timestamp("board_time")) / 60)
       .filter(F.col("travel_min") > 30)
       .count())
# ── 결과 ──
# 412
# → 하차가 없는 이동(14)과 승차가 없는 이동(8)을 먼저 빼고 계산했는지 보십시오. 빼지 않아도 null 이라 결과는 같지만, 의도를 코드에 드러내는 쪽이
# 좋습니다.

# %% [H-5] 15분 Tumbling 창으로 승차를 세어, 가장 붐빈 창 하나를 보이십시오.
(clean.filter(F.col("tag_type") == "IN")
      .groupBy(F.window("tag_time", "15 minutes").alias("w")).count()
      .select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count")
      .orderBy(F.col("count").desc(), "start")
      .show(1))
# ── 결과 ──
# +-------------------+-------------------+-----+
# |              start|                end|count|
# +-------------------+-------------------+-----+
# |2026-10-14 08:00:00|2026-10-14 08:15:00|  121|
# +-------------------+-------------------+-----+
# only showing top 1 row

# %% [H-6] 하루에 두 번 이상 탄(승차 2회 이상) 카드는 몇 장입니까?
(clean.filter(F.col("tag_type") == "IN")
      .groupBy("card_id").count()
      .filter(F.col("count") >= 2)
      .count())
# ── 결과 ──
# 300
# → 묶고(groupBy) → 세고(count) → 묶음을 거르고(filter) → 묶음 수를 센다(count). count 가 두 번 나오는데 뜻이 다릅니다.

# %% [H-7] 원본(raw)에서 격리된 이벤트의 이유별 개수를 구하고, 원본 = 중복 + 격리 + 정상 이 맞는지 한 줄로 찍으십시오.
quarantine = dedup.filter(is_bad).withColumn(
    "reason", F.when(F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids),
                     "UNKNOWN_STATION").otherwise("BAD_TIME"))
quarantine.groupBy("reason").count().orderBy("reason").show()
n_dup = raw.count() - dedup.count()
print(raw.count(), "=", n_dup, "+", quarantine.count(), "+", clean.count())
# ── 결과 ──
# +---------------+-----+
# |         reason|count|
# +---------------+-----+
# |       BAD_TIME|    4|
# |UNKNOWN_STATION|    6|
# +---------------+-----+
#
# 2008 = 20 + 10 + 1978
# → STEP 13 의 복습입니다. 2008 = 20 + 10 + 1978.

# %% [H-8] 탄 역 → 내린 역 짝 중 가장 많은 세 가지를 역 이름으로 보이십시오.
b = st.select(F.col("station_id").alias("board_st"), F.col("station_nm").alias("from_nm"))
a = st.select(F.col("station_id").alias("alight_st"), F.col("station_nm").alias("to_nm"))
top_routes = (trips.join(b, "board_st").join(a, "alight_st")
                   .groupBy("from_nm", "to_nm").count()
                   .orderBy(F.col("count").desc(), "from_nm", "to_nm"))
top_routes.show(3)
# ── 결과 ──
# +-------+--------+-----+
# |from_nm|   to_nm|count|
# +-------+--------+-----+
# |   시청|홍대입구|   26|
# | 광화문|홍대입구|   25|
# | 여의도|    시청|   25|
# +-------+--------+-----+
# only showing top 3 rows
# → 역 마스터를 두 번 붙입니다 — 한 번은 탄 역 이름, 한 번은 내린 역 이름. 같은 표를 두 번 붙일 때는 칸 이름을 미리 바꿔서(alias)
# AMBIGUOUS 오류를 피합니다(10-9).

# %% [H-9] 내린 역의 노선 기준으로 평균 요금이 가장 높은 노선 하나를 보이십시오 (소수 첫째 자리).
(clean.filter(F.col("tag_type") == "OUT")
      .join(st, "station_id")
      .groupBy("line").agg(F.round(F.avg("fare"), 1).alias("avg_fare"))
      .orderBy(F.col("avg_fare").desc())
      .show(1))
# ── 결과 ──
# +-----+--------+
# | line|avg_fare|
# +-----+--------+
# |5호선|  1697.8|
# +-----+--------+
# only showing top 1 row

# %% [H-10] H-8 의 top_routes 를 Parquet 으로 out/final/top_routes 에 덮어쓰기로 저장하고, 다시 읽어 몇 줄인지 세십시오.
top_routes.write.mode("overwrite").parquet("out/final/top_routes")
spark.read.parquet("out/final/top_routes").count()
# ── 결과 ──
# 56
# → 역 8 곳에서 서로 다른 역으로 가는 짝은 최대 8 × 7 = 56 가지입니다. 결과가 56 이면 모든 짝이 한 번 이상 나온 것입니다.
# mode("overwrite") 를 빼면 두 번째 실행부터 PATH_ALREADY_EXISTS.
