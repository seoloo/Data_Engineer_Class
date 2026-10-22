# ==============================================================================
#  STEP 13  정리하기 — 중복 · 격리 · 승하차 짝 맞추기
#  C:\sparkcamp\answer\s13_clean.py
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
print(raw.count(), st.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 8


# %% [13-1] 원본(raw)의 줄 수와 이벤트 번호 가짓수를 다시 확인하십시오.
print(raw.count(), raw.select("event_id").distinct().count())
# ── 결과 ──
# 2008 1988
# → 2008 · 1988. 출발점입니다.

# %% [13-2] event_id 기준으로 중복을 걷어 낸 dedup 을 만들고 세십시오.
dedup = raw.dropDuplicates(["event_id"])
dedup.count()
# ── 결과 ──
# 1988
# → 1988. dropDuplicates([칸]) 는 그 칸이 같은 줄 중 하나만 남깁니다. 칸 목록을 안 주면 모든 칸이 똑같은 줄만 중복으로 봅니다.

# %% [13-2 바꿔 보기 ①] 칸 목록 없이 dropDuplicates() 로 하면?
print(raw.dropDuplicates().count())
# ── 결과 ──
# 1988
# → 이래서 — 1988 — 같습니다. 이 데이터의 재전송은 모든 칸이 똑같은 복사본이라 그렇습니다. 실제 시스템에서는 재전송된 줄의 도착
# 시각(recv_time)이 다를 때가 많아서, 칸 목록 없이 쓰면 못 걸러 냅니다. 그래서 무엇이 같으면 같은 이벤트인지 키(event_id)를 정해서
# 씁니다.

# %% [13-2 바꿔 보기 ②] 키를 잘못 고르면? — card_id 로 중복을 걷어 내기
print(raw.dropDuplicates(["card_id"]).count())
# ── 결과 ──
# 532
# → 이래서 — 2008 → 532 줄. 카드 한 장이 여러 번 탔는데 "카드가 같으면 중복" 이라고 해서 진짜 이벤트를 대량으로 버렸습니다. 오류는 없습니다.
# 중복 제거의 키는 "이 칸이 같으면 정말 같은 사건인가?" 로 고릅니다.

# %% [13-3] 격리 조건을 하나의 변수로 만드십시오 — 역 코드가 비어 있거나, 등록 안 된 역이거나, 태그 시각이 비어 있으면 격리.
valid_ids = [r.station_id for r in st.select("station_id").collect()]
print(valid_ids)
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
# ── 결과 ──
# ['S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08']
# → 역 마스터에서 정상 역 목록을 파이썬 리스트로 꺼냈습니다. ~ 는 "아니다", | 는 "또는". 조건을 변수에 담아 두면 아래에서 두 번(격리 · 정상)
# 씁니다. isNull() 을 맨 앞에 따로 쓴 이유는 13-13 에서 확인합니다 — MySQL 특강의 NOT IN 과 NULL 함정과 같은 이야기입니다.

# %% [13-4] 격리 바구니 quarantine 을 만들고, 이유(reason) 칸을 붙여 이유별로 세십시오.
quarantine = dedup.filter(is_bad).withColumn(
    "reason",
    F.when(F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids), "UNKNOWN_STATION")
     .otherwise("BAD_TIME"))
quarantine.groupBy("reason").count().show()
# ── 결과 ──
# +---------------+-----+
# |         reason|count|
# +---------------+-----+
# |       BAD_TIME|    4|
# |UNKNOWN_STATION|    6|
# +---------------+-----+
# → 미등록 역 6 · 깨진 시각 4. 쪽지가 붙은 격리입니다. (역 코드가 비어 있는 줄도 UNKNOWN_STATION 으로 갑니다 — 이 데이터에는
# 없습니다)

# %% [13-5] 정상 바구니 clean 을 만들고 세십시오.
clean = dedup.filter(~is_bad)
clean.count()
# ── 결과 ──
# 1978
# → 1978. 같은 조건에 ~ 를 붙이면 정확히 나머지가 됩니다. 조건을 변수로 둔 이유입니다.

# %% [13-6] 한 줄도 잃어버리지 않았는지 더해서 확인하십시오.
n_raw, n_dup = raw.count(), raw.count() - dedup.count()
n_q, n_ok = quarantine.count(), clean.count()
print(f"원본 {n_raw} = 중복 {n_dup} + 격리 {n_q} + 정상 {n_ok} → {n_dup + n_q + n_ok}")
print("일치" if n_raw == n_dup + n_q + n_ok else "불일치!")
# ── 결과 ──
# 원본 2008 = 중복 20 + 격리 10 + 정상 1978 → 2008
# 일치
# → 2008 = 20 + 10 + 1978. 파이프라인을 만들면 매번 이 한 줄을 찍게 하십시오. 프로젝트 검증 보고서의 첫 줄이 됩니다.

# %% [13-7] 짝 맞추기 — 정상 이벤트에서 승차 표와 하차 표를 따로 만들고, trip_id 로 붙여 trips 를 만드십시오.
board = clean.filter(F.col("tag_type") == "IN").select(
    "trip_id", "card_id",
    F.col("station_id").alias("board_st"),
    F.col("tag_time").alias("board_time"))

alight = clean.filter(F.col("tag_type") == "OUT").select(
    "trip_id",
    F.col("station_id").alias("alight_st"),
    F.col("tag_time").alias("alight_time"),
    "fare")

trips = board.join(alight, "trip_id", "left")
trips.orderBy("trip_id").show(5)
# ── 결과 ──
# +-------+-------+--------+-------------------+---------+-------------------+----+
# |trip_id|card_id|board_st|         board_time|alight_st|        alight_time|fare|
# +-------+-------+--------+-------------------+---------+-------------------+----+
# |  T0001|  C0575|     S08|2026-10-14 08:02:18|      S03|2026-10-14 08:20:09|1750|
# |  T0002|  C0062|     S04|2026-10-14 07:14:53|      S05|2026-10-14 07:46:01|1550|
# |  T0003|  C0265|     S03|2026-10-14 08:22:35|      S07|2026-10-14 08:54:00|1750|
# |  T0004|  C0500|     S03|2026-10-14 09:00:23|      S01|2026-10-14 09:14:48|1650|
# |  T0005|  C0370|     S05|2026-10-14 09:02:59|      S08|2026-10-14 09:45:42|1650|
# +-------+-------+--------+-------------------+---------+-------------------+----+
# only showing top 5 rows
# → 승차 표를 왼쪽에 두고 left join — 하차가 없는 이동도 남깁니다. 이동 한 건 = 한 줄이 되었습니다.

# %% [13-8] 이동이 몇 건인지, 그중 아직 하차 기록이 없는 이동이 몇 건인지 세십시오.
print(trips.count())
trips.filter(F.col("alight_st").isNull()).count()
# ── 결과 ──
# 992
# 14
# → 992 건 중 14 건. 원래 하차 태그가 없던 12건 + 하차 태그의 시각이 깨져 격리된 2건입니다. 실시간에서는 "아직 안 내린 사람" 입니다.
# STEP 17 에서 이 빈칸이 나중에 채워집니다.

# %% [13-9] 이동 시간(분)을 travel_min 칸으로 만들고, 평균 · 최소 · 최대를 구하십시오.
trips = trips.withColumn(
    "travel_min",
    F.round((F.unix_timestamp("alight_time") - F.unix_timestamp("board_time")) / 60, 1))
trips.agg(F.round(F.avg("travel_min"), 1).alias("avg"),
          F.min("travel_min").alias("min"),
          F.max("travel_min").alias("max")).show()
# ── 결과 ──
# +----+---+----+
# | avg|min| max|
# +----+---+----+
# |27.1|8.0|46.0|
# +----+---+----+
# → 하차가 없는 줄은 travel_min 도 null 이고, avg · min · max 는 null 을 빼고 계산합니다.

# %% [13-10] 하차는 있는데 승차가 없는 이동이 있는지 찾으십시오. (승차 태그가 S99 로 격리된 경우)
orphan = alight.join(board, "trip_id", "left_anti")
print(orphan.count())
orphan.show()
# ── 결과 ──
# 8
# +-------+---------+-------------------+----+
# |trip_id|alight_st|        alight_time|fare|
# +-------+---------+-------------------+----+
# |  T0411|      S03|2026-10-14 07:49:02|1550|
# |  T0987|      S07|2026-10-14 07:50:21|1750|
# |  T0192|      S04|2026-10-14 08:38:18|1750|
# |  T0519|      S03|2026-10-14 08:40:00|1550|
# |  T0107|      S08|2026-10-14 08:40:56|1750|
# |  T0049|      S07|2026-10-14 09:55:21|1550|
# |  T0812|      S03|2026-10-14 10:07:17|1650|
# |  T0701|      S06|2026-10-14 10:18:28|1550|
# +-------+---------+-------------------+----+
# → 8 건 — 승차가 S99(6건)·깨진 시각(2건)으로 격리되어 짝을 잃은 하차입니다. 이것들은 trips 에 들어가지 않았습니다(승차 표가 왼쪽이라).
# 실무에서는 이것도 "짝 없음" 으로 따로 세어 보고합니다.

# %% [13-11] 같은 질문, 다른 답 — 역별 승차 인원을 원본(raw)과 정상(clean)에서 각각 구해 나란히 보십시오.
def board_count(df, name):
    return df.filter(F.col("tag_type") == "IN").groupBy("station_id").agg(F.count("*").alias(name))

board_count(raw, "raw_cnt").join(board_count(clean, "clean_cnt"), "station_id", "left") \
    .orderBy("station_id").show()
# ── 결과 ──
# +----------+-------+---------+
# |station_id|raw_cnt|clean_cnt|
# +----------+-------+---------+
# |       S01|    122|      121|
# |       S02|    130|      127|
# |       S03|    120|      119|
# |       S04|    122|      121|
# |       S05|    129|      126|
# |       S06|    120|      117|
# |       S07|    116|      114|
# |       S08|    147|      147|
# |       S99|      6|     NULL|
# +----------+-------+---------+
# → 중복이 섞였던 역은 숫자가 줄었고, S99 는 정상 쪽에 없습니다(null). 같은 질문인데 답이 다릅니다. 어느 단계의 데이터로 셌는지를 항상 밝혀야
# 하는 이유입니다.

# %% [13-12] 정상 이벤트 기준으로 오늘의 기준 숫자 네 개를 구하십시오 — 승차 수 · 하차 수 · 요금 합계 · 이동 수.
summary = clean.agg(
    F.sum(F.when(F.col("tag_type") == "IN", 1).otherwise(0)).alias("board"),
    F.sum(F.when(F.col("tag_type") == "OUT", 1).otherwise(0)).alias("alight"),
    F.sum("fare").alias("fare_sum"))
summary.show()
print("trips:", trips.count())
# ── 결과 ──
# +-----+------+--------+
# |board|alight|fare_sum|
# +-----+------+--------+
# |  992|   986| 1647800|
# +-----+------+--------+
#
# trips: 992
# → 이 네 숫자를 칠판 구석에 적어 두십시오. STEP 17 의 Delta MERGE 결과와, STEP 19 의 실시간 파이프라인 결과가 이 숫자와 같아야
# 합니다. 다음 과목(Kafka)에서도 다시 맞춰 봅니다.

# %% [13-13] NULL 함정 확인 — 역 코드가 비어 있는 가짜 이벤트 한 줄을 만들어, isNull() 이 없는 규칙과 있는 규칙으로 각각 정상·격리를 세어 보십시오.
probe = spark.createDataFrame([("X1", None, "2026-10-14 08:00:00")],
                              "event_id STRING, station_id STRING, tag_time STRING") \
              .withColumn("tag_time", F.to_timestamp("tag_time"))

no_null  = (~F.col("station_id").isin(valid_ids)) | F.col("tag_time").isNull()      # isNull 없음
print("isNull 없는 규칙 → 격리", probe.filter(no_null).count(),  "/ 정상", probe.filter(~no_null).count())
print("isNull 있는 규칙 → 격리", probe.filter(is_bad).count(),   "/ 정상", probe.filter(~is_bad).count())
# ── 결과 ──
# isNull 없는 규칙 → 격리 0 / 정상 0
# isNull 있는 규칙 → 격리 1 / 정상 0
# → isNull 이 없으면 격리 0 · 정상 0 — 한 줄이 어디에도 없습니다. null 이 isin 을 만나면 결과가 참도 거짓도 아닌 null 이 되고,
# filter 는 null 을 통과시키지 않기 때문입니다. 13-6 의 더하기가 조용히 안 맞게 됩니다. isNull 을 먼저 따로 물으면 격리 1 로 바르게
# 갑니다. 배포 데이터에는 이런 줄이 없지만, 실제 앱 데이터에는 반드시 나옵니다 — 팀 프로젝트의 격리 규칙에도 칸마다 isNull 을 먼저 넣으십시오.
