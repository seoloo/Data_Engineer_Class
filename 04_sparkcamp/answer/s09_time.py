# ==============================================================================
#  STEP 09  시간 다루기 — 태그 시각과 도착 시각
#  C:\sparkcamp\answer\s09_time.py
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
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
raw = spark.read.json("data/tags_day.jsonl")      # 스키마 없이 읽은 것 (시각이 글자)
print(day.count(), raw.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 2008


# %% [09-1] 태그 시각에서 시(hour) 를 꺼내 hh 라는 칸으로 다섯 줄 보십시오.
day.select("event_id", "tag_time", F.hour("tag_time").alias("hh")).show(5)
# ── 결과 ──
# +--------+-------------------+---+
# |event_id|           tag_time| hh|
# +--------+-------------------+---+
# |  E00001|2026-10-14 07:00:00|  7|
# |  E00002|2026-10-14 07:00:01|  7|
# |  E00003|2026-10-14 07:00:02|  7|
# |  E00004|2026-10-14 07:00:07|  7|
# |  E00005|2026-10-14 07:00:20|  7|
# +--------+-------------------+---+
# only showing top 5 rows

# %% [09-2] 태그 시각을 08:31 모양의 글자로 바꿔 보십시오.
day.select("event_id", F.date_format("tag_time", "HH:mm").alias("hhmm")).show(5)
# ── 결과 ──
# +--------+-----+
# |event_id| hhmm|
# +--------+-----+
# |  E00001|07:00|
# |  E00002|07:00|
# |  E00003|07:00|
# |  E00004|07:00|
# |  E00005|07:00|
# +--------+-----+
# only showing top 5 rows
# → HH 는 24시간제 시, mm 은 분. 대문자 MM 은 월입니다 — 자주 틀리는 곳입니다.

# %% [09-3] 시간대별 태그 수를 세어 시 순서로 보십시오.
day.groupBy(F.hour("tag_time").alias("hh")).count().orderBy("hh").show()
# ── 결과 ──
# +----+-----+
# |  hh|count|
# +----+-----+
# |NULL|    5|
# |   7|  463|
# |   8|  850|
# |   9|  587|
# |  10|  103|
# +----+-----+
# → 8시가 가장 바쁩니다. 맨 위의 NULL 5 는 STEP 07 의 깨진 시각입니다. null 도 하나의 묶음으로 나옵니다.

# %% [09-3 바꿔 보기 ①] 시간대별로 셀 때 tag_time 대신 recv_time(도착 시각)으로 세면?
day.groupBy(F.hour("recv_time").alias("hh")).count().orderBy("hh").show()
# ── 결과 ──
# +---+-----+
# | hh|count|
# +---+-----+
# |  7|  463|
# |  8|  843|
# |  9|  592|
# | 10|  110|
# +---+-----+
# → 이래서 — NULL 줄이 사라지고(도착 시각은 안 깨졌으니), 9시 · 10시 칸이 늘었습니다(10시 103 → 110). 9시대에 내린 사람의 기록이
# 늦게 도착해 10시로 넘어갔기 때문입니다. "몇 시에 몇 명이 탔나" 는 댄 시각(이벤트 시간) 으로 세야 합니다 — STEP 12-9 와 다음 과목
# 워터마크의 출발점입니다.

# %% [09-4] 스키마 없이 읽은 raw 의 tag_time 은 무슨 자료형인지 보십시오.
raw.select("tag_time").printSchema()
# ── 결과 ──
# root
#  |-- tag_time: string (nullable = true)
# → string. 글자를 시각으로 바꾸는 일을 직접 해 봅니다.

# %% [09-5] 일부러 틀려 보십시오. raw 의 tag_time 을 to_timestamp 로 바꾸고, 바뀐 값이 비어 있는 줄을 세십시오.
raw.select(F.to_timestamp("tag_time").alias("t")).filter(F.col("t").isNull()).count()
# ── 결과 (오류 — 일부러 틀린 것) ──
# DateTimeException: [CAST_INVALID_INPUT] The value '2026-10-14 07:67:00' of the type "STRING" cannot be cast to "TIMESTAMP" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
# → CAST_INVALID_INPUT — '2026-10-14 07:67:00' 은 시각으로 바꿀 수 없다. 메시지가 친절하게 try_cast 를 쓰라고 알려
# 줍니다. Spark 4 의 엄격 모드입니다.

# %% [09-6] 같은 일을 try_to_timestamp 로 하십시오.
raw.select(F.try_to_timestamp("tag_time").alias("t")).filter(F.col("t").isNull()).count()
# ── 결과 ──
# 5
# → 5. 깨진 값은 null 이 되고 멈추지 않습니다. STEP 07 에서 스키마로 읽었을 때와 같은 숫자입니다.

# %% [09-7] 도착 지연(초) = 도착 시각 − 태그 시각을 delay_sec 칸으로 만들고, 가장 늦게 온 다섯 줄을 보십시오.
late = day.withColumn("delay_sec", F.unix_timestamp("recv_time") - F.unix_timestamp("tag_time"))
late.orderBy(F.col("delay_sec").desc()) \
    .select("event_id", "tag_type", "station_id", "tag_time", "recv_time", "delay_sec").show(5)
# ── 결과 ──
# +--------+--------+----------+-------------------+-------------------+---------+
# |event_id|tag_type|station_id|           tag_time|          recv_time|delay_sec|
# +--------+--------+----------+-------------------+-------------------+---------+
# |  E00211|     OUT|       S05|2026-10-14 07:32:57|2026-10-14 08:11:57|     2340|
# |  E01766|     OUT|       S02|2026-10-14 09:44:45|2026-10-14 10:23:45|     2340|
# |  E01899|     OUT|       S01|2026-10-14 10:03:14|2026-10-14 10:42:14|     2340|
# |  E01757|     OUT|       S02|2026-10-14 09:43:29|2026-10-14 10:21:29|     2280|
# |  E01512|     OUT|       S08|2026-10-14 09:18:53|2026-10-14 09:53:53|     2100|
# +--------+--------+----------+-------------------+-------------------+---------+
# only showing top 5 rows
# → 2,000초 넘게(30분 이상) 늦게 온 하차 태그가 있습니다. 지하 구간 통신 끊김입니다.

# %% [09-8] 1분(60초) 넘게 늦게 온 태그가 몇 개인지 세고, 승차·하차로 나눠 보십시오.
late.filter(F.col("delay_sec") > 60).groupBy("tag_type").count().show()
# ── 결과 ──
# +--------+-----+
# |tag_type|count|
# +--------+-----+
# |     OUT|   32|
# +--------+-----+
# → 늦게 온 것은 전부 하차입니다. 이 데이터에는 늦게 온 하차 태그 30건(+재전송 중복)이 들어 있습니다. 이것들이 다음 과목의 워터마크 실험 재료가
# 됩니다.

# %% [09-9] 8시대(08:00:00 이상 09:00:00 미만)에 찍힌 승차 태그를 세십시오.
day.filter((F.col("tag_type") == "IN") &
           (F.col("tag_time") >= "2026-10-14 08:00:00") &
           (F.col("tag_time") <  "2026-10-14 09:00:00")).count()
# ── 결과 ──
# 453
# → 시각 칸과 글자를 비교해도 Spark 가 알아서 시각으로 바꿔 비교합니다. 범위는 이상 · 미만으로 씁니다 (MySQL 특강과 같은 규칙).

# %% [09-10] 태그 시각을 정시로 잘라(date_trunc) 시간 단위로 승차를 세십시오.
day.filter(F.col("tag_type") == "IN") \
   .groupBy(F.date_trunc("hour", "tag_time").alias("hour_start")).count() \
   .orderBy("hour_start").show()
# ── 결과 ──
# +-------------------+-----+
# |         hour_start|count|
# +-------------------+-----+
# |               NULL|    2|
# |2026-10-14 07:00:00|  305|
# |2026-10-14 08:00:00|  453|
# |2026-10-14 09:00:00|  252|
# +-------------------+-----+
# → 09-3 의 hour 와 비슷하지만 결과가 시각(08:00:00)입니다. 날짜가 여러 날이면 이쪽이 맞습니다. 다음 STEP 의 창(window) 이
# 이것을 5분 · 10분으로 확장한 것입니다.
