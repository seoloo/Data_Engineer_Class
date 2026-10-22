# ==============================================================================
#  STEP 09  시간 다루기 — 태그 시각과 도착 시각
#  C:\sparkcamp\lab\s09_time.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
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


# %% [09-1] 태그 시각에서 시(hour) 를 꺼내 hh 라는 칸으로 다섯 줄 보십시오.
# 이벤트 시간 - 실제 일어난 시간 -> tag_time
# 처리 시간 - 우리에게 도착한 시간 -> recv_time
day.select("event_id", "tag_time", F.hour("tag_time").alias("hh")).show(5)




# %% [09-2] 태그 시각을 08:31 모양의 글자로 바꿔 보십시오.
day.select("event_id", F.date_format("tag_time", "HH:mm")
           .alias("hhmm")).show(5)



# %% [09-3] 시간대별 태그 수를 세어 시 순서로 보십시오.
day.groupBy(F.hour("tag_time").alias("hh")).count()     \
    .orderBy("hh").show()




# %% [09-3 바꿔 보기 ①] 시간대별로 셀 때 tag_time 대신 recv_time(도착 시각)으로 세면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
day.groupBy(F.hour("recv_time").alias("hh")).count()     \
    .orderBy("hh").show()



# %% [09-4] 스키마 없이 읽은 raw 의 tag_time 은 무슨 자료형인지 보십시오.
raw.select("tag_time").printSchema()



# %% [09-5] 일부러 틀려 보십시오. raw 의 tag_time 을 to_timestamp 로 바꾸고, 바뀐 값이 비어 있는 줄을 세십시오.
raw.select(F.to_timestamp("tag_time").alias("t"))       \
    .filter(F.col("t").isNull()).count()



# %% [09-6] 같은 일을 try_to_timestamp 로 하십시오.
raw.select(F.try_to_timestamp("tag_time").alias("t"))       \
    .filter(F.col("t").isNull()).count()



# %% [09-7] 도착 지연(초) = 도착 시각 − 태그 시각을 delay_sec 칸으로 만들고, 가장 늦게 온 다섯 줄을 보십시오.
late = day.withColumn("delay_sec", F.unix_timestamp("recv_time")
                      - F.unix_timestamp("tag_time"))
late.orderBy(F.col("delay_sec").desc())     \
    .select("event_id", "tag_type", "tag_time", "recv_time", "delay_sec")       \
    .show(5)


# %% [09-8] 1분(60초) 넘게 늦게 온 태그가 몇 개인지 세고, 승차·하차로 나눠 보십시오.
late.filter(F.col("delay_sec") > 60).groupBy("tag_type").count().show()



# %% [09-9] 8시대(08:00:00 이상 09:00:00 미만)에 찍힌 승차 태그를 세십시오.
day.filter((F.col("tag_type") == "IN") & 
           (F.col("tag_time") >= "2026-10-14 08:00:00") &
           (F.col("tag_time") >= "2026-10-14 09:00:00")).count() 





# %% [09-10] 태그 시각을 정시로 잘라(date_trunc) 시간 단위로 승차를 세십시오.
day.filter(F.col("tag_type")=="IN")\
        .groupBy(F.date_trunc("hour","tag_time").alias("hour_start")).count()\
        .orderBy("hour_start").show()


