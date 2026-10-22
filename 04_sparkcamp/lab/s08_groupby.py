# ==============================================================================
#  STEP 08  묶어서 세기 — groupBy 와 agg
#  C:\sparkcamp\lab\s08_groupby.py
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
print(day.count())


# %% [08-1] 승차(IN)와 하차(OUT)가 각각 몇 줄인지 세십시오.
day.groupBy("tag_type").count().show()





# %% [08-2] 역별 승차 인원을 많은 순으로 보십시오.
day.filter(F.col("tag_type") == "IN")   \
    .groupBy("station_id").count()      \
    .orderBy(F.col("count").desc()).show()




# %% [08-3] 역별로 태그 수 · 요금 합계 · 카드 가짓수를 한 번에 구하십시오. 역 코드 순으로.
day.groupBy("station_id").agg(      # aggregate(집계하다) - 여러개
    F.count("*").alias("tags"),
    F.sum("fare").alias("fare_sum"),
    F.count_distinct("card_id").alias("cards")
).orderBy("station_id").show()





# %% [08-4] 하차 한 번의 평균 요금을 구하십시오 (소수 첫째 자리 반올림).
day.filter(F.col("tag_type") == "OUT")      \
    .agg(F.round(F.avg("fare"), 1).alias("AVG_fare")).show()



# %% [08-5] 요금 전체 합계를 숫자 하나로 꺼내 파이썬 변수 total 에 담으십시오.
total = day.agg(F.sum("fare")).first()[0]
print(total)



# %% [08-6] 승차가 130 명 이상인 역만 보십시오. (묶은 뒤 거르기)
(
    day.filter(F.col("tag_type") == "IN")
        .groupBy("station_id").agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") >= 130)
        .orderBy(F.col("cnt").desc())
        .show()
)



# %% [08-7] 피벗 — 역마다 승차와 하차를 옆으로 나란히 보십시오.
day.groupBy("station_id").pivot("tag_type", ["IN", "OUT"]).count()      \
    .orderBy("station_id").show()



# %% [08-8] 카드 한 장이 하루에 몇 번 탔는지(승차 수) 세고, 많이 탄 카드 다섯 장을 보십시오.
day.filter(F.col("tag_type") == "IN")       \
    .groupBy("card_id").count()             \
    .orderBy(F.col("count").desc(), "card_id").show(5)





# %% [08-9] 일부러 틀려 보십시오. 역별로 묶은 다음 바로 card_id 칸을 골라 보십시오.




# %% [08-10] 하루 로그의 줄 수와 이벤트 번호의 가짓수를 나란히 세십시오.
day.agg(F.count("*").alias("rows"),
        F.count_distinct("event_id").alias("events")).show()
    
    # 20개의 중복 -> 재전송?



# %% [08-10 바꿔 보기 ①] 승차·하차별로 줄 수와 이벤트 가짓수를 나란히 보면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
day.groupBy("tag_type").agg(F.count("*").alias("rows"),
        F.count_distinct("event_id").alias("events"))        \
        .orderBy("tag_type").show()




# %% [08-11] 어떤 이벤트가 두 번 들어왔는지 다섯 개만 보십시오.
day.groupBy("event_id").count().filter(F.col("count") > 1)      \
    .orderBy("event_id").show(5)




# %%
