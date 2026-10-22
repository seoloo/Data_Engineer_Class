# ==============================================================================
#  STEP 06  골라 보기 — 줄 고르기 · 칸 고르기 · 칸 만들기
#  C:\sparkcamp\lab\s06_select_filter.py
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

mini = spark.read.json("data/tags_mini.jsonl")
day = spark.read.json("data/tags_day.jsonl")
print(mini.count(), day.count())


# %% [06-1] 작은 로그에서 trip_id · tag_type · station_id · fare 만 보십시오.
mini.select("trip_id", "tag_type", "station_id", "fare").show()


# %% [06-2] 작은 로그에서 승차(IN) 만 고르십시오.
mini.filter(F.col("tag_type") == "IN").show()
mini.where("tag_type = 'IN'").show()


# %% [06-3] 같은 일을 where 에 SQL 글자로 써서, 하차(OUT)가 몇 건인지 세십시오.
mini.where("tag_type = 'OUT'").count()



# %% [06-4] 하차이면서 요금이 1,600원 이상인 것을 고르십시오. 조건마다 괄호를 쓰십시오.
mini.filter((F.col("tag_type") == "OUT") & (F.col("fare") >= 1600)).show()
mini.where("tag_type = 'OUT' AND fare >= 1600").show()




# %% [06-4 바꿔 보기 ①] 그리고(&) 를 또는(|) 으로 바꾸면? — 승차이거나 요금 1,600 이상
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [06-5] 일부러 틀려 보십시오. 06-4 를 괄호 없이 쓰십시오.




# %% [06-6] 강남(S01) 또는 역삼(S02)에서 찍힌 태그만 고르십시오. isin 을 쓰십시오.
mini.filter(F.col("station_id").isin("S01", "S02")).select(
    "event_id", "station_id", "fare").show()



# %% [06-7] 작은 로그를 태그 시각이 늦은 것부터 세 줄 보십시오.
mini.orderBy(F.col("tag_time").desc()).show(3)
mini.orderBy(F.col("tag_time", ascending=False)).show(3)




# %% [06-8] 승차는 "승차", 하차는 "하차" 라고 적힌 구분 칸을 새로 만들어 보십시오.
mini.withColumn("구분", F.when(F.col("tag_type") == "IN", "승차").otherwise("하차"))\
    .select("event_id", "tag_type", "구분").show()



# %% [06-9] 요금을 천 원 단위(소수 한 자리)로 바꾼 fare_k 칸을 만들고, 하차만 보십시오.
(mini.filter(F.col("tag_type") == "OUT")
 .withColumn("fare_k", F.round(F.col("fare") / 1000, 1))
 .select("trip_id", "fare", "fare_k")
 .show()
 )



# %% [06-10] station_id 칸 이름을 역코드 로 바꿔 보여 주십시오.
mini.select(F.col("station_id").alias("역코드"), "tag_type").show(3)



# %% [06-11] 작은 로그에 나오는 역이 몇 곳인지 세십시오.
mini.select("station_id").distinct().count()



# %% [06-12] 하루 로그에서 역 코드가 S99 인 줄을 찾아보십시오. 역 마스터에 없는 코드입니다.
day.filter(F.col("station_id") == "S99")\
    .select("event_id", "trip_id", "station_id")\
    .show()



# %% [06-13] 하루 로그에서 요금이 붙은 줄(fare > 0)이 몇 개인지 where 로 세십시오.

# mini.filter((F.col("tag_type") == "OUT") & (F.col("fare") >= 1600)).show()
#   filter
day.filter(F.col("fare") > 0)\
    .count()


#   where
day.where("fare > 0").count()


# %%
