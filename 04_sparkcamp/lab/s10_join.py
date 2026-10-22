# ==============================================================================
#  STEP 10  이어 붙이기 — 역 마스터와 join
#  C:\sparkcamp\lab\s10_join.py
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
st = spark.read.csv("data/stations.csv", header=True)
print(day.count(), st.count())


# %% [10-1] 로그에 역 이름을 붙여 다섯 줄 보십시오. (기본 join)
day.join(st, "station_id")      \
    .select("event_id", "tag_type", "station_id", "station_nm", "line")     \



# %% [10-2] 기본(inner) join 결과가 몇 줄인지 세십시오.




# %% [10-2 바꿔 보기 ①] 붙이지는 말고 "짝이 있는 줄만" 남기면? — left_semi
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
semi = day.join(st, "station_id", "left_semi")
print(semi.count(), len(semi.columns))



# %% [10-2 바꿔 보기 ②] 마스터에서 광화문(S08)이 빠져 있다면? — inner join 줄 수
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
st_miss = st.filter(F.col("station_id") != "S08")
print(day.join(st_miss, "station_id").count())



# %% [10-3] left join 으로 붙이면 몇 줄인지 세십시오.
day.join(st, "station_id", "left").count()



# %% [10-4] left join 결과에서 역 이름이 비어 있는 줄을 보십시오.
day.join(st, "station_id", "left")          \
    .filter(F.col("station_nm").isNull)     \
    .select("event_id", "trip_id", "station_id", "station_nm").show()




# %% [10-5] 같은 6줄을 left_anti 한 번으로 찾으십시오.
unknown = day.join(st, "station_id", "left_anti")
unknown.count()



# %% [10-6] 역 이름으로 승차 인원을 세어 많은 순으로 보십시오.
(
    day.filter(F.col("tag_type") == "IN")       \
    .join(st, "station_id")                     \
    .groupBy("station_nm").count()              \
    .orderBy(F.col("count").desc())             \
    .show()
)



# %% [10-7] 노선별 하차 인원과 요금 합계를 구하십시오.
(       
    day.filter(F.col("tag_type") == "OUT")          \
        .join(st, "station_id")                     \
        .groupBy("line")                            \
        .agg(F.count("*").alias("out_cnt"),        \
            F.sum("fare").alias("out_fare_sum"))     \
        .orderBy("line")
        .show()
)






# %% [10-8] 역 마스터를 broadcast 로 감싸 붙이고, 실행 계획에서 어떤 join 을 썼는지 보십시오.
joined = day.join(F.broadcast(st), "station_id")

joined.explain()



# %% [10-9] 일부러 틀려 보십시오. 조건식으로 붙인 다음 station_id 를 골라 보십시오.




# %% [10-10] 구(gu)별 승차 인원을 구하고, 승차가 가장 많은 구를 한 줄만 보십시오.
(
    day.filter(F.col("tag_type") == "IN")       \
    .join(st, "station_id")                     \
    .groupBy("gu").count()              \
    .orderBy(F.col("count").desc())             \
    .show(1)
)




# %%
