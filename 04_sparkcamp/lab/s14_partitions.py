# ==============================================================================
#  STEP 14  파티션과 셔플 — 계기판으로 보는 Spark 의 속사정
#  C:\sparkcamp\lab\s14_partitions.py
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
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())


# %% [14-1] 원본(raw)이 몇 조각인지 보십시오.
raw.rdd.getNumPartitions()



# %% [14-2] raw 를 4 조각으로 고르게 다시 나누고, 조각마다 몇 줄씩인지 보십시오.
r4 = raw.repartition(4)
print(r4.rdd.getNumPartitions())
print(r4.rdd.glom().map(len).collect())


# %% [14-3] 4 조각을 coalesce 로 1 조각으로 합치십시오.
r1 = r4.coalesce(1)
print(r1.rdd.getNumPartitions())



# %% [14-4] 역 코드 기준으로 4 조각으로 나누고, 조각마다 몇 줄인지 보십시오.
rk = raw.repartition(4, "station_id")
print(rk.rdd.glom().map(len).collect())
# 쏠림(Skew)



# %% [14-4 바꿔 보기 ①] 값이 두 가지뿐인 칸(tag_type)으로 4 조각을 만들면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
print(raw.repartition(4, "tag_type").rdd.glom().map(len).collect())



# %% [14-5] 셔플 뒤 조각 수 설정과 AQE 가 켜져 있는지 보십시오.
# AQE - 적응형 쿼리 실행
# AQE 없다 - 출발 전에 정한 길로 끝까지 간다
# AQE 있다 - 가는 도중에 실제 상황을 보고 판단하여 길을 바꾼다
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.adaptive.enabled"))



# %% [14-6] 역별 승차 집계의 실행 계획을 보고, 셔플 표시 Exchange 를 찾으십시오.
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()



# %% [14-7] 셔플 조각 수를 8 로 바꾸고, 계획에서 숫자가 바뀌었는지 보십시오.
spark.conf.set("spark.sql.shuffle.partitions", "8")
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()



# %% [14-8] 캐시 — clean 을 메모리에 잡아 두고 두 번 세어 보십시오. 그리고 Spark UI 의 Storage 탭을 보십시오.
clean.cache()
print(clean.count())
print(clean.count())



# %% [14-9] 숫자로 비교하기 — 셔플 조각 200 과 8 일 때 같은 집계를 열 번씩 돌려 걸린 시간을 재십시오.
import time

def run(n):
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    spark.conf.set("spark.sql.adaptive.enabled", "false")   # 일시정지
    t0 = time.time()
    for _ in range(10):
        clean.groupBy("station_id", "tag_type").count().collect()
    return round(time.time() - t0, 2)

print("200 조각 : ", run(300), "초")
print("  8 조각 : ", run(8), "초")

spark.conf.set("spark.sql.adaptive.enabled", "true")


# %% [14-10] 카드별 이용 횟수를 많은 순으로 세 줄 보고, Spark UI 의 Stages 탭에서 방금 한 일의 Shuffle Read · Shuffle Write 칸을
# 찾으십시오.
# 서울 - 그룹, 정렬, 집계 
clean.groupBy("card_id").count().orderBy(F.col("count").desc(), "card_id").show()



# %% [14-11] 캐시를 풀고, 설정을 기본으로 되돌리십시오.
clean.unpersist()
spark.conf.set("spark.sql.partitions", "200")
print(spark.conf.get("spark.sql.shuffle.partitions"))



# %%
