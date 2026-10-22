# ==============================================================================
#  STEP 14  파티션과 셔플 — 계기판으로 보는 Spark 의 속사정
#  C:\sparkcamp\lab\s14_partitions.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] event_id 중복 제거와 역·시각 품질 조건을 적용한 clean까지 준비한다. 분할·캐시 실험의 입력 단계를 통일한다.
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
# [셀 설명] 파일에서 읽은 원본 DataFrame이 현재 몇 파티션인지 확인한다.
# [동작] 파티션은 실행을 위한 데이터 조각이다. 입력 파일 개수·파일 크기·읽기 설정 등에 영향을 받으며 local[2]와 같은 뜻은 아니다.
raw.rdd.getNumPartitions()



# %% [14-2] raw 를 4 조각으로 고르게 다시 나누고, 조각마다 몇 줄씩인지 보십시오.
# [셀 설명] 키를 따로 지정하지 않고 네 파티션으로 재분배한 뒤 조각별 행 수를 본다.
# [동작] repartition은 셔플을 통해 재분배한다. glom은 파티션 내용을 묶어 확인하므로 이 작은 실습에서 관찰용으로 사용한다.
r4 = raw.repartition(4)
print(r4.rdd.getNumPartitions())
print(r4.rdd.glom().map(len).collect())


# %% [14-3] 4 조각을 coalesce 로 1 조각으로 합치십시오.
# [셀 설명] 네 파티션을 한 파티션으로 줄이는 coalesce를 확인한다.
# [동작] 보통 기존 조각을 셔플 없이 묶어 줄이며 균등한 재분배를 보장하지 않는다. 한 파티션으로 줄이면 이후 병렬 처리 폭도 줄 수 있다.
r1 = r4.coalesce(1)
print(r1.rdd.getNumPartitions())



# %% [14-4] 역 코드 기준으로 4 조각으로 나누고, 조각마다 몇 줄인지 보십시오.
# [셀 설명] 역 코드를 기준으로 같은 역의 이벤트를 같은 파티션에 모은다.
# [동작] 키의 분포와 해시 배치 때문에 네 조각의 행 수가 고르지 않을 수 있다. 큰 조각의 작업이 오래 걸리는 쏠림을 관찰한다.
rk = raw.repartition(4, "station_id")
print(rk.rdd.glom().map(len).collect())
# 쏠림(Skew)



# %% [14-4 바꿔 보기 ①] 값이 두 가지뿐인 칸(tag_type)으로 4 조각을 만들면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 값이 두 가지인 승하차 코드를 키로 쓸 때 파티션 활용이 어떻게 달라지는지 본다.
# [동작] 같은 키는 같은 조각으로 가므로 네 조각을 만들어도 데이터가 든 조각은 최대 두 개다. 키가 적으면 파티션 수만 늘려도 고르게 분산되지 않는다.
print(raw.repartition(4, "tag_type").rdd.glom().map(len).collect())



# %% [14-5] 셔플 뒤 조각 수 설정과 AQE 가 켜져 있는지 보십시오.
# AQE - 적응형 쿼리 실행
# AQE 없다 - 출발 전에 정한 길로 끝까지 간다
# AQE 있다 - 가는 도중에 실제 상황을 보고 판단하여 길을 바꾼다
# [셀 설명] 셔플 시 초기 파티션 수 설정과 AQE 활성 여부를 확인한다.
# [동작] shuffle.partitions는 조인·집계의 셔플 분할 설정이고 AQE는 실행 중 통계로 계획을 조정한다. AQE 때문에 실제 조각 수가 초기 설정과 다를 수 있다.
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.adaptive.enabled"))



# %% [14-6] 역별 승차 집계의 실행 계획을 보고, 셔플 표시 Exchange 를 찾으십시오.
# [셀 설명] 역별 승차 집계의 물리 실행 계획에서 데이터 교환 지점을 찾아본다.
# [동작] explain의 Exchange는 파티션 간 재배치를 확인하는 단서다. groupBy뿐 아니라 앞선 event_id 중복 제거도 키별로 모으는 작업이 필요하다.
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()



# %% [14-7] 셔플 조각 수를 8 로 바꾸고, 계획에서 숫자가 바뀌었는지 보십시오.
# [셀 설명] 셔플 파티션 설정을 바꾼 뒤 집계 계획에서 분할 수가 어떻게 표현되는지 확인한다.
# [동작] 설정 뒤 agg를 다시 만들어 계획을 비교한다. 설정값이 실행 때 AQE로 조정될 가능성과 계획에 보이는 초기 값을 구분한다.
spark.conf.set("spark.sql.shuffle.partitions", "8")
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()



# %% [14-8] 캐시 — clean 을 메모리에 잡아 두고 두 번 세어 보십시오. 그리고 Spark UI 의 Storage 탭을 보십시오.
# [셀 설명] 반복 사용하는 clean을 캐시 대상으로 등록하고 count로 결과를 계산해 저장한다.
# [동작] 두 번째 액션부터 저장된 결과를 재사용할 수 있다. Storage 탭에서 캐시를 확인하며, 캐시 등록과 실제 계산 시점을 구분한다.
clean.cache()
print(clean.count())
print(clean.count())



# %% [14-9] 숫자로 비교하기 — 셔플 조각 200 과 8 일 때 같은 집계를 열 번씩 돌려 걸린 시간을 재십시오.
# [셀 설명] AQE를 잠시 끄고 동일 집계를 반복해 셔플 파티션 수에 따른 시간을 비교한다.
# [동작] collect는 반복마다 계산을 실행하도록 한다. 현재 출력은 200 조각이지만 호출은 run(300)이므로 비교 조건과 표시가 다르다. 측정 시간은 PC·캐시 상태에 따라 달라진다.
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
# [셀 설명] 카드별 태그 건수를 집계하고 정렬하면서 Spark UI의 셔플 읽기·쓰기 지표를 확인한다.
# [동작] 현재는 승차 필터 없이 정상 승하차 태그 모두를 센다. show()는 기본 최대 20행이므로 제목처럼 세 행만 보려면 show(3)과 비교한다.
clean.groupBy("card_id").count().orderBy(F.col("count").desc(), "card_id").show()



# %% [14-11] 캐시를 풀고, 설정을 기본으로 되돌리십시오.
# [셀 설명] 캐시를 해제하고 실험에서 바꾼 설정을 다음 실습 전에 되돌린다.
# [동작] unpersist는 저장된 결과를 해제한다. 현재 set의 spark.sql.partitions는 오타이며, 의도한 복원 키는 아래 get에서도 읽는 spark.sql.shuffle.partitions다.
clean.unpersist()
spark.conf.set("spark.sql.partitions", "200")
print(spark.conf.get("spark.sql.shuffle.partitions"))



# %%
