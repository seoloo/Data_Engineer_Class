# ==============================================================================
#  STEP 08  묶어서 세기 — groupBy 와 agg
#  C:\sparkcamp\lab\s08_groupby.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 명시적 스키마로 하루 로그를 읽는다. 한 행은 태그 사건이며 동일 카드가 여러 행에 등장할 수 있다.
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
# [셀 설명] 승차와 하차를 구분별로 묶어 각각의 태그 행 수를 센다.
# [동작] groupBy('tag_type').count()는 그룹당 한 행인 결과 DataFrame이다. 전체 행 수를 정수로 받는 day.count()와 구분한다.
day.groupBy("tag_type").count().show()





# %% [08-2] 역별 승차 인원을 많은 순으로 보십시오.
# [셀 설명] 승차 태그만 고른 뒤 역별 승차 건수를 구하고 큰 순서로 정렬한다.
# [동작] 필터는 집계 대상 행을 정하고 groupBy는 묶는 기준을 정한다. 같은 카드의 반복 승차도 각각 한 건으로 센다.
day.filter(F.col("tag_type") == "IN")   \
    .groupBy("station_id").count()      \
    .orderBy(F.col("count").desc()).show()




# %% [08-3] 역별로 태그 수 · 요금 합계 · 카드 가짓수를 한 번에 구하십시오. 역 코드 순으로.
# [셀 설명] 같은 역별 그룹에서 태그 건수·요금 합계·서로 다른 카드 수를 함께 계산한다.
# [동작] agg는 여러 집계식을 받고 alias는 결과 열 이름을 정한다. count(*)와 count_distinct(card_id)는 태그 수와 카드 가짓수로 의미가 다르다.
day.groupBy("station_id").agg(      # aggregate(집계하다) - 여러개
    F.count("*").alias("tags"),
    F.sum("fare").alias("fare_sum"),
    F.count_distinct("card_id").alias("cards")
).orderBy("station_id").show()





# %% [08-4] 하차 한 번의 평균 요금을 구하십시오 (소수 첫째 자리 반올림).
# [셀 설명] 하차 태그만 대상으로 평균 요금을 계산하고 소수 첫째 자리로 표시한다.
# [동작] 필터가 없으면 승차 행까지 집계 대상에 섞일 수 있다. 평균은 어떤 행을 대상으로 구했는지 함께 설명한다.
day.filter(F.col("tag_type") == "OUT")      \
    .agg(F.round(F.avg("fare"), 1).alias("AVG_fare")).show()



# %% [08-5] 요금 전체 합계를 숫자 하나로 꺼내 파이썬 변수 total 에 담으십시오.
# [셀 설명] 한 행짜리 요금 합계 결과에서 실제 숫자를 꺼내 파이썬 변수에 담는다.
# [동작] agg는 DataFrame, first()는 첫 Row, [0]은 그 Row의 첫 값을 반환한다. show로 출력하는 것과 값을 가져오는 것을 구분한다.
total = day.agg(F.sum("fare")).first()[0]
print(total)



# %% [08-6] 승차가 130 명 이상인 역만 보십시오. (묶은 뒤 거르기)
# [셀 설명] 승차 태그를 역별로 센 다음 집계 건수가 130 이상인 역을 고른다.
# [동작] 집계 전 filter는 원천 행을 거르고, 집계 후 filter는 그룹의 통계값을 거른다. 뒤의 조건은 SQL HAVING과 대응한다.
(
    day.filter(F.col("tag_type") == "IN")
        .groupBy("station_id").agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") >= 130)
        .orderBy(F.col("cnt").desc())
        .show()
)



# %% [08-7] 피벗 — 역마다 승차와 하차를 옆으로 나란히 보십시오.
# [셀 설명] 승차와 하차 구분을 열로 펼쳐 역마다 두 건수를 나란히 보여 준다.
# [동작] pivot에 IN·OUT을 명시해 결과 열을 정한다. 행 방향 그룹 집계와 가로 방향 비교 표의 차이를 살펴본다.
day.groupBy("station_id").pivot("tag_type", ["IN", "OUT"]).count()      \
    .orderBy("station_id").show()



# %% [08-8] 카드 한 장이 하루에 몇 번 탔는지(승차 수) 세고, 많이 탄 카드 다섯 장을 보십시오.
# [셀 설명] 카드별 승차 횟수를 세고 많이 이용한 카드 다섯 개를 본다.
# [동작] count 내림차순 뒤 card_id를 추가 정렬 기준으로 두어 동률 순서를 정한다. 태그 수와 중복 제거 후 사건 수는 달라질 수 있다.
day.filter(F.col("tag_type") == "IN")       \
    .groupBy("card_id").count()             \
    .orderBy(F.col("count").desc(), "card_id").show(5)





# %% [08-9] 일부러 틀려 보십시오. 역별로 묶은 다음 바로 card_id 칸을 골라 보십시오.
# [셀 설명] 그룹 집계 후에는 그룹 키와 집계한 열만 남는다는 점을 확인하는 오류 실습이다.
# [동작] 현재 셀은 비어 있다. 역별로 묶은 결과에서 개별 card_id를 보려면 그룹 기준이나 집계 방식을 다시 정해야 한다.




# %% [08-10] 하루 로그의 줄 수와 이벤트 번호의 가짓수를 나란히 세십시오.
# [셀 설명] 전체 행 수와 고유 사건 번호 수를 한 표에서 비교한다.
# [동작] 사건 번호가 모두 채워져 있고 한 사건이 한 ID라는 전제에서 두 값의 차이는 중복 행 점검에 쓸 수 있다.
day.agg(F.count("*").alias("rows"),
        F.count_distinct("event_id").alias("events")).show()
    
    # 20개의 중복 -> 재전송?



# %% [08-10 바꿔 보기 ①] 승차·하차별로 줄 수와 이벤트 가짓수를 나란히 보면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 전체의 행 수·고유 이벤트 수 비교를 승차와 하차별로 나누어 확인한다.
# [동작] groupBy 뒤 같은 두 집계를 적용하면 어느 구분에 중복이 포함되어 있는지 더 구체적으로 볼 수 있다.
day.groupBy("tag_type").agg(F.count("*").alias("rows"),
        F.count_distinct("event_id").alias("events"))        \
        .orderBy("tag_type").show()




# %% [08-11] 어떤 이벤트가 두 번 들어왔는지 다섯 개만 보십시오.
# [셀 설명] 같은 event_id가 여러 번 나타나는 사건을 찾아 중복 후보를 확인한다.
# [동작] event_id별 count를 구한 뒤 1보다 큰 그룹을 고른다. 전체 열이 같은 재전송인지 값이 충돌하는 중복인지는 원문을 추가 비교해야 한다.
day.groupBy("event_id").count().filter(F.col("count") > 1)      \
    .orderBy("event_id").show(5)




# %%
