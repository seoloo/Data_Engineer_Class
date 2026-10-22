# ==============================================================================
#  STEP 13  정리하기 — 중복 · 격리 · 승하차 짝 맞추기
#  C:\sparkcamp\lab\s13_clean.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 원본 태그 raw와 역 목록 st를 읽는다. 이후 raw → dedup → clean·quarantine의 처리 단계를 구분해 사용한다.
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


# %% [13-1] 원본(raw)의 줄 수와 이벤트 번호 가짓수를 다시 확인하십시오.
# [셀 설명] 전체 태그 행 수와 고유 event_id 수를 비교해 중복 수신 규모를 살핀다.
# [동작] distinct가 고유 번호만 남기며 두 번째 print는 원본 행 수와의 차이를 계산한다. 사건 번호가 같은 행의 내용도 같은지는 별도 확인 대상이다.
print(raw.count(), raw.select("event_id").distinct().count())
print(raw.count()-raw.select("event_id").distinct().count(), "개의 차이")



# %% [13-2] event_id 기준으로 중복을 걷어 낸 dedup 을 만들고 세십시오.
# [셀 설명] 같은 event_id를 같은 사건으로 보고 중복 행을 제거한다.
# [동작] 이 실습의 재전송처럼 동일 사건 복사본을 걷어 낸다. 같은 키에 값이 충돌하는 데이터라면 어느 행을 남길지 추가 규칙이 필요하다.
dedup = raw.dropDuplicates(["event_id"])
dedup.count()



# %% [13-2 바꿔 보기 ①] 칸 목록 없이 dropDuplicates() 로 하면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 키를 지정하지 않은 중복 제거와 event_id 기준 제거를 비교한다.
# [동작] dropDuplicates()는 모든 열 값이 같은 행을 중복으로 본다. 재전송 때 recv_time만 달라도 전체 열 기준으로는 중복이 아닐 수 있다.
print(raw.dropDuplicates().count())



# %% [13-2 바꿔 보기 ②] 키를 잘못 고르면? — card_id 로 중복을 걷어 내기
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 중복 키를 card_id로 잘못 잡으면 정상 이용 이력까지 사라짐을 확인한다.
# [동작] 카드 한 장은 여러 사건과 이동을 가질 수 있다. 중복 제거 기준은 같은 카드인지가 아니라 같은 사건인지에 맞춘다.
print(raw.dropDuplicates(["card_id"]).count())




# %% [13-3] 격리 조건을 하나의 변수로 만드십시오 — 역 코드가 비어 있거나, 등록 안 된 역이거나, 태그 시각이 비어 있으면 격리.
# [셀 설명] 미등록·NULL 역 코드 또는 NULL 시각을 격리 대상으로 정하는 공통 조건을 만든다.
# [동작] 작은 역 마스터에서 정상 코드를 가져와 isin으로 검사한다. isNull을 포함해야 NULL인 역도 정상·격리 분류에서 빠지지 않는다.
valid_ids = [r.station_id for r in st.select("station_id").collect()]
print(valid_ids)
is_bad = ((F.col("station_id").isNull()) |
          (~F.col("station_id").isin(valid_ids)) |
          (F.col("tag_time").isNull()))




# %% [13-4] 격리 바구니 quarantine 을 만들고, 이유(reason) 칸을 붙여 이유별로 세십시오.
# reason - 1.BAD_TIME / 2. UNKNWON_STATION
# [셀 설명] 문제 행을 삭제하지 않고 quarantine으로 분리하며 격리 사유를 붙인다.
# [동작] 역 코드 문제가 있으면 UNKNOWN_STATION, 나머지 격리 행은 BAD_TIME으로 분류한다. 두 문제가 동시에 있으면 현재 when 순서상 역 코드 사유가 먼저 붙는다.

quarantine = dedup.filter(is_bad).withColumn(
    "reason", 
    F.when(F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids), "UNKNOWN_STATION").otherwise("BAD_TIME"))

quarantine.groupBy("reason").count().show()




# %% [13-5] 정상 바구니 clean 을 만들고 세십시오.
# [셀 설명] 격리 조건의 부정으로 정상 이벤트만 clean에 남긴다.
# [동작] ~는 Column 조건의 부정이다. 원래 is_bad에 NULL 처리가 있어야 정상과 격리를 빠짐없이 나누는 데 도움이 된다.
clean = dedup.filter(~is_bad)
clean.count()


# %% [13-6] 한 줄도 잃어버리지 않았는지 더해서 확인하십시오.
# [셀 설명] 처리 전 원본 건수가 중복·격리·정상으로 빠짐없이 분류되었는지 검산한다.
# [동작] 원본 = 제거한 중복 + 격리 + 정상이어야 한다. 불일치하면 조건의 NULL 처리나 분류 단계의 행 손실·중복을 확인한다.
n_raw, n_dup = raw.count(), raw.count() - dedup.count()
n_q, n_ok = quarantine.count(), clean.count()
print(f"원본 {n_raw} = 중복 {n_dup} + 격리 {n_q} + 정상 {n_ok} -> {n_dup + n_q + n_ok}")
print("일치" if n_raw == n_dup + n_q + n_ok else "불일치")



# %% [13-7] 짝 맞추기 — 정상 이벤트에서 승차 표와 하차 표를 따로 만들고, trip_id 로 붙여 trips 를 만드십시오.
# [셀 설명] 정상 태그를 승차와 하차로 나누어 trip_id로 연결하고 한 행을 이동 한 건으로 바꾼다.
# [동작] 열 별칭으로 승차·하차 역과 시각을 구분한다. 승차 기준 left join은 하차가 없는 이동도 보존하며, trip_id의 유일성도 결과 행 수에 영향을 준다.
board = clean.filter(F.col("tag_type") == "IN")     \
    .select("trip_id", "card_id", F.col("station_id").alias("board_st"),
                F.col("tag_time").alias("board_time"))

alight = clean.filter(F.col("tag_type") == "OUT")           \
    .select("trip_id", F.col("station_id").alias("alight_st"),
            F.col("tag_time").alias("alight_time"), "fare")

trips = board.join(alight, "trip_id", "left")
trips.orderBy("trip_id").show()


# %% [13-8] 이동이 몇 건인지, 그중 아직 하차 기록이 없는 이동이 몇 건인지 세십시오.
# [셀 설명] 연결한 이동의 총수와 하차가 확인되지 않은 이동 수를 구한다.
# [동작] left join 결과의 alight_st가 NULL이면 짝인 하차가 없다는 뜻으로 센다. 실시간에서는 미도착, 배치에서는 누락·격리 등의 원인도 구분해 해석한다.
print(trips.count())
trips.filter(F.col("alight_st").isNull()).count()


# %% [13-9] 이동 시간(분)을 travel_min 칸으로 만들고, 평균 · 최소 · 최대를 구하십시오.
# [셀 설명] 하차 시각에서 승차 시각을 빼 이동 시간을 분 단위로 계산하고 통계를 구한다.
# [동작] NULL 이동 시간은 avg·min·max 집계에서 제외된다. 현재 max라는 열도 F.min으로 계산하므로 이름만 최대값이며 실제 최대값은 F.max가 필요하다.
trips = trips.withColumn("travel_min",
            F.round((F.unix_timestamp("alight_time") - F.unix_timestamp("board_time")) / 60, 1))

trips.agg(F.round(F.avg("travel_min"), 1).alias("avg"),
            F.min("travel_min").alias("min"),
            F.min("travel_min").alias("max")).show()

# %% [13-10] 하차는 있는데 승차가 없는 이동이 있는지 찾으십시오. (승차 태그가 S99 로 격리된 경우)
# [셀 설명] 하차는 있지만 정상 승차가 없는 이동을 left_anti로 찾는다.
# [동작] 승차 기준 left join에는 이런 하차가 들어가지 않는다. 승차 태그가 격리된 경우 등을 별도로 확인해 이동 통계에서 빠진 이유를 설명한다.
orphan = alight.join(board, "trip_id", "left_anti")
print(orphan.count())
orphan.show()


# %% [13-11] 같은 질문, 다른 답 — 역별 승차 인원을 원본(raw)과 정상(clean)에서 각각 구해 나란히 보십시오.
# [셀 설명] 동일한 역별 승차 질문을 원본과 정제 데이터에 각각 적용해 나란히 비교한다.
# [동작] 함수의 name 인자로 결과 열 이름을 달리해 조인한다. 차이는 중복 제거와 격리의 영향이므로 통계를 낼 때 데이터 처리 단계를 명시한다.
def board_count(df, name):
    return df.filter(F.col("tag_type") == "IN").groupBy("station_id")       \
        .agg(F.count("*").alias(name))

board_count(raw, "raw_cnt").join(board_count(clean, "clean_cnt"), "station_id", "left") \
    .orderBy("station_id").show()



# %% [13-12] 정상 이벤트 기준으로 오늘의 기준 숫자 네 개를 구하십시오 — 승차 수 · 하차 수 · 요금 합계 · 이동 수.
# [셀 설명] 정상 데이터의 승차 수·하차 수·요금 합계와 이동 수를 다음 단계의 비교 기준으로 만든다.
# [동작] when(..., 1).otherwise(0)의 합계로 조건별 건수를 센다. 이벤트 표의 승하차 건수와 이동 표 trips의 행 수가 뜻하는 바를 구분한다.
summary = clean.agg(
    F.sum(
        F.when(
            F.col("tag_type") == "IN", 1).otherwise(0)).alias("board"),
    F.sum(
        F.when(
            F.col("tag_type") == "OUT", 1).otherwise(0)).alias("alight"),
    F.sum("fare").alias("fare_sum"))

summary.show()
print("trips : ", trips.count())




# %% [13-13] NULL 함정 확인 — 역 코드가 비어 있는 가짜 이벤트 한 줄을 만들어, isNull() 이 없는 규칙과 있는 규칙으로 각각 정상·격리를 세어 보십시오.
# [셀 설명] NULL 역 코드가 있는 이벤트를 직접 만들어 격리 조건의 누락을 확인하는 셀이다.
# [동작] 현재 셀은 비어 있다. NULL의 isin 결과와 그 부정은 NULL일 수 있고 filter는 TRUE만 통과시키므로, isNull 없는 조건에서는 양쪽 분류에서 빠질 수 있다.
