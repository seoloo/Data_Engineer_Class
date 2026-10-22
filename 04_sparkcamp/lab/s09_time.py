# ==============================================================================
#  STEP 09  시간 다루기 — 태그 시각과 도착 시각
#  C:\sparkcamp\lab\s09_time.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] day는 시각을 Timestamp로, raw는 자동 추론으로 읽어 변환 동작을 비교한다. 같은 원천을 다르게 읽은 두 표의 역할을 구분한다.
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
# [셀 설명] Timestamp에서 시(hour) 부분을 뽑아 정수 열 hh로 만든다.
# [동작] hour는 날짜를 제외한 시각의 시 부분이다. 여러 날짜가 섞였다면 날짜별 시간 구간과 단순 시별 집계를 구분해야 한다.
day.select("event_id", "tag_time", F.hour("tag_time").alias("hh")).show(5)




# %% [09-2] 태그 시각을 08:31 모양의 글자로 바꿔 보십시오.
# [셀 설명] 시각을 화면에서 보기 좋은 시:분 문자열로 표현한다.
# [동작] date_format의 HH:mm은 24시간제 시와 분이다. 표시용 문자열로 바꾸는 것과 Timestamp 자체를 유지해 계산하는 것을 구분한다.
day.select("event_id", F.date_format("tag_time", "HH:mm")
           .alias("hhmm")).show(5)



# %% [09-3] 시간대별 태그 수를 세어 시 순서로 보십시오.
# [셀 설명] 이벤트가 실제로 발생한 시를 기준으로 태그 건수를 묶는다.
# [동작] F.hour(tag_time)을 그룹 키로 사용한다. 시각이 NULL인 행은 별도의 NULL 그룹에 포함될 수 있으므로 집계 대상을 확인한다.
day.groupBy(F.hour("tag_time").alias("hh")).count()     \
    .orderBy("hh").show()




# %% [09-3 바꿔 보기 ①] 시간대별로 셀 때 tag_time 대신 recv_time(도착 시각)으로 세면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 집계 기준을 사건 발생 시각에서 시스템 도착 시각으로 바꾸어 비교한다.
# [동작] 늦게 도착한 이벤트는 두 시각의 시간대가 다를 수 있다. 이용 시점 통계인지 수신량 통계인지에 따라 기준 열을 선택한다.
day.groupBy(F.hour("recv_time").alias("hh")).count()     \
    .orderBy("hh").show()



# %% [09-4] 스키마 없이 읽은 raw 의 tag_time 은 무슨 자료형인지 보십시오.
# [셀 설명] 스키마 없이 읽은 raw의 태그 시각 자료형을 확인한다.
# [동작] 겉모양이 날짜·시간이어도 문자열일 수 있다. 이후 명시적 변환을 적용할 때 원래 자료형을 먼저 확인한다.
raw.select("tag_time").printSchema()



# %% [09-5] 일부러 틀려 보십시오. raw 의 tag_time 을 to_timestamp 로 바꾸고, 바뀐 값이 비어 있는 줄을 세십시오.
# [셀 설명] 문자열을 Timestamp로 바꿀 때 잘못된 시각 값이 어떻게 처리되는지 확인한다.
# [동작] ANSI 설정 등 실행 조건에 따라 to_timestamp의 변환 실패가 오류로 나타날 수 있다. 의도한 오류 셀을 따로 실행하고 다음 셀과 비교한다.
raw.select(F.to_timestamp("tag_time").alias("t"))       \
    .filter(F.col("t").isNull()).count()



# %% [09-6] 같은 일을 try_to_timestamp 로 하십시오.
# [셀 설명] 변환 실패를 NULL로 받을 수 있는 try_to_timestamp로 문제 행을 센다.
# [동작] 원문을 보존한 채 변환 결과의 NULL을 검사하면 전체 처리를 계속하면서 격리 대상을 찾을 수 있다.
raw.select(F.try_to_timestamp("tag_time").alias("t"))       \
    .filter(F.col("t").isNull()).count()



# %% [09-7] 도착 지연(초) = 도착 시각 − 태그 시각을 delay_sec 칸으로 만들고, 가장 늦게 온 다섯 줄을 보십시오.
# [셀 설명] 수신 시각과 태그 시각의 차이를 초 단위 지연 시간으로 계산한다.
# [동작] unix_timestamp로 초 단위 값을 구해 recv_time - tag_time을 계산한다. 두 시각 중 하나가 NULL이면 지연 값도 NULL이다.
late = day.withColumn("delay_sec", F.unix_timestamp("recv_time")
                      - F.unix_timestamp("tag_time"))
late.orderBy(F.col("delay_sec").desc())     \
    .select("event_id", "tag_type", "tag_time", "recv_time", "delay_sec")       \
    .show(5)


# %% [09-8] 1분(60초) 넘게 늦게 온 태그가 몇 개인지 세고, 승차·하차로 나눠 보십시오.
# [셀 설명] 수신 지연이 60초보다 큰 이벤트를 골라 승하차별 건수를 센다.
# [동작] 기준은 > 60이므로 정확히 60초인 행은 제외된다. 조건의 이상·초과 차이를 확인한다.
late.filter(F.col("delay_sec") > 60).groupBy("tag_type").count().show()



# %% [09-9] 8시대(08:00:00 이상 09:00:00 미만)에 찍힌 승차 태그를 세십시오.
# [셀 설명] 08:00 이상 09:00 미만의 승차처럼 시작 포함·끝 제외 시간 범위를 표현하는 셀이다.
# [동작] 현재 두 번째 시각 조건도 >= 09:00으로 작성되어 9시 이후를 고르는 상태다. 제목의 구간을 만들려면 끝 조건의 < 여부를 확인한다.
day.filter((F.col("tag_type") == "IN") & 
           (F.col("tag_time") >= "2026-10-14 08:00:00") &
           (F.col("tag_time") >= "2026-10-14 09:00:00")).count() 





# %% [09-10] 태그 시각을 정시로 잘라(date_trunc) 시간 단위로 승차를 세십시오.
# [셀 설명] 각 사건 시각을 해당 날짜의 정시로 내려 시간 구간별 승차 건수를 센다.
# [동작] date_trunc('hour', ...)는 날짜와 시를 유지하고 분·초를 자른다. 날짜를 버리는 hour 추출과 달리 날짜별 정시 구간이 구분된다.
day.filter(F.col("tag_type")=="IN")\
        .groupBy(F.date_trunc("hour","tag_time").alias("hour_start")).count()\
        .orderBy("hour_start").show()


