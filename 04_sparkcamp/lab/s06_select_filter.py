# ==============================================================================
#  STEP 06  골라 보기 — 줄 고르기 · 칸 고르기 · 칸 만들기
#  C:\sparkcamp\lab\s06_select_filter.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 작은 로그 mini와 하루 로그 day를 읽어 이후 셀의 입력으로 사용한다. 예제 확인용 데이터와 전체 집계용 데이터를 구분한다.
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
# [셀 설명] 로그에서 이동 번호·승하차 구분·역·요금 열만 골라 본다.
# [동작] select는 결과에 포함할 열과 그 순서를 정한다. 원본 mini 자체를 변경하지 않고 새로운 DataFrame을 만든다.
mini.select("trip_id", "tag_type", "station_id", "fare").show()


# %% [06-2] 작은 로그에서 승차(IN) 만 고르십시오.
# [셀 설명] tag_type이 IN인 승차 태그만 남긴다.
# [동작] filter에 Column 조건을 주는 방식과 where에 SQL 조건 문자열을 주는 방식을 비교한다. 두 표현이 같은 조건을 나타낸다.
mini.filter(F.col("tag_type") == "IN").show()
mini.where("tag_type = 'IN'").show()


# %% [06-3] 같은 일을 where 에 SQL 글자로 써서, 하차(OUT)가 몇 건인지 세십시오.
# [셀 설명] SQL 조건 문자열로 하차 태그를 고르고 그 행 수를 센다.
# [동작] where는 filter와 같은 역할이다. count는 고른 태그 행 수이며 서로 다른 카드나 이동의 수를 자동으로 세는 것은 아니다.
mini.where("tag_type = 'OUT'").count()



# %% [06-4] 하차이면서 요금이 1,600원 이상인 것을 고르십시오. 조건마다 괄호를 쓰십시오.
# [셀 설명] 하차이면서 요금이 기준 이상인 행을 두 조건의 교집합으로 고른다.
# [동작] Column 조건끼리는 각각 괄호로 묶어 &로 결합한다. SQL 문자열에서는 AND를 사용한다.
mini.filter((F.col("tag_type") == "OUT") & (F.col("fare") >= 1600)).show()
mini.where("tag_type = 'OUT' AND fare >= 1600").show()




# %% [06-4 바꿔 보기 ①] 그리고(&) 를 또는(|) 으로 바꾸면? — 승차이거나 요금 1,600 이상
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] AND와 OR로 고른 결과 범위가 어떻게 달라지는지 비교하는 셀이다.
# [동작] 현재 셀은 비어 있다. |는 둘 중 하나만 만족해도 통과하므로 '승차이거나 높은 요금'은 앞 셀의 '하차이면서 높은 요금'과 다른 질문이다.




# %% [06-5] 일부러 틀려 보십시오. 06-4 를 괄호 없이 쓰십시오.
# [셀 설명] 복합 조건에서 괄호가 빠지면 연산자 우선순위 때문에 잘못 해석될 수 있음을 확인한다.
# [동작] 현재 셀은 비어 있다. 파이썬 and 대신 Column용 &를 쓰고 각 비교식을 괄호로 감싸는 이유를 오류와 함께 복습한다.




# %% [06-6] 강남(S01) 또는 역삼(S02)에서 찍힌 태그만 고르십시오. isin 을 쓰십시오.
# [셀 설명] 역 코드가 S01 또는 S02인 태그를 고르고 필요한 열만 남긴다.
# [동작] isin은 값이 지정된 후보 목록 안에 있는지 검사한다. 여러 == 조건을 |로 나열하는 대신 사용할 수 있다.
mini.filter(F.col("station_id").isin("S01", "S02")).select(
    "event_id", "station_id", "fare").show()



# %% [06-7] 작은 로그를 태그 시각이 늦은 것부터 세 줄 보십시오.
# [셀 설명] 태그 시각 내림차순으로 정렬해 가장 늦은 기록 세 행을 본다.
# [동작] 첫 줄의 F.col(...).desc()가 내림차순 표현이다. 둘째 줄은 ascending=False를 F.col에 전달해 잘못된 위치에 옵션을 둔 상태다.
mini.orderBy(F.col("tag_time").desc()).show(3)
mini.orderBy(F.col("tag_time", ascending=False)).show(3)




# %% [06-8] 승차는 "승차", 하차는 "하차" 라고 적힌 구분 칸을 새로 만들어 보십시오.
# [셀 설명] 승하차 코드에서 사람이 읽기 쉬운 구분 열을 만든다.
# [동작] withColumn은 열을 추가하거나 같은 이름의 열을 대체한다. when은 조건에 맞는 값, otherwise는 나머지 값을 지정한다.
mini.withColumn("구분", F.when(F.col("tag_type") == "IN", "승차").otherwise("하차"))\
    .select("event_id", "tag_type", "구분").show()



# %% [06-9] 요금을 천 원 단위(소수 한 자리)로 바꾼 fare_k 칸을 만들고, 하차만 보십시오.
# [셀 설명] 하차 태그의 요금을 천 원 단위로 바꾸고 소수 첫째 자리까지 반올림한다.
# [동작] fare / 1000의 계산 결과를 round로 정리해 fare_k에 담는다. 원래 fare 열도 함께 보면 단위 변환을 검산하기 쉽다.
(mini.filter(F.col("tag_type") == "OUT")
 .withColumn("fare_k", F.round(F.col("fare") / 1000, 1))
 .select("trip_id", "fare", "fare_k")
 .show()
 )



# %% [06-10] station_id 칸 이름을 역코드 로 바꿔 보여 주십시오.
# [셀 설명] 역 코드 열에 표시용 이름을 붙여 선택한다.
# [동작] alias는 이 결과에서 사용할 열 이름을 정한다. mini의 기존 station_id 이름 자체가 변경되는 것은 아니다.
mini.select(F.col("station_id").alias("역코드"), "tag_type").show(3)



# %% [06-11] 작은 로그에 나오는 역이 몇 곳인지 세십시오.
# [셀 설명] 로그에 등장한 서로 다른 역 코드의 개수를 센다.
# [동작] 역 코드를 선택하고 distinct로 같은 값을 한 번씩만 남긴 뒤 count한다. 전체 태그 행 수와 다른 지표다.
mini.select("station_id").distinct().count()



# %% [06-12] 하루 로그에서 역 코드가 S99 인 줄을 찾아보십시오. 역 마스터에 없는 코드입니다.
# [셀 설명] 역 마스터에 없는 S99 코드가 포함된 로그를 찾아 확인한다.
# [동작] 이벤트·이동 번호를 함께 보면 어느 사건이 문제인지 추적할 수 있다. 이후 정제 단계에서 이런 행을 격리한다.
day.filter(F.col("station_id") == "S99")\
    .select("event_id", "trip_id", "station_id")\
    .show()



# %% [06-13] 하루 로그에서 요금이 붙은 줄(fare > 0)이 몇 개인지 where 로 세십시오.
# [셀 설명] 요금이 양수인 태그 행 수를 두 조건 표현 방식으로 센다.
# [동작] Column filter와 SQL where의 결과가 같은지 비교한다. 요금이 양수인 행 수와 요금 합계는 서로 다른 값이다.

# mini.filter((F.col("tag_type") == "OUT") & (F.col("fare") >= 1600)).show()
#   filter
day.filter(F.col("fare") > 0)\
    .count()


#   where
day.where("fare > 0").count()


# %%
