# ==============================================================================
#  STEP 10  이어 붙이기 — 역 마스터와 join
#  C:\sparkcamp\lab\s10_join.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 태그 로그 day와 역 마스터 st를 읽는다. 사건 정보와 기준 정보를 역 코드로 연결할 준비다.
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
# [셀 설명] 로그의 역 코드와 역 마스터를 연결해 역 이름·노선 정보를 붙인다.
# [동작] 기본 join은 inner라 마스터에 짝이 없는 로그가 빠진다. 현재 코드는 열 선택까지만 있고, 제목처럼 다섯 행을 보려면 표시 액션도 필요하다.
day.join(st, "station_id")      \
    .select("event_id", "tag_type", "station_id", "station_nm", "line")     \



# %% [10-2] 기본(inner) join 결과가 몇 줄인지 세십시오.
# [셀 설명] inner join 전후 행 수를 비교해 연결되지 않은 로그가 빠지는지 확인하는 셀이다.
# [동작] 현재 셀은 비어 있다. 오른쪽 마스터의 키가 중복이면 반대로 행 수가 늘 수도 있으므로 키 유일성과 함께 확인한다.




# %% [10-2 바꿔 보기 ①] 붙이지는 말고 "짝이 있는 줄만" 남기면? — left_semi
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 마스터에 짝이 존재하는 왼쪽 로그만 남기되 마스터 열은 붙이지 않는다.
# [동작] left_semi는 존재 여부 검사에 쓴다. count와 columns 길이를 함께 보면 행 선택과 열 결합의 차이를 알 수 있다.
semi = day.join(st, "station_id", "left_semi")
print(semi.count(), len(semi.columns))



# %% [10-2 바꿔 보기 ②] 마스터에서 광화문(S08)이 빠져 있다면? — inner join 줄 수
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 정상 역 하나가 마스터에서 빠졌을 때 inner join 결과가 달라지는지 확인한다.
# [동작] 로그가 정상이어도 기준 마스터가 빠지면 집계 대상에서 사라질 수 있다. 결과 숫자의 변화가 원천 오류만을 뜻하지는 않는다.
st_miss = st.filter(F.col("station_id") != "S08")
print(day.join(st_miss, "station_id").count())



# %% [10-3] left join 으로 붙이면 몇 줄인지 세십시오.
# [셀 설명] 왼쪽 로그를 보존하는 left join의 결과 행 수를 확인한다.
# [동작] 마스터 키가 유일하다면 로그 한 행당 최대 한 행이 연결되고 짝 없는 오른쪽 열은 NULL이 된다. 마스터 키 중복 시 행 증가는 별도 점검한다.
day.join(st, "station_id", "left").count()



# %% [10-4] left join 결과에서 역 이름이 비어 있는 줄을 보십시오.
# [셀 설명] left join 후 역 이름이 NULL인 로그를 찾아 마스터와 연결되지 않은 사건을 살핀다.
# [동작] 현재 isNull은 괄호 없이 메서드 자체를 전달했다. NULL 조건 Column을 만들려면 isNull() 호출이 필요한 부분이다.
day.join(st, "station_id", "left")          \
    .filter(F.col("station_nm").isNull)     \
    .select("event_id", "trip_id", "station_id", "station_nm").show()




# %% [10-5] 같은 6줄을 left_anti 한 번으로 찾으십시오.
# [셀 설명] 마스터에 대응하는 역 코드가 없는 로그를 left_anti로 직접 찾는다.
# [동작] left_anti는 오른쪽에 짝이 없는 왼쪽 행을 남긴다. 데이터 품질 점검에서 미등록 코드나 참조 누락을 찾는 데 사용한다.
unknown = day.join(st, "station_id", "left_anti")
unknown.count()



# %% [10-6] 역 이름으로 승차 인원을 세어 많은 순으로 보십시오.
# [셀 설명] 승차 태그에 역 이름을 붙인 뒤 역 이름별 승차 건수를 집계한다.
# [동작] filter → join → groupBy → 정렬 → 표시 순서다. inner join을 사용했으므로 미등록 역의 승차는 이 결과에서 빠진다.
(
    day.filter(F.col("tag_type") == "IN")       \
    .join(st, "station_id")                     \
    .groupBy("station_nm").count()              \
    .orderBy(F.col("count").desc())             \
    .show()
)



# %% [10-7] 노선별 하차 인원과 요금 합계를 구하십시오.
# [셀 설명] 하차 태그를 노선별로 묶어 하차 건수와 요금 합계를 함께 계산한다.
# [동작] 집계 키인 line은 역 마스터에서 가져온다. 로그의 업무 값과 기준 정보가 결합되어야 가능한 통계의 예다.
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
# [셀 설명] 작은 역 마스터를 broadcast 후보로 지정하고 조인 실행 계획을 확인한다.
# [동작] 작은 표를 실행 측에 전달하면 큰 로그 전체를 키별로 재배치하는 비용을 줄일 수 있다. explain에서 선택된 조인 방식을 확인한다.
joined = day.join(F.broadcast(st), "station_id")

joined.explain()



# %% [10-9] 일부러 틀려 보십시오. 조건식으로 붙인 다음 station_id 를 골라 보십시오.
# [셀 설명] 조건식으로 조인했을 때 양쪽에 같은 열 이름이 남아 생기는 모호성을 확인하는 셀이다.
# [동작] 현재 셀은 비어 있다. 표 별칭으로 열을 구분하거나 사용할 열을 명시해 station_id가 어느 쪽인지 지정하는 방법을 복습한다.




# %% [10-10] 구(gu)별 승차 인원을 구하고, 승차가 가장 많은 구를 한 줄만 보십시오.
# [셀 설명] 역 마스터의 구 정보를 이용해 구별 승차 건수를 구하고 가장 큰 결과 한 행을 본다.
# [동작] orderBy 내림차순 뒤 show(1)로 상위 결과를 표시한다. 조인에서 제외된 역과 동률 처리 기준도 결과 해석에 영향을 줄 수 있다.
(
    day.filter(F.col("tag_type") == "IN")       \
    .join(st, "station_id")                     \
    .groupBy("gu").count()              \
    .orderBy(F.col("count").desc())             \
    .show(1)
)




# %%
