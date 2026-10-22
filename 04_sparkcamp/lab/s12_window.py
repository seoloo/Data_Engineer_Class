# ==============================================================================
#  STEP 12  시간 창으로 묶기 — Tumbling 과 Sliding 을 미리 맛보기
#  C:\sparkcamp\lab\s12_window.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] day에서 승차이면서 사건 시각이 NULL이 아닌 행만 ins로 골라 시간 창 집계 대상으로 삼는다.
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
ins = day.filter((F.col("tag_type") == "IN") & F.col("tag_time").isNotNull())
print(day.count(), ins.count())


# %% [12-1] 승차 태그에 5분 Tumbling 창을 붙여 다섯 줄 보십시오.
# [셀 설명] 각 승차 시각이 어느 5분 구간에 속하는지 시간 창을 붙여 확인한다.
# [동작] window는 start·end가 있는 struct를 반환한다. 창은 시작 시각 포함·끝 시각 제외여서 정확히 경계인 이벤트가 두 Tumbling 창에 중복되지 않는다.
ins.select("event_id", "tag_time", F.window("tag_time", "5 minutes").alias("w"))\
    .show(truncate=False)



# %% [12-2] 5분마다 승차 인원을 세어 시간 순으로 여섯 줄 보십시오.
# [셀 설명] 겹치지 않는 5분 Tumbling 창별로 승차 태그 수를 센다.
# [동작] window를 그룹 키로 사용하고 창 순서대로 정렬한다. 현재 ins는 시각이 정상인 승차 태그이므로 그 범위의 건수를 집계한다.
by5 = ins.groupBy(F.window("tag_time", "5 minutes").alias("w")).count()
by5.orderBy("w").show(6, truncate=False)





# %% [12-2 바꿔 보기 ①] 창을 5분이 아니라 10분으로 늘리면? — 창 개수와 합계
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] Tumbling 창 길이를 5분에서 10분으로 늘려 창 수와 총 건수를 비교한다.
# [동작] 구간이 넓어지면 여러 창이 합쳐질 수 있지만 이벤트가 창 하나에 들어가는 원칙은 같다. 두 집계의 count 합계가 같은지 확인한다.
by10 = ins.groupBy(F.window("tag_time", "10 minutes").alias("w")).count()
print(by5.count(), "개 창 -> 합계", by5.agg(F.sum("count")).first()[0])
print(by10.count(), "개 창 -> 합계", by10.agg(F.sum("count")).first()[0])


# %% [12-3] 창을 start · end 두 칸으로 펼쳐서 보기 좋게 만드십시오.
# [셀 설명] 중첩된 시간 창 구조의 start·end를 각각 일반 열로 꺼낸다.
# [동작] w.start는 struct 안의 필드 참조다. select와 alias로 열을 펼치면 정렬·필터·표시를 이해하기 쉬워진다.

by5f = by5.select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count")
by5f.orderBy("start").show(6)




# %% [12-4] 피크 5분 — 승차가 가장 많았던 5분 창 세 개를 보십시오.
# [셀 설명] 5분 창별 집계에서 승차가 가장 많았던 구간 세 개를 찾는다.
# [동작] count 내림차순 뒤 start를 두 번째 기준으로 두어 동률 때의 표시 순서를 정한다.
by5f.orderBy(F.col("count").desc(), "start").show(3)



# %% [12-5] 역별 · 5분별 승차 인원을 구하고, 강남(S01)만 08:00~08:30 사이를 보십시오.
# [셀 설명] 시간 창과 역 코드를 함께 그룹 키로 삼아 역별 5분 승차량을 구한다.
# [동작] S01과 시간 범위로 집계 결과를 거른다. 현재 끝 조건은 08:50 미만으로 작성되어 제목의 08:30 미만과 범위가 다르다.
st5 = ins.groupBy(F.window("tag_time", "5 minutes").alias("w"), "station_id").count()   \
    .select(F.col("w.start").alias("start"), "station_id", "count")
    
st5.filter((F.col("station_id") == "S01")&
            (F.col("start") >= "2026-10-14 08:00:00")&
            (F.col("start") < "2026-10-14 08:50:00"))   \
            .orderBy("start").show()




# %% [12-6] Tumbling 창의 count 를 전부 더해서 승차 수(ins.count())와 같은지 확인하십시오.
# [셀 설명] Tumbling 창별 count의 합계가 집계 대상 승차 행 수와 같은지 검산한다.
# [동작] 같은 대상에서 각 이벤트가 한 창에만 들어가면 합계가 ins.count()와 같다. 필터 누락이나 집계 범위 차이를 확인하는 방법이다.
print(by5.agg(F.sum("count")).first()[0])
print(ins.count())


# %% [12-7] 10분 창을 5분마다 미는 Sliding 으로 세고, count 를 전부 더해 보십시오.
# [셀 설명] 10분 길이의 창을 5분 간격으로 만들어 겹치는 Sliding 집계를 구한다.
# [동작] 이 설정에서는 이벤트 하나가 두 창에 들어가므로 창별 count의 합계는 대상 행 수의 두 배가 된다. 원천의 중복 수신과는 다른 중복 집계다.
slide = ins.groupBy(F.window("tag_time", "10 minutes", "5 minutes").alias("w")).count()
print(slide.agg(F.sum("count")).first()[0])

slide.select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count") \
    .orderBy("start").show(5)


# %% [12-8] 1시간 Tumbling 창으로 세고, STEP 09 의 date_trunc 결과와 같은지 보십시오.
# [셀 설명] 1시간 Tumbling 창의 시작값과 date_trunc로 자른 정시 집계를 비교한다.
# [동작] 같은 대상·시간 해석·정시 경계라면 두 방법이 같은 시간 구간을 만든다. window는 start와 end를 함께 제공한다.
ins.groupBy(F.window("tag_time", "1 hour").alias("w")).count()                  \
    .select(F.col("w.start").alias("start"), "count").orderBy("start").show()
    
    

# %% [12-9] 하차 태그를 5분 창으로 세는데, 한 번은 tag_time(댄 시각), 한 번은 recv_time(도착 시각) 기준으로 세어 07:30~07:35 창을
# 비교하십시오.
# [셀 설명] 같은 하차 로그를 사건 발생 시각과 도착 시각으로 각각 집계해 늦게 온 기록의 영향을 비교한다.
# [동작] 현재 count_at은 show() 결과인 None을 반환한다. 숫자를 반환해 비교하려면 해당 창의 Row에서 count 값을 꺼내는 방식이 필요하다.
outs = day.filter((F.col("tag_type") == "OUT") & F.col("tag_time").isNotNull());

def count_at(col_name):
    return(
        outs.groupBy(F.window(col_name, "5 minutes").alias("w")).count()        \
            .filter(F.col("w.start") == "2026-10-14 07:30:00")
            #.first()["count"]
            .show()
    )

print("태크 시각 기준 : ", count_at("tag_time"));
print("도착 시각 기준 : ", count_at("recv_time"));



  # %% [12-10] 도착이 10분 넘게 늦은 하차 태그가 이벤트 시간 기준 몇 시 창에 있었는지 세어 보십시오.
# [셀 설명] 수신 지연이 600초보다 큰 하차를 찾아 실제 사건 시각의 시간대별로 센다.
# [동작] 늦게 도착해도 사건이 속한 시간 구간은 tag_time으로 정한다. 이후 스트리밍에서 늦은 이벤트를 언제까지 기다릴지 생각하는 연결 실습이다.
late = outs.filter(F.unix_timestamp("recv_time") - F.unix_timestamp("tag_time") > 600)
late.groupBy(F.window("tag_time", "1 hour").alias("w")).count()     \
    .select(F.col("w.start").alias("start"), "count")               \
    .orderBy("start").show()



# %%
