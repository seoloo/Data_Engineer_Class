# ==============================================================================
#  STEP 12  시간 창으로 묶기 — Tumbling 과 Sliding 을 미리 맛보기
#  C:\sparkcamp\lab\s12_window.py
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
ins = day.filter((F.col("tag_type") == "IN") & F.col("tag_time").isNotNull())
print(day.count(), ins.count())


# %% [12-1] 승차 태그에 5분 Tumbling 창을 붙여 다섯 줄 보십시오.
ins.select("event_id", "tag_time", F.window("tag_time", "5 minutes").alias("w"))\
    .show(truncate=False)



# %% [12-2] 5분마다 승차 인원을 세어 시간 순으로 여섯 줄 보십시오.
by5 = ins.groupBy(F.window("tag_time", "5 minutes").alias("w")).count()
by5.orderBy("w").show(6, truncate=False)





# %% [12-2 바꿔 보기 ①] 창을 5분이 아니라 10분으로 늘리면? — 창 개수와 합계
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
by10 = ins.groupBy(F.window("tag_time", "10 minutes").alias("w")).count()
print(by5.count(), "개 창 -> 합계", by5.agg(F.sum("count")).first()[0])
print(by10.count(), "개 창 -> 합계", by10.agg(F.sum("count")).first()[0])


# %% [12-3] 창을 start · end 두 칸으로 펼쳐서 보기 좋게 만드십시오.

by5f = by5.select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count")
by5f.orderBy("start").show(6)




# %% [12-4] 피크 5분 — 승차가 가장 많았던 5분 창 세 개를 보십시오.
by5f.orderBy(F.col("count").desc(), "start").show(3)



# %% [12-5] 역별 · 5분별 승차 인원을 구하고, 강남(S01)만 08:00~08:30 사이를 보십시오.
st5 = ins.groupBy(F.window("tag_time", "5 minutes").alias("w"), "station_id").count()   \
    .select(F.col("w.start").alias("start"), "station_id", "count")
    
st5.filter((F.col("station_id") == "S01")&
            (F.col("start") >= "2026-10-14 08:00:00")&
            (F.col("start") < "2026-10-14 08:50:00"))   \
            .orderBy("start").show()




# %% [12-6] Tumbling 창의 count 를 전부 더해서 승차 수(ins.count())와 같은지 확인하십시오.
print(by5.agg(F.sum("count")).first()[0])
print(ins.count())


# %% [12-7] 10분 창을 5분마다 미는 Sliding 으로 세고, count 를 전부 더해 보십시오.
slide = ins.groupBy(F.window("tag_time", "10 minutes", "5 minutes").alias("w")).count()
print(slide.agg(F.sum("count")).first()[0])

slide.select(F.col("w.start").alias("start"), F.col("w.end").alias("end"), "count") \
    .orderBy("start").show(5)


# %% [12-8] 1시간 Tumbling 창으로 세고, STEP 09 의 date_trunc 결과와 같은지 보십시오.
ins.groupBy(F.window("tag_time", "1 hour").alias("w")).count()                  \
    .select(F.col("w.start").alias("start"), "count").orderBy("start").show()
    
    

# %% [12-9] 하차 태그를 5분 창으로 세는데, 한 번은 tag_time(댄 시각), 한 번은 recv_time(도착 시각) 기준으로 세어 07:30~07:35 창을
# 비교하십시오.
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
late = outs.filter(F.unix_timestamp("recv_time") - F.unix_timestamp("tag_time") > 600)
late.groupBy(F.window("tag_time", "1 hour").alias("w")).count()     \
    .select(F.col("w.start").alias("start"), "count")               \
    .orderBy("start").show()



# %%
