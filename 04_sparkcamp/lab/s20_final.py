# ==============================================================================
#  마무리  마무리 — 혼자 푸는 열 문제
#  C:\sparkcamp\lab\s20_final.py
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

def to_trips(ev):
    """이벤트(승차·하차 줄)를 이동 한 건 = 한 줄로 접는다"""
    is_in, is_out = F.col("tag_type") == "IN", F.col("tag_type") == "OUT"
    return ev.groupBy("trip_id").agg(
        F.first("card_id").alias("card_id"),
        F.max(F.when(is_in,  F.col("station_id"))).alias("board_st"),
        F.max(F.when(is_in,  F.col("tag_time"))).alias("board_time"),
        F.max(F.when(is_out, F.col("station_id"))).alias("alight_st"),
        F.max(F.when(is_out, F.col("tag_time"))).alias("alight_time"),
        F.max("fare").alias("fare"))

trips = to_trips(clean)
print("trips:", trips.count())


# %% [H-1] 하차 인원이 가장 많은 역 세 곳을 역 이름으로 보이십시오. (clean 기준)




# %% [H-2] 07시대(07:00:00 이상 08:00:00 미만)에 탄 사람(승차)은 몇 명입니까?




# %% [H-3] 노선별 승차 인원을 많은 순으로 보이십시오.




# %% [H-4] 이동 시간이 30분을 넘는 이동은 몇 건입니까? (승차·하차 시각이 둘 다 있는 이동만)




# %% [H-5] 15분 Tumbling 창으로 승차를 세어, 가장 붐빈 창 하나를 보이십시오.




# %% [H-6] 하루에 두 번 이상 탄(승차 2회 이상) 카드는 몇 장입니까?




# %% [H-7] 원본(raw)에서 격리된 이벤트의 이유별 개수를 구하고, 원본 = 중복 + 격리 + 정상 이 맞는지 한 줄로 찍으십시오.




# %% [H-8] 탄 역 → 내린 역 짝 중 가장 많은 세 가지를 역 이름으로 보이십시오.




# %% [H-9] 내린 역의 노선 기준으로 평균 요금이 가장 높은 노선 하나를 보이십시오 (소수 첫째 자리).




# %% [H-10] H-8 의 top_routes 를 Parquet 으로 out/final/top_routes 에 덮어쓰기로 저장하고, 다시 읽어 몇 줄인지 세십시오.
