# ==============================================================================
#  STEP 15  파일로 남기기 — CSV 와 Parquet, 그리고 폴더 나누기
#  C:\sparkcamp\lab\s15_parquet.py
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
import shutil
def ls(path):
    """폴더 안의 파일 이름을 보여 준다 (점으로 시작하는 검사용 파일은 뺀다)"""
    return [f for f in sorted(os.listdir(path)) if not f.startswith(".")]
def du(path):
    """폴더 전체 크기 (KB)"""
    total = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)
    return round(total / 1024, 1)


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
shutil.rmtree("out/s15", ignore_errors=True)
print("out/s15 비움")


# %% [15-1] 정상 이벤트(clean)를 CSV 로 저장하고 폴더 안을 보십시오. 머리글도 넣으십시오.




# %% [15-2] 저장한 CSV 를 다시 읽어 스키마를 보십시오.




# %% [15-3] 같은 clean 을 Parquet 으로 저장하고 폴더 안을 보십시오.




# %% [15-4] Parquet 을 다시 읽어 스키마와 줄 수를 보십시오.




# %% [15-5] 두 폴더의 크기(KB) 를 비교하십시오. (준비 셀의 du 함수)




# %% [15-6] 일부러 틀려 보십시오. 같은 경로에 Parquet 을 한 번 더 쓰십시오.




# %% [15-7] mode 를 append 로 해서 한 번 더 쓰고, 줄 수를 세십시오.




# %% [15-8] mode 를 overwrite 로 써서 원래대로 되돌리고 세십시오.




# %% [15-9] partitionBy — tag_type 별로 폴더를 나눠 저장하고, 폴더 모양을 보십시오.




# %% [15-9 바꿔 보기 ①] 두 칸으로 나누면? — tag_type 아래에 시(hh) 폴더
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [15-10] 나눠 저장한 것을 읽으면서 하차만 거르고, 실행 계획에서 PartitionFilters 를 찾으십시오.




# %% [15-11] 시(hour) 칸을 만들어 시간별 폴더로 저장하고, 폴더 목록을 보십시오. 파일은 폴더마다 하나씩만 생기게 하십시오.
