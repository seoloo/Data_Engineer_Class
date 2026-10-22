# ==============================================================================
#  STEP 05  파일 읽기 — CSV 와 JSON
#  C:\sparkcamp\lab\s05_read_files.py
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


# %% [05-1] 역 마스터를 옵션 없이 읽어 보여 주십시오.
st = spark.read.csv("data/stations.csv")
st.show()


# %% [05-1 바꿔 보기 ①] header 를 줄 때와 안 줄 때 줄 수를 나란히 세면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
print(spark.read.csv("data/stations.csv").count(),
      spark.read.csv("data/stations.csv", header=True).count())



# %% [05-2] 같은 파일을 머리글을 칸 이름으로 읽으십시오.
st = spark.read.csv("data/stations.csv", header=True)
st.show()



# %% [05-3] 역 마스터의 스키마와 행 수를 보십시오.
st.printSchema()
st.count()



# %% [05-4] 작은 로그 tags_mini.jsonl 을 읽어 칸을 자르지 말고 보여 주십시오.
mini = spark.read.json("data/tags_mini.jsonl")
mini.show()





# %% [05-5] 작은 로그의 스키마를 보십시오.

mini.printSchema()


# %% [05-6] 작은 로그가 몇 줄인지, 이벤트 번호는 몇 가지인지 세십시오.
print(mini.count())
print(mini.select("event_id").distinct().count())



# %% [05-7] 하루 로그 tags_day.jsonl 을 day 라는 이름으로 읽고 몇 줄인지 세십시오.
day = spark.read.json("data/tags_day.jsonl")
day.count()



# %% [05-8] 하루 로그의 앞 다섯 줄을 보십시오.
day.show(5)



# %% [05-9] 하루 로그에서 event_id · trip_id · tag_type · station_id · tag_time 다섯 칸만 다섯 줄 보십시오.
day.select("event_id", "trip_id", "tag_type", "station_id", "tag_time").show(5)



# %% [05-10] 일부러 틀려 보십시오. 파일 이름을 tags_dya.jsonl 로 잘못 쳐서 읽어 보십시오.




# %% [05-11] 폴더째 읽기 — data/stream_src 폴더(파일 10개)를 한 번에 읽어 세십시오.
whole = spark.read.json("data/stream_src")
whole.count()



# %% [05-12] 폴더 안에 파일이 몇 개인지 파이썬으로 세어 보십시오.
files = sorted(os.listdir("data/stream_src"))
print(len(files), files[:3])

# %%
