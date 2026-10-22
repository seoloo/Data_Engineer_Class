# ==============================================================================
#  STEP 13  정리하기 — 중복 · 격리 · 승하차 짝 맞추기
#  C:\sparkcamp\lab\s13_clean.py
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
print(raw.count(), st.count())


# %% [13-1] 원본(raw)의 줄 수와 이벤트 번호 가짓수를 다시 확인하십시오.
print(raw.count(), raw.select("event_id").count())
print(raw.count(), raw.select("event_id").count(), "개의 차이")



# %% [13-2] event_id 기준으로 중복을 걷어 낸 dedup 을 만들고 세십시오.
dedup = raw.dropDuplicates(["event_id"])
dedup.count()



# %% [13-2 바꿔 보기 ①] 칸 목록 없이 dropDuplicates() 로 하면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
print(raw.dropDuplicates().count())



# %% [13-2 바꿔 보기 ②] 키를 잘못 고르면? — card_id 로 중복을 걷어 내기
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
print(raw.dropDuplicates(["card_id"]).count())




# %% [13-3] 격리 조건을 하나의 변수로 만드십시오 — 역 코드가 비어 있거나, 등록 안 된 역이거나, 태그 시각이 비어 있으면 격리.
valid_ids = [r.station_id for r in st.select("station_id").collect()]
print(valid_ids)



# %% [13-4] 격리 바구니 quarantine 을 만들고, 이유(reason) 칸을 붙여 이유별로 세십시오.




# %% [13-5] 정상 바구니 clean 을 만들고 세십시오.




# %% [13-6] 한 줄도 잃어버리지 않았는지 더해서 확인하십시오.




# %% [13-7] 짝 맞추기 — 정상 이벤트에서 승차 표와 하차 표를 따로 만들고, trip_id 로 붙여 trips 를 만드십시오.




# %% [13-8] 이동이 몇 건인지, 그중 아직 하차 기록이 없는 이동이 몇 건인지 세십시오.




# %% [13-9] 이동 시간(분)을 travel_min 칸으로 만들고, 평균 · 최소 · 최대를 구하십시오.




# %% [13-10] 하차는 있는데 승차가 없는 이동이 있는지 찾으십시오. (승차 태그가 S99 로 격리된 경우)




# %% [13-11] 같은 질문, 다른 답 — 역별 승차 인원을 원본(raw)과 정상(clean)에서 각각 구해 나란히 보십시오.




# %% [13-12] 정상 이벤트 기준으로 오늘의 기준 숫자 네 개를 구하십시오 — 승차 수 · 하차 수 · 요금 합계 · 이동 수.




# %% [13-13] NULL 함정 확인 — 역 코드가 비어 있는 가짜 이벤트 한 줄을 만들어, isNull() 이 없는 규칙과 있는 규칙으로 각각 정상·격리를 세어 보십시오.
