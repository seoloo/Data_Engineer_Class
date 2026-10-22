# ==============================================================================
#  STEP 07  스키마 직접 정하기 — 짐작하지 말고 설계도를 준다
#  C:\sparkcamp\lab\s07_schema.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys
os.chdir(r"C:\sparkcamp")
os.environ["PYSPARK_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder\
         .master("local[2]")
         .appName("sparkcamp")
         .config("spark.local.dir", r"C:\sparkcamp\tmp")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "준비 완료")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# %% [07-1] 하루 로그를 자동 추론으로 읽고 스키마를 보십시오.
auto = spark.read.json("data/tags_day.jsonl")
auto.printSchema()



# %% [07-2] StructType 으로 스키마를 한 칸씩 정하십시오. 칸 순서는 파일과 같게.
schema = StructType([
    StructField("event_id", StringType()),
    StructField("card_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("tag_type", StringType()),
    StructField("station_id", StringType()),
    StructField("tag_time", TimestampType ()),
    StructField("fare", IntegerType()),
    StructField("recv_time", TimestampType()),
])

print(len(schema.fields),"칸")





# %% [07-3] 그 스키마로 하루 로그를 읽고 스키마를 확인하십시오.

day = spark.read.schema(schema).json("data/tags_day.jsonl")
day.printSchema()





# %% [07-4] 같은 스키마를 글자 한 줄(DDL)로 쓰고, 그것으로 다시 읽어 앞 세 줄을 보십시오.

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
day.show(3)
day.printSchema()


# %% [07-5] 줄 수를 세십시오. 그리고 tag_time 이 비어 있는(null) 줄이 몇 개인지 세십시오.
print(day.count())
day.filter(F.col("tag_time").isNull()).count()

day.where("tag_time IS NULL").count()




# %% [07-5 바꿔 보기 ①] tag_time 을 TIMESTAMP 가 아니라 STRING 으로 읽으면?(replace)
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
as_text = spark.read.schema(SCHEMA.replace("tag_time TIMESTAMP",
                            "tag_time STRING")).json("data/tags_day.jsonl")

print(as_text.filter(F.col("tag_time").isNull()).count())
as_text.filter(F.col("tag_time").contains(":67:"))\
            .select("event_id", "tag_time").show()




# %% [07-6] 그 다섯 줄을 보십시오. 원래 파일에는 뭐라고 적혀 있었는지 자동 추론본(auto)에서 같은 이벤트를 찾아 비교하십시오.
bad_ids = [r.event_id for r in day.filter(F.col("tag_time").isNull()).select("event_id").collect()]
print(bad_ids)
auto.filter(F.col("event_id").isin(bad_ids)).select("event_id", "tag_time").show()



# %% [07-7] 깨진 줄을 원문 그대로 따로 받아 두는 칸(_corrupt_record)을 스키마 끝에 붙여 읽고, 그 칸이 채워진 줄을 보십시오.
day2 = spark.read.schema(SCHEMA + " , _corrupt_record STRING").json("data/tags_day.jsonl")
day2.filter(F.col("_corrupt_record").isNotNull())\
    .select("event_id", "tag_time", "_corrupt_record").show(truncate=200)



# %% [07-8] 읽는 방법(mode)을 바꿔 보십시오. DROPMALFORMED(깨진 줄은 버린다)로 읽어 실제로 몇 줄이 남는지 세십시오.
drop = spark.read.schema(SCHEMA).option("mode","DROPMALFORMED").json("data/tags_day.jsonl")
drop.count()




# %% [07-9] 일부러 틀려 보십시오. mode 를 FAILFAST(깨진 줄이 있으면 멈춘다)로 하고 전부 꺼내 보십시오.
fail = spark.read.schema(SCHEMA).option("mode","FAILTAST").json("data/tags_day.jsonl")
len(fail.collect())



# %% [07-10] 일부러 틀려 보십시오. 스키마의 칸 이름을 tagtime 으로 잘못 쓰고 읽어서, tagtime 이 null 인 줄을 세십시오.
oops = spark.read.schema("event_id STRING, tagtime TIMESTAMP").json("data/tags_day.jsonl")
oops.filter(F.col("tagtime").isNull()).count()
oops.select("tagtime").show(10)


# %% [07-11] 제대로 읽은 day 에서 칸마다 null 이 몇 개인지 한 번에 세십시오.
day.select([F.count(F.when(F.col(c).isNull(),1)).alias(c) for c in day.columns]).show()
# %%
