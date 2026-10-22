# ==============================================================================
#  STEP 07  스키마 직접 정하기 — 짐작하지 말고 설계도를 준다
#  C:\sparkcamp\lab\s07_schema.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 열 자료형을 직접 정의할 StructType·StructField와 기본 타입들도 가져온다. 준비 셀을 먼저 실행해야 다음 셀에서 이름을 사용할 수 있다.
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
# [셀 설명] 스키마를 지정하지 않고 읽었을 때 Spark가 추론한 열 자료형을 확인한다.
# [동작] 이 auto를 명시적 스키마로 읽은 day와 비교한다. 시각을 나타내는 문자열이 어떤 타입으로 남는지 살펴본다.
auto = spark.read.json("data/tags_day.jsonl")
auto.printSchema()



# %% [07-2] StructType 으로 스키마를 한 칸씩 정하십시오. 칸 순서는 파일과 같게.
# [셀 설명] StructType 안에 StructField를 나열해 열 이름과 자료형을 명시한다.
# [동작] 식별자는 String, 요금은 Integer, 사건·도착 시각은 Timestamp로 정한다. len(schema.fields)는 정의한 열의 개수다.
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
# [셀 설명] 앞에서 만든 schema를 읽기 기준으로 전달해 JSONL을 해석한다.
# [동작] 자료형을 명시하면 자동 추론에 의존하지 않는다. 읽기가 끝난 뒤 printSchema로 의도한 자료형이 적용되었는지 확인한다.

day = spark.read.schema(schema).json("data/tags_day.jsonl")
day.printSchema()





# %% [07-4] 같은 스키마를 글자 한 줄(DDL)로 쓰고, 그것으로 다시 읽어 앞 세 줄을 보십시오.
# [셀 설명] 같은 열 정의를 DDL 형식 문자열로 작성해 간결하게 스키마를 전달한다.
# [동작] '열이름 자료형'을 쉼표로 이어 쓴다. 여기서 DDL 문자열은 읽기 스키마 표현이며 MySQL 테이블을 만드는 실행문은 아니다.

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
day.show(3)
day.printSchema()


# %% [07-5] 줄 수를 세십시오. 그리고 tag_time 이 비어 있는(null) 줄이 몇 개인지 세십시오.
# [셀 설명] 전체 행 수와 시각을 읽지 못해 NULL이 된 행 수를 비교한다.
# [동작] isNull()과 SQL의 IS NULL은 빈 값을 검사한다. 자료형을 정하는 것과 품질 문제를 찾아 처리하는 것은 별도 단계다.
print(day.count())
day.filter(F.col("tag_time").isNull()).count()

day.where("tag_time IS NULL").count()




# %% [07-5 바꿔 보기 ①] tag_time 을 TIMESTAMP 가 아니라 STRING 으로 읽으면?(replace)
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 태그 시각을 Timestamp 대신 String으로 읽어 변환 실패 전의 원문을 살펴본다.
# [동작] 문자열로는 남아 있어도 실제 시각으로 해석할 수 없는 값이 있을 수 있다. contains는 원문에서 특정 패턴을 찾는 보조 검사다.
as_text = spark.read.schema(SCHEMA.replace("tag_time TIMESTAMP",
                            "tag_time STRING")).json("data/tags_day.jsonl")

print(as_text.filter(F.col("tag_time").isNull()).count())
as_text.filter(F.col("tag_time").contains(":67:"))\
            .select("event_id", "tag_time").show()




# %% [07-6] 그 다섯 줄을 보십시오. 원래 파일에는 뭐라고 적혀 있었는지 자동 추론본(auto)에서 같은 이벤트를 찾아 비교하십시오.
# [셀 설명] 시각이 NULL인 사건 번호를 찾아 자동 추론본에서 해당 원문을 대조한다.
# [동작] collect는 결과를 파이썬 쪽으로 가져온다. 여기서는 소수의 문제 ID를 확인하는 용도로 쓰며 큰 데이터 전체를 가져오는 방식과 구분한다.
bad_ids = [r.event_id for r in day.filter(F.col("tag_time").isNull()).select("event_id").collect()]
print(bad_ids)
auto.filter(F.col("event_id").isin(bad_ids)).select("event_id", "tag_time").show()



# %% [07-7] 깨진 줄을 원문 그대로 따로 받아 두는 칸(_corrupt_record)을 스키마 끝에 붙여 읽고, 그 칸이 채워진 줄을 보십시오.
# [셀 설명] 읽기 중 문제가 있는 레코드의 원문을 _corrupt_record 열로 함께 확인한다.
# [동작] 원문과 변환 결과를 함께 남기면 원인을 추적하기 쉽다. 스키마 끝에 문자열 열을 추가한 뒤 NULL이 아닌 원문 기록을 조회한다.
day2 = spark.read.schema(SCHEMA + " , _corrupt_record STRING").json("data/tags_day.jsonl")
day2.filter(F.col("_corrupt_record").isNotNull())\
    .select("event_id", "tag_time", "_corrupt_record").show(truncate=200)



# %% [07-8] 읽는 방법(mode)을 바꿔 보십시오. DROPMALFORMED(깨진 줄은 버린다)로 읽어 실제로 몇 줄이 남는지 세십시오.
# [셀 설명] 읽기 모드를 DROPMALFORMED로 바꾸어 잘못된 레코드를 버리는 방식의 영향을 확인한다.
# [동작] 버린 행은 이 결과에서 사라지므로 원본 보존과 격리 방식과 비교한다. count 결과만으로 전체 열의 파싱 검증을 단정하지 말고 실제 열 조회도 함께 살핀다.
drop = spark.read.schema(SCHEMA).option("mode","DROPMALFORMED").json("data/tags_day.jsonl")
drop.count()




# %% [07-9] 일부러 틀려 보십시오. mode 를 FAILFAST(깨진 줄이 있으면 멈춘다)로 하고 전부 꺼내 보십시오.
# [셀 설명] 잘못된 레코드를 만났을 때 즉시 실패시키는 FAILFAST 읽기 모드를 실습한다.
# [동작] 현재 옵션 문자열은 FAILTAST로 적혀 있어 의도한 FAILFAST가 아니다. 정확한 옵션 이름을 확인한 뒤 실제 데이터를 읽는 collect에서의 동작을 비교한다.
fail = spark.read.schema(SCHEMA).option("mode","FAILTAST").json("data/tags_day.jsonl")
len(fail.collect())



# %% [07-10] 일부러 틀려 보십시오. 스키마의 칸 이름을 tagtime 으로 잘못 쓰고 읽어서, tagtime 이 null 인 줄을 세십시오.
# [셀 설명] JSON 필드 이름과 스키마 열 이름이 다르면 값이 채워지지 않을 수 있음을 확인한다.
# [동작] 원문은 tag_time인데 스키마에 tagtime을 정의했다. 이때 NULL은 원천 값 누락뿐 아니라 이름 불일치 때문일 수도 있다.
oops = spark.read.schema("event_id STRING, tagtime TIMESTAMP").json("data/tags_day.jsonl")
oops.filter(F.col("tagtime").isNull()).count()
oops.select("tagtime").show(10)


# %% [07-11] 제대로 읽은 day 에서 칸마다 null 이 몇 개인지 한 번에 세십시오.
# [셀 설명] 각 열의 NULL 개수를 한 번의 집계 결과로 만든다.
# [동작] when은 NULL인 행에서 1을 만들고 나머지는 NULL이 된다. count는 NULL이 아닌 값을 세므로 열별 빈 값 개수가 나온다.
day.select([F.count(F.when(F.col(c).isNull(),1)).alias(c) for c in day.columns]).show()
# %%
