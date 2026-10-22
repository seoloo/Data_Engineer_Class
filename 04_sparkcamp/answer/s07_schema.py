# ==============================================================================
#  STEP 07  스키마 직접 정하기 — 짐작하지 말고 설계도를 준다
#  C:\sparkcamp\answer\s07_schema.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# ── 결과 ──
# Spark 4.1.3 준비 완료


# %% [07-1] 하루 로그를 자동 추론으로 읽고 스키마를 보십시오.
auto = spark.read.json("data/tags_day.jsonl")
auto.printSchema()
# ── 결과 ──
# root
#  |-- card_id: string (nullable = true)
#  |-- event_id: string (nullable = true)
#  |-- fare: long (nullable = true)
#  |-- recv_time: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- trip_id: string (nullable = true)
# → 칸이 알파벳 순, 시각 두 칸이 string, 요금이 long. 짐작한 결과입니다.

# %% [07-2] StructType 으로 스키마를 한 칸씩 정하십시오. 칸 순서는 파일과 같게.
schema = StructType([
    StructField("event_id",   StringType()),
    StructField("card_id",    StringType()),
    StructField("trip_id",    StringType()),
    StructField("tag_type",   StringType()),
    StructField("station_id", StringType()),
    StructField("tag_time",   TimestampType()),
    StructField("fare",       IntegerType()),
    StructField("recv_time",  TimestampType()),
])
print(len(schema.fields), "칸")
# ── 결과 ──
# 8 칸
# → 길지만 한 줄 = 한 칸이라 읽기 쉽습니다. 각 줄 끝 쉼표를 빼먹지 마십시오.

# %% [07-3] 그 스키마로 하루 로그를 읽고 스키마를 확인하십시오.
day = spark.read.schema(schema).json("data/tags_day.jsonl")
day.printSchema()
# ── 결과 ──
# root
#  |-- event_id: string (nullable = true)
#  |-- card_id: string (nullable = true)
#  |-- trip_id: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: timestamp (nullable = true)
#  |-- fare: integer (nullable = true)
#  |-- recv_time: timestamp (nullable = true)
# → 칸 순서가 내가 쓴 대로, tag_time 이 timestamp, fare 가 integer 입니다.

# %% [07-4] 같은 스키마를 글자 한 줄(DDL)로 쓰고, 그것으로 다시 읽어 앞 세 줄을 보십시오.
SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
day.show(3)
# ── 결과 ──
# +--------+-------+-------+--------+----------+-------------------+----+-------------------+
# |event_id|card_id|trip_id|tag_type|station_id|           tag_time|fare|          recv_time|
# +--------+-------+-------+--------+----------+-------------------+----+-------------------+
# |  E00001|  C0401|  T0323|      IN|       S03|2026-10-14 07:00:00|   0|2026-10-14 07:00:01|
# |  E00002|  C0066|  T0727|      IN|       S01|2026-10-14 07:00:01|   0|2026-10-14 07:00:03|
# |  E00003|  C0425|  T0943|      IN|       S01|2026-10-14 07:00:02|   0|2026-10-14 07:00:04|
# +--------+-------+-------+--------+----------+-------------------+----+-------------------+
# only showing top 3 rows
# → 결과는 07-3 과 같습니다. 이 SCHEMA 한 줄을 다음 STEP 부터 계속 씁니다(준비 셀에 들어 있습니다).

# %% [07-5] 줄 수를 세십시오. 그리고 tag_time 이 비어 있는(null) 줄이 몇 개인지 세십시오.
print(day.count())
day.filter(F.col("tag_time").isNull()).count()
# ── 결과 ──
# 2008
# 5
# → 2008 줄 중 5 줄의 시각이 비었습니다. 원래 파일에는 분명히 무언가 적혀 있었습니다.

# %% [07-5 바꿔 보기 ①] tag_time 을 TIMESTAMP 가 아니라 STRING 으로 읽으면?
as_text = spark.read.schema(SCHEMA.replace("tag_time TIMESTAMP", "tag_time STRING")).json("data/tags_day.jsonl")
print(as_text.filter(F.col("tag_time").isNull()).count())
as_text.filter(F.col("tag_time").contains(":67:")).select("event_id", "tag_time").show()
# ── 결과 ──
# 0
# +--------+-------------------+
# |event_id|           tag_time|
# +--------+-------------------+
# |  E00192|2026-10-14 07:67:00|
# +--------+-------------------+
# → 이래서 — null 이 0 이고, 깨진 값 07:67:00 이 원문 그대로 보입니다. null 은 데이터가 비어서가 아니라 "시각으로 읽으라" 는 내
# 설계도 때문에 생긴 것입니다. 원문을 남겨야 하는 Bronze 는 이렇게 글자로 읽어 두고, 시각 바꾸기는 다음 단계에서 합니다(STEP 16 · 19).

# %% [07-6] 그 다섯 줄을 보십시오. 원래 파일에는 뭐라고 적혀 있었는지 자동 추론본(auto)에서 같은 이벤트를 찾아 비교하십시오.
bad_ids = [r.event_id for r in day.filter(F.col("tag_time").isNull()).select("event_id").collect()]
print(bad_ids)
auto.filter(F.col("event_id").isin(bad_ids)).select("event_id", "tag_time").show()
# ── 결과 ──
# ['E00192', 'E01271', 'E01388', 'E01388', 'E01716']
# +--------+-------------------+
# |event_id|           tag_time|
# +--------+-------------------+
# |  E00192|2026-10-14 07:67:00|
# |  E01271|2026-10-14 08:75:00|
# |  E01388|2026-10-14 09:63:00|
# |  E01388|2026-10-14 09:63:00|
# |  E01716|2026-10-14 09:63:00|
# +--------+-------------------+
# → 분 자리가 67 · 75 처럼 60 을 넘는 — 있을 수 없는 시각입니다. 스키마가 "이건 시각이 아니다" 라고 판단해 조용히 null 로 바꿨습니다.
# 오류는 안 났습니다. 그래서 반드시 null 을 세어 봐야 합니다. (E01388 이 두 번 나오는 건 재전송 중복입니다)

# %% [07-7] 깨진 줄을 원문 그대로 따로 받아 두는 칸(_corrupt_record)을 스키마 끝에 붙여 읽고, 그 칸이 채워진 줄을 보십시오.
day2 = spark.read.schema(SCHEMA + ", _corrupt_record STRING").json("data/tags_day.jsonl")
day2.filter(F.col("_corrupt_record").isNotNull()) \
    .select("event_id", "tag_time", "_corrupt_record").show(truncate=60)
# ── 결과 ──
# +--------+--------+------------------------------------------------------------+
# |event_id|tag_time|                                             _corrupt_record|
# +--------+--------+------------------------------------------------------------+
# |  E00192|    NULL|{"event_id": "E00192", "card_id": "C0448", "trip_id": "T0...|
# |  E01271|    NULL|{"event_id": "E01271", "card_id": "C0152", "trip_id": "T0...|
# |  E01388|    NULL|{"event_id": "E01388", "card_id": "C0660", "trip_id": "T0...|
# |  E01388|    NULL|{"event_id": "E01388", "card_id": "C0660", "trip_id": "T0...|
# |  E01716|    NULL|{"event_id": "E01716", "card_id": "C0250", "trip_id": "T0...|
# +--------+--------+------------------------------------------------------------+
# → 문제가 된 줄의 원문이 통째로 남습니다. 나중에 "왜 빠졌는지" 를 설명할 증거입니다. 버리지 말고 따로 둔다 — STEP 13 의 격리와 같은
# 생각입니다.

# %% [07-8] 읽는 방법(mode)을 바꿔 보십시오. DROPMALFORMED(깨진 줄은 버린다)로 읽어 실제로 몇 줄이 남는지 세십시오.
drop = spark.read.schema(SCHEMA).option("mode", "DROPMALFORMED").json("data/tags_day.jsonl")
len(drop.collect())
# ── 결과 ──
# 2003
# → 2003 — 깨진 5줄이 흔적 없이 사라졌습니다. 편해 보이지만 무엇이 빠졌는지 모르게 됩니다. 이 특강은 기본(PERMISSIVE: null 로 두기)을
# 씁니다. (count() 대신 collect 로 센 이유는 아래 주황 박스)

# %% [07-9] 일부러 틀려 보십시오. mode 를 FAILFAST(깨진 줄이 있으면 멈춘다)로 하고 전부 꺼내 보십시오.
fail = spark.read.schema(SCHEMA).option("mode", "FAILFAST").json("data/tags_day.jsonl")
fail.collect()
# ── 결과 (오류 — 일부러 틀린 것) ──
# Py4JJavaError: : org.apache.spark.SparkException: [FAILED_READ_FILE.NO_HINT] Encountered error while reading file file:///C:/sparkcamp/data/tags_day.jsonl.  SQLSTATE: KD001
# Caused by: org.apache.spark.SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [E00192,C0448,T0987,IN,S02,null,0,1791930635000000].
# Caused by: org.apache.spark.SparkDateTimeException: [CAST_INVALID_INPUT] The value '2026-10-14 07:67:00' of the type "STRING" cannot be cast to "TIMESTAMP" because it is malformed. Correct the value as per the syntax, or change it
# → 깨진 줄을 만나는 순간 멈춥니다. 첫 줄(FAILED_READ_FILE)만 보면 원인을 모릅니다. 아래로 내려가 Caused by: 로 시작하는 줄을
# 찾으십시오 — MALFORMED_RECORD_IN_PARSING(깨진 기록) 과 CAST_INVALID_INPUT ... 07:67:00(그 값) 이 진짜
# 원인입니다. "한 줄이라도 이상하면 받지 않겠다" 가 필요한 곳(정산·회계)에서 씁니다.

# %% [07-10] 일부러 틀려 보십시오. 스키마의 칸 이름을 tagtime 으로 잘못 쓰고 읽어서, tagtime 이 null 인 줄을 세십시오.
oops = spark.read.schema("event_id STRING, tagtime TIMESTAMP").json("data/tags_day.jsonl")
oops.filter(F.col("tagtime").isNull()).count()
# ── 결과 ──
# 2008
# → 2008 — 전부 null 입니다. 오류는 없습니다. 파일에 tagtime 이라는 칸이 없으니 "없음" 으로 채운 것입니다. 스키마 이름 오타는 가장
# 조용하고 가장 무서운 실수입니다. 읽은 뒤 null 개수를 꼭 세는 이유입니다.

# %% [07-11] 제대로 읽은 day 에서 칸마다 null 이 몇 개인지 한 번에 세십시오.
day.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in day.columns]).show()
# ── 결과 ──
# +--------+-------+-------+--------+----------+--------+----+---------+
# |event_id|card_id|trip_id|tag_type|station_id|tag_time|fare|recv_time|
# +--------+-------+-------+--------+----------+--------+----+---------+
# |       0|      0|      0|       0|         0|       5|   0|        0|
# +--------+-------+-------+--------+----------+--------+----+---------+
# → 한 줄로 모든 칸의 빈칸 수를 봅니다. tag_time 만 5 입니다. 새 데이터를 받으면 제일 먼저 하는 점검입니다. 이 줄은 외우지 말고 복사해 두고
# 쓰는 도구로 주십시오.
