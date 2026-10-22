# ==============================================================================
#  STEP 10  이어 붙이기 — 역 마스터와 join
#  C:\sparkcamp\answer\s10_join.py
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

SCHEMA = """
event_id STRING, card_id STRING, trip_id STRING, tag_type STRING,
station_id STRING, tag_time TIMESTAMP, fare INT, recv_time TIMESTAMP
"""
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
print(day.count(), st.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 8


# %% [10-1] 로그에 역 이름을 붙여 다섯 줄 보십시오. (기본 join)
day.join(st, "station_id") \
   .select("event_id", "tag_type", "station_id", "station_nm", "line", "tag_time").show(5)
# ── 결과 ──
# +--------+--------+----------+----------+-----+-------------------+
# |event_id|tag_type|station_id|station_nm| line|           tag_time|
# +--------+--------+----------+----------+-----+-------------------+
# |  E00001|      IN|       S03|  홍대입구|2호선|2026-10-14 07:00:00|
# |  E00002|      IN|       S01|      강남|2호선|2026-10-14 07:00:01|
# |  E00003|      IN|       S01|      강남|2호선|2026-10-14 07:00:02|
# |  E00004|      IN|       S07|    여의도|5호선|2026-10-14 07:00:07|
# |  E00005|      IN|       S02|      역삼|2호선|2026-10-14 07:00:20|
# +--------+--------+----------+----------+-----+-------------------+
# only showing top 5 rows
# → 오른쪽에 역 이름과 노선이 붙었습니다. 붙인 기준 칸(station_id)이 맨 앞으로 옵니다.

# %% [10-2] 기본(inner) join 결과가 몇 줄인지 세십시오.
day.join(st, "station_id").count()
# ── 결과 ──
# 2002
# → 2002. 원래 2,008줄에서 6줄이 사라졌습니다. 오류는 없습니다.

# %% [10-2 바꿔 보기 ①] 붙이지는 말고 "짝이 있는 줄만" 남기면? — left_semi
semi = day.join(st, "station_id", "left_semi")
print(semi.count(), len(semi.columns))
# ── 결과 ──
# 2002 8
# → 이래서 — 2002 줄, 칸 8 개 — inner 와 줄 수는 같은데 역 이름 칸이 안 붙었습니다. "등록된 역의 기록만 거르기" 처럼 걸러내기만 할 때
# 씁니다(10-5 의 left_anti 와 반대).

# %% [10-2 바꿔 보기 ②] 마스터에서 광화문(S08)이 빠져 있다면? — inner join 줄 수
st_missing = st.filter(F.col("station_id") != "S08")
print(day.join(st_missing, "station_id").count())
# ── 결과 ──
# 1736
# → 이래서 — 2002 → 1736. 마스터가 한 줄 모자라면 그 역의 기록 266 줄이 오류 없이 통째로 사라집니다. 그래서 처음 붙일 때는 left 로
# 붙여 null 을 세어 봅니다(10-4).

# %% [10-3] left join 으로 붙이면 몇 줄인지 세십시오.
day.join(st, "station_id", "left").count()
# ── 결과 ──
# 2008
# → 2008 — 하나도 빠지지 않습니다.

# %% [10-4] left join 결과에서 역 이름이 비어 있는 줄을 보십시오.
day.join(st, "station_id", "left") \
   .filter(F.col("station_nm").isNull()) \
   .select("event_id", "trip_id", "tag_type", "station_id", "station_nm").show()
# ── 결과 ──
# +--------+-------+--------+----------+----------+
# |event_id|trip_id|tag_type|station_id|station_nm|
# +--------+-------+--------+----------+----------+
# |  E00182|  T0411|      IN|       S99|      NULL|
# |  E00472|  T0192|      IN|       S99|      NULL|
# |  E00475|  T0107|      IN|       S99|      NULL|
# |  E00751|  T0519|      IN|       S99|      NULL|
# |  E01428|  T0049|      IN|       S99|      NULL|
# |  E01809|  T0701|      IN|       S99|      NULL|
# +--------+-------+--------+----------+----------+
# → 사라졌던 6줄 = S99. STEP 06 에서 찾았던 그 6줄입니다. left join → 이름이 null 인 줄 이 "짝 없는 줄 찾기" 의 기본
# 모양입니다.

# %% [10-5] 같은 6줄을 left_anti 한 번으로 찾으십시오.
unknown = day.join(st, "station_id", "left_anti")
unknown.count()
# ── 결과 ──
# 6
# → left_anti = "왼쪽 중 짝이 없는 것만". 오른쪽 칸은 붙지 않습니다. 격리 대상을 찾을 때 가장 짧은 방법입니다.

# %% [10-6] 역 이름으로 승차 인원을 세어 많은 순으로 보십시오.
(day.filter(F.col("tag_type") == "IN")
    .join(st, "station_id")
    .groupBy("station_nm").count()
    .orderBy(F.col("count").desc())
    .show())
# ── 결과 ──
# +----------+-----+
# |station_nm|count|
# +----------+-----+
# |    광화문|  147|
# |      역삼|  130|
# |    서울역|  129|
# |      시청|  122|
# |      강남|  122|
# |      잠실|  120|
# |  홍대입구|  120|
# |    여의도|  116|
# +----------+-----+
# → STEP 08-2 와 같은 숫자가 이름으로 나옵니다. (inner 라서 S99 는 빠졌습니다)

# %% [10-7] 노선별 하차 인원과 요금 합계를 구하십시오.
(day.filter(F.col("tag_type") == "OUT")
    .join(st, "station_id")
    .groupBy("line")
    .agg(F.count("*").alias("out_cnt"), F.sum("fare").alias("fare_sum"))
    .orderBy("line")
    .show())
# ── 결과 ──
# +-----+-------+--------+
# | line|out_cnt|fare_sum|
# +-----+-------+--------+
# |1호선|    259|  422950|
# |2호선|    485|  813750|
# |5호선|    252|  428100|
# +-----+-------+--------+

# %% [10-8] 역 마스터를 broadcast 로 감싸 붙이고, 실행 계획에서 어떤 join 을 썼는지 보십시오.
joined = day.join(F.broadcast(st), "station_id")
joined.explain()
# ── 결과 ──
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Project [station_id#4, event_id#0, card_id#1, trip_id#2, tag_type#3, tag_time#5, fare#6, recv_time#7, station_nm#26, line#27, gu#28]
#    +- BroadcastHashJoin [station_id#4], [station_id#25], Inner, BuildRight, false
#       :- Filter isnotnull(station_id#4)
#       :  +- FileScan json [event_id#0,card_id#1,trip_id#2,tag_type#3,station_id#4,tag_time#5,fare#6,recv_time#7] Batched: false, DataFilters: [isnotnull(station_id#4)], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/C:/sparkcamp/data/tags_day.jsonl], PartitionFilters: [], PushedFilters: [IsNotNull(station_id)], ReadSchema: struct<event_id:string,card_id:string,trip_id:string,tag_type:string,station_id:string,tag_time:t...
#       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=1165]
#          +- Filter isnotnull(station_id#25)
#             +- FileScan csv [station_id#25,station_nm#26,line#27,gu#28] Batched: false, DataFilters: [isnotnull(station_id#25)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/C:/sparkcamp/data/stations.csv], PartitionFilters: [], PushedFilters: [IsNotNull(station_id)], ReadSchema: struct<station_id:string,station_nm:string,line:string,gu:string>
# → 계획 안에서 BroadcastHashJoin 을 찾으십시오. 작은 표를 일꾼 모두에게 복사해 붙였다는 뜻입니다. explain 은 "어떻게 하겠다" 는
# 작업 계획서입니다. 지금은 단어 하나만 찾으면 됩니다.

# %% [10-9] 일부러 틀려 보십시오. 조건식으로 붙인 다음 station_id 를 골라 보십시오.
day.join(st, day.station_id == st.station_id).select("station_id").count()
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [AMBIGUOUS_REFERENCE] Reference `station_id` is ambiguous, could be: [`station_id`, `station_id`]. SQLSTATE: 42704
# → AMBIGUOUS_REFERENCE — station_id 가 두 개라 어느 쪽인지 모른다. 양쪽 칸 이름이 같으면 이름으로(join(st,
# "station_id")) 붙이십시오.

# %% [10-10] 구(gu)별 승차 인원을 구하고, 승차가 가장 많은 구를 한 줄만 보십시오.
(day.filter(F.col("tag_type") == "IN")
    .join(st, "station_id")
    .groupBy("gu").count()
    .orderBy(F.col("count").desc())
    .show(1))
# ── 결과 ──
# +------+-----+
# |    gu|count|
# +------+-----+
# |강남구|  252|
# +------+-----+
# only showing top 1 row
# → 강남구에는 역이 두 곳(강남·역삼)이라 합쳐서 1등입니다. 묶는 기준을 바꾸면 순위가 바뀝니다 — 역별 1등(광화문)과 구별 1등이 다릅니다.
