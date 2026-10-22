# ==============================================================================
#  STEP 14  파티션과 셔플 — 계기판으로 보는 Spark 의 속사정
#  C:\sparkcamp\answer\s14_partitions.py
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
raw = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
valid_ids = [r.station_id for r in st.select("station_id").collect()]
is_bad = (F.col("station_id").isNull() | ~F.col("station_id").isin(valid_ids)
          | F.col("tag_time").isNull())
dedup = raw.dropDuplicates(["event_id"])
clean = dedup.filter(~is_bad)
print(raw.count(), clean.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008 1978


# %% [14-1] 원본(raw)이 몇 조각인지 보십시오.
raw.rdd.getNumPartitions()
# ── 결과 ──
# 1
# → 1. 파일 하나가 작아서(약 400KB) 한 조각에 다 들어갔습니다. 큰 파일은 기본 128MB 마다 한 조각입니다.

# %% [14-2] raw 를 4 조각으로 고르게 다시 나누고, 조각마다 몇 줄씩인지 보십시오.
r4 = raw.repartition(4)
print(r4.rdd.getNumPartitions())
print(r4.rdd.glom().map(len).collect())
# ── 결과 ──
# 4
# [502, 502, 502, 502]
# → 502 × 4 = 2008. 고르게 나뉘었습니다. glom 은 "조각별로 묶어 보기" — 확인용으로만 씁니다.

# %% [14-3] 4 조각을 coalesce 로 1 조각으로 합치십시오.
r1 = r4.coalesce(1)
print(r1.rdd.getNumPartitions())
# ── 결과 ──
# 1
# → coalesce 는 줄이기만 하고 셔플을 안 합니다. 결과 파일을 하나로 만들고 싶을 때 씁니다(STEP 15). 늘릴 때는 repartition.

# %% [14-4] 역 코드 기준으로 4 조각으로 나누고, 조각마다 몇 줄인지 보십시오.
rk = raw.repartition(4, "station_id")
print(rk.rdd.glom().map(len).collect())
# ── 결과 ──
# [742, 1022, 0, 244]
# → 고르지 않습니다. 0 줄짜리 조각도 있습니다. 역 9개를 4 조각에 나누면 어느 조각은 역이 많고 어느 조각은 없습니다. 이것이 쏠림(skew) 입니다.
# 가장 큰 조각을 맡은 일꾼이 끝나야 전체가 끝납니다.

# %% [14-4 바꿔 보기 ①] 값이 두 가지뿐인 칸(tag_type)으로 4 조각을 만들면?
print(raw.repartition(4, "tag_type").rdd.glom().map(len).collect())
# ── 결과 ──
# [0, 1012, 996, 0]
# → 이래서 — 4 조각 중 많아야 두 조각에만 데이터가 들어갑니다. 같은 값은 반드시 같은 조각으로 가니까, 값의 가짓수보다 조각을 많이 만들 수 없습니다.
# 다음 과목에서 Kafka 의 파티션 키를 고를 때 똑같은 이야기가 나옵니다 — 프로젝트 1단계(토픽·파티션 설계).

# %% [14-5] 셔플 뒤 조각 수 설정과 AQE 가 켜져 있는지 보십시오.
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.adaptive.enabled"))
# ── 결과 ──
# 200
# true
# → 기본 200 조각. 우리 데이터에는 너무 많지만 AQE(true) 가 실행하면서 알아서 줄여 줍니다.

# %% [14-6] 역별 승차 집계의 실행 계획을 보고, 셔플 표시 Exchange 를 찾으십시오.
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()
# ── 결과 ──
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- HashAggregate(keys=[station_id#154], functions=[count(1)])
#    +- Exchange hashpartitioning(station_id#154, 200), ENSURE_REQUIREMENTS, [plan_id=293]
#       +- HashAggregate(keys=[station_id#154], functions=[partial_count(1)])
#          +- Project [station_id#154]
#             +- Filter (isnotnull(tag_type#152) AND (((isnotnull(station_id#154) AND station_id#154 IN (S01,S02,S03,S04,S05,S06,S07,S08)) AND isnotnull(tag_time#156)) AND (tag_type#152 = IN)))
#                +- SortAggregate(key=[event_id#0], functions=[first(tag_type#3, false), first(station_id#4, false), first(tag_time#5, false)])
#                   +- Sort [event_id#0 ASC NULLS FIRST], false, 0
#                      +- Exchange hashpartitioning(event_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=286]
#                         +- SortAggregate(key=[event_id#0], functions=[partial_first(tag_type#3, false), partial_first(station_id#4, false), partial_first(tag_time#5, false)])
#                            +- Sort [event_id#0 ASC NULLS FIRST], false, 0
#                               +- FileScan json [event_id#0,tag_type#3,station_id#4,tag_time#5] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/C:/sparkcamp/data/tags_day.jsonl], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<event_id:string,tag_type:string,station_id:string,tag_time:timestamp>
# → Exchange 가 두 번 나옵니다. 위쪽 Exchange hashpartitioning(station_id, 200) 은 "역 코드로 200 조각에 다시
# 나눠 모은다" — groupBy 의 셔플. 아래쪽 hashpartitioning(event_id, 200) 은 dropDuplicates 의 셔플입니다(같은
# 이벤트끼리 모아야 중복을 알 수 있으니까). 계획은 아래에서 위로 읽습니다 — 파일 읽기(FileScan) → 중복 제거 → 거르기 → 세기.

# %% [14-7] 셔플 조각 수를 8 로 바꾸고, 계획에서 숫자가 바뀌었는지 보십시오.
spark.conf.set("spark.sql.shuffle.partitions", "8")
agg = clean.filter(F.col("tag_type") == "IN").groupBy("station_id").count()
agg.explain()
# ── 결과 ──
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- HashAggregate(keys=[station_id#192], functions=[count(1)])
#    +- Exchange hashpartitioning(station_id#192, 8), ENSURE_REQUIREMENTS, [plan_id=331]
#       +- HashAggregate(keys=[station_id#192], functions=[partial_count(1)])
#          +- Project [station_id#192]
#             +- Filter (isnotnull(tag_type#190) AND (((isnotnull(station_id#192) AND station_id#192 IN (S01,S02,S03,S04,S05,S06,S07,S08)) AND isnotnull(tag_time#194)) AND (tag_type#190 = IN)))
#                +- SortAggregate(key=[event_id#0], functions=[first(tag_type#3, false), first(station_id#4, false), first(tag_time#5, false)])
#                   +- Sort [event_id#0 ASC NULLS FIRST], false, 0
#                      +- Exchange hashpartitioning(event_id#0, 8), ENSURE_REQUIREMENTS, [plan_id=324]
#                         +- SortAggregate(key=[event_id#0], functions=[partial_first(tag_type#3, false), partial_first(station_id#4, false), partial_first(tag_time#5, false)])
#                            +- Sort [event_id#0 ASC NULLS FIRST], false, 0
#                               +- FileScan json [event_id#0,tag_type#3,station_id#4,tag_time#5] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex(1 paths)[file:/C:/sparkcamp/data/tags_day.jsonl], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<event_id:string,tag_type:string,station_id:string,tag_time:timestamp>
# → Exchange 의 숫자가 8 이 되었습니다. 설정은 이 SparkSession 이 끝날 때까지 유지됩니다.

# %% [14-8] 캐시 — clean 을 메모리에 잡아 두고 두 번 세어 보십시오. 그리고 Spark UI 의 Storage 탭을 보십시오.
clean.cache()
print(clean.count())
print(clean.count())
# ── 결과 ──
# 1978
# 1978
# → 첫 count 때 메모리에 올리고, 두 번째부터는 파일을 다시 읽지 않습니다. Storage 탭에 clean 이 보입니다. 여러 번 쓰는 중간 결과에만
# 씁니다(메모리를 차지하므로). 다 쓰면 clean.unpersist().

# %% [14-9] 숫자로 비교하기 — 셔플 조각 200 과 8 일 때 같은 집계를 열 번씩 돌려 걸린 시간을 재십시오.
import time

def run(n):
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    spark.conf.set("spark.sql.adaptive.enabled", "false")      # 비교를 위해 잠깐 끈다
    t0 = time.time()
    for _ in range(10):
        clean.groupBy("station_id", "tag_type").count().collect()
    return round(time.time() - t0, 2)

print("200 조각:", run(200), "초")
print("  8 조각:", run(8), "초")
spark.conf.set("spark.sql.adaptive.enabled", "true")          # 다시 켠다
# ── 결과 (작성 환경 — 초 숫자는 PC 마다 다름) ──
# 200 조각: 16.29 초
#   8 조각: 2.72 초
# → 초 숫자는 PC 마다 다릅니다. 작은 데이터에서는 200 조각이 빈 조각을 만드느라 오히려 느립니다. 큰 데이터라면 반대가 될 수 있습니다. 정답은 "재
# 보고 정한다" 입니다. 프로젝트 5단계 보고서에는 이런 표가 들어갑니다.

# %% [14-10] 카드별 이용 횟수를 많은 순으로 세 줄 보고, Spark UI 의 Stages 탭에서 방금 한 일의 Shuffle Read · Shuffle Write 칸을
# 찾으십시오.
clean.groupBy("card_id").count().orderBy(F.col("count").desc(), "card_id").show(3)
# ── 결과 ──
# +-------+-----+
# |card_id|count|
# +-------+-----+
# |  C0007|   12|
# |  C0266|   11|
# |  C0091|   10|
# +-------+-----+
# only showing top 3 rows
# → Stages 탭에 단계가 여러 줄 생깁니다. groupBy 와 orderBy 가 각각 셔플을 부르기 때문입니다. 셔플이 있는 단계에만 Shuffle 칸에
# 숫자가 찍힙니다. 그 숫자(KB)가 "옮긴 양" 입니다.

# %% [14-11] 캐시를 풀고, 설정을 기본으로 되돌리십시오.
clean.unpersist()
spark.conf.set("spark.sql.shuffle.partitions", "200")
print(spark.conf.get("spark.sql.shuffle.partitions"))
# ── 결과 ──
# 200
# → 실험한 설정은 꼭 되돌립니다. 다음 STEP 결과가 이 문서와 같게 나오도록.
