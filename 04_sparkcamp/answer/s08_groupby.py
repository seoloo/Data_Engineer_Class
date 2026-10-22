# ==============================================================================
#  STEP 08  묶어서 세기 — groupBy 와 agg
#  C:\sparkcamp\answer\s08_groupby.py
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
print(day.count())
# ── 결과 ──
# Spark 4.1.3 준비 완료
# 2008


# %% [08-1] 승차(IN)와 하차(OUT)가 각각 몇 줄인지 세십시오.
day.groupBy("tag_type").count().show()
# ── 결과 ──
# +--------+-----+
# |tag_type|count|
# +--------+-----+
# |     OUT|  996|
# |      IN| 1012|
# +--------+-----+
# → 승차가 1,000 이 넘고 하차가 990 대입니다. 이 숫자는 아직 "줄 수" 입니다 — 중복이 섞여 있습니다(08-10).

# %% [08-2] 역별 승차 인원을 많은 순으로 보십시오.
day.filter(F.col("tag_type") == "IN") \
   .groupBy("station_id").count() \
   .orderBy(F.col("count").desc()).show()
# ── 결과 ──
# +----------+-----+
# |station_id|count|
# +----------+-----+
# |       S08|  147|
# |       S02|  130|
# |       S05|  129|
# |       S01|  122|
# |       S04|  122|
# |       S06|  120|
# |       S03|  120|
# |       S07|  116|
# |       S99|    6|
# +----------+-----+
# → 이 표가 오늘의 기준 숫자입니다. STEP 13 에서 정리한 뒤, 그리고 STEP 19 의 실시간 파이프라인에서 같은 숫자가 나오는지 다시 맞춰 봅니다.
# S99 가 끼어 있는 것도 보입니다.

# %% [08-3] 역별로 태그 수 · 요금 합계 · 카드 가짓수를 한 번에 구하십시오. 역 코드 순으로.
day.groupBy("station_id").agg(
    F.count("*").alias("tags"),
    F.sum("fare").alias("fare_sum"),
    F.countDistinct("card_id").alias("cards")
).orderBy("station_id").show()
# ── 결과 ──
# +----------+----+--------+-----+
# |station_id|tags|fare_sum|cards|
# +----------+----+--------+-----+
# |       S01| 233|  193550|  200|
# |       S02| 233|  173650|  201|
# |       S03| 267|  241850|  220|
# |       S04| 256|  218700|  215|
# |       S05| 254|  204250|  205|
# |       S06| 244|  204700|  200|
# |       S07| 249|  222850|  211|
# |       S08| 266|  205250|  220|
# |       S99|   6|       0|    6|
# +----------+----+--------+-----+
# → agg 안에 쉼표로 여러 계산을 넣습니다. 칸마다 alias 로 이름을 꼭 붙이십시오. 안 붙이면 sum(fare) 같은 이름이 생겨 나중에 부르기
# 불편합니다.

# %% [08-4] 하차 한 번의 평균 요금을 구하십시오 (소수 첫째 자리 반올림).
day.filter(F.col("tag_type") == "OUT") \
   .agg(F.round(F.avg("fare"), 1).alias("avg_fare")).show()
# ── 결과 ──
# +--------+
# |avg_fare|
# +--------+
# |  1671.5|
# +--------+
# → groupBy 없이 agg 만 쓰면 표 전체가 한 묶음입니다.

# %% [08-5] 요금 전체 합계를 숫자 하나로 꺼내 파이썬 변수 total 에 담으십시오.
total = day.agg(F.sum("fare")).first()[0]
print(total)
# ── 결과 ──
# 1664800
# → first() 는 첫 줄(Row), [0] 은 그 줄의 첫 값입니다. Spark 결과를 파이썬 숫자로 꺼내는 기본 모양입니다. (이 합계에는 중복이 섞여
# 있습니다 — 08-11)

# %% [08-6] 승차가 130 명 이상인 역만 보십시오. (묶은 뒤 거르기)
(day.filter(F.col("tag_type") == "IN")
    .groupBy("station_id").agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 130)
    .orderBy(F.col("cnt").desc())
    .show())
# ── 결과 ──
# +----------+---+
# |station_id|cnt|
# +----------+---+
# |       S08|147|
# |       S02|130|
# +----------+---+
# → MySQL 의 HAVING 자리입니다. Spark 는 묶은 뒤에 filter 를 한 번 더 쓰면 됩니다.

# %% [08-7] 피벗 — 역마다 승차와 하차를 옆으로 나란히 보십시오.
day.groupBy("station_id").pivot("tag_type", ["IN", "OUT"]).count() \
   .orderBy("station_id").show()
# ── 결과 ──
# +----------+---+----+
# |station_id| IN| OUT|
# +----------+---+----+
# |       S01|122| 111|
# |       S02|130| 103|
# |       S03|120| 147|
# |       S04|122| 134|
# |       S05|129| 125|
# |       S06|120| 124|
# |       S07|116| 133|
# |       S08|147| 119|
# |       S99|  6|NULL|
# +----------+---+----+
# → tag_type 의 값(IN · OUT)이 칸 이름이 되었습니다. 보고서 모양으로 볼 때 씁니다. S99 의 하차는 없어서 NULL 입니다.

# %% [08-8] 카드 한 장이 하루에 몇 번 탔는지(승차 수) 세고, 많이 탄 카드 다섯 장을 보십시오.
day.filter(F.col("tag_type") == "IN") \
   .groupBy("card_id").count() \
   .orderBy(F.col("count").desc(), "card_id").show(5)
# ── 결과 ──
# +-------+-----+
# |card_id|count|
# +-------+-----+
# |  C0007|    6|
# |  C0266|    6|
# |  C0091|    5|
# |  C0136|    5|
# |  C0160|    5|
# +-------+-----+
# only showing top 5 rows
# → 동점일 때 순서가 섞이지 않게 정렬 기준을 하나 더(card_id) 줍니다.

# %% [08-9] 일부러 틀려 보십시오. 역별로 묶은 다음 바로 card_id 칸을 골라 보십시오.
day.groupBy("station_id").select("card_id")
# ── 결과 (오류 — 일부러 틀린 것) ──
# AttributeError: 'GroupedData' object has no attribute 'select'
# → groupBy 만 한 상태는 아직 표가 아닙니다(GroupedData — 묶기만 하고 무엇을 셀지 정하지 않은 상태). 반드시 count · agg 로
# 마무리해야 표가 됩니다.

# %% [08-10] 하루 로그의 줄 수와 이벤트 번호의 가짓수를 나란히 세십시오.
day.agg(F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("events")).show()
# ── 결과 ──
# +----+------+
# |rows|events|
# +----+------+
# |2008|  1988|
# +----+------+
# → 2008 과 1988. 같은 이벤트가 두 번 들어온 줄(재전송)이 20개 있습니다. 지금까지 센 숫자에는 이 20줄이 섞여 있었습니다.

# %% [08-10 바꿔 보기 ①] 승차·하차별로 줄 수와 이벤트 가짓수를 나란히 보면?
day.groupBy("tag_type").agg(F.count("*").alias("rows"),
                            F.countDistinct("event_id").alias("events")).orderBy("tag_type").show()
# ── 결과 ──
# +--------+----+------+
# |tag_type|rows|events|
# +--------+----+------+
# |      IN|1012|  1000|
# |     OUT| 996|   988|
# +--------+----+------+
# → 이래서 — 중복 20줄이 승차·하차 양쪽에 나뉘어 섞여 있습니다. 08-1 에서 센 1,012 · 996 은 줄 수였고, 실제 이벤트는 그보다 적습니다.
# 무엇을 세는지(줄 / 이벤트 / 사람)에 따라 답이 달라집니다.

# %% [08-11] 어떤 이벤트가 두 번 들어왔는지 다섯 개만 보십시오.
day.groupBy("event_id").count().filter(F.col("count") > 1) \
   .orderBy("event_id").show(5)
# ── 결과 ──
# +--------+-----+
# |event_id|count|
# +--------+-----+
# |  E00084|    2|
# |  E00350|    2|
# |  E00373|    2|
# |  E00382|    2|
# |  E00436|    2|
# +--------+-----+
# only showing top 5 rows
# → 개찰구가 "전송 실패" 로 알고 한 번 더 보낸 것입니다. 실시간 시스템에서는 늘 일어납니다. STEP 13 에서 한 번만 세도록 정리하고, 다음 과목에서
# 이것이 Exactly-once(정확히 한 번) 라는 이름으로 다시 나옵니다.
