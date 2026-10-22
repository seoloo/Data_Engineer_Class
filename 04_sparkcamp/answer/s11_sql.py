# ==============================================================================
#  STEP 11  Spark SQL — MySQL 처럼 묻기
#  C:\sparkcamp\answer\s11_sql.py
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


# %% [11-1] day 에 tags, st 에 stations 라는 이름을 붙이십시오.
day.createOrReplaceTempView("tags")
st.createOrReplaceTempView("stations")
spark.sql("SHOW TABLES").show()
# ── 결과 ──
# +---------+---------+-----------+
# |namespace|tableName|isTemporary|
# +---------+---------+-----------+
# |         | stations|       true|
# |         |     tags|       true|
# +---------+---------+-----------+
# → isTemporary = true. 임시 이름표입니다.

# %% [11-2] SQL 로 승차·하차 줄 수를 세십시오.
spark.sql("SELECT tag_type, COUNT(*) AS cnt FROM tags GROUP BY tag_type").show()
# ── 결과 ──
# +--------+----+
# |tag_type| cnt|
# +--------+----+
# |     OUT| 996|
# |      IN|1012|
# +--------+----+
# → STEP 08-1 과 같은 숫자입니다.

# %% [11-3] SQL 로 역 이름별 승차 인원을 많은 순으로 구하십시오. (따옴표 세 개로 여러 줄)
spark.sql("""
    SELECT s.station_nm, COUNT(*) AS cnt
    FROM tags t
    JOIN stations s ON t.station_id = s.station_id
    WHERE t.tag_type = 'IN'
    GROUP BY s.station_nm
    ORDER BY cnt DESC
""").show()
# ── 결과 ──
# +----------+---+
# |station_nm|cnt|
# +----------+---+
# |    광화문|147|
# |      역삼|130|
# |    서울역|129|
# |      시청|122|
# |      강남|122|
# |      잠실|120|
# |  홍대입구|120|
# |    여의도|116|
# +----------+---+
# → MySQL 특강에서 쓰던 SQL 그대로입니다. STEP 10-6 과 같은 숫자.

# %% [11-4] 같은 질문을 DataFrame 함수로 써서 결과가 같은지 비교하십시오.
(day.filter(F.col("tag_type") == "IN")
    .join(st, "station_id")
    .groupBy("station_nm").agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
    .show())
# ── 결과 ──
# +----------+---+
# |station_nm|cnt|
# +----------+---+
# |    광화문|147|
# |      역삼|130|
# |    서울역|129|
# |      시청|122|
# |      강남|122|
# |      잠실|120|
# |  홍대입구|120|
# |    여의도|116|
# +----------+---+
# → 같습니다. 어느 쪽이든 Spark 안에서는 같은 계획이 됩니다.

# %% [11-5] SQL 로 시간대별 승차 인원을 구하십시오. 시각이 깨진 줄은 빼십시오.
spark.sql("""
    SELECT HOUR(tag_time) AS hh, COUNT(*) AS cnt
    FROM tags
    WHERE tag_type = 'IN' AND tag_time IS NOT NULL
    GROUP BY HOUR(tag_time)
    ORDER BY hh
""").show()
# ── 결과 ──
# +---+---+
# | hh|cnt|
# +---+---+
# |  7|305|
# |  8|453|
# |  9|252|
# +---+---+

# %% [11-6] 섞어 쓰기 — SQL 결과를 변수에 받고, 거기에 DataFrame 함수로 비율 칸을 하나 더 붙이십시오.
from pyspark.sql import Window
by_hour = spark.sql("""
    SELECT HOUR(tag_time) AS hh, COUNT(*) AS cnt
    FROM tags WHERE tag_type = 'IN' AND tag_time IS NOT NULL
    GROUP BY HOUR(tag_time)
""")
by_hour.withColumn("ratio", F.round(F.col("cnt") / F.sum("cnt").over(Window.partitionBy()), 3)) \
       .orderBy("hh").show()
# ── 결과 ──
# +---+---+-----+
# | hh|cnt|ratio|
# +---+---+-----+
# |  7|305|0.302|
# |  8|453|0.449|
# |  9|252| 0.25|
# +---+---+-----+
# → ratio = 전체 중 비율. F.sum(...).over(Window.partitionBy()) 는 "모든 줄의 합을 줄마다 옆에" — MySQL 특강
# 부록의 SUM() OVER () 와 같습니다. 8시대가 약 45%.

# %% [11-7] SQL 로 이벤트 번호의 가짓수와 전체 줄 수를 세십시오.
spark.sql("SELECT COUNT(*) AS rows, COUNT(DISTINCT event_id) AS events FROM tags").show()
# ── 결과 ──
# +----+------+
# |rows|events|
# +----+------+
# |2008|  1988|
# +----+------+
# → 2008 과 1988. 중복 20줄은 SQL 로 봐도 그대로입니다.

# %% [11-8] SQL 의 CTE(WITH) 로, 역별 승차 수를 먼저 구하고 그중 평균보다 많은 역만 고르십시오.
spark.sql("""
    WITH board AS (
        SELECT station_id, COUNT(*) AS cnt
        FROM tags WHERE tag_type = 'IN'
        GROUP BY station_id
    )
    SELECT * FROM board
    WHERE cnt > (SELECT AVG(cnt) FROM board)
    ORDER BY cnt DESC
""").show()
# ── 결과 ──
# +----------+---+
# |station_id|cnt|
# +----------+---+
# |       S08|147|
# |       S02|130|
# |       S05|129|
# |       S01|122|
# |       S04|122|
# |       S06|120|
# |       S03|120|
# |       S07|116|
# +----------+---+
# → MySQL 특강 부록 C 의 WITH 와 같습니다. 서브쿼리가 두 겹이 넘으면 CTE 가 읽기 쉽습니다.

# %% [11-9] 일부러 틀려 보십시오. 표 이름을 tagz 로 잘못 쓰십시오.
spark.sql("SELECT * FROM tagz")
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `tagz` cannot be found. Verify the spelling and correctness of the schema and catalog.
# → TABLE_OR_VIEW_NOT_FOUND. 이름을 붙이지 않았거나(createOrReplaceTempView), 오타입니다. 인터랙티브 창을 새로
# 열었다면 이름도 다시 붙여야 합니다.

# %% [11-10] SQL 결과를 pandas 표로 꺼내 맨 위 세 줄을 보십시오. (작은 결과만)
spark.sql("""
    SELECT station_id, COUNT(*) AS cnt FROM tags
    WHERE tag_type = 'OUT' GROUP BY station_id ORDER BY cnt DESC
""").toPandas().head(3)
# ── 결과 ──
#   station_id  cnt
# 0        S03  147
# 1        S04  134
# 2        S07  133
# → 집계로 작게 줄인 다음 toPandas. 이 순서를 지키면 노트북이 멈추지 않습니다.
