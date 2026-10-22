# ==============================================================================
#  STEP 11  Spark SQL — MySQL 처럼 묻기
#  C:\sparkcamp\lab\s11_sql.py
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
day = spark.read.schema(SCHEMA).json("data/tags_day.jsonl")
st = spark.read.csv("data/stations.csv", header=True)
print(day.count(), st.count())


# %% [11-1] day 에 tags, st 에 stations 라는 이름을 붙이십시오.
day.createOrReplaceTempView("tags")
st.createOrReplaceTempView("stations")
spark.sql("SHOW TABLES").show()



# %% [11-2] SQL 로 승차·하차 줄 수를 세십시오.
spark.sql("SELECT tag_type, COUNT(*) AS cnt FROM tags GROUP BY tag_type").show()          \
    




# %% [11-3] SQL 로 역 이름별 승차 인원을 많은 순으로 구하십시오. (따옴표 세 개로 여러 줄)
spark.sql("""
          SELECT s.station_nm, COUNT(*) AS cnt
          FROM tags t
          JOIN stations s ON t.station_id = s.station_id
          WHERE t.tag_type = 'IN'
          GROUP BY s.station_nm
          ORDER BY cnt DESC
        """).show()






# %% [11-4] 같은 질문을 DataFrame 함수로 써서 결과가 같은지 비교하십시오.

(
    day.filter(F.col("tag_type") == "IN")       \
        .join(st, "station_id")
        .groupBy("station_nm").agg(F.count("*").alias("cnt"))
        .orderBy(F.col("cnt").desc())
        .show()       
    
)



# %% [11-5] SQL 로 시간대별 승차 인원을 구하십시오. 시각이 깨진 줄은 빼십시오.
spark.sql("""
    SELECT HOUR(tag_time) AS hh, count(*) AS cnt
    FROM tags
    WHERE tag_type = 'IN' AND tag_time IS NOT NULL
    GROUP BY HOUR(tag_time)
    ORDER BY hh
""").show()



# %% [11-6] 섞어 쓰기 — SQL 결과를 변수에 받고, 거기에 DataFrame 함수로 비율 칸을 하나 더 붙이십시오.
from pyspark.sql import Window
by_hour = spark.sql("""
            SELECT HOUR(tag_time) AS hh, count(*) AS cnt
            FROM tags
            WHERE tag_type = 'IN' AND tag_time IS NOT NULL
            GROUP BY HOUR(tag_time)
""")

by_hour.withColumn("ratio", F.round(F.col("cnt") / F.sum("cnt")
                    .over(Window.partitionBy()),3))     \
                    .orderBy("hh").show()



# %% [11-7] SQL 로 이벤트 번호의 가짓수와 전체 줄 수를 세십시오.

spark.sql("""
    select count(*) as rows, count(distinct event_id) as events
    from tags 
""").show()


# %% [11-8] SQL 의 CTE(WITH) 로, 역별 승차 수를 먼저 구하고 그중 평균보다 많은 역만 고르십시오.
spark.sql("""
    WITH board AS(
        SELECT station_id, COUNT(*) AS cnt
        FROM tags
        WHERE tag_type = 'IN'
        GROUP BY station_id
    )
    SELECT * FROM board
    WHERE cnt > (SELECT AVG(cnt) FROM board)
    ORDER BY cnt DESC
""").show()



# %% [11-9] 일부러 틀려 보십시오. 표 이름을 tagz 로 잘못 쓰십시오.




# %% [11-10] SQL 결과를 pandas 표로 꺼내 맨 위 세 줄을 보십시오. (작은 결과만)
# 하차한 인원수를 SQL 구해서 pandas로 표시

spark.sql("""
    SELECT station_id, COUNT(*) AS cnt FROM tags
    WHERE tag_type = 'OUT'
    GROUP BY station_id
    ORDER BY cnt DESC
""").toPandas()


# %%
