# ==============================================================================
#  STEP 11  Spark SQL — MySQL 처럼 묻기
#  C:\sparkcamp\lab\s11_sql.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
# [확인] 태그 로그 day와 역 마스터 st를 읽는다. 사건 정보와 기준 정보를 역 코드로 연결할 준비다.
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
# [셀 설명] DataFrame을 SQL에서 사용할 수 있도록 현재 Spark 세션에 임시 뷰 이름을 등록한다.
# [동작] createOrReplaceTempView는 같은 이름이 있으면 대체한다. 파일이나 영구 테이블 저장이 아니므로 세션을 새로 만들면 다시 등록해야 한다.
day.createOrReplaceTempView("tags")
st.createOrReplaceTempView("stations")
spark.sql("SHOW TABLES").show()



# %% [11-2] SQL 로 승차·하차 줄 수를 세십시오.
# [셀 설명] SQL의 GROUP BY와 COUNT로 승차·하차별 태그 행 수를 센다.
# [동작] spark.sql은 결과 DataFrame을 반환하고 show가 결과를 실행·표시한다. 임시 뷰 tags를 먼저 등록해야 한다.
spark.sql("SELECT tag_type, COUNT(*) AS cnt FROM tags GROUP BY tag_type").show()          \
    




# %% [11-3] SQL 로 역 이름별 승차 인원을 많은 순으로 구하십시오. (따옴표 세 개로 여러 줄)
# [셀 설명] 태그와 역 마스터를 SQL JOIN으로 연결해 역 이름별 승차 건수를 구한다.
# [동작] t·s는 표 별칭이다. WHERE로 승차를 고른 뒤 GROUP BY로 묶고 ORDER BY cnt DESC로 큰 순서로 표시한다.
spark.sql("""
          SELECT s.station_nm, COUNT(*) AS cnt
          FROM tags t
          JOIN stations s ON t.station_id = s.station_id
          WHERE t.tag_type = 'IN'
          GROUP BY s.station_nm
          ORDER BY cnt DESC
        """).show()






# %% [11-4] 같은 질문을 DataFrame 함수로 써서 결과가 같은지 비교하십시오.
# [셀 설명] 앞 셀의 SQL 질문을 DataFrame 메서드 연결로 다시 표현한다.
# [동작] WHERE는 filter, JOIN은 join, GROUP BY는 groupBy와 agg에 대응한다. 조건·조인 종류·집계 대상을 맞추어 결과를 비교한다.

(
    day.filter(F.col("tag_type") == "IN")       \
        .join(st, "station_id")
        .groupBy("station_nm").agg(F.count("*").alias("cnt"))
        .orderBy(F.col("cnt").desc())
        .show()       
    
)



# %% [11-5] SQL 로 시간대별 승차 인원을 구하십시오. 시각이 깨진 줄은 빼십시오.
# [셀 설명] 시각이 정상인 승차에서 시(hour)별 태그 건수를 SQL로 집계한다.
# [동작] IS NOT NULL로 시각이 없는 행을 제외한다. HOUR는 날짜를 제외한 시 부분이므로 여러 날짜 데이터라면 날짜별 집계와 구분한다.
spark.sql("""
    SELECT HOUR(tag_time) AS hh, count(*) AS cnt
    FROM tags
    WHERE tag_type = 'IN' AND tag_time IS NOT NULL
    GROUP BY HOUR(tag_time)
    ORDER BY hh
""").show()



# %% [11-6] 섞어 쓰기 — SQL 결과를 변수에 받고, 거기에 DataFrame 함수로 비율 칸을 하나 더 붙이십시오.
# [셀 설명] SQL 집계 결과에 DataFrame 연산을 붙여 시간대별 승차 비중을 구한다.
# [동작] 각 cnt를 전체 cnt 합계로 나눈다. over(Window.partitionBy())는 행을 유지하며 전체를 참조하는 분석 윈도우로, STEP 12의 시간 구간 window와 역할이 다르다.
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
# [셀 설명] SQL에서 전체 행 수와 서로 다른 이벤트 번호 수를 한 번에 확인한다.
# [동작] COUNT(*)는 행을 세고 COUNT(DISTINCT event_id)는 NULL을 제외한 고유 번호를 센다. 반복 수신된 사건이 있는지 점검하는 기준이다.

spark.sql("""
    select count(*) as rows, count(distinct event_id) as events
    from tags 
""").show()


# %% [11-8] SQL 의 CTE(WITH) 로, 역별 승차 수를 먼저 구하고 그중 평균보다 많은 역만 고르십시오.
# [셀 설명] 역별 승차 집계를 CTE에 이름 붙여 두고 그 평균보다 큰 역을 고른다.
# [동작] WITH board AS는 질의 안에서 쓸 중간 결과를 정의한다. 안쪽에서 그룹별 건수를 만들고 바깥에서 그 결과의 평균과 비교한다.
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
# [셀 설명] 등록하지 않은 뷰 이름을 사용했을 때 표·뷰를 찾지 못하는 오류를 확인한다.
# [동작] 현재 셀은 비어 있다. tagz 오타와 임시 뷰 미등록은 모두 이름 확인이 필요한 원인이므로 현재 세션 상태와 함께 점검한다.




# %% [11-10] SQL 결과를 pandas 표로 꺼내 맨 위 세 줄을 보십시오. (작은 결과만)
# 하차한 인원수를 SQL 구해서 pandas로 표시
# [셀 설명] SQL로 작게 집계한 결과를 pandas로 가져와 살펴본다.
# [동작] toPandas는 결과 전체를 드라이버로 가져온다. 현재 코드는 head(3)이 빠져 있으므로 제목의 상위 세 행만 표시하는 단계까지는 작성되지 않았다.

spark.sql("""
    SELECT station_id, COUNT(*) AS cnt FROM tags
    WHERE tag_type = 'OUT'
    GROUP BY station_id
    ORDER BY cnt DESC
""").toPandas()


# %%
