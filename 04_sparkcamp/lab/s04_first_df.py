# ==============================================================================
#  STEP 04  Spark 는 무엇을 하는가 — 첫 DataFrame
#  C:\sparkcamp\lab\s04_first_df.py
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


# %% [04-1] 아래 다섯 건의 이동 기록으로 DataFrame 을 만들고 보여 주십시오. 칸 이름은 trip_id · board · alight · fare.
rows = [("T001", "강남", "홍대입구", 1650),
        ("T002", "잠실", "강남", 1650),
        ("T003", "홍대입구", "시청", 1550),
        ("T004", "서울역", "여의도", 1650),
        ("T005", "강남", "역삼", 1550), 
        ]

df = spark.createDataFrame(rows, ["trip_id", "borad", "alright", "fare"])
df.show() # 전체출력 아님 - 상위 20줄만




# %% [04-2] 그 표의 스키마(칸 이름과 자료형)를 보십시오.
df.printSchema()


# %% [04-3] 행이 몇 개인지 세십시오.

df.count()



# %% [04-4] 칸 이름 목록을 보십시오.
df.columns



# %% [04-5] 게으름 확인 — 요금이 1,600원 넘는 이동만 고르는 주문서를 만들고, 그것을 print 해 보십시오.
cheap = df.filter(df.fare > 1600).select("trip_id")
print(cheap)
cheap.show()



# %% [04-5 바꿔 보기 ①] 주문서에 한 줄 더 적으면? — filter 뒤에 select 를 이어 붙여 print
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
cheap = df.filter(df.fare > 9999).select("trip_id")
print(cheap)
cheap.show()



# %% [04-5 바꿔 보기 ②] 조건에 맞는 줄이 하나도 없으면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [04-6] 이제 "주문이요!" — 같은 것을 show 와 count 로 실행하십시오.




# %% [04-7] 이 표가 몇 조각(Partition) 으로 나뉘어 있는지, 내 노트북의 기본 일꾼 수가 몇인지 보십시오.
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)




# %% [04-7 바꿔 보기 ①] (강사 시연) 준비 셀의 일꾼 수를 1 로 — .master("local[1]") 로 고치고 Restart → 준비 셀 · 04-1 · 04-7 다시 실행
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [04-7 바꿔 보기 ②] (강사 시연) 일꾼 수를 4 로 — .master("local[4]")
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [04-8] 탑승역별로 몇 건인지 세고, 브라우저의 Spark UI Jobs 탭을 새로 고쳐 보십시오.
df.groupBy("board").count().orderBy("board").show()



# %% [04-9] 일부러 틀려 보십시오. 없는 칸 price 를 골라 보십시오.






# %% [04-10] Spark 표를 pandas 표로 꺼내 보십시오.
pdf = df.toPandas()
print(type(pdf))
pdf
# %%
