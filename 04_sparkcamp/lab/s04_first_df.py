# ==============================================================================
#  STEP 04  Spark 는 무엇을 하는가 — 첫 DataFrame
#  C:\sparkcamp\lab\s04_first_df.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
# [셀 설명] 이 STEP의 실행 위치와 Python을 맞추고 SparkSession을 준비한다.
# [동작] local[2]는 로컬 작업 스레드 두 개를 뜻한다. getOrCreate는 세션을 재사용할 수 있으므로 앞선 실험 설정이 남아 있는지도 확인한다.
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
# [셀 설명] 파이썬 튜플 목록을 열 이름이 있는 Spark DataFrame으로 만든다.
# [동작] createDataFrame의 열 이름 순서는 튜플의 값 순서와 대응한다. show는 기본 최대 20행을 표시한다.
# [확인] 현재 열 이름이 borad·alright로 작성되어 제목의 board·alight와 다르다. 이후 04-8에서 board를 찾을 때 이 이름 차이를 확인한다.
rows = [("T001", "강남", "홍대입구", 1650),
        ("T002", "잠실", "강남", 1650),
        ("T003", "홍대입구", "시청", 1550),
        ("T004", "서울역", "여의도", 1650),
        ("T005", "강남", "역삼", 1550), 
        ]

df = spark.createDataFrame(rows, ["trip_id", "borad", "alright", "fare"])
df.show() # 전체출력 아님 - 상위 20줄만




# %% [04-2] 그 표의 스키마(칸 이름과 자료형)를 보십시오.
# [셀 설명] 열 이름과 자료형, NULL 허용 여부가 어떻게 만들어졌는지 확인한다.
# [동작] printSchema는 표의 구조를 출력한다. 행 데이터 미리 보기인 show와 구분한다.
df.printSchema()


# %% [04-3] 행이 몇 개인지 세십시오.
# [셀 설명] DataFrame에 들어 있는 전체 행 수를 계산한다.
# [동작] df.count()는 정수를 반환하는 액션이다. 이 예제의 행은 이동 한 건이므로 이동 기록의 개수를 센다.

df.count()



# %% [04-4] 칸 이름 목록을 보십시오.
# [셀 설명] DataFrame에 등록된 열 이름들을 파이썬 리스트로 확인한다.
# [동작] columns는 속성으로 읽는다. 열 이름 오타나 예상과 다른 이름을 먼저 점검할 때 유용하다.
df.columns



# %% [04-5] 게으름 확인 — 요금이 1,600원 넘는 이동만 고르는 주문서를 만들고, 그것을 print 해 보십시오.
# [셀 설명] 조건으로 행을 고르고 열을 선택한 계산 계획과 실제 실행을 구분한다.
# [동작] filter는 조건, select는 남길 열을 지정한다. print(cheap)은 DataFrame 표현을 보여 주고, 현재 코드에 추가된 cheap.show()는 결과 계산을 실행한다.
cheap = df.filter(df.fare > 1600).select("trip_id")
print(cheap)
cheap.show()



# %% [04-5 바꿔 보기 ①] 주문서에 한 줄 더 적으면? — filter 뒤에 select 를 이어 붙여 print
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] filter 뒤에 select를 이어서 필요한 결과 열만 남기는 흐름을 확인한다.
# [동작] 현재 조건은 fare > 9999라 예제 데이터에는 해당 행이 없다. 빈 결과에도 trip_id 열 구조는 남으며 오류와는 다르다.
cheap = df.filter(df.fare > 9999).select("trip_id")
print(cheap)
cheap.show()



# %% [04-5 바꿔 보기 ②] 조건에 맞는 줄이 하나도 없으면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 조건에 맞는 행이 없을 때 DataFrame과 count의 결과를 예상하는 연습이다.
# [동작] 현재 셀은 비어 있다. 앞 셀의 빈 결과를 이용하면 show에는 행이 없고 count는 0이 되는 것을 비교할 수 있다.




# %% [04-6] 이제 "주문이요!" — 같은 것을 show 와 count 로 실행하십시오.
# [셀 설명] 계산 계획을 실제로 실행하는 액션인 show와 count를 비교하는 셀이다.
# [동작] 현재 셀은 비어 있다. show는 화면 표시 후 None을 반환하고, count는 행 수를 정수로 반환한다.




# %% [04-7] 이 표가 몇 조각(Partition) 으로 나뉘어 있는지, 내 노트북의 기본 일꾼 수가 몇인지 보십시오.
# [셀 설명] 데이터의 파티션 수와 현재 SparkContext의 기본 병렬도를 각각 확인한다.
# [동작] 파티션은 데이터 조각이고 defaultParallelism은 실행 환경의 기본 병렬도다. local[2]를 썼다고 모든 DataFrame이 언제나 두 파티션인 것은 아니다.
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)




# %% [04-7 바꿔 보기 ①] (강사 시연) 준비 셀의 일꾼 수를 1 로 — .master("local[1]") 로 고치고 Restart → 준비 셀 · 04-1 · 04-7 다시 실행
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 로컬 작업 스레드를 하나로 바꾼 뒤 병렬도와 파티션 구성을 관찰하는 시연이다.
# [동작] 현재 셀은 비어 있다. 기존 세션을 재사용하면 master 변경이 적용되지 않을 수 있으므로 안내대로 세션을 재시작해 비교한다.




# %% [04-7 바꿔 보기 ②] (강사 시연) 일꾼 수를 4 로 — .master("local[4]")
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 로컬 작업 스레드를 네 개로 바꾼 환경을 이전 설정과 비교하는 시연이다.
# [동작] 현재 셀은 비어 있다. local[4]는 로컬 스레드 수 설정이며 컴퓨터 네 대를 뜻하지 않는다.




# %% [04-8] 탑승역별로 몇 건인지 세고, 브라우저의 Spark UI Jobs 탭을 새로 고쳐 보십시오.
# [셀 설명] 탑승역별로 이동 기록을 묶고 건수를 계산한 뒤 역 이름순으로 표시한다.
# [동작] groupBy(...).count()는 집계 결과 DataFrame이며 show에서 실행한다. 현재 04-1의 열 이름은 borad이므로 board 참조와 일치하는지 먼저 확인한다.
df.groupBy("board").count().orderBy("board").show()



# %% [04-9] 일부러 틀려 보십시오. 없는 칸 price 를 골라 보십시오.
# [셀 설명] 스키마에 없는 열을 선택했을 때 열 이름 관련 오류를 읽는 연습이다.
# [동작] 현재 셀은 비어 있다. price가 실제 열 목록에 있는지 df.columns와 비교하며 오류 원인을 찾는다.






# %% [04-10] Spark 표를 pandas 표로 꺼내 보십시오.
# [셀 설명] 작은 Spark DataFrame을 현재 파이썬 프로세스에서 다루는 pandas DataFrame으로 가져온다.
# [동작] toPandas는 전체 결과를 드라이버 쪽으로 가져오므로 작은 결과에 사용한다. type(pdf)로 반환된 객체의 종류를 확인한다.
pdf = df.toPandas()
print(type(pdf))
pdf
# %%
