# ==============================================================================
#  STEP 04  Spark 는 무엇을 하는가 — 첫 DataFrame
#  C:\sparkcamp\answer\s04_first_df.py
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
# ── 결과 ──
# Spark 4.1.3 준비 완료


# %% [04-1] 아래 다섯 건의 이동 기록으로 DataFrame 을 만들고 보여 주십시오. 칸 이름은 trip_id · board · alight · fare.
rows = [("T001", "강남",   "홍대입구", 1650),
        ("T002", "잠실",   "강남",     1650),
        ("T003", "홍대입구", "시청",   1550),
        ("T004", "서울역", "여의도",   1650),
        ("T005", "강남",   "역삼",     1550)]
df = spark.createDataFrame(rows, ["trip_id", "board", "alight", "fare"])
df.show()
# ── 결과 ──
# +-------+--------+--------+----+
# |trip_id|   board|  alight|fare|
# +-------+--------+--------+----+
# |   T001|    강남|홍대입구|1650|
# |   T002|    잠실|    강남|1650|
# |   T003|홍대입구|    시청|1550|
# |   T004|  서울역|  여의도|1650|
# |   T005|    강남|    역삼|1550|
# +-------+--------+--------+----+
# → show() 는 위에서 20줄까지 표로 보여 줍니다. 파이썬 리스트가 Spark 의 표가 되었습니다.

# %% [04-2] 그 표의 스키마(칸 이름과 자료형)를 보십시오.
df.printSchema()
# ── 결과 ──
# root
#  |-- trip_id: string (nullable = true)
#  |-- board: string (nullable = true)
#  |-- alight: string (nullable = true)
#  |-- fare: long (nullable = true)
# → 숫자는 long(큰 정수), 글자는 string 으로 Spark 가 짐작했습니다. nullable = true 는 "비어 있어도 된다" 입니다.

# %% [04-3] 행이 몇 개인지 세십시오.
df.count()
# ── 결과 ──
# 5

# %% [04-4] 칸 이름 목록을 보십시오.
df.columns
# ── 결과 ──
# ['trip_id', 'board', 'alight', 'fare']
# → 결과가 파이썬 리스트입니다. 표가 아니라 그냥 파이썬 값이 나오는 것도 있습니다.

# %% [04-5] 게으름 확인 — 요금이 1,600원 넘는 이동만 고르는 주문서를 만들고, 그것을 print 해 보십시오.
cheap = df.filter(df.fare > 1600)
print(cheap)
# ── 결과 ──
# DataFrame[trip_id: string, board: string, alight: string, fare: bigint]
# → 표가 나오지 않습니다. 주문서(변환)만 적었기 때문입니다. 칸 목록만 보입니다.

# %% [04-5 바꿔 보기 ①] 주문서에 한 줄 더 적으면? — filter 뒤에 select 를 이어 붙여 print
print(df.filter(df.fare > 1600).select("trip_id"))
# ── 결과 ──
# DataFrame[trip_id: string]
# → 이래서 — 여전히 표가 아니라 칸 목록만 나옵니다. 그런데 칸이 trip_id 하나로 줄었습니다. Spark 는 계산은 안 했지만 결과가 어떤 모양일지는
# 이미 압니다. 주문서를 계속 적어도 주방은 아직 움직이지 않습니다.

# %% [04-5 바꿔 보기 ②] 조건에 맞는 줄이 하나도 없으면?
df.filter(df.fare > 9999).show()
# ── 결과 ──
# +-------+-----+------+----+
# |trip_id|board|alight|fare|
# +-------+-----+------+----+
# +-------+-----+------+----+
# → 이래서 — 오류가 아니라 머리글만 있는 빈 표가 나옵니다. "주문이요!" 를 했고 주방은 일을 했는데, 체에 걸린 것이 없었을 뿐입니다. 빈 결과와 오류는
# 다릅니다.

# %% [04-6] 이제 "주문이요!" — 같은 것을 show 와 count 로 실행하십시오.
cheap.show()
cheap.count()
# ── 결과 ──
# +-------+------+--------+----+
# |trip_id| board|  alight|fare|
# +-------+------+--------+----+
# |   T001|  강남|홍대입구|1650|
# |   T002|  잠실|    강남|1650|
# |   T004|서울역|  여의도|1650|
# +-------+------+--------+----+
#
# 3
# → show · count 가 실행(Action) 입니다. 이때 비로소 계산합니다.

# %% [04-7] 이 표가 몇 조각(Partition) 으로 나뉘어 있는지, 내 노트북의 기본 일꾼 수가 몇인지 보십시오.
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)
# ── 결과 ──
# 2
# 2
# → 2 와 2. 준비 셀에서 local[2](일꾼 두 명)로 정했기 때문에 누구 노트북에서나 같습니다. local[*] 로 바꾸면 내 노트북의 CPU 코어
# 수(4·8·16…)를 따라갑니다. 수업에서는 결과를 맞추려고 2 로 고정했습니다.

# %% [04-7 바꿔 보기 ①] (강사 시연) 준비 셀의 일꾼 수를 1 로 — .master("local[1]") 로 고치고 Restart → 준비 셀 · 04-1 · 04-7 다시 실행
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)
print(df.rdd.glom().map(len).collect())     # 조각마다 몇 줄씩
# ── 결과 ──
# 1
# 1
# [5]
# → 이래서 — 일꾼이 하나면 조각도 하나, 다섯 줄이 전부 한 조각에 들어갑니다. glom 은 "조각별로 묶어 보기" 입니다(STEP 14 에서 다시
# 씁니다).

# %% [04-7 바꿔 보기 ②] (강사 시연) 일꾼 수를 4 로 — .master("local[4]")
print(df.rdd.getNumPartitions())
print(spark.sparkContext.defaultParallelism)
print(df.rdd.glom().map(len).collect())
# ── 결과 ──
# 4
# 4
# [1, 1, 1, 2]
# → 이래서 — 같은 다섯 줄이 네 조각으로 나뉩니다. 코드는 한 글자도 안 바꿨는데 조각 수가 달라졌습니다. local[*] 로 두면 학생 노트북 코어 수(4
# · 8 · 16 …)에 따라 이 숫자가 다 다릅니다. 이 수업이 local[2] 로 고정한 이유입니다. 확인한 뒤 준비 셀을 local[2] 로 되돌리고
# Restart 하십시오.

# %% [04-8] 탑승역별로 몇 건인지 세고, 브라우저의 Spark UI Jobs 탭을 새로 고쳐 보십시오.
df.groupBy("board").count().orderBy("board").show()
# ── 결과 ──
# +--------+-----+
# |   board|count|
# +--------+-----+
# |    강남|    2|
# |  서울역|    1|
# |    잠실|    1|
# |홍대입구|    1|
# +--------+-----+
# → Jobs 탭에 새 줄이 생겼습니다. 실행(Action)할 때마다 일이 하나씩 쌓입니다. 줄을 클릭하면 몇 조각으로 나눠 했는지 보입니다.

# %% [04-9] 일부러 틀려 보십시오. 없는 칸 price 를 골라 보십시오.
df.select("price")
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `price` cannot be resolved. Did you mean one of the following? [`fare`, `alight`, `board`, `trip_id`]. SQLSTATE: 42703;
# → UNRESOLVED_COLUMN = 그런 칸이 없다. 친절하게 "혹시 이것들 중 하나?" 하고 비슷한 칸 이름을 제안합니다. 오류 맨 끝까지 읽는 습관을
# 들이십시오.

# %% [04-10] Spark 표를 pandas 표로 꺼내 보십시오.
pdf = df.toPandas()
print(type(pdf))
pdf
# ── 결과 ──
# <class 'pandas.core.frame.DataFrame'>
#   trip_id board alight  fare
# 0    T001    강남   홍대입구  1650
# 1    T002    잠실     강남  1650
# 2    T003  홍대입구     시청  1550
# 3    T004   서울역    여의도  1650
# 4    T005    강남     역삼  1550
# → 익숙한 pandas 입니다. 단, 결과를 내 노트북 메모리로 전부 가져옵니다. 1억 줄짜리 표에 이걸 하면 노트북이 멈춥니다 — 작게 줄인 다음에만
# 씁니다.
