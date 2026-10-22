# ==============================================================================
#  STEP 18  Structured Streaming 입문 — 끝나지 않는 표
#  C:\sparkcamp\answer\s18_streaming.py
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

import shutil, time

def drop(n):
    """stream_src 의 n 번째 조각을 stream_in 으로 옮겨 놓는다 = 새 데이터가 도착했다"""
    name = f"part_{n:02d}.jsonl"
    shutil.copy(f"data/stream_src/{name}", f"data/stream_in/_{name}")      # 밑줄 이름으로 복사하고
    os.replace(f"data/stream_in/_{name}", f"data/stream_in/{name}")        # 다 되면 이름을 바꾼다
    print("도착:", name, "/ stream_in 파일 수:", len(os.listdir("data/stream_in")))

def ls(path):
    return [f for f in sorted(os.listdir(path)) if not f.startswith(".")]
# ── 결과 ──
# Spark 4.1.3 준비 완료


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for q in spark.streams.active:      # 돌고 있는 쿼리가 있으면 먼저 멈춘다
    q.stop()
for p in ["data/stream_in", "out/s18", "chk/s18"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs("data/stream_in")
print("stream_in · out/s18 · chk/s18 비움")
# ── 결과 ──
# stream_in · out/s18 · chk/s18 비움


# %% [18-1] 첫 번째 조각을 도착시키십시오.
drop(1)
# ── 결과 ──
# 도착: part_01.jsonl / stream_in 파일 수: 1
# → stream_in 에 파일이 1개. 아직 아무도 읽지 않았습니다.

# %% [18-2] stream_in 폴더를 스트리밍으로 읽는 sdf 를 만들고, 스트리밍인지 물어보십시오.
sdf = spark.readStream.schema(SCHEMA).json("data/stream_in")
print(sdf.isStreaming)
sdf.printSchema()
# ── 결과 ──
# True
# root
#  |-- event_id: string (nullable = true)
#  |-- card_id: string (nullable = true)
#  |-- trip_id: string (nullable = true)
#  |-- tag_type: string (nullable = true)
#  |-- station_id: string (nullable = true)
#  |-- tag_time: timestamp (nullable = true)
#  |-- fare: integer (nullable = true)
#  |-- recv_time: timestamp (nullable = true)
# → True. read → readStream 한 단어만 바뀌었습니다. 아직 아무것도 읽지 않았습니다(게으름 — STEP 04).

# %% [18-3] 일부러 틀려 보십시오. 스키마 없이 readStream 을 하십시오.
spark.readStream.json("data/stream_in")
# ── 결과 (오류 — 일부러 틀린 것) ──
# IllegalArgumentException: Schema must be specified when creating a streaming source DataFrame. If some files already exist in the directory, then depending on the file format you may be able to create a static DataFrame on that directory with 'spark.read.load(directory)' and infer schema from it.
# → Schema must be specified — 스트리밍은 스키마가 필수입니다(STEP 07 에서 예고한 것). 끝없이 들어오는 데이터를 미리 훑어볼 수
# 없기 때문입니다.

# %% [18-4] 일부러 틀려 보십시오. sdf 의 줄 수를 count() 로 세십시오.
sdf.count()
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: Queries with streaming sources must be executed with writeStream.start(), or from a streaming table or flow definition within a Spark Declarative Pipeline.;
# → must be executed with writeStream.start() — 끝나지 않는 표는 "지금 몇 줄?" 에 답할 수 없습니다. 스트리밍은
# writeStream 으로 어딘가에 계속 쓰게 해야 움직입니다.

# %% [18-5] sdf 를 memory 싱크(live_raw 라는 연습용 표)로 3초마다 쓰는 쿼리를 시작하고, 처리가 끝나면 live_raw 를 세십시오.
q = (sdf.writeStream
         .format("memory")
         .queryName("live_raw")
         .outputMode("append")
         .trigger(processingTime="3 seconds")
         .start())
q.processAllAvailable()
spark.sql("SELECT COUNT(*) AS n FROM live_raw").show()
# ── 결과 ──
# +---+
# |  n|
# +---+
# |201|
# +---+
# → 201 — 첫 조각의 줄 수입니다. start() 를 한 순간부터 이 쿼리는 뒤에서 계속 돌고 있습니다. 셀이 끝나도 멈추지 않습니다.

# %% [18-6] 두 번째 조각을 도착시키고, 처리가 끝나면 다시 세십시오.
drop(2)
q.processAllAvailable()
spark.sql("SELECT COUNT(*) AS n FROM live_raw").show()
# ── 결과 ──
# 도착: part_02.jsonl / stream_in 파일 수: 2
# +---+
# |  n|
# +---+
# |402|
# +---+
# → 402. 우리는 아무것도 다시 실행하지 않았습니다. 파일을 놓기만 했는데 표가 늘었습니다. 이것이 스트리밍입니다.

# %% [18-7] 쿼리가 지금까지 처리한 마이크로 배치마다 몇 줄씩 읽었는지 보십시오.
print(q.status["message"])
print([(p["batchId"], p["numInputRows"]) for p in q.recentProgress if p["numInputRows"] > 0])
# ── 결과 ──
# Waiting for next trigger
# [(0, 201), (1, 201)]
# → 배치 0 에 201줄, 배치 1 에 201줄. 새로 온 것만 읽었습니다. recentProgress 의 숫자들(처리 속도·걸린 시간)이 다음 과목
# 5단계(지연·처리량 측정) 의 재료입니다.

# %% [18-8] 쿼리를 멈추고, 아직 돌고 있는지 확인하십시오.
q.stop()
print(q.isActive)
# ── 결과 ──
# False
# → False. 다 쓴 쿼리는 반드시 stop() 합니다. 안 멈추면 계속 CPU 를 씁니다.

# %% [18-9] 실시간 전광판 — 승차만 골라 역별로 세는 쿼리를 complete 모드로 시작하고 결과를 보십시오.
board_q = (sdf.filter(F.col("tag_type") == "IN")
               .groupBy("station_id").count()
               .writeStream
               .format("memory")
               .queryName("live_station")
               .outputMode("complete")
               .trigger(processingTime="3 seconds")
               .start())
board_q.processAllAvailable()
spark.sql("SELECT * FROM live_station ORDER BY station_id").show()
# ── 결과 ──
# +----------+-----+
# |station_id|count|
# +----------+-----+
# |       S01|   32|
# |       S02|   32|
# |       S03|   24|
# |       S04|   29|
# |       S05|   32|
# |       S06|   30|
# |       S07|   39|
# |       S08|   44|
# |       S99|    1|
# +----------+-----+
# → 새 쿼리는 폴더에 이미 있는 두 조각부터 읽습니다. 집계는 complete — 매번 전체 표를 새로 내보냅니다(역이 8~9개뿐이니 괜찮습니다).

# %% [18-10] 세 번째 조각을 도착시키고 전광판을 다시 보십시오.
drop(3)
board_q.processAllAvailable()
spark.sql("SELECT * FROM live_station ORDER BY station_id").show()
# ── 결과 ──
# 도착: part_03.jsonl / stream_in 파일 수: 3
# +----------+-----+
# |station_id|count|
# +----------+-----+
# |       S01|   55|
# |       S02|   43|
# |       S03|   40|
# |       S04|   45|
# |       S05|   46|
# |       S06|   43|
# |       S07|   52|
# |       S08|   62|
# |       S99|    3|
# +----------+-----+
# → 역마다 숫자가 올라갔습니다. STEP 08-2 의 역별 승차 집계와 같은 코드인데, 이번에는 데이터가 올 때마다 저절로 갱신됩니다. 프로젝트 2단계의
# "실시간 집계" 가 이 모양입니다.

# %% [18-11] 일부러 틀려 보십시오. 같은 집계를 append 모드로 시작하십시오.
(sdf.groupBy("station_id").count()
    .writeStream.format("memory").queryName("bad").outputMode("append").start())
# ── 결과 (오류 — 일부러 틀린 것) ──
# AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append. This output mode is not supported for streaming aggregations without watermark on streaming DataFrames/DataSets. SQLSTATE: 42KDE
# → append 모드는 집계에 쓸 수 없다(워터마크가 없으면). append 는 "한 번 내보낸 줄은 다시 안 바뀐다" 는 약속인데, 역별 숫자는 계속
# 바뀌니까요. 워터마크로 "이 창은 이제 안 바뀐다" 를 정하면 append 도 됩니다 — 다음 과목의 핵심입니다.

# %% [18-12] 전광판 쿼리를 멈추십시오.
board_q.stop()
print(spark.streams.active)
# ── 결과 ──
# []
# → [] — 돌고 있는 쿼리가 없습니다. spark.streams.active 는 지금 돌고 있는 쿼리 목록입니다.

# %% [18-13] 파일로 쓰는 쿼리를 만드십시오. 새 묶음(마이크로 배치)이 올 때마다 save 함수가 불려서 out/s18/raw 폴더에 append 로 붙이게
# 하고(foreachBatch), 체크포인트는 chk/s18/raw, 있는 것만 처리하고 끝내기(availableNow). 한 번 실행하고 줄 수를 세십시오.
def save(batch_df, batch_id):
    """새로 온 묶음 하나를 받아서 하는 일"""
    print(f"  배치 {batch_id}: {batch_df.count()} 줄")
    batch_df.write.mode("append").parquet("out/s18/raw")

def run_raw():
    qq = (spark.readStream.schema(SCHEMA).json("data/stream_in")
          .writeStream
          .foreachBatch(save)
          .option("checkpointLocation", "chk/s18/raw")
          .trigger(availableNow=True)
          .start())
    qq.awaitTermination()                       # 다 처리하고 끝날 때까지 기다린다
    return spark.read.parquet("out/s18/raw").count()

run_raw()
# ── 결과 ──
#   배치 0: 603 줄
# 603
# → 603 = 201 × 3 (지금까지 도착한 세 조각). foreachBatch 는 "묶음이 올 때마다 이 함수를 불러 달라" — 함수 안에서는 평소 배치
# 코드(write · MERGE …)를 마음대로 씁니다. STEP 19 에서 이 안에 MERGE 를 넣습니다. availableNow 는 "지금 있는 것만 다
# 처리하고 스스로 멈춘다" 입니다.

# %% [18-14] 네 번째 조각을 도착시키고 같은 함수를 다시 실행하십시오.
drop(4)
run_raw()
# ── 결과 ──
# 도착: part_04.jsonl / stream_in 파일 수: 4
#   배치 1: 201 줄
# 804
# → 배치 1: 201 줄 → 804 = 603 + 201. 이미 읽은 세 조각은 다시 읽지 않고 새로 온 네 번째만 처리했습니다. 체크포인트가 "어디까지
# 읽었나" 를 기억했기 때문입니다.

# %% [18-15] 체크포인트 폴더 안을 들여다보십시오. 그리고 offsets 폴더 안도 보십시오.
print(ls("chk/s18/raw"))
print(ls("chk/s18/raw/offsets"))
# ── 결과 ──
# ['commits', 'metadata', 'offsets', 'sources']
# ['0', '1']
# → offsets(이번에 어디까지 읽겠다) · commits(어디까지 끝냈다) · sources(읽은 파일 목록) · metadata(쿼리 번호). 파일 0
# · 1 = 배치 두 번. 이 폴더가 곧 "계산서 옆의 메모" 입니다.

# %% [18-16] 새 조각 없이 한 번 더 실행해 보십시오. 그 다음 체크포인트를 지우고 다시 실행해 보십시오.
print("그대로 다시:", run_raw())
shutil.rmtree("chk/s18/raw")
print("메모를 지우고:", run_raw())
# ── 결과 ──
# 그대로 다시: 804
#   배치 0: 804 줄
# 메모를 지우고: 1608
# → 그대로 다시 하면 배치가 아예 안 생기고 804 그대로(읽을 것이 없음). 체크포인트를 지우면 배치 0 에 804 줄을 처음부터 다시 읽어 1608 — 두
# 배. 메모를 잃은 직원이 처음 접시부터 다시 계산해 이중 청구한 것입니다. 체크포인트는 절대 지우지 않습니다. 그리고 STEP 17 에서 본 것처럼 쓰는 쪽을
# MERGE(멱등)로 만들면, 혹시 다시 읽더라도 결과가 두 배가 되지 않습니다 — 그것이 STEP 19 입니다.
