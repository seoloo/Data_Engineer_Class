# ==============================================================================
#  STEP 18  Structured Streaming 입문 — 끝나지 않는 표
#  C:\sparkcamp\lab\s18_streaming.py
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

import shutil, time

def drop(n):
    """stream_src 의 n 번째 조각을 stream_in 으로 옮겨 놓는다 = 새 데이터가 도착했다"""
    name = f"part_{n:02d}.jsonl"
    shutil.copy(f"data/stream_src/{name}", f"data/stream_in/_{name}")      # 밑줄 이름으로 복사하고
    os.replace(f"data/stream_in/_{name}", f"data/stream_in/{name}")        # 다 되면 이름을 바꾼다
    print("도착:", name, "/ stream_in 파일 수:", len(os.listdir("data/stream_in")))

def ls(path):
    return [f for f in sorted(os.listdir(path)) if not f.startswith(".")]


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
for q in spark.streams.active:      # 돌고 있는 쿼리가 있으면 먼저 멈춘다
    q.stop()
for p in ["data/stream_in", "out/s18", "chk/s18"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs("data/stream_in")
print("stream_in · out/s18 · chk/s18 비움")


# %% [18-1] 첫 번째 조각을 도착시키십시오.




# %% [18-2] stream_in 폴더를 스트리밍으로 읽는 sdf 를 만들고, 스트리밍인지 물어보십시오.




# %% [18-3] 일부러 틀려 보십시오. 스키마 없이 readStream 을 하십시오.




# %% [18-4] 일부러 틀려 보십시오. sdf 의 줄 수를 count() 로 세십시오.




# %% [18-5] sdf 를 memory 싱크(live_raw 라는 연습용 표)로 3초마다 쓰는 쿼리를 시작하고, 처리가 끝나면 live_raw 를 세십시오.




# %% [18-6] 두 번째 조각을 도착시키고, 처리가 끝나면 다시 세십시오.




# %% [18-7] 쿼리가 지금까지 처리한 마이크로 배치마다 몇 줄씩 읽었는지 보십시오.




# %% [18-8] 쿼리를 멈추고, 아직 돌고 있는지 확인하십시오.




# %% [18-9] 실시간 전광판 — 승차만 골라 역별로 세는 쿼리를 complete 모드로 시작하고 결과를 보십시오.




# %% [18-10] 세 번째 조각을 도착시키고 전광판을 다시 보십시오.




# %% [18-11] 일부러 틀려 보십시오. 같은 집계를 append 모드로 시작하십시오.




# %% [18-12] 전광판 쿼리를 멈추십시오.




# %% [18-13] 파일로 쓰는 쿼리를 만드십시오. 새 묶음(마이크로 배치)이 올 때마다 save 함수가 불려서 out/s18/raw 폴더에 append 로 붙이게
# 하고(foreachBatch), 체크포인트는 chk/s18/raw, 있는 것만 처리하고 끝내기(availableNow). 한 번 실행하고 줄 수를 세십시오.




# %% [18-14] 네 번째 조각을 도착시키고 같은 함수를 다시 실행하십시오.




# %% [18-15] 체크포인트 폴더 안을 들여다보십시오. 그리고 offsets 폴더 안도 보십시오.




# %% [18-16] 새 조각 없이 한 번 더 실행해 보십시오. 그 다음 체크포인트를 지우고 다시 실행해 보십시오.
