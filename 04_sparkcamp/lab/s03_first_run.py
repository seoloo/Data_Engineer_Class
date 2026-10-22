# ==============================================================================
#  STEP 03  winutils 와 첫 실행 — Spark 에 시동 걸기
#  C:\sparkcamp\lab\s03_first_run.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================


# %% [03-1] 지금 쓰는 파이썬의 버전과 위치를 출력하십시오.
import sys
print(sys.version)
print(sys.executable)




# %% [03-2] JAVA_HOME 과 HADOOP_HOME 이 보이는지 출력하십시오.

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




# %% [03-3] Spark 에 시동을 거십시오. 아래 준비 셀을 손으로 한 번 쳐 봅니다. (다음 STEP 부터는 파일 맨 위에 미리 들어 있습니다)
import os
print(os.environ.get("JAVA_HOME"))
print(os.environ.get("HADOOP_HOME"))



# %% [03-4] Spark 의 버전을 물어보십시오.
spark.version



# %% [03-5] 첫 계산 — 0 부터 999,999 까지 백만 개의 숫자를 만들고 개수를 세십시오.
spark.range(10000000).count()




# %% [03-6] Spark UI(계기판) 주소를 출력하고, 브라우저로 열어 보십시오.
print(spark.sparkContext.uiWebUrl)





# %% [03-7] 일부러 틀려 보십시오. count 를 cont 로 잘못 쳐 보십시오.




# %% [03-8] 통역사 확인 — 숫자 세 개를 파일로 저장하고, 생긴 파일 목록을 보십시오.
spark.range(3).write.mode("overwrite").csv("out/check")
print([f for f in sorted(os.listdir("out/check")) if not f.startswith(".")])



# %% [03-8 바꿔 보기 ①] 숫자를 하나만 쓰면? — range(3) 을 range(1) 로, 폴더는 out/check1
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.

spark.range(1).write.mode("overwrite").csv("out/check1")
for f in sorted(os.listdir("out/check1")):
    if f.startswith("part"):
        print(f[:10], "->", open("out/check1/" + f).read().split())


# %% [03-8 바꿔 보기 ②] 파일 하나로 받고 싶으면? — 쓰기 전에 coalesce(1) 을 끼운다
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.

spark.range(10).coalesce(2).write.mode("overwrite").csv("out/check1")
for f in sorted(os.listdir("out/check1")):
    if f.startswith("part"):
        print(f[:10], "->", open("out/check1/" + f).read().split())


# %% [03-8 바꿔 보기 ③] overwrite 대신 append 로 두 번 쓰면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
spark.range(50).write.mode("overwrite").csv("out/check3")
spark.range(50).write.mode("append").csv("out/check3")
parts = [ f for f in os.listdir("out/check3") if f.startswith("part")]
print(len(parts), "개 파일 / ", spark.read.csv("out/check3").count(), "줄")





# %% [03-9] 저장한 파일을 다시 읽어 개수를 세십시오.
spark.read.csv("out/check").count()



# %%
