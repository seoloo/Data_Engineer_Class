# ==============================================================================
#  STEP 03  winutils 와 첫 실행 — Spark 에 시동 걸기
#  C:\sparkcamp\lab\s03_first_run.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
# ==============================================================================


# %% [03-1] 지금 쓰는 파이썬의 버전과 위치를 출력하십시오.
# [셀 설명] 현재 셀을 실행하는 Python의 버전과 실행 파일 경로를 확인한다.
# [동작] sys.version은 버전 정보, sys.executable은 인터프리터 경로다. VS Code에서 원본 폴더의 가상환경을 선택했는지 확인한다.
import sys
print(sys.version)
print(sys.executable)




# %% [03-2] JAVA_HOME 과 HADOOP_HOME 이 보이는지 출력하십시오.
# [셀 설명] 현재 작성된 코드는 SparkSession을 만들고 Spark 실행을 준비하는 부분이다.
# [동작] chdir는 상대 경로의 기준을 맞추고, PYSPARK_PYTHON은 작업용 Python을 현재 인터프리터로 지정한다. local[2]는 로컬 작업 스레드 두 개를 뜻한다.
# [확인] 제목은 환경변수 확인이지만 그 코드는 다음 03-3에 있다. getOrCreate는 기존 세션이 있으면 재사용하며 Spark 버전 출력으로 준비 상태를 확인한다.

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
# [셀 설명] 현재 작성된 코드는 Java와 Hadoop 도구의 설치 위치가 환경변수로 전달되는지 확인한다.
# [동작] os.environ.get은 환경변수 값을 읽으며 없으면 None을 반환한다. 설치 후 실행 중이던 VS Code는 재시작해야 새 환경변수가 반영될 수 있다.
# [확인] 제목의 SparkSession 생성 코드는 현재 03-2에 있으므로 실제 코드와 제목을 구분해서 읽는다.
import os
print(os.environ.get("JAVA_HOME"))
print(os.environ.get("HADOOP_HOME"))



# %% [03-4] Spark 의 버전을 물어보십시오.
# [셀 설명] 현재 SparkSession이 사용하는 Spark의 버전을 확인한다.
# [동작] spark.version은 속성이므로 괄호 없이 읽는다. 패키지 설치 버전과 실행 중인 세션 버전을 비교할 때 사용한다.
spark.version



# %% [03-5] 첫 계산 — 0 부터 999,999 까지 백만 개의 숫자를 만들고 개수를 세십시오.
# [셀 설명] range로 연속 정수 DataFrame을 만들고 count 액션으로 실제 계산을 실행한다.
# [동작] range(n)은 0부터 n-1까지 만든다. 현재 코드는 10,000,000건으로, 제목의 백만 건과 다르게 값을 바꿔 놓은 상태다.
spark.range(10000000).count()




# %% [03-6] Spark UI(계기판) 주소를 출력하고, 브라우저로 열어 보십시오.
# [셀 설명] 현재 Spark 애플리케이션의 작업 현황을 볼 수 있는 Spark UI 주소를 확인한다.
# [동작] uiWebUrl을 브라우저에서 열면 Jobs·Stages·Storage 등을 볼 수 있다. 세션이 끝나면 이 실행 중 UI도 사용할 수 없게 된다.
print(spark.sparkContext.uiWebUrl)





# %% [03-7] 일부러 틀려 보십시오. count 를 cont 로 잘못 쳐 보십시오.
# [셀 설명] 존재하지 않는 메서드를 호출했을 때 오류 메시지에서 오타를 찾는 연습이다.
# [동작] 현재 셀은 비어 있다. count와 cont의 차이를 확인하고, 의도한 오류는 다른 정상 셀과 따로 실행한다.




# %% [03-8] 통역사 확인 — 숫자 세 개를 파일로 저장하고, 생긴 파일 목록을 보십시오.
# [셀 설명] 작은 DataFrame을 CSV로 저장해 Windows의 Spark 파일 쓰기 환경을 확인한다.
# [동작] csv의 경로는 결과 디렉터리다. overwrite는 기존 결과를 대체하며, part 파일들과 관리용 파일이 생길 수 있다.
spark.range(3).write.mode("overwrite").csv("out/check")
print([f for f in sorted(os.listdir("out/check")) if not f.startswith(".")])



# %% [03-8 바꿔 보기 ①] 숫자를 하나만 쓰면? — range(3) 을 range(1) 로, 폴더는 out/check1
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 행 수가 한 개일 때 실제 생성된 데이터 파일과 각 파일의 내용을 관찰한다.
# [동작] 행 수와 파티션 수는 서로 다르므로 한 행이라고 반드시 한 파일만 생기는 것은 아니다. part로 시작하는 데이터 파일을 골라 읽는다.

spark.range(1).write.mode("overwrite").csv("out/check1")
for f in sorted(os.listdir("out/check1")):
    if f.startswith("part"):
        print(f[:10], "->", open("out/check1/" + f).read().split())


# %% [03-8 바꿔 보기 ②] 파일 하나로 받고 싶으면? — 쓰기 전에 coalesce(1) 을 끼운다
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 저장 전 파티션 수를 조절하면 결과 파일 구성에 어떤 영향이 있는지 확인한다.
# [동작] 현재 코드는 range(10).coalesce(2)이므로 제목의 한 파일 실험과 다르다. 한 파티션으로 줄이는 실험에서는 coalesce(1)과 비교한다.

spark.range(10).coalesce(2).write.mode("overwrite").csv("out/check1")
for f in sorted(os.listdir("out/check1")):
    if f.startswith("part"):
        print(f[:10], "->", open("out/check1/" + f).read().split())


# %% [03-8 바꿔 보기 ③] overwrite 대신 append 로 두 번 쓰면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 덮어쓰기와 이어 쓰기를 비교하고 최종 저장된 행 수를 확인한다.
# [동작] 현재는 50행을 overwrite한 뒤 50행을 append하므로 저장 내용은 합계 100행이 된다. 파일 개수와 데이터 행 수를 따로 센다.
spark.range(50).write.mode("overwrite").csv("out/check3")
spark.range(50).write.mode("append").csv("out/check3")
parts = [ f for f in os.listdir("out/check3") if f.startswith("part")]
print(len(parts), "개 파일 / ", spark.read.csv("out/check3").count(), "줄")





# %% [03-9] 저장한 파일을 다시 읽어 개수를 세십시오.
# [셀 설명] 저장한 CSV 디렉터리를 다시 읽어 쓰기 전후 데이터 행 수를 비교한다.
# [동작] spark.read.csv는 디렉터리 안의 데이터 파일들을 읽는다. count로 전체 행 수를 세어 저장이 의도대로 되었는지 확인한다.
spark.read.csv("out/check").count()



# %%
