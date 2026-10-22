# ==============================================================================
#  STEP 03  winutils 와 첫 실행 — Spark 에 시동 걸기
#  C:\sparkcamp\answer\s03_first_run.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================


# %% [03-1] 지금 쓰는 파이썬의 버전과 위치를 출력하십시오.
import sys
print(sys.version)
print(sys.executable)
# ── 결과 (학생 PC 화면 — 버전 숫자는 PC 마다 다름) ──
# 3.13.7 (tags/v3.13.7:bcee1c3, Aug 14 2025, 14:15:11) [MSC v.1944 64 bit (AMD64)]
# C:\sparkcamp\.venv\Scripts\python.exe
# → 위치에 .venv 가 들어 있어야 합니다. 다른 경로가 나오면 STEP 02 의 8번(인터프리터 선택)을 다시 하십시오.

# %% [03-2] JAVA_HOME 과 HADOOP_HOME 이 보이는지 출력하십시오.
import os
print(os.environ.get("JAVA_HOME"))
print(os.environ.get("HADOOP_HOME"))
# ── 결과 (학생 PC 화면) ──
# C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot\
# C:\hadoop
# → 둘 중 하나라도 None 이면 환경 변수가 안 잡힌 것입니다. VS Code 를 완전히 닫았다 여는 것을 빼먹은 경우가 대부분입니다.

# %% [03-3] Spark 에 시동을 거십시오. 아래 준비 셀을 손으로 한 번 쳐 봅니다. (다음 STEP 부터는 파일 맨 위에 미리 들어 있습니다)
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
# → Spark 4.1.3 준비 완료 가 나오면 성공입니다. 이 셀은 파일을 새로 열 때마다 맨 처음에 한 번 실행합니다.

# %% [03-4] Spark 의 버전을 물어보십시오.
spark.version
# ── 결과 ──
# '4.1.3'
# → 셀의 마지막 줄이 값이면 print 없이도 그 값이 보입니다. 주피터와 같습니다.

# %% [03-5] 첫 계산 — 0 부터 999,999 까지 백만 개의 숫자를 만들고 개수를 세십시오.
spark.range(1000000).count()
# ── 결과 ──
# 1000000
# → 준비 셀의 local[2](일꾼 두 명)가 백만 개를 반씩 나눠 셌습니다. 이 셈은 자바 엔진 안에서만 일어나서 파이썬 일꾼을 부르지 않습니다(04-1
# 에서 처음 부릅니다). 백만 개도 1초 안팎입니다.

# %% [03-6] Spark UI(계기판) 주소를 출력하고, 브라우저로 열어 보십시오.
print(spark.sparkContext.uiWebUrl)
# ── 결과 (학생 PC 화면 — PC 이름에 따라 다름) ──
# http://DESKTOP-ABC123:4040
# → 주소 앞부분은 PC 이름에 따라 다릅니다. 브라우저에는 localhost:4040 을 쳐도 같습니다. Jobs 탭에 방금 한 count 가 한 줄 보이면
# 계기판이 살아 있는 것입니다.

# %% [03-7] 일부러 틀려 보십시오. count 를 cont 로 잘못 쳐 보십시오.
spark.range(10).cont()
# ── 결과 (오류 — 일부러 틀린 것) ──
# PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute `cont` is not supported.
# → 빨간 글씨가 길게 나와도 맨 아래 한 줄만 읽습니다. ATTRIBUTE_NOT_SUPPORTED = "그런 기능은 없다" 입니다. 오타입니다.

# %% [03-8] 통역사 확인 — 숫자 세 개를 파일로 저장하고, 생긴 파일 목록을 보십시오.
spark.range(3).write.mode("overwrite").csv("out/check")
print([f for f in sorted(os.listdir("out/check")) if not f.startswith(".")])
# ── 결과 ──
# ['_SUCCESS', 'part-00000-303f1455-0e43-4f87-8213-3f56a335cfa3-c000.csv', 'part-00001-303f1455-0e43-4f87-8213-3f56a335cfa3-c000.csv']
# → part-0000... 로 시작하는 파일과 _SUCCESS 가 보이면 winutils 가 제대로 놓인 것입니다. 오류가 나면 부록 A 의
# HADOOP_HOME 줄을 보십시오. part 파일이 두 개 — 일꾼 두 명이 한 개씩 썼기 때문입니다. (점으로 시작하는 .crc 검사용 파일은 목록에서
# 뺐습니다)

# %% [03-8 바꿔 보기 ①] 숫자를 하나만 쓰면? — range(3) 을 range(1) 로, 폴더는 out/check1
spark.range(1).write.mode("overwrite").csv("out/check1")
for f in sorted(os.listdir("out/check1")):
    if f.startswith("part"):
        print(f[:10], "→", open("out/check1/" + f).read().split())
# ── 결과 ──
# part-00000 → []
# part-00001 → ['0']
# → 이래서 — part 파일은 여전히 두 개이고 하나는 비어 있습니다. 파일 수는 데이터 양이 아니라 일꾼 수(local[2]) 가 정합니다. 일꾼 둘이
# 출근했는데 일감이 하나뿐이라 한 명은 빈 종이를 냈습니다. STEP 14 의 "조각이 너무 많으면 빈 조각이 생긴다" 의 가장 작은 모습입니다.

# %% [03-8 바꿔 보기 ②] 파일 하나로 받고 싶으면? — 쓰기 전에 coalesce(1) 을 끼운다
spark.range(10).coalesce(1).write.mode("overwrite").csv("out/check2")
for f in sorted(os.listdir("out/check2")):
    if f.startswith("part"):
        print(f[:10], "→", open("out/check2/" + f).read().split())
# ── 결과 ──
# part-00000 → ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
# → 이래서 — coalesce(1) 은 "쓰기 전에 조각을 하나로 합쳐라" 입니다. 그래서 part 파일이 하나, 안에 0~9 가 다 들어 있습니다. 엑셀로
# 열어 볼 보고서처럼 파일 하나가 꼭 필요할 때 씁니다. 대신 일꾼 하나가 전부 쓰니 큰 데이터에서는 느립니다(STEP 14 · 15-11).

# %% [03-8 바꿔 보기 ③] overwrite 대신 append 로 두 번 쓰면?
spark.range(3).write.mode("overwrite").csv("out/check3")
spark.range(3).write.mode("append").csv("out/check3")
parts = [f for f in os.listdir("out/check3") if f.startswith("part")]
print(len(parts), "개 파일 /", spark.read.csv("out/check3").count(), "줄")
# ── 결과 ──
# 4 개 파일 / 6 줄
# → 이래서 — append 는 지우지 않고 파일을 옆에 더 놓습니다. 0, 1, 2 가 두 벌이 되어 6줄입니다. 이 "두 배가 되는 장면" 이 STEP
# 15-7(append 두 번) · STEP 18-16(체크포인트를 지우면 두 배) · STEP 17(그래서 MERGE) 까지 이어지는 이 과목의 복선입니다.
# 여기서 한 번 보여 두면 뒤에서 "아까 그거" 로 받을 수 있습니다.

# %% [03-9] 저장한 파일을 다시 읽어 개수를 세십시오.
spark.read.csv("out/check").count()
# ── 결과 ──
# 3
# → 3. 쓰고 → 읽고 → 세기. 오늘 설치는 여기까지입니다.
