# ==============================================================================
#  STEP 05  파일 읽기 — CSV 와 JSON
#  C:\sparkcamp\lab\s05_read_files.py
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


# %% [05-1] 역 마스터를 옵션 없이 읽어 보여 주십시오.
# [셀 설명] CSV를 기본 옵션으로 읽었을 때 머리글도 데이터로 취급되는 모습을 확인한다.
# [동작] header를 지정하지 않으면 열 이름은 _c0 같은 기본 이름이고 첫 줄도 행에 포함된다.
st = spark.read.csv("data/stations.csv")
st.show()


# %% [05-1 바꿔 보기 ①] header 를 줄 때와 안 줄 때 줄 수를 나란히 세면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [셀 설명] 같은 CSV에서 header 옵션 유무가 행 수에 미치는 영향을 비교한다.
# [동작] 이 파일처럼 첫 줄이 머리글이면 header=True에서 그 줄을 열 이름으로 쓰므로 데이터 행 수가 하나 줄어든다.
print(spark.read.csv("data/stations.csv").count(),
      spark.read.csv("data/stations.csv", header=True).count())



# %% [05-2] 같은 파일을 머리글을 칸 이름으로 읽으십시오.
# [셀 설명] CSV 첫 줄을 열 이름으로 사용해 역 목록을 읽는다.
# [동작] header=True는 이름을 지정하는 옵션이다. 숫자 자료형 자동 추론까지 켜는 옵션은 아니므로 다음 셀에서 스키마도 확인한다.
st = spark.read.csv("data/stations.csv", header=True)
st.show()



# %% [05-3] 역 마스터의 스키마와 행 수를 보십시오.
# [셀 설명] 역 목록의 열 자료형과 데이터 행 수를 함께 확인한다.
# [동작] printSchema는 구조, count는 행 수를 확인한다. 원천을 읽은 뒤 두 가지를 확인하면 머리글·자료형 문제를 빨리 발견할 수 있다.
st.printSchema()
st.count()



# %% [05-4] 작은 로그 tags_mini.jsonl 을 읽어 칸을 자르지 말고 보여 주십시오.
# [셀 설명] 한 줄에 JSON 객체 한 개가 있는 JSONL 로그를 DataFrame으로 읽는다.
# [동작] spark.read.json은 JSON 필드들을 열로 만든다. 현재 show()는 기본 표시이므로 긴 값을 끝까지 보려면 truncate=False 옵션과 비교한다.
mini = spark.read.json("data/tags_mini.jsonl")
mini.show()





# %% [05-5] 작은 로그의 스키마를 보십시오.
# [셀 설명] JSON 값으로부터 자동 추론된 자료형을 확인한다.
# [동작] 시각처럼 보이는 JSON 문자열이 Timestamp로 읽혔는지 String으로 읽혔는지 살펴본다. 이후 STEP 07에서 명시적 스키마와 비교한다.

mini.printSchema()


# %% [05-6] 작은 로그가 몇 줄인지, 이벤트 번호는 몇 가지인지 세십시오.
# [셀 설명] 전체 로그 행 수와 서로 다른 이벤트 번호 개수를 비교해 중복 여부를 살핀다.
# [동작] select만으로 중복은 사라지지 않는다. event_id를 선택한 뒤 distinct와 count를 이어서 사건 번호 가짓수를 센다.
print(mini.count())
print(mini.select("event_id").distinct().count())



# %% [05-7] 하루 로그 tags_day.jsonl 을 day 라는 이름으로 읽고 몇 줄인지 세십시오.
# [셀 설명] 하루 로그 전체를 day라는 DataFrame으로 읽고 행 수를 센다.
# [동작] 여기서는 스키마를 자동 추론한다. count는 원본 태그 행 수이며 카드 수나 이동 수와는 다르다.
day = spark.read.json("data/tags_day.jsonl")
day.count()



# %% [05-8] 하루 로그의 앞 다섯 줄을 보십시오.
# [셀 설명] 하루 로그의 일부 행을 미리 보며 필드와 값의 모습을 확인한다.
# [동작] show(5)는 최대 다섯 행 표시다. orderBy가 없으므로 특정 정렬 순서가 보장되는 조회로 해석하지 않는다.
day.show(5)



# %% [05-9] 하루 로그에서 event_id · trip_id · tag_type · station_id · tag_time 다섯 칸만 다섯 줄 보십시오.
# [셀 설명] 확인하려는 다섯 열만 선택해 화면에 표시한다.
# [동작] select는 열 선택이고 show(5)는 결과 행 표시다. 열 선택과 행 개수 제한의 역할을 구분한다.
day.select("event_id", "trip_id", "tag_type", "station_id", "tag_time").show(5)



# %% [05-10] 일부러 틀려 보십시오. 파일 이름을 tags_dya.jsonl 로 잘못 쳐서 읽어 보십시오.
# [셀 설명] 파일 경로를 잘못 적었을 때 경로 관련 오류를 확인하는 연습이다.
# [동작] 현재 셀은 비어 있다. 파일 이름 철자와 작업 디렉터리를 점검하며, 상대 경로는 준비 셀에서 정한 C:\sparkcamp를 기준으로 읽힌다.




# %% [05-11] 폴더째 읽기 — data/stream_src 폴더(파일 10개)를 한 번에 읽어 세십시오.
# [셀 설명] 폴더 안 여러 JSONL 파일을 한 번의 읽기로 묶어 배치 데이터처럼 처리한다.
# [동작] spark.read.json에 디렉터리를 주면 그 안의 대상 파일들을 읽는다. stream_src라는 폴더 이름만으로 스트리밍 실행이 되는 것은 아니다.
whole = spark.read.json("data/stream_src")
whole.count()



# %% [05-12] 폴더 안에 파일이 몇 개인지 파이썬으로 세어 보십시오.
# [셀 설명] 파이썬으로 입력 폴더의 항목 목록과 개수를 확인한다.
# [동작] os.listdir은 파일 내용이 아닌 디렉터리 항목 이름을 반환하고 sorted는 이름순으로 정렬한다. 이 개수와 전체 데이터 행 수를 구분한다.
files = sorted(os.listdir("data/stream_src"))
print(len(files), files[:3])

# %%
