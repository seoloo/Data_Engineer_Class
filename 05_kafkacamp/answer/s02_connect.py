# ==============================================================================
#  STEP 02  Kafka 띄우기 — 컨테이너 하나, 실습 폴더 하나
#  C:\kafkacamp\answer\s02_connect.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  정답 파일입니다. 먼저 lab 파일에 혼자 써 본 다음에 여십시오.
#  '# ── 결과' 아래 주석은 작성 환경에서 실제로 실행한 출력입니다.
# ==============================================================================

# %% [준비] 실행만 하세요 — 이 파일을 열면 맨 처음 한 번 (Shift+Enter)
import os, sys, json, time
os.chdir(r"C:\kafkacamp")
sys.path.insert(0, os.getcwd())          # tools\replay.py 를 import 하려고
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
BOOT = "localhost:9092"
admin = AdminClient({"bootstrap.servers": BOOT})
print("Kafka 연결:", admin.list_topics(timeout=5).brokers)
# ── 결과 ──
# Kafka 연결: {1: BrokerMetadata(1, localhost:9092)}


# %% [02-5] 파이썬에서 Kafka 클러스터의 아이디를 꺼내 보십시오. ([준비] 셀을 먼저 실행)
admin.list_topics(timeout=5).cluster_id
# ── 결과 ──
# '4L6g3nShT-eMCtK--X86sw'
# → kafka1.yml 의 CLUSTER_ID 와 같은 글자입니다. 파이썬(내 PC) → localhost:9092 → 포트 연결 → 컨테이너 안 Kafka,
# 이 길이 뚫렸다는 뜻입니다. list_topics 는 이름과 달리 브로커 · 토픽 · 파티션 정보를 한 번에 받아 오는 함수입니다(이 묶음을 메타데이터라고
# 부릅니다).

# %% [02-6] 지금 Kafka 에 있는 토픽 목록을 보십시오.
admin.list_topics(timeout=5).topics
# ── 결과 ──
# {}
# → {} — 아직 아무것도 없습니다. 다음 STEP 에서 첫 토픽을 만듭니다.

# %% [02-7] 일부러 틀려 보십시오. 주소를 localhost:9093 (없는 문)으로 바꿔 붙어 보십시오.
bad = AdminClient({"bootstrap.servers": "localhost:9093"})
bad.list_topics(timeout=5)
# ── 결과 (오류 — 일부러 틀린 것) ──
# KafkaException: KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}
# → Broker transport failure — 그 주소에 Kafka 가 없다. 5초(timeout=5) 동안 문을 두드리다가 포기합니다. 수업 중에 이
# 오류가 나면 ① Docker Desktop 이 켜져 있나 ② docker ps 에 kafka 가 Up 인가 ③ 주소가 localhost:9092 인가 세
# 가지를 봅니다.
