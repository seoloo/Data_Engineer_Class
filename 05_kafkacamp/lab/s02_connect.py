# ==============================================================================
#  STEP 02  Kafka 띄우기 — 컨테이너 하나, 실습 폴더 하나
#  C:\kafkacamp\lab\s02_connect.py
#  셀 안에 커서를 두고 Shift+Enter 로 실행합니다. 위에서부터 순서대로.
#  문제 아래 빈 줄에 직접 치십시오. 복사·붙여넣기 하지 않습니다.
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


# %% [02-5] 파이썬에서 Kafka 클러스터의 아이디를 꺼내 보십시오. ([준비] 셀을 먼저 실행)
admin.list_topics(timeout=5).cluster_id
# 브로커-토픽-파이션 정보 -> 메타데이터





# %% [02-6] 지금 Kafka 에 있는 토픽 목록을 보십시오.




# %% [02-7] 일부러 틀려 보십시오. 주소를 localhost:9093 (없는 문)으로 바꿔 붙어 보십시오.
