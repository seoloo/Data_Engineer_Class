# ==============================================================================
#  STEP 08  키 설계 — 무엇으로 나누느냐가 속도와 순서를 정한다
#  C:\kafkacamp\lab\s08_keys.py
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


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
old = [t for t in ["by_card", "by_station", "by_card6", "by_station6", "probe3"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)


# %% [08-1] 키 C001 을 tags3(지금 파티션 4개)에 파이썬 기본 프로듀서와 murmur2 프로듀서로 한 번씩 보내고, 각각 몇 번 파티션에 갔는지 보십시오.




# %% [08-1 바꿔 보기 ①] 키 없이 4줄을 murmur2_random 프로듀서로 보내면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [08-2] 토픽의 파티션별 메시지 수를 돌려주는 함수 counts(토픽) 을 만들고 tags3 에 써 보십시오.




# %% [08-3] 파티션 3개짜리 토픽 by_card 와 by_station 을 만들고, 하루 로그 2,008줄을 각각 카드 번호 키 · 역 코드 키로 보내십시오.




# %% [08-4] 역 코드가 어느 장부로 가는지 확인하십시오. by_station 과 같은 3개짜리 임시 토픽 probe3 에 역마다 한 줄씩 보내고, 전달 보고로 장부 번호를
# 모읍니다.




# %% [08-5] 장부를 6개로 늘린 by_card6 · by_station6 를 만들어 같은 2,008줄을 보내고 비교하십시오.




# %% [08-6] 일부러 틀려 보십시오. 숫자(정수)를 키로 넣어 보십시오.
