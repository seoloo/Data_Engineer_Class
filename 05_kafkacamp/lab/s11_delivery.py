# ==============================================================================
#  STEP 11  전달 보장 — 잃어버리기 · 두 번 세기 · 정확히 한 번
#  C:\kafkacamp\lab\s11_delivery.py
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

def counts(topic):                 # STEP 08 에서 만든 함수
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "counter"})
    parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions)
    out = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part))
        out.append(hi - lo)
    c.close()
    return out


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
old = [t for t in ["tx_demo", "tx_demo2"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
have = [g.group_id for g in admin.list_consumer_groups().result().valid]
gone = [g for g in ["g11a", "g11b", "g11c", "g11d", "g11-tx_demo-read_committed", "g11-tx_demo-read_uncommitted", "g11-tx_demo2-read_uncommitted"] if g in have]
for g in gone:
    admin.delete_consumer_groups([g])[g].result()
print("지운 그룹:", gone)
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)


# %% [11-1] 처리 결과를 담을 가짜 DB(리스트) 와, 장애를 흉내 낼 수 있는 처리 함수 run 을 만드십시오. when 이 "before" 면 커밋 → 처리,
# "after" 면 처리 → 100개마다 커밋. crash_at 번째 메시지에서 일부러 오류를 냅니다.




# %% [11-2] at-most-once — 그룹 g11a 로, 550번째 메시지에서 장애가 나게 돌리십시오.




# %% [11-3] 장애를 고치고 다시 켜서 끝까지 돌린 뒤, DB 의 줄 수와 이벤트 가짓수를 보십시오.




# %% [11-4] at-least-once — 새 그룹 g11b 로 "처리 → 100개마다 커밋" 을 하되, 같은 550번째에서 장애를 내고, 다시 켜서 끝까지 돌리십시오.




# %% [11-4 바꿔 보기 ①] 커밋 간격을 100 → 10 으로 줄이면 중복이 몇 개가 될까?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [11-5] exactly-once(효과적으로) — DB 를 event_id 를 키로 하는 사전으로 바꿔, 11-4 와 똑같이 장애를 내고 다시 돌리십시오. (사전에 같은
# 키로 넣으면 덮어쓴다 = 멱등)




# %% [11-6] 11-4 의 그룹 g11b 를 처음으로 되돌린 뒤 한 번 더 돌리면(재처리) 리스트 DB 와 사전 DB 는 각각 어떻게 될까요? 사전 쪽(g11c)으로
# 확인하십시오.




# %% [11-7] 프로듀서 쪽의 장치 — 멱등 프로듀서(enable.idempotence) 를 켜고 보내 보십시오.




# %% [11-8] 트랜잭션 — 세 줄을 한 묶음으로 보내다가 취소(abort) 하고, read_committed 컨슈머와 기본 컨슈머로 각각 읽어 보십시오.




# %% [11-8 바꿔 보기 ①] flush 없이 바로 abort 하면? — 새 토픽 tx_demo2 에
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
