# ==============================================================================
#  STEP 06  파이썬 프로듀서 — confluent-kafka 로 보내기
#  C:\kafkacamp\lab\s06_producer.py
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
old = [t for t in ["mini", "mini_nokey"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)


# %% [06-1] 파티션 3개짜리 토픽 mini 를 파이썬으로 만들고, 만들어졌는지 확인하십시오.
fs = admin.create_topics([NewTopic("mini", num_partitions=3, replication_factor=1)])
fs["mini"].result()
names = sorted(t for t in admin.list_topics(timeout = 5).topics if not t.startswith("__"))
print(names)



# %% [06-2] 프로듀서를 만들고, hello 토픽에 글자 한 줄을 보내십시오. 그리고 flush 하십시오.
p = Producer({"bootstrap.servers" : BOOT})
p.produce("hello", value="hello from python")
p.flush()




# %% [06-3] tags_mini.jsonl 의 첫 줄을 사전으로 읽고, 카드 번호를 키로 mini 에 보내십시오.
with open("data/tags_mini.jsonl", encoding = "utf-8") as f:
    first = json.loads(f.readline())
print(first["event_id"], first["card_id"], first["tag_type"])

p.produce("mini", key = first["card_id"], value = json.dumps(first))
p.flush()


# %% [06-4] 전달 보고 함수 report 를 만들고, 그 함수를 붙여서 tags_mini 의 나머지 11줄을 보내십시오.

def report(err, msg):
    if err:
        print("실패 : ", err)
    else:
        print(msg.key().decode(), "-> 파티션", msg.partition(), "오프셋", msg.offset())
        

with open("data/tags_mini.jsonl", encoding="utf-8") as f:
    lines = f.readlines()[1:]
    
for line in lines:
    rec = json.loads(line)
    p.produce("mini", key = rec["card_id"], value=line.strip(), on_delivery=report)

print("대기열 : ", len(p))
p.flush()



# %% [06-4 바꿔 보기 ①] 키 없이 보내면? — 같은 11줄을 key 없이 새 토픽 mini_nokey 에
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
admin.create_topics([NewTopic("mini_nokey", num_partitions=3, replication_factor=1)])["mini_nokey"].result()
parts = []
for line in lines:
    p.produce("mini_nokey", value=line.strip(), on_delivery=lambda e, m: parts.append(m.partition()))
p.flush()
print(sorted(parts))
# ── 결과 ──
# [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2]
# → 이래서 — 키가 없으면 라이브러리가 장부를 마음대로 고릅니다 — 한 장부에 몰아서 보내다가(sticky, 묶어 보내기 효율 때문) 다른 장부로 옮기기도
# 합니다. 강사 PC 에서는 세 장부에 4 · 4 · 3 으로 흩어졌고, 실행할 때마다 다릅니다(한 장부에 다 몰리기도 함). 같은 카드의 승차와 하차가 어느
# 장부로 갈지 보장이 없다는 뜻입니다. 순서가 중요한 데이터에는 반드시 키를 붙입니다. 순서가 상관없는 로그(웹 접속 기록 등)는 키 없이 보내도 됩니다.




# %% [06-5] mini 의 파티션마다 메시지가 몇 개인지 세십시오. (임시 컨슈머의 get_watermark_offsets — "장부의 처음 번호, 끝 번호")
c = Consumer({"bootstrap.servers" : BOOT, "group.id" : "counter"})
for part in range(3):
    lo, hi = c.get_watermark_offsets(TopicPartition("mini", part))
    print("파티션", part, ":", hi - lo, "개")   # [수정] hi - 10 → hi - lo
    # Hi(끝 번호) - lo(처음번호) = 쌓인 개수
c.close()



# %% [06-6] 대기열이 정말 비동기인지 보십시오. 1,000개를 produce 한 직후의 대기열 수와, flush 후의 대기열 수를 찍으십시오.
for i in range(1000):
    p.produce("hello", value = f"bulk {i}")
    
print("produce 직후 대기열 : ", len(p))
p.flush()
print("flush 후 대기열 : ", len(p))



# %% [06-7] 일부러 틀려 보십시오. 사전을 json.dumps 없이 그대로 value 에 넣어 보십시오.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
p.produce("mini", key=first["card_id"], value=first)
# ── 결과 (오류 — 일부러 틀린 것) ──
# TypeError: a bytes-like object is required, not 'dict'
# → TypeError — value 는 글자나 바이트여야 한다. Kafka 에는 "사전" 이라는 개념이 없습니다. 보내기 전에 직렬화(serialize) —
# 약속한 모양의 글자로 바꾸기 — 를 해야 합니다. 이 과목의 약속은 JSON 한 줄입니다.





