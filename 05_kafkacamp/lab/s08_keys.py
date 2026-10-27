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
def where(err, msg):
    if err:
        print("실패")
    
    print(msg.key().decode(), "-> 파티션", msg.partition())
    

p_default = Producer({"bootstrap.servers" : BOOT})
p_default.produce("tag3", key = "C001", value = "from python default", on_delivery = where)
p_default.flush()   # 몇 개 남았는지

p = Producer({"bootstrap.servers" : BOOT, "partitioner" : "murmur2_random"})
p.produce("tag3", key = "C001", value = "from python murmur2", on_delivery = where)
p.flush()



# %% [08-1 바꿔 보기 ①] 키 없이 4줄을 murmur2_random 프로듀서로 보내면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# 간단히 보내기 (즉시 처리) / 키 없이 즉시

got = []
for i in range(4):
    p.produce("tags3", value = f"nokey {i}", on_delivery = lambda e, m:got.append(m.partition()))
p.flush()
got
print(got)
 

# %% [08-2] 토픽의 파티션별 메시지 수를 돌려주는 함수 counts(토픽) 을 만들고 tags3 에 써 보십시오.
def counts(topic):
    c = Consumer({"bootstrap.servers" : BOOT, "group.id" : "counter"})
    parts = sorted(c.list_topics(topic, timeout = 5).topics[topic].partitions)
    out = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part))
        out.append(hi - lo)
    c.close()
    return out

counts("tag3")

# %% [08-3] 파티션 3개짜리 토픽 by_card 와 by_station 을 만들고, 하루 로그 2,008줄을 각각 카드 번호 키 · 역 코드 키로 보내십시오.
for t in ["by_card", "by_station"]:
    admin.create_topics([NewTopic(t, num_partitions = 3, replication_factor = 1)])[t].result()

with open("data/tags_day.jsonl", encoding = "utf-8") as f:
    day = [json.loads(line) for line in f]

for rec in day:
    v = json.dumps(rec)
    p.produce("by_card", key = rec["card_id"], value = v)
    p.produce("by_station", key = rec["station_id"], value = v)

p.flush()

print(len(day))
print("by_card : ", counts("by_card"))
print("by_station : ", counts("by_station"))


# %% [08-4] 역 코드가 어느 장부로 가는지 확인하십시오. by_station 과 같은 3개짜리 임시 토픽 probe3 에 역마다 한 줄씩 보내고, 전달 보고로 장부 번호를
# 모읍니다.
admin.create_topics([NewTopic("probe3", num_partitions = 3, replication_factor = 1)])["probe3"].result()
st_part = {}        # 왜 set을 사용하나? !!!! -> ㅈㄴ 중요!!! -> 면접에 나온다.

def remember(err, msg):
    st_part[msg.key().decode()] = msg.partition()
    
for s in sorted({rec["station_id"] for rec in day}):
    p.produce("probe3", key = s, value = "x", on_delivery = remember)
    
p.flush()

for part in range(3):
    print(part, [s for s, q in sorted(st_part.items()) if q == part])


# %% [08-5] 장부를 6개로 늘린 by_card6 · by_station6 를 만들어 같은 2,008줄을 보내고 비교하십시오.
for t in ["by_card6", "by_station6"]:
    admin.create_topics([NewTopic(t, num_partitions = 6, replication_factor = 1)])[t].result()
    
for rec in day:
    v = json.dumps(rec)
    p.produce("by_card6", key = rec["card_id"], value = v)
    p.produce("by_station6", key = rec["station_id"], value = v)
    
p.flush()

print("by_card6         :", counts("by_card6"))
print("by_station6      :", counts("by_station6"))




# %% [08-6] 일부러 틀려 보십시오. 숫자(정수)를 키로 넣어 보십시오.





