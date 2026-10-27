# ==============================================================================
#  STEP 08  키 설계 — 무엇으로 나누느냐가 속도와 순서를 정한다
#  C:\kafkacamp\answer\s08_keys.py
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


# %% [초기화] 이 STEP 을 처음 시작할 때 한 번만 — 결과 폴더를 비웁니다
#    중간에 다시 누르면 지금까지 만든 결과가 지워집니다. 처음부터 다시 할 때만 누르십시오.
old = [t for t in ["by_card", "by_station", "by_card6", "by_station6", "probe3"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)
# ── 결과 ──
# 지운 토픽: []


# %% [08-1] 키 C001 을 tags3(지금 파티션 4개)에 파이썬 기본 프로듀서와 murmur2 프로듀서로 한 번씩 보내고, 각각 몇 번 파티션에 갔는지 보십시오.
def where(err, msg):
    print(msg.key().decode(), "→ 파티션", msg.partition())

p_default = Producer({"bootstrap.servers": BOOT})
p_default.produce("tags3", key="C001", value="from python default", on_delivery=where)
p_default.flush()

p = Producer({"bootstrap.servers": BOOT, "partitioner": "murmur2_random"})
p.produce("tags3", key="C001", value="from python murmur2", on_delivery=where)
p.flush()
# ── 결과 ──
# C001 → 파티션 2
# C001 → 파티션 1
# 0
# → 터미널에서 보낸 C001(04-11)은 1번이었습니다. 파이썬 기본은 다른 번호, murmur2 로 맞춘 쪽은 1번 — 터미널과 같습니다. 앞으로 이
# 과목의 파이썬 프로듀서는 전부 두 번째 모양(p)으로 만듭니다.

# %% [08-1 바꿔 보기 ①] 키 없이 4줄을 murmur2_random 프로듀서로 보내면?
got = []
for i in range(4):
    p.produce("tags3", value=f"nokey {i}", on_delivery=lambda e, m: got.append(m.partition()))
p.flush()
got
# ── 결과 ──
# [3, 3, 3, 3]
# → 이래서 — 키가 없으면 murmur2 를 쓸 수 없어 random 쪽 규칙이 쓰입니다 — 한 장부에 몰아 보내다 옮기는 방식(06-4 바꿔 보기)이라
# 4줄이 한 장부에 들어가기 쉽고, 어느 장부인지는 실행할 때마다 다릅니다. murmur2_random = 키가 있으면 murmur2, 없으면 무작위.

# %% [08-2] 토픽의 파티션별 메시지 수를 돌려주는 함수 counts(토픽) 을 만들고 tags3 에 써 보십시오.
def counts(topic):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "counter"})
    parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions)
    out = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part))
        out.append(hi - lo)
    c.close()
    return out

counts("tags3")
# ── 결과 (바꿔 보기의 4줄이 어느 장부에 몰렸느냐에 따라 PC 마다 다름) ──
# [3, 4, 13, 5]
# → 장부 4개의 개수가 리스트로 나옵니다. 05-6 의 LOG-END-OFFSET(3 · 3 · 12 · 1)에 08-1 에서 보낸 두 줄(2번에 하나,
# 1번에 하나)과 바꿔 보기에서 키 없이 보낸 4줄(한 장부에 몰림)이 더해진 숫자입니다. 바꿔 보기를 안 했으면 [3, 4, 13, 1] 입니다. 이 함수를
# 이 STEP 내내 씁니다.

# %% [08-3] 파티션 3개짜리 토픽 by_card 와 by_station 을 만들고, 하루 로그 2,008줄을 각각 카드 번호 키 · 역 코드 키로 보내십시오.
for t in ["by_card", "by_station"]:
    admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)])[t].result()

with open("data/tags_day.jsonl", encoding="utf-8") as f:
    day = [json.loads(line) for line in f]

for rec in day:
    v = json.dumps(rec)
    p.produce("by_card", key=rec["card_id"], value=v)
    p.produce("by_station", key=rec["station_id"], value=v)
p.flush()
print(len(day))
print("by_card   :", counts("by_card"))
print("by_station:", counts("by_station"))
# ── 결과 ──
# 2008
# by_card   : [686, 698, 624]
# by_station: [743, 489, 776]
# → 둘 다 합계 2,008 인데 나뉜 모양이 다릅니다. 카드 키는 624 ~ 698 로 비슷하고, 역 키는 489 ~ 776 — 가장 무거운 장부가 가장
# 가벼운 장부의 1.6배입니다. 장부 하나를 컨슈머 하나가 맡으니(05-9), 역 키로 나누면 가장 바쁜 컨슈머가 전체 속도를 정합니다.

# %% [08-4] 역 코드가 어느 장부로 가는지 확인하십시오. by_station 과 같은 3개짜리 임시 토픽 probe3 에 역마다 한 줄씩 보내고, 전달 보고로 장부 번호를
# 모읍니다.
admin.create_topics([NewTopic("probe3", num_partitions=3, replication_factor=1)])["probe3"].result()
st_part = {}
def remember(err, msg):
    st_part[msg.key().decode()] = msg.partition()

for s in sorted({rec["station_id"] for rec in day}):
    p.produce("probe3", key=s, value="x", on_delivery=remember)
p.flush()
for part in range(3):
    print(part, [s for s, q in sorted(st_part.items()) if q == part])
# ── 결과 ──
# 0 ['S01', 'S06', 'S08']
# 1 ['S02', 'S04']
# 2 ['S03', 'S05', 'S07', 'S99']
# → 역 아홉 곳(S99 포함)이 세 장부에 나뉜 모양입니다. 파티션 수가 같으면 같은 키는 어느 토픽에서나 같은 번호로 갑니다(murmur2 % 3).
# 08-3 에서 무거웠던 장부에 붐비는 역이 몇 곳 겹쳤는지 1과목 08-2 의 역별 승차 수와 같이 보십시오.

# %% [08-5] 장부를 6개로 늘린 by_card6 · by_station6 를 만들어 같은 2,008줄을 보내고 비교하십시오.
for t in ["by_card6", "by_station6"]:
    admin.create_topics([NewTopic(t, num_partitions=6, replication_factor=1)])[t].result()
for rec in day:
    v = json.dumps(rec)
    p.produce("by_card6", key=rec["card_id"], value=v)
    p.produce("by_station6", key=rec["station_id"], value=v)
p.flush()
print("by_card6   :", counts("by_card6"))
print("by_station6:", counts("by_station6"))
# ── 결과 ──
# by_card6   : [297, 375, 261, 389, 323, 363]
# by_station6: [743, 0, 522, 0, 489, 254]
# → 카드 키는 여섯 장부가 261 ~ 389 로 여전히 고르고, 역 키는 0 인 장부가 둘(노는 컨슈머 둘)이고 743 짜리 장부는 그대로입니다 — 늘린
# 장부가 무거운 장부를 나눠 주지 못했습니다. 키의 가짓수가 적으면 파티션을 늘려도 소용없습니다. 파티션 수 ≤ 키 가짓수, 그리고 키가 고르게 퍼지는지가
# 설계의 핵심입니다.

# %% [08-6] 일부러 틀려 보십시오. 숫자(정수)를 키로 넣어 보십시오.
p.produce("by_card", key=401, value="x")
# ── 결과 (오류 — 일부러 틀린 것) ──
# TypeError: a bytes-like object is required, not 'int'
# → TypeError — 키도 값과 마찬가지로 글자나 바이트만. 카드 번호가 숫자라도 str(401) 로 바꿔 넣습니다.
