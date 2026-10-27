# ==============================================================================
#  STEP 06  파이썬 프로듀서 — confluent-kafka 로 보내기
#  C:\kafkacamp\answer\s06_producer.py
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
old = [t for t in ["mini", "mini_nokey"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)
# ── 결과 ──
# 지운 토픽: []


# %% [06-1] 파티션 3개짜리 토픽 mini 를 파이썬으로 만들고, 만들어졌는지 확인하십시오.
fs = admin.create_topics([NewTopic("mini", num_partitions=3, replication_factor=1)])
fs["mini"].result()
names = sorted(t for t in admin.list_topics(timeout=5).topics if not t.startswith("__"))
print(names)
# ── 결과 ──
# ['hello', 'mini', 'tags3']
# → create_topics 는 "만들어 달라" 는 요청을 보내고 영수증(future) 을 돌려줍니다. .result() 는 "다 만들어질 때까지 기다려라".
# __ 로 시작하는 이름(__consumer_offsets)은 Kafka 가 내부에서 쓰는 토픽이라 뺐습니다 — 컨슈머 그룹의 책갈피(STEP 05)가 바로 이
# 토픽에 저장됩니다.

# %% [06-2] 프로듀서를 만들고, hello 토픽에 글자 한 줄을 보내십시오. 그리고 flush 하십시오.
p = Producer({"bootstrap.servers": BOOT})
p.produce("hello", value="hello from python")
p.flush()
# ── 결과 ──
# 0
# → 0 — 못 보낸 것이 0개. STEP 03 의 콘솔 컨슈머로 hello 를 보면 이 줄이 맨 끝에 있습니다. 파이썬과 터미널이 같은 장부를 쓰고 있습니다.

# %% [06-3] tags_mini.jsonl 의 첫 줄을 사전으로 읽고, 카드 번호를 키로 mini 에 보내십시오.
with open("data/tags_mini.jsonl", encoding="utf-8") as f:
    first = json.loads(f.readline())
print(first["event_id"], first["card_id"], first["tag_type"])
p.produce("mini", key=first["card_id"], value=json.dumps(first))
p.flush()
# ── 결과 ──
# M01 C001 IN
# 0
# → M01 C001 IN, 그리고 0. json.dumps 로 사전을 글자로 바꿔 보냅니다. 받는 쪽은 json.loads 로 다시 사전으로 만듭니다(STEP
# 07). Kafka 는 가운데에서 글자(바이트)만 옮깁니다.

# %% [06-4] 전달 보고 함수 report 를 만들고, 그 함수를 붙여서 tags_mini 의 나머지 11줄을 보내십시오.
def report(err, msg):
    if err:
        print("실패:", err)
    else:
        print(msg.key().decode(), "→ 파티션", msg.partition(), "오프셋", msg.offset())

with open("data/tags_mini.jsonl", encoding="utf-8") as f:
    lines = f.readlines()[1:]
for line in lines:
    rec = json.loads(line)
    p.produce("mini", key=rec["card_id"], value=line.strip(), on_delivery=report)
print("대기열:", len(p))
p.flush()
# ── 결과 (파티션 묶음이 찍히는 순서는 실행할 때마다 다를 수 있음) ──
# 대기열: 11
# C003 → 파티션 0 오프셋 0
# C003 → 파티션 0 오프셋 1
# C002 → 파티션 1 오프셋 0
# C005 → 파티션 1 오프셋 1
# C006 → 파티션 1 오프셋 2
# C005 → 파티션 1 오프셋 3
# C002 → 파티션 1 오프셋 4
# C005 → 파티션 1 오프셋 5
# C004 → 파티션 2 오프셋 1
# C004 → 파티션 2 오프셋 2
# C001 → 파티션 2 오프셋 3
# 0
# → 대기열: 11 이 먼저 찍히고, 영수증 11장이 flush 하는 동안 찍힙니다. 영수증은 produce 할 때가 아니라 도착했을 때 옵니다. 영수증이
# 파티션별로 묶여 오는 것도 보십시오 — 라이브러리가 같은 장부로 갈 것끼리 모아서(배치) 보냈기 때문입니다(묶음이 오는 순서는 실행할 때마다 다를 수 있음).
# 같은 카드(C005 · C003 …)의 줄은 언제나 같은 파티션 — 키의 힘(STEP 04). 파일의 줄을 그대로(line.strip()) 보내도 됩니다.
# 이미 JSON 글자이기 때문입니다.

# %% [06-4 바꿔 보기 ①] 키 없이 보내면? — 같은 11줄을 key 없이 새 토픽 mini_nokey 에
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
c = Consumer({"bootstrap.servers": BOOT, "group.id": "counter"})
for part in range(3):
    lo, hi = c.get_watermark_offsets(TopicPartition("mini", part))
    print("파티션", part, ":", hi - lo, "개")
c.close()
# ── 결과 ──
# 파티션 0 : 2 개
# 파티션 1 : 6 개
# 파티션 2 : 4 개
# → 합계 12. 터미널의 kafka-get-offsets.sh 와 같은 일입니다. hi(끝 번호) − lo(처음 번호) = 쌓인 개수. 이 세 줄을 STEP
# 08 에서 함수로 만들어 계속 씁니다.

# %% [06-6] 대기열이 정말 비동기인지 보십시오. 1,000개를 produce 한 직후의 대기열 수와, flush 후의 대기열 수를 찍으십시오.
for i in range(1000):
    p.produce("hello", value=f"bulk {i}")
print("produce 직후 대기열:", len(p))
p.flush()
print("flush 후 대기열:", len(p))
# ── 결과 (첫 줄의 숫자는 PC 마다 조금 다를 수 있음) ──
# produce 직후 대기열: 1000
# flush 후 대기열: 0
# → produce 를 1,000번 했는데 직후에는 대부분 아직 대기열에 있습니다(숫자는 PC 마다 조금 다를 수 있음). flush 후에는 0. 그래서
# 프로그램 끝에는 반드시 flush — 빼먹으면 대기열에 남은 메시지는 보내지지 않고 사라집니다.

# %% [06-7] 일부러 틀려 보십시오. 사전을 json.dumps 없이 그대로 value 에 넣어 보십시오.
p.produce("mini", key=first["card_id"], value=first)
# ── 결과 (오류 — 일부러 틀린 것) ──
# TypeError: a bytes-like object is required, not 'dict'
# → TypeError — value 는 글자나 바이트여야 한다. Kafka 에는 "사전" 이라는 개념이 없습니다. 보내기 전에 직렬화(serialize) —
# 약속한 모양의 글자로 바꾸기 — 를 해야 합니다. 이 과목의 약속은 JSON 한 줄입니다.
