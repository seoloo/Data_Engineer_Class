# ==============================================================================
#  STEP 11  전달 보장 — 잃어버리기 · 두 번 세기 · 정확히 한 번
#  C:\kafkacamp\answer\s11_delivery.py
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

def counts(topic):                 # STEP 08 에서 만든 함수
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "counter"})
    parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions)
    out = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part))
        out.append(hi - lo)
    c.close()
    return out
# ── 결과 ──
# Kafka 연결: {1: BrokerMetadata(1, localhost:9092)}


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
# ── 결과 ──
# 지운 그룹: []
# 지운 토픽: []


# %% [11-1] 처리 결과를 담을 가짜 DB(리스트) 와, 장애를 흉내 낼 수 있는 처리 함수 run 을 만드십시오. when 이 "before" 면 커밋 → 처리,
# "after" 면 처리 → 100개마다 커밋. crash_at 번째 메시지에서 일부러 오류를 냅니다.
def run(group, when, db, crash_at=None):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["tags"])
    n, empty = 0, 0
    try:
        while empty < 5:
            m = c.poll(1.0)
            if m is None:
                empty += 1
                continue
            empty = 0
            if when == "before":
                c.commit(message=m, asynchronous=False)      # 먼저 책갈피
            if crash_at is not None and n == crash_at:
                raise RuntimeError(f"장애! {n}번째 메시지 처리 중")
            db.append(json.loads(m.value())["event_id"])     # 처리 = DB 에 저장
            n += 1
            if when == "after" and n % 100 == 0:
                c.commit(asynchronous=False)                 # 처리한 뒤 100개마다 책갈피
        if when == "after" and n % 100:
            c.commit(asynchronous=False)                     # 남은 자투리도 책갈피
    finally:
        c.close()
    return n
# ── 결과 ──
# (출력 없음)
# → 정의만 했으니 출력이 없습니다. c.commit(message=m) 은 "이 메시지 다음 자리" 를 책갈피로 꽂습니다. finally 의 close 는
# 장애가 나도 컨슈머를 닫아 줍니다(그룹에서 빠지기).

# %% [11-2] at-most-once — 그룹 g11a 로, 550번째 메시지에서 장애가 나게 돌리십시오.
db_a = []
try:
    run("g11a", "before", db_a, crash_at=550)
except RuntimeError as e:
    print(e)
print("DB:", len(db_a))
# ── 결과 ──
# 장애! 550번째 메시지 처리 중
# DB: 550
# → 장애! 550번째 메시지 처리 중, DB 에는 550 개. 그런데 550번째 메시지의 책갈피는 이미 꽂혀 있습니다(먼저 커밋했으니까).

# %% [11-3] 장애를 고치고 다시 켜서 끝까지 돌린 뒤, DB 의 줄 수와 이벤트 가짓수를 보십시오.
run("g11a", "before", db_a)
print("DB:", len(db_a), "/ 이벤트 가짓수:", len(set(db_a)))
# ── 결과 ──
# DB: 2007 / 이벤트 가짓수: 1987
# → DB: 2007 — 2,008 중 1개를 잃었습니다. 장애 순간의 메시지는 책갈피는 넘어갔는데 저장은 안 됐습니다. 다시 켜도 책갈피 뒤부터 읽으니 영영
# 돌아오지 않습니다. 이벤트 가짓수 1987 — 잃은 1개가 진짜 이벤트였습니다(재전송 20줄 중 하나였다면 1988 이 남습니다. 파티션을 읽는 순서에 따라
# PC 마다 그럴 수 있습니다).

# %% [11-4] at-least-once — 새 그룹 g11b 로 "처리 → 100개마다 커밋" 을 하되, 같은 550번째에서 장애를 내고, 다시 켜서 끝까지 돌리십시오.
db_b = []
try:
    run("g11b", "after", db_b, crash_at=550)
except RuntimeError as e:
    print(e)
print("장애 때 DB:", len(db_b))
run("g11b", "after", db_b)
print("DB:", len(db_b), "/ 이벤트 가짓수:", len(set(db_b)))
# ── 결과 ──
# 장애! 550번째 메시지 처리 중
# 장애 때 DB: 550
# DB: 2058 / 이벤트 가짓수: 1988
# → 장애 때 550, 다시 돌린 뒤 2058. 마지막 책갈피가 500 에 있었으니, 500~549 의 50개를 두 번 처리했습니다. 잃은 것은 없지만(가짓수
# 1988) DB 에는 중복 50 + 원래 있던 재전송 20 = 70줄이 더 들어 있습니다.

# %% [11-4 바꿔 보기 ①] 커밋 간격을 100 → 10 으로 줄이면 중복이 몇 개가 될까?
def run10(group, db, crash_at=None):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["tags"])
    n, empty = 0, 0
    try:
        while empty < 5:
            m = c.poll(1.0)
            if m is None:
                empty += 1
                continue
            empty = 0
            if crash_at is not None and n == crash_at:
                raise RuntimeError("장애!")
            db.append(json.loads(m.value())["event_id"])
            n += 1
            if n % 10 == 0:
                c.commit(asynchronous=False)
        if n % 10:
            c.commit(asynchronous=False)
    finally:
        c.close()

db_d = []
try:
    run10("g11d", db_d, crash_at=555)
except RuntimeError:
    pass
run10("g11d", db_d)
print("DB:", len(db_d), "→ 장애로 생긴 중복:", len(db_d) - 2008)
# ── 결과 ──
# DB: 2013 → 장애로 생긴 중복: 5
# → 이래서 — 중복 5개 — 마지막 책갈피가 550 에 있어서 550~554 만 다시 했습니다. 커밋을 자주 할수록 중복은 줄지만, 커밋도 Kafka 와
# 주고받는 일이라 느려집니다(2,008번 커밋하면 2,008번 왕복). 중복을 0 으로 만드는 커밋 간격은 없습니다 — 그래서 결국 11-5 의 멱등 저장이
# 답입니다. Spark 는 이 간격을 마이크로 배치 하나로 잡습니다(STEP 15).

# %% [11-5] exactly-once(효과적으로) — DB 를 event_id 를 키로 하는 사전으로 바꿔, 11-4 와 똑같이 장애를 내고 다시 돌리십시오. (사전에 같은
# 키로 넣으면 덮어쓴다 = 멱등)
def run_idem(group, db, crash_at=None):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["tags"])
    n, empty = 0, 0
    try:
        while empty < 5:
            m = c.poll(1.0)
            if m is None:
                empty += 1
                continue
            empty = 0
            if crash_at is not None and n == crash_at:
                raise RuntimeError(f"장애! {n}번째 메시지 처리 중")
            rec = json.loads(m.value())
            db[rec["event_id"]] = rec                     # 키로 덮어쓰기 (upsert)
            n += 1
            if n % 100 == 0:
                c.commit(asynchronous=False)
        if n % 100:
            c.commit(asynchronous=False)
    finally:
        c.close()

db_c = {}
try:
    run_idem("g11c", db_c, crash_at=550)
except RuntimeError as e:
    print(e)
run_idem("g11c", db_c)
print("DB:", len(db_c))
# ── 결과 ──
# 장애! 550번째 메시지 처리 중
# DB: 1988
# → DB: 1988 — 1과목의 "중복 제거 후 이벤트 수" 와 같습니다. 장애로 다시 받은 50개도, 개찰구의 재전송 20개도 같은 칸을 다시 덮었을
# 뿐입니다. 다시 읽기(at-least-once) + 멱등 저장 = 정확히 한 번의 결과. STEP 15 에서 Spark 의 체크포인트 + Delta MERGE
# 로 똑같이 합니다.

# %% [11-6] 11-4 의 그룹 g11b 를 처음으로 되돌린 뒤 한 번 더 돌리면(재처리) 리스트 DB 와 사전 DB 는 각각 어떻게 될까요? 사전 쪽(g11c)으로
# 확인하십시오.
from confluent_kafka import ConsumerGroupTopicPartitions
tps = [TopicPartition("tags", p, 0) for p in range(3)]
admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions("g11c", tps)])["g11c"].result()
run_idem("g11c", db_c)
print("재처리 뒤 DB:", len(db_c))
# ── 결과 ──
# 재처리 뒤 DB: 1988
# → 여전히 1988. 2,008줄을 통째로 한 번 더 받았는데도 결과가 그대로입니다(리스트였다면 4,066). STEP 05-8 의 "오프셋 되돌리기" 를
# 파이썬으로 한 것입니다(alter_consumer_group_offsets). 재처리가 무섭지 않은 시스템 — 이것이 멱등의 진짜 쓸모입니다.

# %% [11-7] 프로듀서 쪽의 장치 — 멱등 프로듀서(enable.idempotence) 를 켜고 보내 보십시오.
pi = Producer({"bootstrap.servers": BOOT, "partitioner": "murmur2_random",
               "enable.idempotence": True})
pi.produce("hello", value="sent once, even if retried")
pi.flush()
# ── 결과 ──
# 0
# → 0 — 겉으로는 똑같습니다. 차이는 네트워크가 흔들려 프로듀서가 다시 보낼 때 나옵니다. 멱등 프로듀서는 메시지마다 번호를 붙여 보내서, 브로커가 같은
# 번호를 두 번 받으면 하나를 버립니다. 즉 프로듀서 → Kafka 구간의 중복을 막습니다(Kafka → 컨슈머 구간은 11-5 처럼 받는 쪽이 막음). 자바
# 프로듀서(Spark · 터미널)는 Kafka 4 에서 기본이 켜짐, confluent-kafka 는 직접 켭니다.

# %% [11-8] 트랜잭션 — 세 줄을 한 묶음으로 보내다가 취소(abort) 하고, read_committed 컨슈머와 기본 컨슈머로 각각 읽어 보십시오.
admin.create_topics([NewTopic("tx_demo", num_partitions=1, replication_factor=1)])["tx_demo"].result()
pt = Producer({"bootstrap.servers": BOOT, "transactional.id": "tx-11"})
pt.init_transactions()
pt.begin_transaction()
for i in range(3):
    pt.produce("tx_demo", value=f"tx message {i}")
pt.flush()                        # 세 줄을 브로커까지 실제로 보낸 뒤
pt.abort_transaction()            # 취소!

def read_n(iso, topic="tx_demo"):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"g11-{topic}-{iso}",
                  "auto.offset.reset": "earliest", "isolation.level": iso})
    c.subscribe([topic])
    got, empty = 0, 0
    while empty < 5:
        m = c.poll(1.0)
        if m is None:
            empty += 1
            continue
        got += 1
    c.close()
    return got

print("read_committed  :", read_n("read_committed"))
print("read_uncommitted:", read_n("read_uncommitted"))
# ── 결과 ──
# read_committed  : 0
# read_uncommitted: 3
# → read_committed 는 0, read_uncommitted 는 3. 취소된 세 줄은 장부에 적히긴 했지만 "취소됨" 표시가 붙어서, 커밋된 것만
# 읽는 컨슈머에게는 보이지 않습니다. "전부 보이거나, 전부 안 보이거나" — Spark 가 결과를 Kafka 로 쓸 때와 프로젝트 3단계 발표에서 쓰는
# 단어입니다. (confluent-kafka 컨슈머의 기본값이 read_committed 입니다)

# %% [11-8 바꿔 보기 ①] flush 없이 바로 abort 하면? — 새 토픽 tx_demo2 에
admin.create_topics([NewTopic("tx_demo2", num_partitions=1, replication_factor=1)])["tx_demo2"].result()
pt.begin_transaction()
for i in range(3):
    pt.produce("tx_demo2", value=f"tx message {i}")
pt.abort_transaction()            # flush 없이 바로 취소
print("read_uncommitted:", read_n("read_uncommitted", "tx_demo2"))
# ── 결과 ──
# read_uncommitted: 0
# → 이래서 — read_uncommitted 도 0 — 세 줄이 장부에 아예 적히지 않았습니다. produce 는 우체통(대기열)에 넣기만 하고(STEP
# 06), abort 는 아직 우체통에 있는 메시지를 버립니다. 11-8 에서 flush 를 먼저 한 이유가 이것입니다 — 보낸 뒤에 취소해야 "적혔지만
# 취소됨" 표시가 붙은 세 줄을 볼 수 있습니다. 실제 서비스에서는 이 차이를 신경 쓸 필요가 없습니다 — 어느 쪽이든 커밋된 것만 읽는 컨슈머에게는 안
# 보인다는 결과는 같습니다.
