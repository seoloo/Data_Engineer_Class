# ==============================================================================
#  STEP 07  파이썬 컨슈머 — poll 로 꺼내고, commit 으로 책갈피
#  C:\kafkacamp\answer\s07_consumer.py
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


# %% [07-1] 그룹 g07 로 mini 를 처음부터 끝까지 읽어 몇 개인지 세십시오. (위 설명의 반복문 그대로)
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07", "auto.offset.reset": "earliest"})
c.subscribe(["mini"])
got, empty = [], 0
while empty < 5:
    m = c.poll(1.0)
    if m is None:
        empty += 1
        continue
    empty = 0
    got.append(m)
c.close()
len(got)
# ── 결과 ──
# 12
# → 12 — STEP 06 에서 보낸 그대로. 마지막 메시지를 받은 뒤 약 5초 더 기다렸다가 끝납니다. 이 기다림이 "5번 연속 빈손" 입니다.

# %% [07-2] 받은 메시지 앞 네 개의 파티션 · 오프셋 · 키 · 이벤트 번호 · 승하차를 찍으십시오.
for m in got[:4]:
    rec = json.loads(m.value())
    print(m.partition(), m.offset(), m.key().decode(), rec["event_id"], rec["tag_type"])
# ── 결과 (파티션끼리의 순서는 실행할 때마다 다를 수 있음) ──
# 2 0 C001 M01 IN
# 2 1 C004 M04 IN
# 2 2 C004 M10 OUT
# 2 3 C001 M11 OUT
# → json.loads 는 바이트도 받아 줍니다. 같은 파티션 안에서는 오프셋이 0, 1, 2 순서대로 옵니다. 파티션끼리의 순서는 정해져 있지 않습니다 —
# 받은 순서는 실행할 때마다 다를 수 있습니다.

# %% [07-3] 같은 그룹 g07 로 다시 읽어 몇 개인지 세십시오. (07-1 의 반복문을 함수 read_all 로 만들어 두고 부릅니다)
def read_all(group, topic="mini", reset="earliest"):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": group, "auto.offset.reset": reset})
    c.subscribe([topic])
    got, empty = [], 0
    while empty < 5:
        m = c.poll(1.0)
        if m is None:
            empty += 1
            continue
        empty = 0
        got.append(m)
    c.close()
    return got

len(read_all("g07"))
# ── 결과 ──
# 0
# → 0 — g07 의 책갈피가 07-1 의 끝(close 할 때 커밋)에 있기 때문입니다. reset="earliest" 는 책갈피가 없을 때만
# 쓰입니다(05-5 와 같은 이야기).

# %% [07-4] 새 그룹 g07b 로 읽으면 몇 개입니까? 그리고 새 그룹 g07c 를 reset="latest" 로 읽으면?
print(len(read_all("g07b")))
print(len(read_all("g07c", reset="latest")))
# ── 결과 ──
# 12
# 0
# → 12 와 0. 새 그룹은 책갈피가 없으니 auto.offset.reset 을 따릅니다 — earliest 는 처음부터, latest 는 "지금부터 새로
# 오는 것만"(그런데 새로 온 것이 없으니 0). 실시간 대시보드는 latest(과거는 필요 없음), 정산은 earliest(하나도 빠지면 안 됨)로 씁니다.

# %% [07-4 바꿔 보기 ①] group.id 를 빼고 컨슈머를 만들면?
c = Consumer({"bootstrap.servers": BOOT, "auto.offset.reset": "earliest"})
c.subscribe(["mini"])
# ── 결과 ──
# ValueError: Failed to create consumer: group.id must be set
# → 이래서 — 컨슈머를 만드는 순간 오류 — group.id must be set. confluent-kafka 의 컨슈머는 그룹 이름이 꼭 있어야 합니다.
# 책갈피를 누구 이름으로 저장할지 정해야 하기 때문입니다. 07-9 처럼 장부를 직접 고르는(assign) 경우에도 이름 하나는 줍니다.

# %% [07-5] 수동 커밋 — 그룹 g07m 으로 자동 커밋을 끄고 5개만 읽은 뒤 직접 커밋하고 닫으십시오.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07m",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["mini"])
done = 0
while done < 5:
    m = c.poll(1.0)
    if m is None:
        continue
    done += 1            # 여기서 "처리" 를 했다고 치자
c.commit(asynchronous=False)
c.close()
print("처리:", done)
# ── 결과 ──
# 처리: 5
# → c.commit() 은 지금까지 poll 로 받은 자리를 책갈피로 저장합니다. asynchronous=False 는 "저장될 때까지 기다려라".

# %% [07-6] g07m 으로 다시 읽으면 몇 개가 남아 있습니까?
len(read_all("g07m"))
# ── 결과 ──
# 7
# → 7 = 12 − 5. 책갈피를 꽂은 자리부터 이어서 읽었습니다.

# %% [07-7] 장애 흉내 — 그룹 g07x 로 자동 커밋을 끄고 5개를 읽은 뒤, 커밋하지 않고 닫으십시오. 그리고 다시 읽으십시오.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07x",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["mini"])
done = 0
while done < 5:
    m = c.poll(1.0)
    if m is not None:
        done += 1
c.close()                       # 커밋 없이 꺼짐 = 처리 도중 장애
print("다시 읽은 개수:", len(read_all("g07x")))
# ── 결과 ──
# 다시 읽은 개수: 12
# → 12 — 처음부터 다시. 5개를 처리했지만 책갈피를 못 꽂고 꺼졌기 때문에, 다시 켜면 그 5개를 또 받습니다. 이것이 중복(at-least-once) 의
# 정체입니다. STEP 11 에서 2,008줄로 크게 겪어 봅니다.

# %% [07-8] 새 그룹 g07p 로 mini 를 전부 읽어 pandas 표로 만들고, 승하차별 줄 수와 이벤트 가짓수를 보십시오.
import pandas as pd
rows = [json.loads(m.value()) for m in read_all("g07p")]
df = pd.DataFrame(rows)
print(df.groupby("tag_type").size())
print("이벤트 가짓수:", df["event_id"].nunique())
# ── 결과 ──
# tag_type
# IN     6
# OUT    6
# dtype: int64
# 이벤트 가짓수: 11
# → IN 6 · OUT 6, 그런데 이벤트는 11가지 — M07 이 두 번 들어 있습니다(1과목 05-6 의 그 재전송). Kafka 는 받은 것을 그대로
# 전합니다. 중복을 걸러 주지 않습니다. 걸러 내는 것은 받는 쪽의 일입니다(STEP 11 · 15).

# %% [07-9] 특정 장부만 읽기 — 그룹 없이 mini 의 1번 파티션 0번부터 직접 지정(assign)해서 읽으십시오.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07a"})
c.assign([TopicPartition("mini", 1, 0)])
one, empty = [], 0
while empty < 3:
    m = c.poll(1.0)
    if m is None:
        empty += 1
        continue
    one.append((m.offset(), m.key().decode()))
c.close()
print(one)
# ── 결과 ──
# [(0, 'C002'), (1, 'C005'), (2, 'C006'), (3, 'C005'), (4, 'C002'), (5, 'C005')]
# → 1번 장부의 6개만 — C002 · C005 · C006 의 기록입니다. subscribe = 그룹에게 맡기기, assign = 내가 직접 고르기.
# assign 은 그룹의 나눠 읽기(05-9)를 하지 않습니다. 터미널의 --partition 1 --offset 0 과 같습니다.

# %% [07-10] 일부러 틀려 보십시오. 아무것도 없는 상태(새 그룹, latest)에서 poll 결과에 바로 .value() 를 불러 보십시오.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07e", "auto.offset.reset": "latest"})
c.subscribe(["mini"])
m = c.poll(2.0)
print(m.value())
# ── 결과 (오류 — 일부러 틀린 것) ──
# AttributeError: 'NoneType' object has no attribute 'value'
# → 'NoneType' object has no attribute 'value' — 2초 동안 온 것이 없어서 m 이 None 입니다. 반복문의 if m is
# None: continue 가 이 오류를 막는 줄입니다. 바이브 코딩으로 받은 코드에서 가장 흔하게 빠지는 줄입니다.
