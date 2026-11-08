# ==============================================================================
#  STEP 07  파이썬 컨슈머 — poll 로 꺼내고, commit 으로 책갈피
#  C:\kafkacamp\lab\s07_consumer.py
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


# %% [07-1] 그룹 g07 로 mini 를 처음부터 끝까지 읽어 몇 개인지 세십시오. (위 설명의 반복문 그대로)
c = Consumer({"bootstrap.servers" : BOOT, "group.id" :"g08", "auto.offset.reset" : "earliest"})
c.subscribe(["mini"])
got = []
empty = 0
while empty < 5:
    m = c.poll(1.0)
    if m is None:
        empty += 1
        continue
    empty = 0
    got.append(m)

c.close()
len(got)


# %% [07-2] 받은 메시지 앞 네 개의 파티션 · 오프셋 · 키 · 이벤트 번호 · 승하차를 찍으십시오.
for m in got[:4]:
    rec = json.loads(m.value())
    print(m.partition(), m.offset(), m.key().decode(),
        rec["event_id"], rec["tag_type"])


# %% [07-3] 같은 그룹 g07 로 다시 읽어 몇 개인지 세십시오. (07-1 의 반복문을 함수 read_all 로 만들어 두고 부릅니다)
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
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

len(read_all("g08"))    # 내 07-1 은 g07 대신 g08 로 읽었으므로 같은 그룹 g08 을 다시 부른다 (g07 이면 책갈피가 없어 12개가 나온다)
# ── 결과 ──
# 0
# → 0 — g07 의 책갈피가 07-1 의 끝(close 할 때 커밋)에 있기 때문입니다. reset="earliest" 는 책갈피가 없을 때만
# 쓰입니다(05-5 와 같은 이야기).




# %% [07-4] 새 그룹 g07b 로 읽으면 몇 개입니까? 그리고 새 그룹 g07c 를 reset="latest" 로 읽으면?
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
print(len(read_all("g07b")))
print(len(read_all("g07l", reset="latest")))    # 문제의 g07c 는 내 07-7 에서 장애 실험용으로 쓰므로 겹치지 않게 g07l 로 바꿨다
# ── 결과 ──
# 12
# 0
# → 12 와 0. 새 그룹은 책갈피가 없으니 auto.offset.reset 을 따릅니다 — earliest 는 처음부터, latest 는 "지금부터 새로
# 오는 것만"(그런데 새로 온 것이 없으니 0). 실시간 대시보드는 latest(과거는 필요 없음), 정산은 earliest(하나도 빠지면 안 됨)로 씁니다.




# %% [07-4 바꿔 보기 ①] group.id 를 빼고 컨슈머를 만들면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
c = Consumer({"bootstrap.servers": BOOT, "auto.offset.reset": "earliest"})
c.subscribe(["mini"])
# ── 결과 ──
# ValueError: Failed to create consumer: group.id must be set
# → 이래서 — 컨슈머를 만드는 순간 오류 — group.id must be set. confluent-kafka 의 컨슈머는 그룹 이름이 꼭 있어야 합니다.
# 책갈피를 누구 이름으로 저장할지 정해야 하기 때문입니다. 07-9 처럼 장부를 직접 고르는(assign) 경우에도 이름 하나는 줍니다.




# %% [07-5] 수동 커밋 — 그룹 g07m 으로 자동 커밋을 끄고 5개만 읽은 뒤 직접 커밋하고 닫으십시오.
# "enable.auto.commit" : 수동 커밋
c = Consumer({"bootstrap.servers" : BOOT, "group.id" :"g07m", 
              "auto.offset.reset" : "earliest", "enable.auto.commit" : False})
c.subscribe(["mini"])
done = 0
while done < 5:
    m = c.poll(1.0)
    if m is None:
        continue
    done += 1
c.commit(asynchronous = False)
c.close()
print("처리 :", done)


# %% [07-6] g07m 으로 다시 읽으면 몇 개가 남아 있습니까?
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
 
len(read_all("g07m"))



# %% [07-7] 장애 흉내 — 그룹 g07x 로 자동 커밋을 끄고 5개를 읽은 뒤, 커밋하지 않고 닫으십시오. 그리고 다시 읽으십시오.
c = Consumer({"bootstrap.servers":BOOT,"group.id":"g07c",
              "auto.offset.reset":"earliest","enable.auto.commit":False})
c.subscribe(["mini"])
done = 0
while done < 5:
    m = c.poll(1.0)
    if m is None:
        continue
    done += 1
c.close()
print("다시 읽은 개수 :", len(read_all("g07c")))



# %% [07-8] 새 그룹 g07p 로 mini 를 전부 읽어 pandas 표로 만들고, 승하차별 줄 수와 이벤트 가짓수를 보십시오.
import pandas as pd
rows = [json.loads(m.value()) for m in read_all("g07pp")]
df = pd.DataFrame(rows)
print(df.groupby("tag_type").size())
print("the number of events:", df['event_id'].nunique())




# %% [07-9] 특정 장부만 읽기 — 그룹 없이 mini 의 1번 파티션 0번부터 직접 지정(assign)해서 읽으십시오.
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
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
# [보충] 수업 시간에 넘어간 셀 — answer 정답을 기준으로 채웠습니다. 아래 결과는 직접 실행한 값이 아니라 정답 파일의 기대 출력입니다.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g07e", "auto.offset.reset": "latest"})
c.subscribe(["mini"])
m = c.poll(2.0)
print(m.value())
# ── 결과 (오류 — 일부러 틀린 것) ──
# AttributeError: 'NoneType' object has no attribute 'value'
# → 'NoneType' object has no attribute 'value' — 2초 동안 온 것이 없어서 m 이 None 입니다. 반복문의 if m is
# None: continue 가 이 오류를 막는 줄입니다. 바이브 코딩으로 받은 코드에서 가장 흔하게 빠지는 줄입니다.

