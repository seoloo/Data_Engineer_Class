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




# %% [07-4] 새 그룹 g07b 로 읽으면 몇 개입니까? 그리고 새 그룹 g07c 를 reset="latest" 로 읽으면?




# %% [07-4 바꿔 보기 ①] group.id 를 빼고 컨슈머를 만들면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




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




# %% [07-10] 일부러 틀려 보십시오. 아무것도 없는 상태(새 그룹, latest)에서 poll 결과에 바로 .value() 를 불러 보십시오.
