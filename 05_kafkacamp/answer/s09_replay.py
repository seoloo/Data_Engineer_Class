# ==============================================================================
#  STEP 09  재생기 만들기 — 하루 로그를 진짜 개찰구처럼 흘려 보내기
#  C:\kafkacamp\answer\s09_replay.py
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
old = [t for t in ["tags", "tags_live", "speed_test"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)
# ── 결과 ──
# 지운 토픽: []


# %% [09-1] 파티션 3개짜리 토픽 tags 와 tags_live 를 만드십시오.
for t in ["tags", "tags_live"]:
    admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)])[t].result()
print(counts("tags"), counts("tags_live"))
# ── 결과 ──
# [0, 0, 0] [0, 0, 0]
# → 둘 다 [0, 0, 0]. [준비] 셀에 STEP 08 의 counts 가 들어 있습니다.

# %% [09-2] jsonl 파일을 읽어 한 줄씩 보내는 함수 send_file(path, topic) 을 만드십시오. 키는 카드 번호, 값은 줄 그대로. 보낸 줄 수를
# 돌려줍니다.
p = Producer({"bootstrap.servers": BOOT, "partitioner": "murmur2_random"})

def send_file(path, topic):
    n = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            p.produce(topic, key=rec["card_id"], value=line)
            n += 1
    p.flush()
    return n
# ── 결과 ──
# (출력 없음)
# → 정의만 했으니 출력이 없습니다. if not line: continue 는 파일 끝의 빈 줄을 건너뜁니다. 함수 끝의 flush 를 꼭
# 확인하십시오(STEP 06).

# %% [09-3] 하루 로그 전체를 tags 로 보내고, 파티션별 개수를 보십시오.
t0 = time.time()
n = send_file("data/tags_day.jsonl", "tags")
print(n, "줄,", round(time.time() - t0, 2), "초")
print(counts("tags"))
# ── 결과 (걸린 초는 PC 마다 다름) ──
# 2008 줄, 0.02 초
# [686, 698, 624]
# → 2008 줄이 1초도 안 되어 들어갑니다(시간은 PC 마다 다름) — produce 가 우체통에 넣기만 하고, 라이브러리가 묶어서 보내기
# 때문입니다(STEP 06). 파티션별 개수는 STEP 08 의 by_card 와 똑같습니다 — 같은 키, 같은 규칙(murmur2), 같은 파티션 수이기
# 때문입니다.

# %% [09-4] 도착 간격을 흉내 내며 보내도록 send_file 에 speed 를 더한 함수 replay(path, topic, speed) 를 만들고, part_01 을
# speed=600(10분 → 1초)으로 tags_live 에 보내십시오.
from datetime import datetime
FMT = "%Y-%m-%d %H:%M:%S"

def replay(path, topic, speed):
    n, prev = 0, None
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            t = datetime.strptime(rec["recv_time"], FMT)
            if prev is not None:
                gap = (t - prev).total_seconds() / speed
                if gap > 0:
                    p.flush()
                    time.sleep(gap)
            prev = t
            p.produce(topic, key=rec["card_id"], value=line)
            n += 1
    p.flush()
    return n

t0 = time.time()
print(replay("data/stream_src/part_01.jsonl", "tags_live", 600), "줄,", round(time.time() - t0, 1), "초")
# ── 결과 (초는 PC 마다 조금 다름) ──
# 201 줄, 12.4 초
# → 201 줄, 10초 남짓(강사 PC 12.4초). part_01 은 07:00:01 ~ 07:31:58 에 도착한 기록이라 실제로는 32분 걸린 것을
# 600배 빠르게 보냈습니다. 계산으로는 32분 ÷ 600 = 3.2초인데 더 걸린 까닭은, 줄마다 하는 flush(Kafka 까지 갔다 오는 왕복) 와 윈도의
# time.sleep 최소 단위(약 0.016초) 가 201번 쌓이기 때문입니다. 옆 터미널의 콘솔 컨슈머에는 줄이 띄엄띄엄 들어옵니다 — 아침 7시대 초반이
# 한산하기 때문입니다.

# %% [09-4 바꿔 보기 ①] speed 를 6000 으로(10배 더 빠르게) 바꾸면?
t0 = time.time()
print(replay("data/stream_src/part_01.jsonl", "speed_test", 6000), "줄,", round(time.time() - t0, 1), "초")
# ── 결과 ──
# 201 줄, 9.4 초
# → 이래서 — 201 줄, 약 9초 — 10배 빠르게 했는데 시간은 조금밖에 안 줄었습니다. 기다리는 시간은 3.2초 → 0.3초로 줄었지만, 남은 9초는
# 줄마다의 flush 왕복과 sleep 최소 단위입니다. speed 를 아무리 올려도 이 바닥 밑으로는 못 내려갑니다 — 이렇게 전체 속도를 붙잡고 있는 가장
# 느린 곳을 병목이라고 부릅니다(STEP 17). 수업에서는 600 정도가 눈으로 보기 좋습니다. (speed_test 토픽은 자동으로 생깁니다)

# %% [09-5] tags_live 의 메시지 하나를 꺼내 Kafka 의 timestamp 와 데이터 안의 tag_time · recv_time 을 나란히 찍으십시오.
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g09"})
c.assign([TopicPartition("tags_live", 0, 0)])
m = c.poll(5.0)
c.close()
rec = json.loads(m.value())
kind, ms = m.timestamp()
print("Kafka timestamp :", datetime.fromtimestamp(ms / 1000))
print("tag_time        :", rec["tag_time"])
print("recv_time       :", rec["recv_time"])
# ── 결과 (첫 줄은 실행한 날짜·시각 — PC 마다 다름) ──
# Kafka timestamp : 2026-10-16 12:36:17.761000
# tag_time        : 2026-10-14 07:00:01
# recv_time       : 2026-10-14 07:00:03
# → Kafka timestamp 는 오늘 지금(방금 보낸 시각), tag_time · recv_time 은 2026-10-14 아침. m.timestamp()
# 는 (종류, 밀리초) 두 개를 돌려주는데, 종류 1 = CreateTime(보낸 쪽이 붙인 시각)입니다. 집계는 tag_time 으로, 지연 측정은 Kafka
# timestamp 로 — 이 구분이 3부 내내 나옵니다.

# %% [09-6] 완성본 tools\replay.py 를 불러와, part_02 를 tags_live 로 보내고 개수를 확인하십시오.
from tools.replay import send_part
print(send_part(2))
print(sum(counts("tags_live")))
# ── 결과 ──
# 201
# 402
# → 201 줄을 보냈고 tags_live 는 모두 402 줄(part_01 + part_02). send_part(n) 은
# data\stream_src\part_0n.jsonl 을 tags_live 로 보내는 함수입니다. VS Code 왼쪽 탐색기에서 tools\replay.py
# 를 열어 우리가 만든 send_file · replay 와 같은 일을 하는지 읽어 보십시오.

# %% [09-7] 새 그룹으로 tags 를 전부 읽어, 1과목과 같은 숫자가 나오는지 확인하십시오 — 줄 수 · 승하차별 줄 수 · 이벤트 가짓수.
import pandas as pd
c = Consumer({"bootstrap.servers": BOOT, "group.id": "g09check", "auto.offset.reset": "earliest"})
c.subscribe(["tags"])
rows, empty = [], 0
while empty < 5:
    m = c.poll(1.0)
    if m is None:
        empty += 1
        continue
    empty = 0
    rows.append(json.loads(m.value()))
c.close()
df = pd.DataFrame(rows)
print(len(df))
print(df.groupby("tag_type").size().to_dict())
print(df["event_id"].nunique())
# ── 결과 ──
# 2008
# {'IN': 1012, 'OUT': 996}
# 1988
# → 2008 · {IN 1012, OUT 996} · 1988 — 1과목 08-1 · 08-10 과 똑같습니다. 파일 → Kafka → 파이썬을 거쳐도 한
# 줄도 늘거나 줄지 않았습니다. 그리고 재전송 20줄은 그대로 들어 있습니다.

# %% [09-8] 일부러 틀려 보십시오. 없는 조각 send_part(11) 을 보내 보십시오.
send_part(11)
# ── 결과 (오류 — 일부러 틀린 것) ──
# FileNotFoundError: [Errno 2] No such file or directory: 'data/stream_src/part_11.jsonl'
# → FileNotFoundError — part_11.jsonl 은 없다(조각은 1~10). 오류 메시지의 경로를 보고 무엇이 없는지 찾습니다.
