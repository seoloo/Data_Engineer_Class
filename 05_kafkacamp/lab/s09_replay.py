# ==============================================================================
#  STEP 09  재생기 만들기 — 하루 로그를 진짜 개찰구처럼 흘려 보내기
#  C:\kafkacamp\lab\s09_replay.py
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
old = [t for t in ["tags", "tags_live", "speed_test"] if t in admin.list_topics(timeout=5).topics]
for t in old:
    admin.delete_topics([t])[t].result()
time.sleep(3)          # 지우기는 뒤에서 조금 더 걸린다
print("지운 토픽:", old)


# %% [09-1] 파티션 3개짜리 토픽 tags 와 tags_live 를 만드십시오.




# %% [09-2] jsonl 파일을 읽어 한 줄씩 보내는 함수 send_file(path, topic) 을 만드십시오. 키는 카드 번호, 값은 줄 그대로. 보낸 줄 수를
# 돌려줍니다.




# %% [09-3] 하루 로그 전체를 tags 로 보내고, 파티션별 개수를 보십시오.




# %% [09-4] 도착 간격을 흉내 내며 보내도록 send_file 에 speed 를 더한 함수 replay(path, topic, speed) 를 만들고, part_01 을
# speed=600(10분 → 1초)으로 tags_live 에 보내십시오.




# %% [09-4 바꿔 보기 ①] speed 를 6000 으로(10배 더 빠르게) 바꾸면?
#    위 정답을 이렇게 바꾸면 어떻게 보일지 먼저 예상하고, 직접 쳐서 확인하십시오.




# %% [09-5] tags_live 의 메시지 하나를 꺼내 Kafka 의 timestamp 와 데이터 안의 tag_time · recv_time 을 나란히 찍으십시오.




# %% [09-6] 완성본 tools\replay.py 를 불러와, part_02 를 tags_live 로 보내고 개수를 확인하십시오.




# %% [09-7] 새 그룹으로 tags 를 전부 읽어, 1과목과 같은 숫자가 나오는지 확인하십시오 — 줄 수 · 승하차별 줄 수 · 이벤트 가짓수.




# %% [09-8] 일부러 틀려 보십시오. 없는 조각 send_part(11) 을 보내 보십시오.
