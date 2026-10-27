# -*- coding: utf-8 -*-
"""교통카드 로그 재생기 — STEP 09 에서 직접 만든 것의 완성본.

파이썬 셀에서:
    from tools.replay import send_file, send_part, counts
    send_part(3)                       # data/stream_src/part_03.jsonl 을 tags_live 토픽으로
    send_file("data/tags_day.jsonl", "tags")
    send_file("data/stream_src/part_01.jsonl", "tags_live", speed=600)   # 10분 → 1초 빠르기로

터미널(PowerShell)에서:
    .\\.venv\\Scripts\\python.exe tools\\replay.py part 3
"""
import json
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer, Producer, TopicPartition

BOOT = "localhost:9092"
FMT = "%Y-%m-%d %H:%M:%S"


def _producer():
    # 자바(Spark · 터미널)와 같은 파티션 규칙 (STEP 08)
    return Producer({"bootstrap.servers": BOOT, "partitioner": "murmur2_random"})


def send_file(path, topic, speed=None):
    """jsonl 파일을 한 줄 = 메시지 하나로 보낸다. 키 = 카드 번호.
    speed 를 주면 recv_time 간격을 speed 배 빠르게 흉내 내며 보낸다 (600 = 10분이 1초)"""
    p = _producer()
    n = 0
    prev = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if speed:
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


def send_part(n, topic="tags_live", speed=None):
    """data/stream_src/part_0n.jsonl 하나를 보낸다 (1~10)"""
    return send_file(f"data/stream_src/part_{n:02d}.jsonl", topic, speed)


def counts(topic):
    """파티션마다 쌓인 메시지 수"""
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "counter"})
    parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions)
    out = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part))
        out.append(hi - lo)
    c.close()
    return out


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "part":
        k = int(sys.argv[2])
        print(f"part_{k:02d} →", send_part(k), "줄 보냄")
    elif len(sys.argv) >= 3 and sys.argv[1] == "file":
        print(send_file(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "tags"), "줄 보냄")
    else:
        print(__doc__)
