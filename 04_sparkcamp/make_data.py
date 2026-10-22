# -*- coding: utf-8 -*-
"""
교통카드 승하차 태그 로그 — 실습 데이터 만들기
실행:  python make_data.py
몇 번을 실행해도 같은 데이터가 만들어집니다 (random.seed 고정).

만들어지는 것
  data/stations.csv            역 마스터 8개
  data/tags_mini.jsonl         손으로 검산하는 작은 로그 12줄
  data/tags_day.jsonl          하루 아침(07~10시) 태그 로그 2,008줄
  data/stream_src/part_01.jsonl ~ part_10.jsonl
                               스트리밍 실습용으로 하루 로그를 10조각 낸 것
"""
import json
import os
import random
from datetime import datetime, timedelta

random.seed(20261014)
os.makedirs("data", exist_ok=True)
os.makedirs("data/stream_src", exist_ok=True)

# ---------------------------------------------------------------- 역 마스터
STATIONS = [
    ("S01", "강남",     "2호선", "강남구"),
    ("S02", "역삼",     "2호선", "강남구"),
    ("S03", "홍대입구", "2호선", "마포구"),
    ("S04", "시청",     "1호선", "중구"),
    ("S05", "서울역",   "1호선", "중구"),
    ("S06", "잠실",     "2호선", "송파구"),
    ("S07", "여의도",   "5호선", "영등포구"),
    ("S08", "광화문",   "5호선", "종로구"),
]
with open("data/stations.csv", "w", encoding="utf-8", newline="") as f:
    f.write("station_id,station_nm,line,gu\n")
    for s in STATIONS:
        f.write(",".join(s) + "\n")

FMT = "%Y-%m-%d %H:%M:%S"


def ev(eid, card, trip, typ, st, t, fare, recv):
    return {"event_id": eid, "card_id": card, "trip_id": trip, "tag_type": typ,
            "station_id": st, "tag_time": t, "fare": fare, "recv_time": recv}


# ---------------------------------------------------------------- 작은 로그 12줄
mini = [
    ev("M01", "C001", "T001", "IN",  "S01", "2026-10-14 08:01:10", 0,    "2026-10-14 08:01:11"),
    ev("M02", "C002", "T002", "IN",  "S06", "2026-10-14 08:03:05", 0,    "2026-10-14 08:03:06"),
    ev("M03", "C003", "T003", "IN",  "S03", "2026-10-14 08:05:00", 0,    "2026-10-14 08:05:01"),
    ev("M04", "C004", "T004", "IN",  "S05", "2026-10-14 08:07:30", 0,    "2026-10-14 08:07:31"),
    ev("M05", "C005", "T005", "IN",  "S01", "2026-10-14 08:09:45", 0,    "2026-10-14 08:09:46"),
    ev("M06", "C006", "T006", "IN",  "S08", "2026-10-14 08:12:00", 0,    "2026-10-14 08:12:01"),
    ev("M07", "C005", "T005", "OUT", "S02", "2026-10-14 08:12:30", 1550, "2026-10-14 08:12:31"),
    ev("M08", "C003", "T003", "OUT", "S04", "2026-10-14 08:22:15", 1550, "2026-10-14 08:22:16"),
    ev("M09", "C002", "T002", "OUT", "S01", "2026-10-14 08:24:50", 1650, "2026-10-14 08:24:51"),
    ev("M10", "C004", "T004", "OUT", "S07", "2026-10-14 08:29:00", 1650, "2026-10-14 08:29:01"),
    ev("M11", "C001", "T001", "OUT", "S03", "2026-10-14 08:31:40", 1650, "2026-10-14 08:31:41"),
    ev("M07", "C005", "T005", "OUT", "S02", "2026-10-14 08:12:30", 1550, "2026-10-14 08:12:58"),  # 같은 이벤트가 한 번 더 (재전송)
]
with open("data/tags_mini.jsonl", "w", encoding="utf-8") as f:
    for e in mini:
        f.write(json.dumps(e, ensure_ascii=False) + "\n")

# ---------------------------------------------------------------- 하루 로그
N_TRIPS = 1000
IDS = [s[0] for s in STATIONS]
BASE = datetime(2026, 10, 14, 7, 0, 0)

# 08시대에 사람이 몰리게: 07시 300 · 08시 450 · 09시 250
starts = ([random.randint(0, 3599) for _ in range(300)] +
          [random.randint(3600, 7199) for _ in range(450)] +
          [random.randint(7200, 10799) for _ in range(250)])
random.shuffle(starts)

events = []
no_out = set(random.sample(range(N_TRIPS), 12))          # 하차 태그가 없는 이동 12건
late_pool = []
for i in range(N_TRIPS):
    trip = f"T{i + 1:04d}"
    card = f"C{random.randint(1, 700):04d}"
    a = random.choice(IDS)
    b = random.choice([x for x in IDS if x != a])
    t_in = BASE + timedelta(seconds=starts[i])
    events.append(ev(None, card, trip, "IN", a, t_in.strftime(FMT), 0,
                     (t_in + timedelta(seconds=random.randint(1, 3))).strftime(FMT)))
    if i in no_out:
        continue
    t_out = t_in + timedelta(minutes=random.randint(8, 45), seconds=random.randint(0, 59))
    gap = abs(IDS.index(a) - IDS.index(b))
    fare = 1550 + 100 * min(gap // 2, 3)
    out = ev(None, card, trip, "OUT", b, t_out.strftime(FMT), fare,
             (t_out + timedelta(seconds=random.randint(1, 3))).strftime(FMT))
    events.append(out)
    late_pool.append(out)

# 지하 구간에서 통신이 끊겨 늦게 도착한 하차 태그 30건 (태그는 제때, 도착은 20~40분 뒤)
for e in random.sample(late_pool, 30):
    t = datetime.strptime(e["tag_time"], FMT)
    e["recv_time"] = (t + timedelta(minutes=random.randint(20, 40))).strftime(FMT)

# 실제 태그 시각 순으로 정렬하고 이벤트 번호를 붙인다
events.sort(key=lambda e: (e["tag_time"], e["trip_id"], e["tag_type"]))
for k, e in enumerate(events, 1):
    e["event_id"] = f"E{k:05d}"

# 오염 ① 등록되지 않은 역 코드 S99 (게이트 설정 오류) 6건
for e in random.sample([e for e in events if e["tag_type"] == "IN"], 6):
    e["station_id"] = "S99"
# 오염 ② 시각이 깨진 태그 4건 (분이 61, 63 ...)
for e in random.sample([e for e in events if e["station_id"] != "S99"], 4):
    hh = e["tag_time"][11:13]
    e["tag_time"] = f"2026-10-14 {hh}:{random.choice([61, 63, 67, 75])}:00"

# 오염 ③ 같은 이벤트가 두 번 들어온 재전송 20건 (event_id 까지 같다)
dups = [dict(e) for e in random.sample(events, 20)]

# 파일에 쓰는 순서 = 서버에 도착한 순서 (recv_time 순)
lines = events + dups


def recv_key(e):
    return e["recv_time"], e["event_id"]


lines.sort(key=recv_key)
with open("data/tags_day.jsonl", "w", encoding="utf-8") as f:
    for e in lines:
        f.write(json.dumps(e, ensure_ascii=False) + "\n")

# 스트리밍 실습용 10조각 (도착 순서 그대로 자른다)
size = (len(lines) + 9) // 10
for p in range(10):
    with open(f"data/stream_src/part_{p + 1:02d}.jsonl", "w", encoding="utf-8") as f:
        for e in lines[p * size:(p + 1) * size]:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

print("stations.csv      :", len(STATIONS), "역")
print("tags_mini.jsonl   :", len(mini), "줄")
print("tags_day.jsonl    :", len(lines), "줄")
print("stream_src        : part_01 ~ part_10")
print("완료")
