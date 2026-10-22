"""특강 실습 데이터를 만든다.  실행: 터미널에서  python make_data.py

만드는 파일 (data 폴더)
  orders.json        주문 5건 (중첩 JSON)         ← 1~7, 9~10교시
  products.csv       상품 3종                     ← 5, 6, 9, 10교시
  orders_dirty.json  같은 가게의 '지저분한' 주문 8건 ← 8교시, 10교시 종합 과제
"""
import csv
import json
from pathlib import Path

DATA = Path("data")
DATA.mkdir(exist_ok=True)

orders = [
    {"order_id": "O001", "ordered_at": "2026-09-01", "status": "paid",
     "customer": {"id": "C01", "city": "서울"},
     "items": [{"product_id": "P001", "qty": 2}, {"product_id": "P002", "qty": 1}]},
    {"order_id": "O002", "ordered_at": "2026-09-02", "status": "paid",
     "customer": {"id": "C02", "city": "부산"},
     "items": [{"product_id": "P003", "qty": 3}]},
    {"order_id": "O003", "ordered_at": "2026-09-03", "status": "refunded",
     "customer": {"id": "C01", "city": "서울"},
     "items": [{"product_id": "P001", "qty": 1}]},
    {"order_id": "O004", "ordered_at": "2026-09-04", "status": "paid",
     "customer": {"id": "C03", "city": "대전"},
     "items": [{"product_id": "P002", "qty": 2}, {"product_id": "P003", "qty": 1}]},
    {"order_id": "O005", "ordered_at": "2026-09-05", "status": "paid",
     "customer": {"id": "C04", "city": "서울"},
     "items": [{"product_id": "P001", "qty": 1}]},
]

products = [
    {"product_id": "P001", "product_name": "노트", "category": "문구", "unit_price": 5000},
    {"product_id": "P002", "product_name": "펜", "category": "문구", "unit_price": 2000},
    {"product_id": "P003", "product_name": "텀블러", "category": "생활", "unit_price": 12000},
]

# 8교시용: 실제로 자주 만나는 문제를 한 건씩 넣었다
dirty = [
    orders[0],                                                   # 정상 (상품 2개)
    orders[1],                                                   # 정상
    orders[1],                                                   # ← 같은 주문이 두 번 들어옴
    {"order_id": "O003", "ordered_at": "2026-09-03", "status": "paid",
     "customer": {"id": "C03", "city": "대전"},
     "items": [{"product_id": "P002", "qty": "2"}]},            # ← 수량이 문자열 "2"
    {"order_id": "O004", "ordered_at": "2026-09-04", "status": "paid",
     "customer": {"id": "C04", "city": "서울"},
     "items": [{"product_id": "P001", "qty": None}]},           # ← 수량 없음
    {"order_id": "O005", "ordered_at": "2026-09-05", "status": "paid",
     "customer": {"id": "C05", "city": "부산"},
     "items": [{"product_id": "P003", "qty": "두개"}]},         # ← 수량이 글자
    {"order_id": "O006", "ordered_at": "2026-02-30", "status": "paid",
     "customer": {"id": "C06", "city": "대전"},
     "items": [{"product_id": "P001", "qty": 1}]},              # ← 없는 날짜
    {"order_id": "O007", "ordered_at": "2026-09-07", "status": "paid",
     "customer": {"id": "C07"},
     "items": [{"product_id": "P002", "qty": 4}]},              # ← 도시 정보 없음
]

(DATA / "orders.json").write_bytes(
    json.dumps(orders, ensure_ascii=False, indent=2).encode("utf-8"))
(DATA / "orders_dirty.json").write_bytes(
    json.dumps(dirty, ensure_ascii=False, indent=2).encode("utf-8"))
with (DATA / "products.csv").open("w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(products[0].keys()))
    writer.writeheader()
    writer.writerows(products)

for name in ["orders.json", "products.csv", "orders_dirty.json"]:
    print(f"만듦  data/{name}")
