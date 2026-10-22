# 5교시 — 표를 CSV 로 저장하고 다시 읽기

# %% [5-A] 4교시의 rows 다시 만들기
# > [5-1] 4교시 [4-D] 코드를 복사해 와서 실행하세요 (이 셀은 채워져 있습니다)
import json
with open("data/orders.json", "r", encoding="utf-8") as f:
    orders = json.load(f)
rows = []
for order in orders:
    for item in order["items"]:
        rows.append({
            "order_id": order["order_id"],
            "ordered_at": order["ordered_at"],
            "status": order["status"],
            "customer_id": order["customer"]["id"],
            "city": order["customer"]["city"],
            "product_id": item["product_id"],
            "qty": item["qty"],
        })

for row in rows:
    print(row)



# %% [5-B] CSV 로 저장 — DictWriter
# > [5-2] 열 이름 목록을 먼저 정하고, 머리줄 → 행 순서로 저장하세요
import csv
fieldnames = ["order_id","ordered_at","status","customer_id","city","product_id","qty"]
with open("data/order_lines.csv","w",encoding="utf-8",newline="") as f:
    writer = csv.DictWriter(f,fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

from pathlib import Path
print(Path("data/ordeR_lines.csv").read_text(encoding="utf-8"))
    


# %% [5-C] 읽기 ① — csv.reader 는 리스트로
# > [5-3] reader 로 읽고 첫 두 줄을 출력하세요. 첫 줄이 무엇인가요?
with open("data/order_lines.csv","r",encoding="utf-8",newline="") as f:
    table = list(csv.reader(f))
print(len(table))
print(table[0])
print(table[1])

for t in table:
    print(t)


# %% [5-D] 읽기 ② — DictReader 는 딕셔너리로
# > [5-4] DictReader 로 읽고 첫 행을 출력하세요. qty 값을 눈여겨보세요
with open("data/order_lines.csv","r",encoding="utf-8",newline="") as f:
    lines = list(csv.DictReader(f))
print(len(lines))
print(lines[0])
print(lines[0]["qty"])
print(type(lines[0]["qty"]))

# %% [5-E] CSV 에서 읽은 숫자로 계산하면
# > [5-5] 첫 두 행의 qty 를 더하고, 전체 합계도 sum 으로 구해 보세요
print(lines[0]["qty"] + lines[1]["qty"])


# %% [5-F] int() 로 바꾸고 계산
# > [5-6] int() 로 바꿔서 다시 더하세요. 4교시 합계와 같아야 합니다
print(int(lines[0]["qty"]) + int(lines[1]["qty"]))


# %% [5-G] utf-8-sig 의 정체 — 파일 맨 앞 3바이트
# > [5-7] utf-8 로도 저장해 보고, 두 파일의 맨 앞 3바이트를 비교하세요



# %% [5-H] (혼자) 상품 CSV 로 매출 계산
# > [5-8] data/products.csv 를 DictReader 로 읽고, 가격 3개의 합계를 구하세요
with open("data/products.csv","r",encoding="utf-8",newline="") as f:
    products = list(csv.DictReader(f))

pro_sum = 0
for product in products:
    pro_sum += int(product["unit_price"])
print(pro_sum)
# > [5-9] {상품번호: 가격} 딕셔너리 price 를 만드세요
price = {}
for p in products:
    price[p["product_id"]] = int(p["unit_price"])
print(price)
# > [5-10] lines 를 돌면서 paid 행의 (수량 × 가격) 합계 = 매출을 구하세요
revenue = 0
for row in lines:
    if row["status"] =="paid":
        revenue += int(row["qty"]) * price[row["product_id"]]

print("매출 :",revenue)



# %%
