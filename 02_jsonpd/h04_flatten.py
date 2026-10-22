# 4교시 — 중첩을 표로 펼치기 (한 행이 무엇인가)

# %% [4-A] 주문 5건 읽기
import json
with open("data/order.json", "r", encoding="utf-8") as f:
    orders = json.load(f)
print(len(orders))



# %% [4-B] 바깥 반복문만으로 행 만들기 — 틀린 방법
# > [4-1] 주문 한 건을 한 행으로 만들어 보세요. items 칸을 눈여겨보세요
rows = []
for order in orders:
    rows.append({"order_id":order["order_id"],"items":order["items"]})

for row in rows:
    print(row)

# %% [4-C] 안쪽 반복문 — 상품마다 한 번
# > [4-2] for 안에 for 를 하나 더 넣어 상품마다 한 줄씩 출력하세요
for order in orders:
    for item in order["items"]:
        print(order["order_id"],item["product_id"],item["qty"])

for order in orders:
    for item in order["items"]:
        if item["qty"] == 1:
            print(item["product_id"])


# %% [4-D] 한 행 = 주문 상품 1개
# > [4-3] print 대신 rows.append 로 딕셔너리를 쌓으세요 (열 9개)
rows = []
for order in orders:
    for item in order["items"]:
        rows.append({
            "order_id": order["order_id"],
            "status": order["status"],
            "customer_id": order["customer"]["id"],
            "city": order["customer"]["city"],
            "product_id": item["product_id"],
            "qty": item["qty"],
        })
print((rows))

ans = "city"
for r in rows:
    print(r[ans])




# %% [4-E] 주문은 몇 건? — 행 수로 세면 안 된다
# > [4-4] 주문 수를 두 가지 방법으로 세어 보세요
print("전체 행의 수 : ",len(rows))
set_order = set()
for row in rows:
    set_order.add(row["order_id"])

print(set_order)
print(len(set_order))
print(sorted(set_order))

# %% [4-F] 반복문으로 합계
# > [4-5] 전체 수량 합계와, 결제 완료(paid) 행만의 수량 합계를 구하세요
total = 0
for row in rows:
    total += row["qty"]
print("전체 수량 합계",total)

paid_total = 0
for row in rows:
    if row["status"] == "paid":
        paid_total += row["qty"]
print("결제 완료 수량 합계",paid_total)
print("미결제 수량 합계",total-paid_total)
# %% [4-G] (혼자) 상품별 수량
# > [4-6] paid 행만 모아 paid_rows 리스트를 만들고 len() 을 출력하세요
# > [4-7] 상품번호별 수량 합계를 딕셔너리로 만드세요 (paid 만)
