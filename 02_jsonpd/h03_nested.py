# 3교시 — 중첩된 JSON 에서 값 꺼내기

# %% [3-A] 주문 5건 읽기
# > [3-1] 2교시와 같은 방법으로 orders 를 읽으세요
import json
with open("data/order.json","r",encoding="utf-8") as f:
    orders = json.load(f)
print(type(orders),len(orders))


# %% [3-B] 리스트에 키로 물어보면
# > [3-2] orders["order_id"] 를 실행해 보세요 (실패합니다)
print(orders["order_id"])


# %% [3-C] 첫 주문 한 건만 보기
# > [3-3] 첫 주문을 first 에 담고, 보기 좋게 출력하세요
first = orders[4]
print(type(first))
print(first["order_id"])
print(json.dumps(first,ensure_ascii=False,indent=2))
print(first.keys())



# %% [3-D] 한 계단씩 내려가기
# > [3-4] 한 줄 칠 때마다 type() 을 함께 보면서 한 계단씩 내려가세요
print(type(first["customer"]))
print(type(first["customer"]["city"]))
print(first["customer"])
print((first["customer"]["city"]))


# %% [3-E] 없는 키를 물어보면
# > [3-5] 전화번호(phone)는 데이터에 없습니다. 대괄호와 get 을 비교하세요
print(first["customer"].get("phone"))
print(first["customer"].get("phone","없음"))


# %% [3-F] 5건 전체를 돌면서
# > [3-6] for 문으로 주문번호·도시·상품 개수를 한 줄씩 출력하세요
for order in orders:
    print(order["order_id"],order["customer"]["city"],len(order["items"]))


# %% [3-G] (혼자) 원하는 값 찾기
# > [3-7] 주문마다 주문번호와 status 를 출력하세요
# orders[0]["order_id"]
for order in orders:
    print(order["order_id"],order["status"])

# > [3-8] 모든 주문의 상품 줄 수(len(items))를 더하면 몇 개인가요?
total_items = 0
for order in orders:
    total_items += len(order["items"])
print("총 상품 개수 : ",total_items)

total_items2 = [len(order["items"]) for order in orders]
print("총 상품 개수 : ",sum(total_items2))


# > [3-9] status 가 "paid" 가 아닌(!=) 주문의 번호는?
for order in orders:
    if order["status"] != "paid":
        print("결제 되지 않은 주문 번호 : ",order["order_id"])


# %%
