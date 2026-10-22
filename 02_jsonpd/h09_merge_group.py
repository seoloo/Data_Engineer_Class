# 9교시 — 두 표 합치기(merge)와 묶어서 집계(groupby)

# %% [9-A] 두 표 읽기
# > [9-1] 주문 상품 표와 상품 표를 읽고 각각 모양을 확인하세요
import pandas as pd
lines = pd.read_csv("data/order_lines.csv")
products = pd.read_csv("data/products.csv")

print(lines.shape, products.shape)
print(lines)
print(products)

# %% [9-B] product_id 로 합치기
# > [9-2] 주문 상품 표 왼쪽에 상품 정보를 붙이세요. 행 수가 그대로인가요?
merged = lines.merge(products, on="product_id", how="left")
print(merged[["order_id", "product_id", "qty", "product_name", "category", "unit_price"]])


# %% [9-C] 상품 표에 같은 상품이 두 줄이면 — 틀린 결과
# > [9-3] 상품 표에 P001 이 한 줄 더 들어갔다고 치고 다시 합쳐 보세요
bad_product = pd.concat([products, products.iloc[[0]]], ignore_index=True)
print(bad_product)

bad_merged = lines.merge(bad_product, on="product_id", how="left")
print(bad_product)
print(len(lines), "->", len(bad_merged))

bad_merged["amount"] = bad_merged["qty"] * bad_merged["unit_price"]
print(bad_merged.loc[bad_merged["status"] == "paid", "amount"].sum()) # 84000


# %% [9-D] validate — 합치기 전에 약속을 걸어 둔다
# > [9-4] validate="many_to_one" 을 넣어 같은 코드를 실행하세요
good_merged = lines.merge(products, on="product_id", how="left", validate="many_to_one")
good_merged["amount"] = good_merged["qty"] * good_merged["unit_price"]
print(good_merged.loc[good_merged["status"] == "paid", "amount"].sum())

# %% [9-E] 상품 표에 없는 상품 — inner 는 조용히 버린다
# > [9-5] 상품 표에 없는 P009 주문이 들어왔다고 치고 inner 와 left 를 비교하세요
extra = pd.concat([
    lines,
    pd.DataFrame([{
        "order_id": "O006",
        "ordered_at": "2026-09-06",
        "status": "paid",
        "customer_id": "C05",
        "city": "부산",
        "product_id": "P005",
        "qty": 1
    }])
], ignore_index=True)
print(len(extra))
print(extra.merge(products, on="product_id", how="inner"))

check = extra.merge(products, on="product_id", how="left", indicator=True)
print(check["_merge"].value_counts())

print(check.loc[check["_merge"] != "both", ["order_id", "product_id", "product_name", "_merge"]])

# 곧드

# %% [9-F] 금액 계산과 매출
# > [9-6] 금액 열을 만들고 paid 매출과 전체 금액을 비교하세요
merged["amount"] = merged["qty"] * merged["unit_price"]
paid = merged[merged["status"]=="paid"].copy()

print("paid 된 금액 : ", paid["amount"].sum())
print("전체 금액 : ", merged["amount"].sum())
print("미결제금액 : ", merged["amount"].sum() - paid["amount"].sum())




# %% [9-G] groupby 를 아무 생각 없이 쓰면
# > [9-7] 카테고리로 묶어 .sum() 을 그대로 해 보세요. 어떤 열이 이상한가요?
print(paid.groupby("category").sum()[["order_id", "status", "qty", "amount"]])




# %% [9-H] groupby — 무엇을 묶고, 어느 열을, 어떻게
# > [9-8] 카테고리별 매출(amount 합계)만 구하세요
print(paid.groupby("category")["amount"].sum())




# %% [9-I] 여러 값을 한 번에 — agg
# > [9-9] 카테고리별 수량 합계와 매출 합계를 이름을 붙여 한 표로 만드세요
by_category = paid.groupby("category",as_index=False).agg(
    quantity = ("qty", "sum"),
    revenue = ("amount", "sum"),
)

print(by_category)

by_city = paid.groupby("city", as_index=False)["amount"].sum()
print(by_city)

print(by_category["revenue"].sum(), by_city["amount"].sum())

# 가장 저렴한 검증 방법


# %% [9-J] (혼자) 상품별 매출 순위
# > [9-10] paid 에서 상품명별 수량·매출을 구하고 매출이 큰 순으로 정렬하세요
# > [9-11] 합계가 69,000원인지 확인하세요
