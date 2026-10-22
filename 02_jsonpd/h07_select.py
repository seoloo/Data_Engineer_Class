# 7교시 — 원하는 행 고르기와 새 열 만들기

# %% [7-A] 표 읽기
import pandas as pd
df = pd.read_csv("data/order_lines.csv")
print(df.shape)



# %% [7-B] 조건식의 결과는 True/False 한 줄
# > [7-1] status 가 paid 인지 묻는 식만 먼저 출력하세요
mask = df["status"] == "paid"
print(mask)
print(mask.sum())


# %% [7-C] True 인 행만 남기기
# > [7-2] 대괄호 안에 mask 를 넣어 행을 고르세요
paid = df[mask]
print(paid)
print(len(paid))


# %% [7-D] loc[행 조건, 열 목록]
# > [7-3] loc 로 행과 열을 한 번에 고르세요
print(df.loc[df["status"] == "paid", ["order_id", "product_id", "qty"]])
# loc == location


# %% [7-E] 조건 두 개 — and 를 쓰면
# > [7-4] "paid 이면서 수량 2개 이상" 을 and 로 써 보세요



# %% [7-F] 조건 두 개 — & 인데 괄호가 없으면
# > [7-5] and 를 & 로 바꾸고, 괄호를 빼고 실행해 보세요



# %% [7-G] 조건 두 개 — & 와 괄호
# > [7-6] 조건마다 괄호를 씌우세요. 또는 은 | 입니다
print(df[(df["status"] == "paid") & (df["qty"] >=2 )])

print(df[(df["city"] == "부산") | (df["city"] >= "대전" )])

print(df[df["city"].isin(["부산", "대전"])])

# %% [7-H] 세어 보기와 정렬
# > [7-7] 값마다 몇 번 나오는지 세고, 수량이 많은 순으로 정렬하세요
print(df["status"].value_counts())
print(df["city"].value_counts())

print(df.sort_values("qty").head(3))
print(df.sort_values("qty", ascending=False).head(3))

# %% [7-I] 새 열 만들기
# > [7-8] 계산한 결과를 새 열 이름에 대입하세요
df["is_bulk"] = df["qty"] >= 2
print(df[["order_id", "product_id", "qty", "is_bulk"]])


# %% [7-J] 가격이 없는데 금액을 계산하면
# > [7-9] qty × unit_price 로 금액을 구해 보세요



# %% [7-K] 딕셔너리로 값 붙이기 — map
# > [7-10] 5교시에 만든 가격 딕셔너리를 map 으로 붙이고 금액을 계산하세요
price = { "P001" : 5000, "P002" : 2000, "P003" : 12000}
df["unit_price"] = df["product_id"].map(price)
print(df["unit_price"])
df["amount"] = df["qty"] * df["unit_price"]
print(df[["order_id", "status", "product_id", "qty", "unit_price", "amount"]])

# %% [7-L] (혼자) 조건 연습
# > [7-11] 서울의 paid 행만 골라 수량이 많은 순으로 정렬하세요
seoul_paid = df[(df["city"] == "서울") & (df["status"] == "paid")]
print(seoul_paid.sort_values("qty", ascending=False))

# > [7-12] 금액이 10,000원 이상인 행은 몇 개인가요?
print((df["amount"] >= 10000).sum())

# > [7-13] refunded 행의 금액 합계는?
print(df.loc[df["status"] == "refunded", "amount"].sum())


# %%
