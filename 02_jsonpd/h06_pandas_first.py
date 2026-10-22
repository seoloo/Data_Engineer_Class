# 6교시 — pandas 첫 만남 (+ NumPy 다리)

# %% [6-A] pandas 불러오기
# > [6-1] pandas 를 pd 라는 이름으로 불러오고 버전을 확인하세요
import pandas as pd
print(pd.__version__)


# %% [6-B] CSV 한 줄로 읽기
# > [6-2] 5교시에 만든 order_lines.csv 를 read_csv 로 읽고 그대로 출력하세요

df = pd.read_csv("data/order_lines.csv")
print(df)

# %% [6-C] 표를 받으면 제일 먼저 보는 네 가지
# > [6-3] shape → columns → head → dtypes 순서로 확인하세요
print(df.shape)
print(df.columns.to_list())
print(df.head())
print(df.dtypes)

# %% [6-D] 숫자는 이미 숫자다 — 5교시와 비교
# > [6-4] qty 합계를 구하세요. 5교시 [5-5]에서는 무엇이 나왔나요?
print(df["qty"].sum())
print(df["qty"].max())
print(df["qty"].mean())
print(df["qty"].median())


# %% [6-E] 열 하나 vs 열 여러 개
# > [6-5] 대괄호 한 겹과 두 겹을 비교하세요

print(type(df["qty"]))
print(df["qty"])
print(type(df[["order_id", "qty"]]))
print(df[["order_id", "qty"]])

# %% [6-F] 열 두 개를 한 겹 대괄호로 쓰면
# > [6-6] 대괄호를 한 겹만 쓰면 어떻게 되나요?



# %% [6-G] 4교시 이중 for 문을 한 줄로 — json_normalize
# > [6-7] orders.json 을 읽어 json_normalize 에 그냥 넣어 보세요. items 칸은?
import json
with open("data/orders.json", "r", encoding="utf-8") as f:
    orders = json.load(f)
wide = pd.json_normalize(orders)
print(wide.shape)
print(wide.columns.to_list())
print(wide[["order_id", "items"]])

# %% [6-H] record_path 와 meta — 무엇을 한 행으로 할지 알려 준다
# > [6-8] record_path="items" 로 상품 한 개를 한 행으로 만들고, meta 로 주문 정보를 붙이세요

flat = pd.json_normalize(orders, record_path="items", 
                         meta=["order_id", "ordered_at", "status", ["customer", "id"], ["customer", "city"]])
print(flat)
print(flat.shape)

# %% [6-I] NumPy 다리 ① — 리스트 × 2 와 배열 × 2
# > [6-9] 같은 숫자를 리스트와 NumPy 배열로 만들어 각각 × 2 해 보세요
import numpy as np
qty_list = [2, 1, 3]
#print("R" * 2)
print(qty_list * 2)
qty_list2 = np.array(qty_list)
print(qty_list2 * 2)
print(type(qty_list2), qty_list2.dtype, qty_list2.shape)

# %% [6-J] NumPy 다리 ② — pandas 열 안에는 배열이 있다
# > [6-10] df["qty"] 를 NumPy 배열로 꺼내 보고, 조건식 결과를 확인하세요
arr = df["qty"].to_numpy()
print(type(arr), arr)
print(arr >= 2)
print(arr[arr >= 2])
print(df["qty"] * 2)

# %% [6-K] (혼자) 상품 표 살펴보기
# > [6-11] data/products.csv 를 products 로 읽고 shape 와 dtypes 를 확인하세요
# > [6-12] unit_price 합계와 product_name 열만 출력하세요 (5교시 [5-8] 답과 같은가요?)
