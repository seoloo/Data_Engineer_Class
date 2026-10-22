# 10교시 — 처음부터 끝까지 한 번에: 읽기 → 펼치기 → 검사 → 합치기 → 집계 → 저장 → 그림(파이프라인)

# %% [10-A] 한 셀 파이프라인
# > [10-1] 강사와 함께 번호 주석 ①~⑦ 을 먼저 적고, 그 아래를 채우세요
import json
from pathlib import Path
import pandas as pd

# 1 - 읽기
with open("data/orders.json", "r", encoding="utf-8") as f:
    orders = json.load(f)
products = pd.read_csv("data/products.csv")

#2 - 펼치기(한 행으로 만들기 - 주문 상품 1개씩)
meta = ["order_id", "ordered_at", "status", ["customer", "id"], ["customer", "city"]]
lines = pd.json_normalize(orders, record_path="items", meta=meta, errors="ignore")

lines = lines.rename(columns={"customer.id" : "customer_id", "customer.city" : "customer_city"} )
print(lines)

# 3 - 검사
if lines["order_id"].isna().any():
    raise ValueError("주문번호가 미어 있는 행이 있습니다.")

if(lines["qty"] <= 0).any():
    raise ValueError("수량이 0 이하인 행이 있습니다.")

# 4 - 합치기
joined = lines.merge(products, on="product_id", how="left",
                     validate="many_to_one", indicator=True)

if(joined["_merge"] != "both").any():
    raise ValueError("상품 표에 없는 상품이 있습니다.")

joined = joined.drop(columns="_merge")

# 5 - 집계 (연산 + 그룹화)
joined["amount"] = joined["qty"] * joined["unit_price"]
paid = joined[joined["status"] == "paid"].copy()

summary = paid.groupby("category", as_index=False).agg(
    quantity = ("qty", "sum"),
    revenue = ("amount", "sum")
)

# 6 - 저장
Path("output").mkdir(exist_ok=True)
summary.to_csv("output/category_revenue.csv", index=False, encoding="utf-8")
print(summary)
print("총 매출 : ", summary["revenue"].sum())


# %% [10-B] 저장한 파일 다시 읽어 확인 — index=False 를 빼면
# > [10-2] index=False 없이도 저장해 보고, 두 파일을 다시 읽어 열을 비교하세요
summary.to_csv("output/category_revenue_idx.csv", encoding="utf-8")
print(pd.read_csv("output/category_revenue.csv"))
print(pd.read_csv("output/category_revenue_idx.csv"))


# %% [10-C] 막대그래프 — 글꼴을 안 정하면
# > [10-3] 카테고리별 매출을 막대그래프로 그려 저장하세요 (한글을 눈여겨보세요)
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(5, 3.5))
ax.bar(summary["category"], summary["revenue"])
ax.set_title("카테고리별 매출")
fig.savefig("output/revenue_no_font.png", dpi = 120)
plt.close(fig)


# %% [10-D] 한글 글꼴·축 이름·단위
# > [10-4] 글꼴을 정하고, 축 이름과 단위, 막대 위 숫자를 넣어 다시 저장하세요
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False
fig, ax = plt.subplots(figsize=(5, 3.5))
bars = ax.bar(summary["category"], summary["revenue"], color = "#80ff00dd")
ax.bar_label(bars, fmt="{:,.0f}원")
ax.set_title("카테고리별 매출(결제 완료)")
ax.set_xlabel("카테고리")
ax.set_ylabel("매출(원)")
ax.set_ylim(0, 55000)
fig.tight_layout()
fig.savefig("output/category_revenue.png", dpi = 120)
plt.close(fig)
print(Path("output/category_revenue.png").exists())

# 한글이 깨지면 한글 폰트로 변경해주면 됨

# %% [10-E] (혼자) 종합 과제 — 지저분한 주문으로 같은 일 하기
# > [10-5] data/orders_dirty.json 으로 [10-A] 를 다시 하세요. 8교시 정리 과정을 ②와 ③ 사이에 넣습니다
# >        (완전 중복 제거 → 수량·날짜 변환 → 격리 → 도시 "미상")
# > [10-6] 카테고리별 매출을 output/dirty_category_revenue.csv 로 저장하고,
# >        격리 행 수와 정상 행 수, 총 매출을 출력하세요
