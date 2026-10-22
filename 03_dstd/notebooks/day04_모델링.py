# %% [markdown]
# # 4일차 — 한 행의 의미에서 개념 모델까지
#
# `# %%` 가 셀 구분선입니다. 셀마다 Shift+Enter 로, 위에서부터 순서대로 실행하십시오.
# F5(디버그)는 쓰지 않습니다. 중간부터 다시 하려면 그 셀 위의 Run Above 를 누르십시오.
# 오늘은 모델을 머리로 정하지 않습니다. 업무설명서로 가설을 세우고, 데이터로 확인합니다.

# %%
# [셀 1] 준비
import sys, os
from pathlib import Path
import pandas as pd


def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "make_dataset.py").exists():
            return p
    return start


try:
    HERE = Path(__file__).resolve().parent
except NameError:
    HERE = Path.cwd()
ROOT = find_root(HERE)
os.chdir(ROOT)
sys.path.insert(0, str(ROOT / "src"))

FULL = ROOT / "data" / "raw" / "common"
MY_MAP = ROOT / "docs" / "매핑표.xlsx"

from rules import load_sources, load_code_map, classify_orders, parse_amount, show
from grain import grain_table, key_check, relation_check

cust, order, prod = load_sources(FULL)
print(f"확장본 — 고객 {len(cust)} / 주문항목 {len(order):,} / 상품 {len(prod)}")
if MY_MAP.exists():
    r = classify_orders(order, cust, prod, load_code_map(MY_MAP))   # 3일차 분류를 그대로 가져온다
    print("3일차 분류:", r["분류"].value_counts().to_dict())
else:
    print("docs\\매핑표.xlsx 가 없습니다 — 셀 4·6 은 건너뜁니다")

# %%
# [셀 2] 1교시 — 요구사항: "고객별로 몇 번 샀고 얼마나 썼나". 그런데 이 파일로 세면?
print("주문 건수를 len() 으로 세면 :", f"{len(order):,}") # 주문 항목 수 
print("주문번호 종류 (nunique)     :", f"{order['ORD_NO'].nunique():,}") # 주문건수
print()
fee = order["DLV_FEE"].map(lambda v: parse_amount(v)[0])
by_row = int(fee.sum())
by_order = int(order.assign(f=fee).drop_duplicates("ORD_NO")["f"].sum())
print(f"배송비 — 행마다 더하면   {by_row:>12,}원")
print(f"배송비 — 주문마다 한 번  {by_order:>12,}원")
print(f"                          {by_row / by_order:.2f}배 부풀려짐")

# %%
# [셀 3] 2교시 — 틀린 결과 먼저: 원천 그대로 '한 행의 의미'를 판정하면
COLS = ["CUSTNO", "ORD_DT", "PAY_MTHD", "DLV_FEE", "ORD_ST", "CANCEL_DT", "GOODS_CD", "QTY", "AMT"]
print(order.groupby("ORD_NO").size().value_counts().sort_index().rename("주문 수").rename_axis("항목 수").to_frame().T.to_string())
print()
show(grain_table(order, "ORD_NO", COLS))

# %%
# [셀 4] 2교시 — 3일차에 격리·제거한 행을 빼고 다시 판정하면
if MY_MAP.exists():
    clean = r[~r["분류"].isin(["격리", "제거"])]
    show(grain_table(clean, "ORD_NO", COLS))
    print()
    bad = order.groupby("ORD_NO")[["ORD_DT", "DLV_FEE"]].nunique()
    bad = bad[(bad > 1).any(axis=1)].index
    print("원천 그대로일 때 값이 갈렸던 주문:", list(bad))
    print("그 주문들의 3일차 규칙:", r[r["ORD_NO"].isin(bad)]["규칙ID"].unique().tolist())

# %%
# [셀 5] 3교시 — 식별자 후보: 한 행을 콕 집을 수 있는가?
show(pd.DataFrame([
    key_check(cust, ["CUST_NO"]), key_check(cust, ["HP"]), key_check(cust, ["EMAIL"]),
    key_check(cust, ["CUST_NM"]),
    key_check(prod, ["goodsCode"]), key_check(prod, ["goodsName"]),
]))

# %%
# [셀 6] 4교시 — 주문 파일의 식별자: 무엇을 정리해야 성립하나
steps = [("원천 그대로 — ORD_NO 하나", order, ["ORD_NO"]),
         ("원천 그대로 — ORD_NO + ORD_SEQ", order, ["ORD_NO", "ORD_SEQ"])]
if MY_MAP.exists():
    steps += [("R01(완전 중복) 제거 후", r[r["규칙ID"] != "R01"], ["ORD_NO", "ORD_SEQ"]),
              ("R02(키 중복) 격리 후", r[~r["규칙ID"].isin(["R01", "R02"])], ["ORD_NO", "ORD_SEQ"])]
rows = []
for label, df, cols in steps:
    k = key_check(df, cols)
    k["단계"] = label
    rows.append(k)
show(pd.DataFrame(rows), ["단계", "행수", "빈 값 행", "겹치는 행", "판정"])

# %%
# [셀 7] 5교시 — AS-IS 의 관계: 값으로 이어지는가
# 역공학 - 있는 데이터를 거꾸로 모델을 그려내는 것

show(pd.DataFrame([
    relation_check(order, "CUSTNO", cust, "CUST_NO", empty_values=("", "GUEST")),
    relation_check(order, "GOODS_CD", prod, "goodsCode"),
]))
print()
print("한 번도 주문되지 않은 상품:", int((~prod["goodsCode"].isin(set(order["GOODS_CD"]))).sum()), "개")
print("한 번도 주문하지 않은 고객:", int((~cust["CUST_NO"].isin(set(order["CUSTNO"]))).sum()), "명")

# %%
# [셀 8] 6교시 — 반복되는 값 찾기: 코드일까, 그냥 숫자일까
rows = []
for fname, df in [("cust_master", cust), ("order_2026", order), ("product_api_response", prod)]:
    for c in df.columns:
        if c == "원천행":
            continue
        n = df[c].nunique()
        if n <= 20:
            rows.append({"파일": fname, "컬럼": c, "고유값": n,
                         "값 예": ", ".join(sorted(map(str, df[c].unique()))[:6]) + (" …" if n > 6 else "")})
show(pd.DataFrame(rows))

# %%
# [셀 9] 5~6교시 — 내 AS-IS 모델 검사 (docs\모델_ASIS.md)
from check_model import run as check_model

f = ROOT / "docs" / "모델_ASIS.md"
if f.exists():
    rep, _ = check_model(f, asis=True)
else:
    print("docs\\모델_ASIS.md 가 없습니다 — templates\\모델_ASIS_템플릿.md 를 복사해 작성하십시오")

# %%
# [셀 10] 7교시 — 내 TO-BE 개념 모델 검사 (docs\모델_TOBE.md)
f = ROOT / "docs" / "모델_TOBE.md"
if f.exists():
    rep, _ = check_model(f)
else:
    print("docs\\모델_TOBE.md 가 없습니다 — templates\\모델_TOBE_템플릿.md 를 복사해 작성하십시오")

# %%
# [셀 11] 8교시 — Python 에서 MySQL 실습 DB 로 접속 (비밀번호를 물으면 root 비밀번호)
from check_mysql import run as check_mysql

check_mysql()
