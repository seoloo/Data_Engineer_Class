# %% [markdown]
# # 2일차 — 표준 사전과 매핑표
#
# `# %%` 가 셀 구분선입니다. 셀마다 Shift+Enter 로, 위에서부터 순서대로 실행하십시오.
# F5(디버그)는 쓰지 않습니다. 중간부터 다시 하려면 그 셀 위의 Run Above 를 누르십시오.

# %%
# [셀 1] 준비
import sys, os
from datetime import datetime
from pathlib import Path
import json
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
sys.path.insert(0, str(ROOT / "src"))           # src 안의 검사기를 불러오기 위해

BASE = ROOT / "data" / "sample"                  # 오늘도 소형본으로 시작한다
print("프로젝트 루트:", ROOT)
print("원천 폴더   :", BASE)

# %%
# [셀 2] 원천 3종을 '올바른 옵션으로' 읽는다 — 1일차에 배운 그대로
cust = pd.read_csv(BASE / "cust_master.csv", encoding="euc-kr", dtype=str)
order = pd.read_csv(BASE / "order_2026.csv", encoding="utf-8-sig", dtype=str)
with open(BASE / "product_api_response.json", encoding="utf-8") as f:
    # parse_int / parse_float=str — 숫자를 원천에 적힌 글자 그대로 받는다 (7900 이 7900.0 이 되지 않게)
    prod = pd.DataFrame(json.load(f, parse_int=str, parse_float=str)["items"])

sources = {"cust_master.csv": cust, "order_2026.csv": order, "product_api_response.json": prod}
for name, df in sources.items():
    print(f"{name:28s} {len(df):>4}행 × {df.shape[1]:>2}열")

# %%
# [셀 3] 컬럼 인벤토리 — 세 파일의 모든 컬럼을 한 표로
rows = []
for name, df in sources.items():
    for col in df.columns:
        s = df[col]
        sample = s.dropna()
        rows.append({
            "원천파일": name,
            "원천컬럼": col,
            "예시값": sample.iloc[0] if len(sample) else "",
            "결측": int(s.isna().sum()),
            "고유값": int(s.nunique()),
        })
inventory = pd.DataFrame(rows)
print(f"전체 {len(inventory)}개 컬럼")
print(inventory.to_string(index=False))

# %%
# [셀 4] 같은 대상, 다른 이름 — 이름이 다른데 정말 같은 값인가?
pairs = [
    ("고객번호", cust["CUST_NO"], order["CUSTNO"]),
    ("상품번호", prod["goodsCode"], order["GOODS_CD"]),
]
for label, master, ref in pairs:
    ref_vals = ref.dropna()
    matched = ref_vals.isin(set(master)).sum()
    print(f"[{label}] 마스터 {master.name!r} ↔ 주문 {ref.name!r}")
    print(f"  마스터 예시: {list(master.head(3))}")
    print(f"  주문   예시: {list(ref_vals.head(3))}")
    print(f"  주문 값 {len(ref_vals)}개 중 마스터에 있는 값 {matched}개")
    print()

# %%
# [셀 5] 품질 6대 차원 — 1일차에 본 현상을 숫자로 확인한다
FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d-%m-%Y"]


def parse_date(s):
    if not isinstance(s, str) or not s.strip():
        return None
    for fmt in FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


dates = order["ORD_DT"].map(parse_date)
evidence = [
    ("완전성", "EMAIL 이 비어 있는 고객", int(cust["EMAIL"].isna().sum())),
    ("유일성", "(ORD_NO, ORD_SEQ) 가 겹치는 행", int(order.duplicated(["ORD_NO", "ORD_SEQ"], keep=False).sum())),
    ("유효성", "날짜로 읽을 수 없는 ORD_DT", int(dates.isna().sum())),
    ("일관성", "ORD_ST 의 서로 다른 표기 수", int(order["ORD_ST"].nunique())),
    ("정확성", "고객 마스터에 없는 고객번호(비회원 제외)",
     int((~order["CUSTNO"].isin(set(cust["CUST_NO"])) & order["CUSTNO"].notna()
          & (order["CUSTNO"] != "GUEST")).sum())),
    ("적시성", "기준일(2026-06-30) 이후 주문 행",
     int(sum(1 for d in dates if d and d > datetime(2026, 6, 30).date()))),
]
def pad(t, w):                                   # 한글은 두 칸으로 계산해 줄을 맞춘다
    return t + " " * max(1, w - sum(2 if ord(c) > 0x1100 else 1 for c in t))


print(pad("차원", 8) + pad("무엇을 셌나", 44) + "건수")
for dim, what, n in evidence:
    print(pad(dim, 8) + pad(what, 44) + f"{n:>3}")

# %%
# [셀 6] 검사기 시연 — 일부러 틀리게 만든 연습 사전을 검사한다
from check_dictionary import run

rep, stats = run(ROOT / "templates" / "표준사전_연습.xlsx")

# %%
# [셀 7] 내 사전 검사 — Google Sheets 에서 내려받은 docs\표준사전.xlsx
my_dict = ROOT / "docs" / "표준사전.xlsx"
if my_dict.exists():
    rep, stats = run(my_dict)
else:
    print("docs\\표준사전.xlsx 가 아직 없습니다.")
    print("Google Sheets → 파일 → 다운로드 → Microsoft Excel(.xlsx) 로 받아")
    print(f"{my_dict} 에 저장한 뒤 이 셀을 다시 실행하십시오.")

# %%
# [셀 8] 코드매핑 초안 만들기 — 원천에 실제로 나오는 코드 값을 전부 뽑는다
from openpyxl import Workbook

targets = [
    ("order_2026.csv", "ORD_ST", "ORD_STAT", order),
    ("order_2026.csv", "PAY_MTHD", "PAY_MTHD", order),
    ("cust_master.csv", "GRD_CD", "CUST_GRD", cust),
    ("product_api_response.json", "taxType", "TAX_TP", prod),
    ("product_api_response.json", "currency", "CRNCY", prod),
]
draft = []
for fname, col, group, df in targets:
    for v in sorted(df[col].dropna().unique()):
        draft.append([fname, col, str(v), group, "", ""])

out = ROOT / "data" / "work" / "코드매핑_초안.xlsx"
out.parent.mkdir(parents=True, exist_ok=True)
wb = Workbook()
ws = wb.active
ws.title = "코드매핑"
ws.append(["원천파일", "원천컬럼", "원천값", "표준코드그룹", "표준코드값", "처리"])
for r in draft:
    ws.append(r)
for row in ws.iter_rows(min_row=2):             # 원천값·표준코드값은 텍스트로 — 01 이 1 로 바뀌지 않게
    row[2].number_format = "@"
    row[4].number_format = "@"
wb.save(out)

print(f"{len(draft)}개 원천 값을 뽑았습니다 → {out.relative_to(ROOT)}")
for fname, col, group, df in targets:
    vals = [r[2] for r in draft if r[1] == col]
    print(f"  {col:<9} → {group:<9} {len(vals):>2}개  {vals}")

# %%
# [셀 9] 사전 + 매핑표 함께 검사 — 원천 컬럼이 빠짐없이 매핑됐는지까지 본다
my_map = ROOT / "docs" / "매핑표.xlsx"
if my_dict.exists() and my_map.exists():
    rep, stats = run(my_dict, my_map, BASE)
else:
    print("docs\\표준사전.xlsx 와 docs\\매핑표.xlsx 가 모두 있어야 합니다.")

# %%
# [셀 10] 확장본으로 확인 — 소형본(41행)에서 통과한 매핑표가 4,000행에서도 통과할까?
FULL = ROOT / "data" / "raw" / "common"
if my_dict.exists() and my_map.exists():
    rep, stats = run(my_dict, my_map, FULL)
else:
    print("docs\\표준사전.xlsx 와 docs\\매핑표.xlsx 가 모두 있어야 합니다.")

# %%
