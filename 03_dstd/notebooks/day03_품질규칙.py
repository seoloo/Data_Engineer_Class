# %% [markdown]
# # 3일차 — 품질 진단과 변환 규칙
#
# `# %%` 가 셀 구분선입니다. 셀마다 Shift+Enter 로, 위에서부터 순서대로 실행하십시오.
# F5(디버그)는 쓰지 않습니다. 중간부터 다시 하려면 그 셀 위의 Run Above 를 누르십시오.
# 오늘도 원천 파일은 고치지 않습니다. 결과는 data\work 에만 씁니다.

# %%
# [셀 1] 준비
import sys, os, warnings
from datetime import date
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

BASE = ROOT / "data" / "sample"                  # 소형본 41행
FULL = ROOT / "data" / "raw" / "common"          # 확장본 3,952행
MY_DICT = ROOT / "docs" / "표준사전.xlsx"
MY_MAP = ROOT / "docs" / "매핑표.xlsx"
MY_RULES = ROOT / "docs" / "변환규칙정의서.xlsx"
WORK = ROOT / "data" / "work"
WORK.mkdir(parents=True, exist_ok=True)

from rules import load_sources
cust, order, prod = load_sources(FULL)           # 오늘은 주로 확장본을 본다
print("프로젝트 루트:", ROOT)
print(f"확장본 — 고객 {len(cust)} / 주문항목 {len(order):,} / 상품 {len(prod)}")
for f in [MY_DICT, MY_MAP]:
    print(f"{f.relative_to(ROOT)}: {'있음' if f.exists() else '없음 ← 2일차 산출물이 필요합니다'}")

# %%
# [셀 2] 1교시 — 2일차 과제 확인: 매핑표가 확장본에서도 오류 0건인가
from check_dictionary import run as check_dict

if MY_DICT.exists() and MY_MAP.exists():
    rep, _ = check_dict(MY_DICT, MY_MAP, FULL)
else:
    print("docs\\표준사전.xlsx 와 docs\\매핑표.xlsx 가 모두 있어야 합니다.")

# %%
# [셀 3] 2교시 — 형식 서명 프로파일링: 값이 아니라 '모양'을 센다
from profile_data import profile_frame, print_profile, signature

print("서명 예:", {v: signature(v) for v in [" 19,800 ", "107730.0", "2026/03/15", "GUEST", "배송완료"]})
print()
print_profile("order_2026.csv", profile_frame(order))

# %%
# [셀 4] 3교시 — 결측은 한 종류가 아니다
raw = pd.read_csv(FULL / "order_2026.csv", encoding="utf-8-sig", dtype=str)   # 1일차 방식 그대로
print("CUSTNO 가 비어 있음 (isna)  :", int(raw["CUSTNO"].isna().sum()))
print("CUSTNO 가 'GUEST'           :", int((raw["CUSTNO"] == "GUEST").sum()))
print("→ 업무설명서 2.1: 둘 다 '비회원'. isna() 만 세면 절반을 놓친다")
print()
c = pd.read_csv(FULL / "cust_master.csv", encoding="euc-kr", dtype=str)
print("EMAIL 빈 값     :", int(c["EMAIL"].isna().sum()), " — 선택 항목. 비어 있어도 정상 (채우지 않는다)")
print("CANCEL_DT 빈 값 :", int(raw["CANCEL_DT"].isna().sum()), " — '취소 안 됨' 이라는 뜻이 있는 빈 값")

# %%
# [셀 5] 3교시 — 선행 0: dtype=str 을 빼면 조인이 조용히 0건이 된다
bad = pd.read_csv(FULL / "order_2026.csv", encoding="utf-8-sig")               # dtype 없음
print("GOODS_CD dtype :", bad["GOODS_CD"].dtype, "  예:", bad["GOODS_CD"].head(3).tolist())
print("상품 마스터 예 :", prod["goodsCode"].head(3).tolist())
print("주문 상품이 마스터에 있는 행 (dtype 없음) :", int(bad["GOODS_CD"].isin(prod["goodsCode"]).sum()))
print("주문 상품이 마스터에 있는 행 (dtype=str)  :", int(order["GOODS_CD"].isin(prod["goodsCode"]).sum()))

# %%
# [셀 6] 4교시 — 틀린 결과 먼저: drop_duplicates() 한 줄이면 끝일까?
src = order.drop(columns="원천행")
once = src.drop_duplicates()
print(f"drop_duplicates() 전 {len(src):,}행 → 후 {len(once):,}행  ({len(src) - len(once)}행 제거)")
k = once.duplicated(["ORD_NO", "ORD_SEQ"], keep=False)
print(f"그런데 아직 키가 겹치는 행: {int(k.sum())}행")
from rules import show
show(once[k].sort_values(["ORD_NO", "ORD_SEQ"]), ["ORD_NO", "ORD_SEQ", "GOODS_CD", "QTY", "AMT"])

# %%
# [셀 7] 5교시 — 틀린 결과 먼저: pandas 에게 날짜를 알아서 읽으라고 하면
s = order["ORD_DT"]
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    auto = pd.to_datetime(s, errors="coerce")
mixed = pd.to_datetime(s, errors="coerce", format="mixed")
print(f"① errors='coerce'               → NaT {int(auto.isna().sum()):,}행 / {len(s):,}행  (경고 없음)")
print(f"② format='mixed'                → NaT {int(mixed.isna().sum())}행")
dm = s.str.match(r"^\d{2}-\d{2}-\d{4}$")
print(f"   그런데 일-월-년 {int(dm.sum())}행 중 월별 분포:")
print("  ", mixed[dm].dt.month.value_counts().sort_index().to_dict())
print("   원래 값 예:", s[dm].head(3).tolist(), "→", mixed[dm].head(3).dt.date.astype(str).tolist())
print(f"   이 회사 주문은 1~7월뿐인데 8~12월 주문이 {int((mixed.dt.month >= 8).sum())}행 생겼다 (경고 없음)")

# %%
# [셀 8] 5교시 — 혼자 치기: my_parse_date 를 완성하십시오 (rules.py 를 보기 전에)
from datetime import datetime


def my_parse_date(s):
    """성공하면 (date, None), 실패하면 (None, 사유)
    허용 형식 4가지: 2026-03-15 / 2026/03/15 / 20260315 / 15-03-2026 (일이 먼저)
    사유는 셋 중 하나로: "빈 값" / "허용하지 않는 형식" / "존재하지 않는 날짜"
    """
    # 여기를 채우십시오
    return None, "미구현"


DATE_CASES = [("2026-03-15", date(2026, 3, 15)), ("2026/03/15", date(2026, 3, 15)),
              ("20260315", date(2026, 3, 15)), ("15-03-2026", date(2026, 3, 15)),
              ("09-06-2026", date(2026, 6, 9)), ("2026-02-30", "존재하지 않는 날짜"),
              ("2026-13-05", "존재하지 않는 날짜"), ("03/15/2026", "허용하지 않는 형식"), ("", "빈 값")]
ok = 0
for raw_v, want in DATE_CASES:
    got, why = my_parse_date(raw_v)
    good = (got == want and why is None) if isinstance(want, date) else (got is None and why == want)
    ok += good
    print(f"{'통과' if good else '실패'}  {raw_v!r:14} → {got!s:10} {why or ''}")
print(f"\n{ok}/{len(DATE_CASES)} 통과")

# %%
# [셀 9] 6교시 — 틀린 결과 먼저: 금액을 숫자로 바꾸기
amt = order["AMT"]
try:
    amt.astype(int)
except ValueError as e:
    print("① astype(int)             →", type(e).__name__, "-", str(e)[:60])
num = pd.to_numeric(amt, errors="coerce")
print(f"② to_numeric(coerce)      → NaN {int(num.isna().sum())}행 (경고 없음), 합계 {num.sum():,.0f}")
fixed = pd.to_numeric(amt.str.strip().str.replace(",", ""), errors="coerce")
print(f"③ 공백·콤마 정리 후       → NaN {int(fixed.isna().sum())}행,          합계 {fixed.sum():,.0f}")
print(f"   ②가 조용히 빠뜨린 금액: {fixed.sum() - num.sum():,.0f}원")

# %%
# [셀 10] 6교시 — 혼자 치기: my_parse_amount 를 완성하십시오
def my_parse_amount(s):
    """성공하면 (int, None), 실패하면 (None, 사유)
    ' 19,800 ' → 19800, '107730.0' → 107730. 소수점 이하가 0 이 아니면 실패
    사유는 셋 중 하나로: "빈 값" / "숫자가 아님" / "소수점 이하 값 있음"
    """
    # 여기를 채우십시오
    return None, "미구현"


AMT_CASES = [("19800", 19800), (" 19800 ", 19800), ("19,800", 19800), (" 19,800 ", 19800),
             ("107730.0", 107730), ("12.5", "소수점 이하 값 있음"), ("", "빈 값"), ("abc", "숫자가 아님")]
ok = 0
for raw_v, want in AMT_CASES:
    got, why = my_parse_amount(raw_v)
    good = (got == want and why is None) if isinstance(want, int) else (got is None and why == want)
    ok += good
    print(f"{'통과' if good else '실패'}  {raw_v!r:12} → {got!s:8} {why or ''}")
print(f"\n{ok}/{len(AMT_CASES)} 통과")

# %%
# [셀 11] 6교시 — 통화 환산과 코드 매핑 (rules.py 의 함수를 쓴다)
from rules import load_code_map, classify_products, map_code, show

if MY_MAP.exists():
    cmap = load_code_map(MY_MAP)
    pp = classify_products(prod, cmap)
    print("외화 상품 — 원본은 그대로, 원화정가금액·환산여부를 새로 붙인다")
    show(pp[pp["환산여부"] == "Y"], ["goodsCode", "unitPrice", "currency", "원화정가금액", "환산여부"])
    print()
    print("taxType 매핑 결과:", pp["과세상태"].value_counts().to_dict())
    st = pd.Series([map_code(cmap, "order_2026.csv", "ORD_ST", v)[1] for v in order["ORD_ST"]])
    print("ORD_ST 매핑 결과 :", st.value_counts().to_dict())
else:
    print("docs\\매핑표.xlsx 가 필요합니다.")

# %%
# [셀 12] 7교시 — 행마다 분류를 붙인다 (소형본): 정상 / 제거 / 제외 / 격리
from rules import classify_orders, summary, balance

if MY_MAP.exists():
    c0, o0, p0 = load_sources(BASE)
    r0 = classify_orders(o0, c0, p0, cmap)
    show(summary(r0))
    n, parts, same = balance(r0)
    print(f"\n원천 {n} = 정상 {parts['정상']} + 제거 {parts['제거']} + 제외 {parts['제외']} + 격리 {parts['격리']}"
          f"  → {'일치' if same else '불일치!'}")
    print()
    show(r0[r0["분류"] == "격리"], ["원천행", "ORD_NO", "ORD_SEQ", "규칙ID", "사유"])

# %%
# [셀 13] 8교시 — 내 변환 규칙 정의서 검사
from check_rulebook import run as check_rules

if MY_RULES.exists():
    rep, _ = check_rules(MY_RULES)
else:
    print("docs\\변환규칙정의서.xlsx 가 아직 없습니다.")
    print("Google Sheets → 파일 → 다운로드 → Microsoft Excel(.xlsx) 로 받아 이 위치에 저장하십시오.")

# %%
# [셀 14] 8교시 — 확장본 전체에 적용하고 결과를 남긴다
if MY_MAP.exists():
    r = classify_orders(order, cust, prod, cmap)
    t = summary(r)
    show(t)
    n, parts, same = balance(r)
    print(f"\n원천 {n:,} = 정상 {parts['정상']:,} + 제거 {parts['제거']} + 제외 {parts['제외']} + 격리 {parts['격리']}"
          f"  → {'일치' if same else '불일치!'}")
    print(f"결제수단 확인필요(F01) 표시: {int((r['확인필요'] == 'Y').sum())}행 — 격리하지 않고 정상 흐름에 남음")
    keep = ["원천행", "ORD_NO", "ORD_SEQ", "CUSTNO", "GOODS_CD", "ORD_DT", "AMT", "ORD_ST", "규칙ID", "사유", "해당규칙"]
    r[r["분류"] == "격리"][keep].to_csv(WORK / "격리_주문항목.csv", index=False, encoding="utf-8-sig")
    pp[pp["분류"] == "격리"].to_csv(WORK / "격리_상품.csv", index=False, encoding="utf-8-sig")
    t.to_csv(WORK / "처리요약.csv", index=False, encoding="utf-8-sig")
    print("\n저장: data\\work\\격리_주문항목.csv · 격리_상품.csv · 처리요약.csv")
