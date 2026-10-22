# %% [markdown]
# # 1일차 — 원천 데이터 첫 탐색
#
# VS Code에서 이 파일을 열면 `# %%` 가 셀 구분선이 됩니다.
# 셀 왼쪽 위의 `Run Cell` 을 누르거나 Shift+Enter 로 한 셀씩 실행하십시오.
# 위에서부터 순서대로 실행해야 합니다.

# %%
# [셀 1] 준비 — 실행 위치와 버전 확인
#
# 이 셀만은 데이터가 없어도 실행됩니다. 커널이 제대로 잡혔는지 확인하는 용도입니다.
import sys, os
from pathlib import Path
import pandas as pd


def find_root(start: Path) -> Path:
    """src/make_dataset.py 가 있는 폴더를 프로젝트 루트로 본다."""
    for p in [start, *start.parents]:
        if (p / "src" / "make_dataset.py").exists():
            return p
    return start


try:
    HERE = Path(__file__).resolve().parent      # 파일로 실행한 경우
except NameError:
    HERE = Path.cwd()                           # 대화형 창에서 실행한 경우

ROOT = find_root(HERE)
os.chdir(ROOT)                                  # 어디서 실행하든 루트를 기준으로 맞춘다

print("Python     :", sys.version.split()[0])
print("pandas     :", pd.__version__)
print("프로젝트 루트:", ROOT)
print("작업 폴더   :", Path.cwd())

# %%
# [셀 2] 파일이 제자리에 있는지 확인
#
# 여기부터는 원천 데이터가 있어야 합니다. 없으면 아래 안내대로 먼저 만드십시오.
BASE = ROOT / "data" / "sample"                  # 소형본으로 시작한다
BASE_CM = ROOT / "data" / "raw" / "common"

missing = []
for name in ["cust_master.csv", "order_2026.csv", "product_api_response.json"]:
    fp = BASE / name
    if fp.exists():
        print(f"{name:28s} 있음   {fp.stat().st_size:>7,} bytes")
    else:
        print(f"{name:28s} 없음 ***")
        missing.append(name)

if missing:
    print()
    print("=" * 62)
    print("원천 데이터가 아직 없습니다. PowerShell 에서 아래를 실행하십시오.")
    print()
    print("  cd " + str(ROOT))
    print("  .\\.venv\\Scripts\\python.exe src\\make_dataset.py "
          "--variant common --size sample --out data\\sample")
    print()
    print("실행한 뒤 이 셀을 다시 돌리면 '있음' 으로 바뀝니다.")
    print("=" * 62)

# %%
# [셀 3] 고객 파일을 '아무 옵션 없이' 읽어 본다 — 일부러 틀리게 읽는다
cust_bad = pd.read_csv(BASE / "cust_master.csv", encoding="euc-kr")
print(cust_bad[["CUST_NO", "CUST_NM", "GRD_CD"]].head(4).to_string(index=False))
print()
print("CUST_NO 의 자료형 :", cust_bad["CUST_NO"].dtype)
# 무엇이 사라졌는지 눈으로 확인할 것

# %%
# [셀 4] dtype=str 로 다시 읽는다
cust = pd.read_csv(BASE / "cust_master.csv", encoding="euc-kr", dtype=str)
print(cust[["CUST_NO", "CUST_NM", "GRD_CD"]].head(4).to_string(index=False))
print()
print("CUST_NO 의 자료형 :", cust["CUST_NO"].dtype)

# %%
# [셀 5] 인코딩을 틀리면 어떻게 되는가 — 일부러 실패시킨다
try:
    pd.read_csv(BASE / "cust_master.csv", encoding="utf-8", dtype=str)
    #pd.read_csv(BASE / "cust_master.csv", encoding="euc-kr", dtype=str)
except UnicodeDecodeError as e:
    print("UnicodeDecodeError 발생")
    print(" ", e)

# %%
# [셀 6] 주문 파일 — BOM 이 붙은 UTF-8

# BOM - Byte Order Mark (바이트 순서 표시)
# 국내 대부분 기업은 " enc-kr" // 최근 시스템 "utf-8", "cp949"

import csv

with open(BASE / "order_2026.csv", encoding="utf-8", newline="") as f:
    header_bad = next(csv.reader(f))
print("내장 csv + encoding='utf-8'     첫 컬럼명:", repr(header_bad[0]))

with open(BASE / "order_2026.csv", encoding="utf-8-sig", newline="") as f:
    header_ok = next(csv.reader(f))
print("내장 csv + encoding='utf-8-sig' 첫 컬럼명:", repr(header_ok[0]))

# %%
# [셀 7] pandas 로 주문 파일 읽기 — 반드시 utf-8-sig 와 dtype=str
#order = pd.read_csv(BASE / "order_2026.csv", encoding="utf-8-sig", dtype=str)
order = pd.read_csv(BASE_CM / "order_2026.csv", encoding="utf-8-sig", dtype=str)
print("행 수 :", len(order))
print("컬럼  :", list(order.columns))
print()
print(order.head(6).to_string(index=False))

# %%
# [셀 8] 상품 API 응답 — items 아래로 한 단계 들어가야 표가 나온다
import json

with open(BASE / "product_api_response.json", encoding="utf-8") as f:
    payload = json.load(f)

print("최상위 키 :", list(payload.keys()))
print("totalCount:", payload["totalCount"])
prod = pd.DataFrame(payload["items"])
print()
print(prod.to_string(index=False))

# %%
# [셀 9] 오늘의 질문 — 답을 적어 두고 내일 확인한다
print("Q1. order_2026.csv 의 한 행은 무엇을 뜻하는가?")
print("Q2. 같은 대상을 가리키는데 이름이 다른 컬럼을 찾아라.")
print("Q3. ORD_ST 에 몇 가지 표기가 나오는가?")
print()
print("ORD_ST 값 목록:", sorted(order["ORD_ST"].unique()))
print("DLV_FEE 를 주문번호별로 보면:")
print(order.groupby("ORD_NO")["DLV_FEE"].apply(list).head(4).to_string())
