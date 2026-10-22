# 8교시 — 지저분한 데이터 정리하기 (지우지 말고 나눈다)

# %% [8-A] 지저분한 주문 읽기
# > [8-1] orders_dirty.json 을 읽고 몇 건인지 확인하세요
import json
import pandas as pd
with open("data/orders_dirty.json", "r", encoding="utf-8") as f:
    dirty = json.load(f)
print(len(dirty))



# %% [8-B] 6교시 방법 그대로 펼치면
# > [8-2] 6교시 [6-8] 의 json_normalize 코드를 그대로 써 보세요
meta = ["order_id", "ordered_at", "status", ["customer", "id"], ["customer", "city"]]
raw = pd.json_normalize(dirty, record_path="items", meta=meta)


# %% [8-C] 없는 키는 비워 두기 — errors="ignore"
# > [8-3] errors="ignore" 를 넣어 다시 펼치세요. 도시 칸은?
raw = pd.json_normalize(dirty, record_path="items", meta=meta, errors="ignore")
#raw = pd.json_normalize(dirty, record_path="items", meta=meta, errors="raise")

print(raw)
print(raw.shape)


# %% [8-D] 합계부터 내 보면
# > [8-4] qty 합계를 구해 보세요
print(raw["qty"].dtype)
print((raw["qty"]).sum())


# %% [8-E] 중복 찾기 — 주문번호로 지우면 안 되는 이유
# > [8-5] 완전히 같은 행이 몇 개인지 세고, 주문번호 기준으로도 지워 보세요
print(raw.duplicated().sum())
print(raw[raw.duplicated(keep=False)])

wrong = raw.drop_duplicates(subset="order_id")
print(wrong)
print(wrong[wrong["order_id"] == "O001"])

# %% [8-F] 완전 중복만 지운다
# > [8-6] subset 없이 drop_duplicates 하세요. O001 은 몇 행인가요?
step1 = raw.drop_duplicates().copy()
print(step1[step1["order_id"] == "O001"])


# %% [8-G] 숫자로 바꾸기 — 실패하면 멈춘다
# > [8-7] pd.to_numeric 으로 qty 를 숫자로 바꿔 보세요
pd.to_numeric(step1["qty"])


# %% [8-H] 숫자로 바꾸기 — 실패한 칸은 비워 둔다
# > [8-8] errors="coerce" 를 넣고, 원래 값과 나란히 보세요
step1["qty_num"] = pd.to_numeric(step1["qty"], errors="coerce") # 격리
print(step1)
print(step1[["order_id", "qty"]])


# %% [8-I] 날짜로 바꾸기
# > [8-9] 날짜도 coerce 로 바꿔 보고, 마지막 줄에서 coerce 없이도 실행해 보세요
step1["date"] = pd.to_datetime(step1["ordered_at"], errors="coerce")
print(step1[["order_id", "ordered_at", "date"]])

# %% [8-J] 빈칸을 0 으로 채우면 — 틀린 방법
# > [8-10] 모르는 수량을 0 으로 채우고 행 수·평균을 비교하세요
print(step1["qty_num"].count(), step1["qty_num"].mean())

filled = step1["qty_num"].fillna(0) # null은 0으로 대체
print(len(filled), filled.mean())

# %% [8-K] 판단할 수 없는 행은 따로 — 격리
# > [8-11] 수량 또는 날짜가 비어 있는 행을 bad, 나머지를 good 으로 나누세요
bad_mask = step1["qty_num"].isna() | step1["date"].isna()
bad = step1[bad_mask].copy()
good = step1[~bad_mask].copy()
print(len(bad), len(good))
print(good["customer.city"])

# %% [8-L] 판단할 수 있는 빈칸은 채운다
# > [8-12] 도시는 모르면 "미상" 으로 채워도 됩니다. 숫자 열도 정수로 정리하세요
good["customer.city"] = good["customer.city"].fillna("미상")
good["qty"] = good["qty_num"].astype("int64")
good = good.drop(columns=["qty_num"])
print(good)

# %% [8-M] 건수 맞추기 — 한 행도 사라지지 않았다
# > [8-13] 원본 = 중복 + 격리 + 정상 이 맞는지 확인하고, 격리 행을 파일로 남기세요
from pathlib import Path
n_dup = len(raw) - len(step1)

# 전체 = 중복 + 격리 + 정상
print(len(raw), "=", n_dup, "+", len(bad), "+", len(good))

from pathlib import Path
Path("output").mkdir(exist_ok=True)
bad.to_csv("output/quarantine.csv", index=False, encoding="utf-8")

print(pd.read_csv("output/quarantine.csv")[["order_id", "qty", "ordered_at"]])

# %% [8-N] (혼자) 정리된 표로 답하기
# > [8-14] good 의 수량 합계는?
# > [8-15] good 에서 도시별 행 수를 세세요 (value_counts)
# > [8-16] 격리된 행이 3개인 이유를 행마다 한 줄로 적어 보세요 (주석으로)

