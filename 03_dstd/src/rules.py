# -*- coding: utf-8 -*-
"""
rules.py — 변환 규칙 함수 모음 (3일차에 만들고 7일차 ETL 이 그대로 가져다 쓴다)

원칙
  판단할 수 있는 것은 고친다          → 변환
  완전히 같은 행이 또 들어간 것은 뺀다 → 제거 (건수는 남긴다)
  업무 정책상 집계에서 빼는 것        → 제외 (오류가 아니다)
  어느 값이 맞는지 판단할 수 없는 것  → 격리 (삭제하지 않는다)

모든 함수는 "값, 사유" 두 개를 돌려준다. 사유가 None 이면 성공이다.
실패를 예외로 던지지 않고 사유로 돌려주는 이유 — 한 행이 실패해도 나머지 3,951행은 계속 처리해야 하기 때문이다.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

# ── 업무설명서에서 온 상수 ──────────────────────────────────────
AS_OF = date(2026, 6, 30)                 # 2.8 기준일
OBS_START = date(2026, 1, 1)              # 2.8 관측 기간 시작
FX = {"KRW": Decimal("1"), "USD": Decimal("1350"), "JPY": Decimal("9.2")}   # 2.5 고정 환율
GUEST_VALUES = {"", "GUEST"}              # 2.1 비회원 표기

# ── 규칙 목록 — 변환규칙정의서의 규칙ID 와 1:1 로 맞아야 한다 ────────
#   분류: 변환 / 제거 / 제외 / 격리 / 표시
RULES = {
    "R01": ("제거", "(ORD_NO, ORD_SEQ) 와 나머지 값까지 완전히 같은 행 — 첫 행만 남긴다"),
    "R02": ("격리", "(ORD_NO, ORD_SEQ) 는 같은데 값이 다른 행 — 둘 다 격리"),
    "R03": ("격리", "한 주문 안에서 주문 단위 속성(고객·주문일·결제수단·배송비·상태)이 다름 — 주문 전체 격리"),
    "R04": ("격리", "ORD_DT 를 날짜로 바꿀 수 없음 (허용 형식 아님 / 존재하지 않는 날짜)"),
    "R05": ("격리", "AMT 를 원 단위 정수로 바꿀 수 없음"),
    "R06": ("격리", "ORD_ST 가 표준 상태 6개 중 무엇인지 정할 수 없음 (확인필요 / 매핑 없음)"),
    "R07": ("격리", "CUSTNO 가 고객 마스터에 없음 (비회원 표기 제외)"),
    "R08": ("격리", "GOODS_CD 가 상품 마스터에 없음"),
    "E01": ("제외", "비회원 주문 (CUSTNO 공란·GUEST) — 매출에는 포함, 고객 특성 집계에서 제외"),
    "E02": ("제외", "취소·환불 (CANCEL_DT 있음 또는 상태 CANCELED·REFUNDED)"),
    "E03": ("제외", "관측 기간 밖 (ORD_DT > 2026-06-30) — 정답 기간 데이터"),
    "F01": ("표시", "PAY_MTHD = ETC — 격리하지 않고 확인필요로 표시"),
    "P01": ("격리", "상품 taxType 이 정의 없는 값 (Z 등)"),
    "P02": ("변환", "외화 단가를 고정 환율로 원화 환산 — 원본 보존, 환산여부 기록"),
    "C01": ("변환", "날짜 4가지 형식 → DATE (ORD_DT·JOIN_DT·regDate 공통)"),
    "C02": ("변환", "금액 공백·콤마·.0 정리 → 원 단위 정수"),
    "C03": ("변환", "원천 코드 표기 → 표준 코드값 (코드매핑 시트)"),
}
ORDER_PRIORITY = ["R01", "R02", "R03", "R04", "R05", "R06", "R07", "R08", "E01", "E02", "E03"]

# ── 날짜 ────────────────────────────────────────────────────────
DATE_FORMATS = [                          # 2.9 — 이 네 가지만 허용한다
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
    (re.compile(r"^\d{4}/\d{2}/\d{2}$"), "%Y/%m/%d"),
    (re.compile(r"^\d{8}$"), "%Y%m%d"),
    (re.compile(r"^\d{2}-\d{2}-\d{4}$"), "%d-%m-%Y"),   # 일이 먼저 온다
]


def parse_date(s):
    """'15-03-2026' → (date(2026,3,15), None) / '2026-02-30' → (None, '존재하지 않는 날짜')"""
    if s is None or (isinstance(s, float) and pd.isna(s)) or str(s).strip() == "":
        return None, "빈 값"
    s = str(s).strip()
    for pat, fmt in DATE_FORMATS:
        if pat.match(s):
            try:
                return datetime.strptime(s, fmt).date(), None
            except ValueError:
                return None, "존재하지 않는 날짜"
    return None, "허용하지 않는 형식"


# ── 금액 ────────────────────────────────────────────────────────
AMOUNT_RE = re.compile(r"^-?\d+(\.\d+)?$")


def parse_amount(s):
    """' 19,800 ' → (19800, None) / '107730.0' → (107730, None) / '12.5' → (None, '소수점 이하 값 있음')"""
    if s is None or (isinstance(s, float) and pd.isna(s)) or str(s).strip() == "":
        return None, "빈 값"
    t = str(s).strip().replace(",", "")
    if not AMOUNT_RE.match(t):
        return None, "숫자가 아님"
    d = Decimal(t)
    if d != d.to_integral_value():
        return None, "소수점 이하 값 있음"
    return int(d), None


def to_krw(amount, currency):
    """('22.15', 'USD') → (29903, True, None) / ('39900', 'KRW') → (39900, False, None)"""
    cur = (currency or "").strip()
    if cur not in FX:
        return None, False, f"환율 없는 통화 '{cur}'"
    try:
        d = Decimal(str(amount).strip().replace(",", ""))
    except InvalidOperation:
        return None, False, "숫자가 아님"
    krw = (d * FX[cur]).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(krw), cur != "KRW", None


# ── 코드 매핑 ───────────────────────────────────────────────────
def load_code_map(mapping_path):
    """매핑표.xlsx 「코드매핑」 시트 → {(원천파일, 원천컬럼, 원천값): (표준코드값, 처리)}"""
    from openpyxl import load_workbook
    wb = load_workbook(mapping_path, read_only=True, data_only=True)
    ws = wb["코드매핑"]
    rows = ws.iter_rows(values_only=True)
    head = [str(h).strip() if h is not None else "" for h in next(rows)]
    ix = {h: i for i, h in enumerate(head)}
    cmap = {}
    for r in rows:
        if r is None or all(v is None for v in r):
            continue
        get = lambda k: ("" if r[ix[k]] is None else str(r[ix[k]]).strip())
        cmap[(get("원천파일"), get("원천컬럼"), get("원천값"))] = (get("표준코드값") or None, get("처리"))
    wb.close()
    return cmap


def map_code(cmap, fname, col, raw):
    """→ (표준코드값, 상태)  상태: '매핑' / '확인필요' / '매핑없음'"""
    key = (fname, col, "" if raw is None or (isinstance(raw, float) and pd.isna(raw)) else str(raw))
    if key not in cmap:
        return None, "매핑없음"
    std, how = cmap[key]
    if how == "확인필요" or not std:
        return None, "확인필요"
    return std, "매핑"


# ── 원천 읽기 (1일차에 배운 옵션 그대로) ──────────────────────────
def load_sources(base):
    base = Path(base)
    cust = pd.read_csv(base / "cust_master.csv", encoding="euc-kr", dtype=str, keep_default_na=False)
    order = pd.read_csv(base / "order_2026.csv", encoding="utf-8-sig", dtype=str, keep_default_na=False)
    with open(base / "product_api_response.json", encoding="utf-8") as f:
        prod = pd.DataFrame(json.load(f, parse_int=str, parse_float=str)["items"])
    order.insert(0, "원천행", range(2, len(order) + 2))   # 스프레드시트 행 번호 (머리행 = 1)
    return cust, order, prod


# ── 상품 분류 ───────────────────────────────────────────────────
def classify_products(prod, cmap):
    p = prod.copy()
    p["TAX_TP_CD"], p["과세상태"] = zip(*[map_code(cmap, "product_api_response.json", "taxType", v) for v in p["taxType"]])
    conv = [to_krw(a, c) for a, c in zip(p["unitPrice"], p["currency"])]
    p["원화정가금액"] = [c[0] for c in conv]
    p["환산여부"] = ["Y" if c[1] else "N" for c in conv]
    p["분류"] = ["격리" if s != "매핑" else "정상" for s in p["과세상태"]]
    p["규칙ID"] = ["P01" if s != "매핑" else "" for s in p["과세상태"]]
    return p


# ── 주문 항목 분류 ──────────────────────────────────────────────
def classify_orders(order, cust, prod, cmap):
    """주문 항목 한 행마다 분류(정상/제거/제외/격리)와 규칙ID 하나를 붙인다. 행은 하나도 지우지 않는다."""
    o = order.copy()
    F = "order_2026.csv"
    hit = {k: pd.Series(False, index=o.index) for k in ORDER_PRIORITY}
    why = {k: pd.Series("", index=o.index) for k in ORDER_PRIORITY}
    src_cols = [c for c in order.columns if c != "원천행"]

    # 값 변환 (C01·C02·C03)
    d = [parse_date(v) for v in o["ORD_DT"]]
    o["ORD_DT_STD"] = [x[0] for x in d]
    a = [parse_amount(v) for v in o["AMT"]]
    o["SALE_AMT"] = [x[0] for x in a]
    f = [parse_amount(v) for v in o["DLV_FEE"]]
    o["DLV_FEE_AMT"] = [x[0] for x in f]
    st = [map_code(cmap, F, "ORD_ST", v) for v in o["ORD_ST"]]
    o["ORD_STAT_CD"] = [x[0] for x in st]
    pm = [map_code(cmap, F, "PAY_MTHD", v) for v in o["PAY_MTHD"]]
    o["PAY_MTHD_CD"] = [x[0] for x in pm]
    o["확인필요"] = ["Y" if x[1] != "매핑" else "" for x in pm]            # F01

    # R01 완전 중복 — 두 번째부터 제거
    full_dup = o.duplicated(src_cols, keep="first")
    hit["R01"] = full_dup
    why["R01"][full_dup] = "완전히 같은 행이 앞에 있음"

    # R02 키 중복·값 불일치 — 제거하고 남은 행 기준
    live = o[~full_dup]
    key_dup = live.duplicated(["ORD_NO", "ORD_SEQ"], keep=False)
    idx = live.index[key_dup]
    hit["R02"][idx] = True
    why["R02"][idx] = "같은 키에 다른 값이 있음"

    # R03 주문 단위 속성 불일치 — 표기 차이가 아니라 '값'이 다른지 본다
    lv = o.loc[~full_dup].copy()
    lv["_ORD_DT"] = [str(v) if v else r for v, r in zip(lv["ORD_DT_STD"], lv["ORD_DT"])]
    lv["_ORD_ST"] = [v if v else r for v, r in zip(lv["ORD_STAT_CD"], lv["ORD_ST"])]
    lv["_FEE"] = [str(v) if v is not None else r for v, r in zip(lv["DLV_FEE_AMT"], lv["DLV_FEE"])]
    head_cols = {"CUSTNO": "고객", "_ORD_DT": "주문일", "PAY_MTHD": "결제수단", "_FEE": "배송비", "_ORD_ST": "상태"}
    nun = lv.groupby("ORD_NO")[list(head_cols)].nunique()
    bad = nun[(nun > 1).any(axis=1)]
    for ord_no, r in bad.iterrows():
        which = ",".join(head_cols[c] for c in head_cols if r[c] > 1)
        m = o["ORD_NO"] == ord_no
        hit["R03"] |= m
        why["R03"][m] = f"한 주문 안에서 {which} 값이 다름"

    # R04 날짜 · R05 금액 · R06 상태
    for i, (v, reason) in enumerate(d):
        if reason:
            hit["R04"].iat[i] = True
            why["R04"].iat[i] = f"'{o['ORD_DT'].iat[i]}' — {reason}"
    for i, (v, reason) in enumerate(a):
        if reason:
            hit["R05"].iat[i] = True
            why["R05"].iat[i] = f"'{o['AMT'].iat[i]}' — {reason}"
    for i, (v, s) in enumerate(st):
        if s != "매핑":
            hit["R06"].iat[i] = True
            why["R06"].iat[i] = f"'{o['ORD_ST'].iat[i]}' — {s}"

    # R07 · R08 참조
    guest = o["CUSTNO"].str.strip().isin(GUEST_VALUES)
    no_cust = ~guest & ~o["CUSTNO"].isin(set(cust["CUST_NO"]))
    hit["R07"] = no_cust
    why["R07"][no_cust] = "고객 마스터에 없는 '" + o["CUSTNO"][no_cust] + "'"
    no_prod = ~o["GOODS_CD"].isin(set(prod["goodsCode"]))
    hit["R08"] = no_prod
    why["R08"][no_prod] = "상품 마스터에 없는 '" + o["GOODS_CD"][no_prod] + "'"

    # E01 · E02 · E03 정상 제외
    hit["E01"] = guest
    why["E01"][guest] = "비회원 '" + o["CUSTNO"][guest].replace("", "(공란)") + "'"
    canc = (o["CANCEL_DT"].str.strip() != "") | o["ORD_STAT_CD"].isin(["CANCELED", "REFUNDED"])
    hit["E02"] = canc
    why["E02"][canc] = "취소·환불"
    late = o["ORD_DT_STD"].map(lambda x: x is not None and x > AS_OF)
    hit["E03"] = late
    why["E03"][late] = "관측 기간 밖 (정답 기간)"

    # 우선순위대로 규칙 하나만 고른다
    o["규칙ID"] = "N00"
    o["분류"] = "정상"
    o["사유"] = ""
    o["해당규칙"] = ""
    for k in reversed(ORDER_PRIORITY):
        m = hit[k]
        o.loc[m, "규칙ID"] = k
        o.loc[m, "분류"] = RULES[k][0]
        o.loc[m, "사유"] = why[k][m]
    o["해당규칙"] = [",".join(k for k in ORDER_PRIORITY if hit[k].iat[i]) for i in range(len(o))]
    return o


def summary(o):
    """분류·규칙별 건수 표"""
    t = (o.groupby(["분류", "규칙ID"]).size().rename("행수").reset_index())
    order = {"정상": 0, "제거": 1, "제외": 2, "격리": 3}
    t["_o"] = t["분류"].map(order)
    t = t.sort_values(["_o", "규칙ID"]).drop(columns="_o")
    t["설명"] = [RULES[k][1].split(" — ")[0] if k in RULES else "규칙에 걸리지 않음" for k in t["규칙ID"]]
    return t.reset_index(drop=True)


def balance(o):
    """원천 행 수 = 정상 + 제거 + 제외 + 격리 — 한 행도 사라지면 안 된다"""
    n = o["분류"].value_counts()
    parts = {k: int(n.get(k, 0)) for k in ["정상", "제거", "제외", "격리"]}
    return len(o), parts, len(o) == sum(parts.values())


def _w(t):
    return sum(2 if ord(c) > 0x1100 else 1 for c in str(t))


def show(df, cols=None):
    """한글 폭을 계산해 왼쪽 정렬로 표를 찍는다 (pandas to_string 은 한글 줄이 어긋난다)"""
    d = df[cols] if cols else df
    d = d.astype(str)
    widths = [max(_w(c), *(_w(v) for v in d[c])) for c in d.columns]
    line = lambda vals: "  ".join(str(v) + " " * (w - _w(v)) for v, w in zip(vals, widths)).rstrip()
    print(line(d.columns))
    for row in d.itertuples(index=False):
        print(line(row))
