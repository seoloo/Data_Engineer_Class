# -*- coding: utf-8 -*-
"""
make_dataset.py — 한빛유통 레거시 원천 데이터 생성기

「데이터 표준화 및 AI Ready Data 구축 전략」 과목 실습용.
세 개의 레거시 시스템에서 내보낸 것처럼 보이는 원천 파일 3종을 생성한다.

  1) cust_master.csv          회원관리 시스템 내보내기   (EUC-KR)
  2) order_2026.csv           구 주문관리 시스템 내보내기 (UTF-8 BOM)
  3) product_api_response.json 신 상품 시스템 API 응답    (UTF-8)

사용 예)
  python src/make_dataset.py --variant common --size sample --out data/sample
  python src/make_dataset.py --variant common --size full   --out data/raw/common
  python src/make_dataset.py --variant A      --size full   --out data/raw/teamA

같은 인자로 재실행하면 항상 동일한 바이트가 나온다(시드 고정).
주입한 오류는 <out>/../../instructor 가 아니라 --answer 로 지정한 폴더에 기록한다.
기본값은 out 폴더 옆의 _answer 폴더.

표준 라이브러리만 사용한다. pandas 는 필요하지 않다.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 고정 상수 — 문서와 반드시 일치시킬 것
# ─────────────────────────────────────────────────────────────
AS_OF_DATE = date(2026, 6, 30)      # 실습 기준일
OBS_START = date(2026, 1, 1)        # 관측 기간 시작
OBS_END = date(2026, 6, 30)         # 관측 기간 종료
LABEL_START = date(2026, 7, 1)      # 정답 기간 시작
LABEL_END = date(2026, 7, 31)       # 정답 기간 종료
USD_RATE = 1350                     # 고정 환율 (원/USD)
JPY_RATE = 9.2                      # 고정 환율 (원/JPY)

SURNAMES = ["김", "이", "박", "최", "정", "강", "조", "윤", "장", "임",
            "한", "오", "서", "신", "권", "황", "안", "송", "류", "홍"]
GIVEN_NAMES = ["가온", "다빈", "도현", "라온", "민서", "보람", "산하", "새롬", "서준", "세아",
               "소윤", "수빈", "슬기", "시온", "아름", "예린", "우진", "유나", "윤호", "은결",
               "이든", "재윤", "주안", "지후", "찬영", "채원", "하늘", "하린", "한결", "해든",
               "현우", "혜원", "호준", "다온", "미르", "나린", "단비", "온유", "지율", "태희"]
SIDO = ["서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
        "대전광역시", "울산광역시", "경기도", "강원특별자치도", "충청북도",
        "충청남도", "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도"]

CATEGORY_ITEMS = {
    "주방": ["스테인리스 냄비", "코팅 프라이팬", "실리콘 주걱", "유리 밀폐용기", "대나무 도마",
             "주방가위", "계량컵 세트", "실리콘 냄비받침"],
    "욕실": ["샤워 헤드", "칫솔 살균기", "논슬립 욕실매트", "수건걸이", "코너 선반",
             "비누 디스펜서", "배수구 트랩"],
    "청소": ["극세사 걸레", "돌돌이 클리너", "정전기 청소포", "창틀 브러시", "다용도 솔",
             "먼지떨이", "스팀 청소포"],
    "수납": ["접이식 수납박스", "투명 리빙박스", "서랍 정리함", "옷걸이 세트", "진공 압축팩",
             "신발 정리대", "다용도 바구니"],
    "침구": ["극세사 이불", "구스 베개", "매트리스 커버", "라텍스 토퍼", "여름 인견 패드",
             "차렵 이불세트", "경추 베개"],
    "생활잡화": ["멀티탭", "LED 무드등", "문틈 방풍패킹", "미끄럼방지 테이프", "우산꽂이",
                 "빨래건조대", "다용도 후크"],
}
SPECS = ["소형", "중형", "대형", "2입", "4입", "리필", "기본형", "프리미엄"]

PAY_METHODS = ["CARD", "BANK", "VBANK", "MOBILE"]
GRADE_CODES = ["G01", "G02", "G03", "G04"]

# 업무 상태(정답). 원천 파일에는 변형된 표기로 저장된다.
CANON_STATUS = ["RECEIVED", "PAID", "SHIPPING", "DELIVERED", "CANCELED", "REFUNDED"]

# ─────────────────────────────────────────────────────────────
# 변형(팀)별 설정
# ─────────────────────────────────────────────────────────────
VARIANTS = {
    "common": dict(
        seed=20261002,
        order_cust_col="CUSTNO",
        order_goods_col="GOODS_CD",
        status_raw={
            "RECEIVED": ["01", "1"],
            "PAID": ["02", "결제완료"],
            "SHIPPING": ["03", "배송중"],
            "DELIVERED": ["04", "COMPLETE", "완료", "CMPL", "배송완료"],
            "CANCELED": ["09", "취소", "CANCEL"],
            "REFUNDED": ["10", "환불", "REFUND"],
        },
        status_unknown="99",
        currencies={"USD": 6},          # USD 로 등록된 상품 수
        thousand_unit_category=None,    # 단가를 천원 단위로 등록한 카테고리
        bad_date_months=[3],
    ),
    "A": dict(
        seed=20261003,
        order_cust_col="MEM_ID",
        order_goods_col="ITEM_NO",
        status_raw={
            "RECEIVED": ["10", "1"],
            "PAID": ["20", "2"],
            "SHIPPING": ["30", "3"],
            "DELIVERED": ["40", "4", "40.0"],
            "CANCELED": ["90", "9"],
            "REFUNDED": ["91"],
        },
        status_unknown="88",
        currencies={"USD": 6},
        thousand_unit_category=None,
        bad_date_months=[2],
    ),
    "B": dict(
        seed=20261004,
        order_cust_col="MEM_NO",
        order_goods_col="ITEM_CD",
        status_raw={
            "RECEIVED": ["RCV", "RECV"],
            "PAID": ["PAID", "PAY"],
            "SHIPPING": ["SHIP", "SHIPPING"],
            "DELIVERED": ["DONE", "DELIVERED", "CMPLT"],
            "CANCELED": ["CNCL", "CANCEL"],
            "REFUNDED": ["RFND", "REFUND"],
        },
        status_unknown="UNKN",
        currencies={},
        thousand_unit_category="침구",   # 침구 단가만 천원 단위
        bad_date_months=[5],
    ),
    "C": dict(
        seed=20261005,
        order_cust_col="CUST_ID_OLD",
        order_goods_col="PRD_CODE",
        status_raw={
            "RECEIVED": ["접수", "접수완료"],
            "PAID": ["결제완료", "결제"],
            "SHIPPING": ["배송중", "출고"],
            "DELIVERED": ["배송완료", "완료", "DONE"],
            "CANCELED": ["취소", "주문취소"],
            "REFUNDED": ["환불", "반품환불"],
        },
        status_unknown="확인불가",
        currencies={"USD": 4, "JPY": 2},
        thousand_unit_category=None,
        bad_date_months=[1, 6],
    ),
}

SIZES = {
    "sample": dict(n_cust=8, n_prod=6, n_order=14,
                   err=dict(email_missing=1, dup_full=2, dup_partial=1, bad_date=1,
                            orphan_cust=1, orphan_goods=1, status_unknown=1,
                            pay_etc=1, tax_unknown=1, guest=2, withdrawn=1,
                            order_attr_conflict=1)),
    "full": dict(n_cust=300, n_prod=80, n_order=1500,
                 err=dict(email_missing=36, dup_full=20, dup_partial=5, bad_date=3,
                          orphan_cust=18, orphan_goods=11, status_unknown=7,
                          pay_etc=9, tax_unknown=2, guest=25, withdrawn=14,
                          order_attr_conflict=6)),
}

BAD_DATE_LITERALS = ["2026-02-30", "2026-13-05"]


# ─────────────────────────────────────────────────────────────
# 정답 기록기
# ─────────────────────────────────────────────────────────────
class AnswerLog:
    """주입한 오류를 행 단위로 기록한다. 정답을 손으로 쓰지 않기 위한 장치."""

    FIELDS = ["파일", "행번호", "키", "컬럼", "원래값", "주입값", "오류유형", "기대처리"]

    def __init__(self):
        self.rows = []
        self.counts = {}          # 행 기준 건수
        self.order_counts = {}    # 주문 기준 건수(주문 단위 주입만)

    def add(self, 파일, 행번호, 키, 컬럼, 원래값, 주입값, 오류유형, 기대처리):
        self.rows.append({
            "파일": 파일, "행번호": 행번호, "키": 키, "컬럼": 컬럼,
            "원래값": "" if 원래값 is None else str(원래값),
            "주입값": "" if 주입값 is None else str(주입값),
            "오류유형": 오류유형, "기대처리": 기대처리,
        })
        self.counts[오류유형] = self.counts.get(오류유형, 0) + 1

    def bump(self, 오류유형, n=1):
        self.counts[오류유형] = self.counts.get(오류유형, 0) + n

    def bump_order(self, 오류유형, n=1):
        self.order_counts[오류유형] = self.order_counts.get(오류유형, 0) + n

    def write_csv(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.FIELDS)
            w.writeheader()
            for r in sorted(self.rows, key=lambda x: (x["파일"], x["행번호"])):
                w.writerow(r)


# ─────────────────────────────────────────────────────────────
# 값 포맷터 — 형식 불일치를 만드는 부분
# ─────────────────────────────────────────────────────────────
def fmt_date(rng: random.Random, d: date) -> str:
    r = rng.random()
    if r < 0.60:
        return d.isoformat()                      # 2026-03-15
    if r < 0.80:
        return d.strftime("%Y/%m/%d")             # 2026/03/15
    if r < 0.95:
        return d.strftime("%Y%m%d")               # 20260315
    return d.strftime("%d-%m-%Y")                 # 15-03-2026


def fmt_amount(rng: random.Random, amount: int) -> str:
    r = rng.random()
    if r < 0.50:
        return str(amount)                        # 12000
    if r < 0.75:
        return f"{amount:,}"                      # 12,000
    if r < 0.90:
        return f" {amount} "                      # ' 12000 '
    return f"{amount}.0"                          # 12000.0


def rand_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


# ─────────────────────────────────────────────────────────────
# 생성
# ─────────────────────────────────────────────────────────────
def build_customers(rng, n, err, log):
    """고객 마스터. CUST_NO 는 7자리 zero-fill 문자열."""
    rows = []
    used_names = set()
    for i in range(1, n + 1):
        while True:
            nm = rng.choice(SURNAMES) + rng.choice(GIVEN_NAMES)
            if nm not in used_names or len(used_names) > n * 3:
                used_names.add(nm)
                break
        cust_no = f"{i:07d}"
        rows.append({
            "CUST_NO": cust_no,
            "CUST_NM": nm,
            "HP": f"010-0000-{rng.randint(0, 9999):04d}",
            "EMAIL": f"user{i:04d}@example.com",
            "JOIN_DT": fmt_date(rng, rand_date(rng, date(2022, 1, 1), date(2026, 5, 31))),
            "GRD_CD": rng.choice(GRADE_CODES),
            "ADDR_SIDO": rng.choice(SIDO),
            "DEL_YN": "N",
        })

    # 오류 2: 선행 0 은 생성 자체로 이미 존재 → 집계만 기록
    log.bump("식별자_선행0", len(rows))

    # 오류 3-a: EMAIL 결측
    for idx in rng.sample(range(len(rows)), err["email_missing"]):
        old = rows[idx]["EMAIL"]
        rows[idx]["EMAIL"] = ""
        log.add("cust_master.csv", idx + 1, rows[idx]["CUST_NO"], "EMAIL",
                old, "", "결측_이메일", "결측 유지. 필수 항목이 아니므로 제거하지 않는다")

    # 탈퇴 회원(오류 아님. 처리 기준이 필요한 정상 상태)
    for idx in rng.sample(range(len(rows)), err["withdrawn"]):
        rows[idx]["DEL_YN"] = "Y"
        log.add("cust_master.csv", idx + 1, rows[idx]["CUST_NO"], "DEL_YN",
                "N", "Y", "탈퇴회원_정상상태",
                "오류가 아니다. 주문 이력은 보존하되 마케팅 대상에서는 제외한다")
    return rows


def build_products(rng, n, cfg, err, log):
    """상품 마스터. goodsCode 는 4자리 zero-fill 문자열."""
    pool = []
    for cat, items in CATEGORY_ITEMS.items():
        for it in items:
            for sp in SPECS:
                pool.append((cat, f"{it} {sp}"))
    rng.shuffle(pool)

    rows = []
    for i in range(1, n + 1):
        cat, nm = pool[i - 1]
        price = rng.choice([3900, 5900, 7900, 9900, 12900, 15900, 19900,
                            24900, 29900, 39900, 49900, 59900])
        rows.append({
            "goodsCode": f"{i:04d}",
            "goodsName": nm,
            "categoryName": cat,
            "unitPrice": price,
            "currency": "KRW",
            "taxType": rng.choice(["TAX", "TAX", "TAX", "FREE"]),
            "useYn": "Y" if rng.random() > 0.08 else "N",
            "regDate": fmt_date(rng, rand_date(rng, date(2021, 1, 1), date(2026, 4, 30))),
        })

    log.bump("식별자_선행0", len(rows))

    # 오류 5-a: 통화 차이
    for cur, cnt in cfg["currencies"].items():
        cnt = min(cnt, max(1, n // 10))
        for idx in rng.sample(range(len(rows)), cnt):
            old_price, old_cur = rows[idx]["unitPrice"], rows[idx]["currency"]
            if cur == "USD":
                rows[idx]["unitPrice"] = round(old_price / USD_RATE, 2)
            else:
                rows[idx]["unitPrice"] = round(old_price / JPY_RATE, 1)
            rows[idx]["currency"] = cur
            log.add("product_api_response.json", idx + 1, rows[idx]["goodsCode"], "unitPrice/currency",
                    f"{old_price} {old_cur}", f"{rows[idx]['unitPrice']} {cur}",
                    "통화_차이", f"고정 환율로 KRW 환산({cur}). 환산 여부를 별도 컬럼에 기록")

    # 오류 5-b: 천원 단위 등록 (변형 B)
    tcat = cfg["thousand_unit_category"]
    if tcat:
        for idx, r in enumerate(rows):
            if r["categoryName"] == tcat:
                old = r["unitPrice"]
                r["unitPrice"] = round(old / 1000, 1)
                log.add("product_api_response.json", idx + 1, r["goodsCode"], "unitPrice",
                        old, r["unitPrice"], "금액단위_천원",
                        f"{tcat} 카테고리 단가는 천원 단위. 1000을 곱해 원 단위로 통일")

    # 오류 8-c: 의미 확정 불가 taxType
    for idx in rng.sample(range(len(rows)), err["tax_unknown"]):
        old = rows[idx]["taxType"]
        rows[idx]["taxType"] = "Z"
        log.add("product_api_response.json", idx + 1, rows[idx]["goodsCode"], "taxType",
                old, "Z", "의미확정불가_taxType",
                "업무설명서에 없는 값. 삭제·추측 금지. 격리 후 담당자 확인 목록에 등재")
    return rows


def build_orders(rng, cfg, n_order, customers, products, err, log):
    """주문 항목을 만든다.

    한 행 = 주문 항목(ORD_NO + ORD_SEQ 복합키).
    주문 단위 속성(고객, 주문일, 주문상태, 결제수단, 배송비, 취소일)은
    주문 단위로 결정하고, 그 주문의 모든 항목 행에 같은 값을 반복 기재한다.
    구 시스템의 실제 동작을 재현한 것이며, 헤더/항목 분리의 근거가 된다.
    """
    cust_ids = [c["CUST_NO"] for c in customers]
    goods = {p["goodsCode"]: p for p in products}
    goods_ids = list(goods.keys())
    fname = "order_2026.csv"

    orders = {}       # ord_no -> 주문 단위 속성
    order_seq = []    # 주문 순서 보존
    items = []        # 항목 단위 속성
    by_order = {}     # ord_no -> [items 인덱스]

    for o in range(1, n_order + 1):
        ord_no = f"ORD26{o:06d}"
        ord_dt = rand_date(rng, OBS_START, LABEL_END)
        r = rng.random()
        if r < 0.06:
            st = "CANCELED"
        elif r < 0.09:
            st = "REFUNDED"
        elif r < 0.80:
            st = "DELIVERED"
        elif r < 0.88:
            st = "SHIPPING"
        elif r < 0.95:
            st = "PAID"
        else:
            st = "RECEIVED"
        orders[ord_no] = dict(
            cust=rng.choice(cust_ids),
            ord_dt=ord_dt,
            ord_dt_str=fmt_date(rng, ord_dt),      # 주문 단위로 형식 결정
            st=st,
            st_raw=None,                            # 아래에서 결정
            pay=rng.choice(PAY_METHODS),
            dlv_fee=rng.choice([0, 0, 2500, 3000]),
            cancel_dt=(ord_dt + timedelta(days=rng.randint(0, 7))
                       if st in ("CANCELED", "REFUNDED") else None),
        )
        order_seq.append(ord_no)
        by_order[ord_no] = []
        for seq in range(1, rng.choices([1, 2, 3, 4], weights=[18, 27, 30, 25])[0] + 1):
            gcode = rng.choice(goods_ids)
            qty = rng.choices([1, 2, 3, 4, 5], weights=[55, 25, 12, 5, 3])[0]
            base = goods[gcode]["unitPrice"]
            if goods[gcode]["currency"] == "USD":
                base = round(base * USD_RATE)
            elif goods[gcode]["currency"] == "JPY":
                base = round(base * JPY_RATE)
            if cfg["thousand_unit_category"] and goods[gcode]["categoryName"] == cfg["thousand_unit_category"]:
                base = round(base * 1000)
            disc = rng.choice([1.0, 1.0, 1.0, 0.95, 0.9, 0.85])
            items.append(dict(ord_no=ord_no, seq=seq, goods=gcode, qty=qty,
                              amt=int(round(base * qty * disc / 10) * 10),
                              override={}))
            by_order[ord_no].append(len(items) - 1)

    # 상태 표기(주문 단위)
    for ord_no, m in orders.items():
        m["st_raw"] = rng.choice(cfg["status_raw"][m["st"]])

    def pick_orders(n, pool=None):
        pool = pool or order_seq
        return rng.sample(pool, min(n, len(pool)))

    used = set()

    def log_order(ord_no, col, old, new, kind, todo):
        """주문 단위 주입은 그 주문의 모든 항목 행을 정답에 기록한다."""
        for i in by_order[ord_no]:
            log.add(fname, i + 1, f"{ord_no}-{items[i]['seq']}", col, old, new, kind, todo)
        log.bump_order(kind)

    # ── 오류 7-a: 존재하지 않는 고객 참조 (주문 단위) ──────────
    for ord_no in pick_orders(err["orphan_cust"]):
        used.add(ord_no)
        old = orders[ord_no]["cust"]
        orders[ord_no]["cust"] = f"{rng.randint(9000, 9999):07d}"
        log_order(ord_no, "고객식별자", old, orders[ord_no]["cust"], "미존재참조_고객",
                  "삭제 금지. 격리 테이블로 분리하고 FK 위반 목록으로 보고")

    # ── 비회원 주문(오류 아님, 주문 단위) ─────────────────────
    for ord_no in pick_orders(err["guest"], [o for o in order_seq if o not in used]):
        used.add(ord_no)
        old = orders[ord_no]["cust"]
        orders[ord_no]["cust"] = rng.choice(["", "GUEST"])
        log_order(ord_no, "고객식별자", old, orders[ord_no]["cust"] or "(공란)",
                  "비회원주문_정상제외",
                  "오류가 아니다. 매출에는 포함, 고객별 구매 특성 집계에서는 제외")

    # ── 오류 8-a: 의미 확정 불가 상태 코드 (주문 단위) ────────
    for ord_no in pick_orders(err["status_unknown"]):
        old = orders[ord_no]["st_raw"]
        orders[ord_no]["st_raw"] = cfg["status_unknown"]
        log_order(ord_no, "ORD_ST", old, cfg["status_unknown"], "의미확정불가_상태코드",
                  "표준 코드에 없는 값. 추측 매핑 금지. 격리 후 담당자 확인 목록에 등재")

    # ── 오류 8-b: PAY_MTHD = ETC (주문 단위) ──────────────────
    for ord_no in pick_orders(err["pay_etc"]):
        old = orders[ord_no]["pay"]
        orders[ord_no]["pay"] = "ETC"
        log_order(ord_no, "PAY_MTHD", old, "ETC", "의미확정불가_결제수단",
                  "업무설명서에 정의 없음. 격리하지 않되 '확인필요'로 표시하고 집계에서 구분")

    # ── 오류 4: 잘못된 날짜 (주문 단위) ───────────────────────
    cand = [o for o in order_seq if orders[o]["ord_dt"].month in cfg["bad_date_months"]] or order_seq
    for k, ord_no in enumerate(pick_orders(err["bad_date"] * len(BAD_DATE_LITERALS), cand)):
        lit = BAD_DATE_LITERALS[k % len(BAD_DATE_LITERALS)]
        old = orders[ord_no]["ord_dt_str"]
        orders[ord_no]["ord_dt_str"] = lit
        log_order(ord_no, "ORD_DT", old, lit, "잘못된날짜",
                  "존재하지 않는 날짜. 임의 보정 금지. 격리 후 원천 담당자 확인")

    # ── 오류 7-b: 존재하지 않는 상품 참조 (항목 단위) ─────────
    for i in rng.sample(range(len(items)), err["orphan_goods"]):
        old = items[i]["goods"]
        items[i]["goods"] = f"{rng.randint(9000, 9999):04d}"
        log.add(fname, i + 1, f"{items[i]['ord_no']}-{items[i]['seq']}", "상품식별자",
                old, items[i]["goods"], "미존재참조_상품",
                "삭제 금지. 격리 테이블로 분리하고 FK 위반 목록으로 보고")

    # ── 오류 9: 주문 단위 속성이 항목 행마다 다른 경우 ────────
    multi = [o for o in order_seq if len(by_order[o]) >= 2]
    for ord_no in pick_orders(err["order_attr_conflict"], multi):
        i = by_order[ord_no][-1]
        col, old, new = ("DLV_FEE", str(orders[ord_no]["dlv_fee"]),
                         str(orders[ord_no]["dlv_fee"] + 2500))
        if rng.random() < 0.4:
            col, old = "ORD_DT", orders[ord_no]["ord_dt_str"]
            new = fmt_date(rng, orders[ord_no]["ord_dt"] + timedelta(days=1))
        items[i]["override"][col] = new
        log.add(fname, i + 1, f"{ord_no}-{items[i]['seq']}", col, old, new,
                "주문단위속성_불일치",
                "한 주문 안에서 주문 단위 속성이 항목마다 다르다. "
                "헤더를 만들 수 없으므로 해당 주문 전체를 격리하고 원천 담당자 확인")

    # ── 문자열 표기로 변환 ────────────────────────────────────
    rows = []
    for i, it in enumerate(items):
        m = orders[it["ord_no"]]
        ov = it["override"]
        rows.append({
            "ORD_NO": it["ord_no"],
            "ORD_SEQ": str(it["seq"]),
            cfg["order_cust_col"]: m["cust"],
            cfg["order_goods_col"]: it["goods"],
            "ORD_DT": ov.get("ORD_DT", m["ord_dt_str"]),
            "QTY": str(it["qty"]),
            "AMT": fmt_amount(rng, it["amt"]),
            "ORD_ST": m["st_raw"],
            "PAY_MTHD": m["pay"],
            "DLV_FEE": ov.get("DLV_FEE", str(m["dlv_fee"])),
            "CANCEL_DT": m["cancel_dt"].isoformat() if m["cancel_dt"] else "",
        })
    log.bump("날짜형식_불일치", len(rows))
    log.bump("금액형식_불일치", len(rows))
    log.bump("상태코드_불일치", len(rows))
    log.bump("배송비_주문단위값_항목행반복", len(rows))

    # ── 오류 3-b: 완전 중복 ───────────────────────────────────
    base_n = len(rows)
    for i in rng.sample(range(base_n), err["dup_full"]):
        dup = dict(rows[i])
        rows.append(dup)
        log.add(fname, len(rows), f"{dup['ORD_NO']}-{dup['ORD_SEQ']}", "(행 전체)",
                "(원본 존재)", "(완전 동일 행)", "중복_완전일치",
                "1건만 유지하고 제거 건수를 로그에 남긴다")

    # ── 오류 3-c: 값이 다른 부분 중복 ─────────────────────────
    for i in rng.sample(range(base_n), err["dup_partial"]):
        dup = dict(rows[i])
        old_amt = dup["AMT"]
        try:
            v = int(float(old_amt.replace(",", "").strip()))
        except ValueError:
            v = 10000
        dup["AMT"] = str(v + 1000)
        rows.append(dup)
        log.add(fname, len(rows), f"{dup['ORD_NO']}-{dup['ORD_SEQ']}", "AMT",
                old_amt, dup["AMT"], "중복_부분불일치",
                "키는 같은데 값이 다르다. 임의 선택 금지. 격리 후 원천 담당자 확인")

    return rows, orders


# ─────────────────────────────────────────────────────────────
# 파일 쓰기
# ─────────────────────────────────────────────────────────────
def write_customers(rows, path: Path):
    """회원관리 시스템 내보내기 — EUC-KR. 인코딩 실습 재료."""
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\r\n")
    w.writeheader()
    w.writerows(rows)
    path.write_bytes(buf.getvalue().encode("euc-kr"))


def write_orders(rows, path: Path):
    """구 주문관리 시스템 내보내기 — UTF-8 BOM. encoding 미지정 시 첫 컬럼명이 깨진다."""
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\r\n")
    w.writeheader()
    w.writerows(rows)
    path.write_bytes(b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8"))


def write_products(rows, path: Path):
    """신 상품 시스템 API 응답 — UTF-8. items 아래로 한 단계 들어가야 표가 나온다."""
    payload = {
        "resultCode": "00",
        "resultMsg": "정상",
        "totalCount": len(rows),
        "items": rows,
    }
    # write_text 는 Windows 에서 줄바꿈을 CRLF 로 바꿔 OS 마다 바이트가 달라진다.
    # 바이트로 직접 써서 어느 OS 에서든 같은 파일(LF)이 나오게 한다.
    path.write_bytes(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ─────────────────────────────────────────────────────────────
def main(argv=None):
    ap = argparse.ArgumentParser(description="한빛유통 레거시 원천 데이터 생성기")
    ap.add_argument("--variant", choices=list(VARIANTS), default="common")
    ap.add_argument("--size", choices=list(SIZES), default="sample")
    ap.add_argument("--out", required=True, help="원천 파일을 저장할 폴더")
    ap.add_argument("--answer", default=None, help="정답 파일 폴더 (기본: <out>/_answer)")
    ap.add_argument("--no-answer", action="store_true", help="정답 파일(_answer)을 만들지 않는다 — 학생 배포용")
    args = ap.parse_args(argv)

    cfg = VARIANTS[args.variant]
    size = SIZES[args.size]
    rng = random.Random(cfg["seed"] + (0 if args.size == "full" else 7))
    log = AnswerLog()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    ans = Path(args.answer) if args.answer else out / "_answer"

    customers = build_customers(rng, size["n_cust"], size["err"], log)
    products = build_products(rng, size["n_prod"], cfg, size["err"], log)
    orders, _ = build_orders(rng, cfg, size["n_order"], customers, products, size["err"], log)

    f_cust = out / "cust_master.csv"
    f_ord = out / "order_2026.csv"
    f_prod = out / "product_api_response.json"
    write_customers(customers, f_cust)
    write_orders(orders, f_ord)
    write_products(products, f_prod)

    if not args.no_answer:
        log.write_csv(ans / "오류주입명세.csv")

    summary = {
        "variant": args.variant,
        "size": args.size,
        "seed": cfg["seed"],
        "기준일": AS_OF_DATE.isoformat(),
        "관측기간": [OBS_START.isoformat(), OBS_END.isoformat()],
        "정답기간": [LABEL_START.isoformat(), LABEL_END.isoformat()],
        "환율_고정": {"USD": USD_RATE, "JPY": JPY_RATE},
        "고객컬럼_주문파일": cfg["order_cust_col"],
        "상품컬럼_주문파일": cfg["order_goods_col"],
        "행수": {"cust_master.csv": len(customers),
                "order_2026.csv": len(orders),
                "product_api_response.json": len(products)},
        "오류유형별_건수_행기준": dict(sorted(log.counts.items())),
        "오류유형별_건수_주문기준": dict(sorted(log.order_counts.items())),
        "sha256": {p.name: sha256(p) for p in (f_cust, f_ord, f_prod)},
    }
    if not args.no_answer:
        (ans / "생성요약.json").write_bytes(
            json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"))

    print(f"[생성 완료] variant={args.variant} size={args.size} → {out}")
    for k, v in summary["행수"].items():
        print(f"  {k:28s} {v:>6,} 행")
    print("  오류유형별 건수 (행 기준 / 괄호는 주문 기준):")
    for k, v in summary["오류유형별_건수_행기준"].items():
        o = summary["오류유형별_건수_주문기준"].get(k)
        tail = f"  (주문 {o}건)" if o else ""
        print(f"    {k:32s} {v:>6,}{tail}")
    print(f"  정답 파일 → {ans}" if not args.no_answer else "  정답 파일은 만들지 않았습니다 (--no-answer)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
