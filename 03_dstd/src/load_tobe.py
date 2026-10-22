# -*- coding: utf-8 -*-
"""
load_tobe.py — 3일차 규칙으로 정리한 데이터를 TO-BE 테이블 4개에 적재 (강사 제공)

  넣는 것   : 정상 + 제외 (비회원·취소·7월도 DB 에는 넣는다 — 제외는 '분석할 때' 빼는 것)
  안 넣는 것: 격리 (3일차 R02~R08) · 제거 (R01) — 건수만 보고한다. 격리 테이블은 7일차
  주문 한 건 → ORD 한 행, 주문 항목 → ORD_ITEM 여러 행 (4일차 헤더·항목 분리)
  과세구분 Z 상품은 상품 행은 넣고 TAX_TP_CD 만 비운다 — 그 상품의 주문 99행을 살리기 위해

실행 (C:\\dstd 에서, docs\\ddl.sql 로 테이블을 만든 뒤)
  .\\.venv\\Scripts\\python.exe src\\load_tobe.py
이미 적재돼 있으면 멈춘다. 다시 하려면 sql\\day05_reset.sql → docs\\ddl.sql → 이 스크립트
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rules import (load_sources, load_code_map, classify_orders, classify_products,   # noqa: E402
                   parse_date, map_code, GUEST_VALUES)

ROOT = Path(__file__).resolve().parents[1]
TABLES = ["CUST", "PRD", "ORD", "ORD_ITEM"]


def clean(v):
    """pandas 가 남긴 NaN 은 NULL 로, numpy 숫자는 파이썬 숫자로 — PyMySQL 이 받을 수 있게."""
    if v is None:
        return None
    if isinstance(v, float) and v != v:
        return None
    if hasattr(v, "item") and not isinstance(v, (str, bytes)):
        return v.item()
    return v


def cleaned(rows):
    return [tuple(clean(v) for v in r) for r in rows]


def prepare(base, mapping):
    cust, order, prod = load_sources(base)
    cmap = load_code_map(mapping)
    r = classify_orders(order, cust, prod, cmap)
    pp = classify_products(prod, cmap)

    cust_rows = [(c.CUST_NO, c.CUST_NM, c.HP, c.EMAIL or None, parse_date(c.JOIN_DT)[0],
                  map_code(cmap, "cust_master.csv", "GRD_CD", c.GRD_CD)[0] or c.GRD_CD,
                  c.ADDR_SIDO, c.DEL_YN) for c in cust.itertuples(index=False)]
    prd_rows = [(p.goodsCode, p.goodsName, p.categoryName, p.unitPrice,
                 map_code(cmap, "product_api_response.json", "currency", p.currency)[0] or p.currency,
                 p.원화정가금액, p.환산여부, p.TAX_TP_CD, p.useYn, parse_date(p.regDate)[0])
                for p in pp.itertuples(index=False)]

    ok = r[r["분류"].isin(["정상", "제외"])]
    head = ok.drop_duplicates("ORD_NO")                     # R03 을 걸렀으니 주문 단위 값은 주문마다 하나
    ord_rows = [(h.ORD_NO, None if h.CUSTNO.strip() in GUEST_VALUES else h.CUSTNO, h.ORD_DT_STD,
                 h.PAY_MTHD_CD, h.DLV_FEE_AMT, h.ORD_STAT_CD, parse_date(h.CANCEL_DT)[0])
                for h in head.itertuples(index=False)]
    item_rows = [(i.ORD_NO, int(i.ORD_SEQ), i.GOODS_CD, int(i.QTY), i.SALE_AMT)
                 for i in ok.itertuples(index=False)]

    report = {
        "원천 주문항목": len(r),
        "적재 (정상)": int((r["분류"] == "정상").sum()),
        "적재 (제외 — DB 에는 넣음)": int((r["분류"] == "제외").sum()),
        "안 넣음 (제거 R01)": int((r["분류"] == "제거").sum()),
        "안 넣음 (격리)": int((r["분류"] == "격리").sum()),
    }
    data = {"CUST": cust_rows, "PRD": prd_rows, "ORD": ord_rows, "ORD_ITEM": item_rows}
    return {k: cleaned(v) for k, v in data.items()}, report


def load(conn, data):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ORD")
        if cur.fetchone()[0]:
            print("이미 적재돼 있습니다. 다시 하려면: sql\\day05_reset.sql → docs\\ddl.sql → 이 스크립트")
            return False
        for t in TABLES:                                    # 부모부터 — FK 때문에 순서가 중요하다
            rows = data[t]
            ph = ", ".join(["%s"] * len(rows[0]))
            cur.executemany(f"INSERT INTO {t} VALUES ({ph})", rows)
    conn.commit()
    return True


def print_report(data, report):
    w = lambda t: sum(2 if ord(c) > 0x1100 else 1 for c in t)
    for k, v in report.items():
        print(f"  {k}{' ' * (30 - w(k))}{v:>6,}")
    print("  " + "-" * 36)
    print(f"  테이블별 적재 행 — CUST {len(data['CUST'])} · PRD {len(data['PRD'])} · "
          f"ORD {len(data['ORD']):,} · ORD_ITEM {len(data['ORD_ITEM']):,}")


if __name__ == "__main__":
    import db
    mapping = ROOT / "docs" / "매핑표.xlsx"
    if not mapping.exists():
        print("docs\\매핑표.xlsx 가 없습니다 (2일차 산출물)")
        sys.exit(2)
    data, report = prepare(ROOT / "data" / "raw" / "common", mapping)
    print_report(data, report)
    try:
        conn = db.connect()
    except Exception as e:                       # noqa: BLE001
        why, todo = db.explain(e)
        print(f"[ 실패 ] {why}\n         → {todo}")
        sys.exit(1)
    with conn:
        try:
            if load(conn, data):
                print("적재 완료. sql\\day05_join.sql 로 확인하십시오.")
        except Exception as e:                   # noqa: BLE001
            why, todo = db.explain(e)
            print(f"[ 실패 ] {why}\n         → {todo}")
            sys.exit(1)
