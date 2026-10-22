# -*- coding: utf-8 -*-
"""
load_stage.py — 원천 3종을 스테이징 테이블(stg_*)에 "있는 그대로" 적재

  고치지 않는다: '0000123' 은 '0000123', ' 19,800 ' 은 ' 19,800 ', 빈 값은 빈 문자열
  다시 실행하면 stg_ 테이블의 행을 비우고 새로 넣는다 (실습 DB dstd_lab 안에서만)

실행 (C:\\dstd 에서, sql\\day05_stage.sql 을 먼저 실행한 뒤)
  .\\.venv\\Scripts\\python.exe src\\load_stage.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rules import load_sources          # noqa: E402  원천 읽는 법은 1일차 그대로

ROOT = Path(__file__).resolve().parents[1]
COLS = {
    "stg_cust": ["CUST_NO", "CUST_NM", "HP", "EMAIL", "JOIN_DT", "GRD_CD", "ADDR_SIDO", "DEL_YN"],
    "stg_order": ["ORD_NO", "ORD_SEQ", "CUSTNO", "GOODS_CD", "ORD_DT", "QTY", "AMT", "ORD_ST", "PAY_MTHD", "DLV_FEE", "CANCEL_DT"],
    "stg_prod": ["goodsCode", "goodsName", "categoryName", "unitPrice", "currency", "taxType", "useYn", "regDate"],
}


def prepare(base):
    cust, order, prod = load_sources(base)
    out = {}
    for name, df in [("stg_cust", cust), ("stg_order", order), ("stg_prod", prod)]:
        rows = []
        for i, r in enumerate(df[COLS[name]].itertuples(index=False), start=2):
            rows.append((i, *[("" if v is None else str(v)) for v in r]))
        out[name] = rows
    return out


def load(conn, data):
    with conn.cursor() as cur:
        for name, rows in data.items():
            cur.execute(f"DELETE FROM {name}")                     # 스테이징만 비운다
            ph = ", ".join(["%s"] * (len(COLS[name]) + 1))
            cur.executemany(f"INSERT INTO {name} (SRC_ROW, {', '.join(COLS[name])}) VALUES ({ph})", rows)
    conn.commit()
    with conn.cursor() as cur:
        for name in data:
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            n = cur.fetchone()[0]
            print(f"{name:<10} {n:>5,}행  (원천 {len(data[name]):,}행 {'일치' if n == len(data[name]) else '불일치!'})")


if __name__ == "__main__":
    import db
    base = ROOT / "data" / "raw" / "common"
    data = prepare(base)
    try:
        conn = db.connect()
    except Exception as e:                       # noqa: BLE001
        why, todo = db.explain(e)
        print(f"[ 실패 ] {why}\n         → {todo}")
        sys.exit(1)
    with conn:
        load(conn, data)
