# -*- coding: utf-8 -*-
"""
grain.py — 한 행의 의미(grain)와 식별자를 데이터로 확인한다

  grain_table     "주문번호가 같으면 이 컬럼 값은 하나뿐인가?" → 주문 단위 / 항목 단위
  key_check       "이 컬럼(조합)으로 한 행을 콕 집을 수 있는가?" → 빈 값·중복을 센다
  relation_check  "자식의 값이 부모에 다 있는가?" → 연결 / 비어 있음 / 끊김

모델은 머리로 정하지 않는다. 업무설명서로 가설을 세우고, 데이터로 확인한다.
"""
from __future__ import annotations

import pandas as pd


def grain_table(df: pd.DataFrame, key: str, cols: list[str]) -> pd.DataFrame:
    """key 가 같은 행들 안에서 각 컬럼 값이 몇 가지인지 센다.

    항목이 하나뿐인 주문은 어떤 컬럼이든 값이 하나라서 판정에 쓸 수 없다.
    그래서 항목이 2개 이상인 주문만 본다.
    데이터는 '반례'를 보여 줄 뿐이다. 반례가 0 이라고 주문 단위가 증명되는 것은 아니다 — 근거는 업무 규칙.
    """
    size = df.groupby(key)[key].transform("size")
    multi_df = df[size >= 2]
    groups = multi_df.groupby(key)
    n_keys = groups.ngroups
    rows = []
    for c in cols:
        nun = groups[c].nunique(dropna=False)
        multi = int((nun > 1).sum())
        rows.append({
            "컬럼": c,
            "값이 갈리는 주문": multi,
            "항목 2개 이상 주문": n_keys,
            "판정": "주문 단위" if multi == 0 else "항목 단위",
        })
    return pd.DataFrame(rows)


def key_check(df: pd.DataFrame, cols: list[str], blank=("",)) -> dict:
    """후보키 검사 — 빈 값이 없고(NOT NULL) 겹치지 않아야(UNIQUE) 식별자가 될 수 있다."""
    sub = df[cols].astype(str)
    empty = sub.apply(lambda s: s.str.strip().isin(blank)).any(axis=1)
    dup = sub.duplicated(keep=False)
    ok = (not empty.any()) and (not dup.any())
    return {
        "후보": " + ".join(cols),
        "행수": len(df),
        "빈 값 행": int(empty.sum()),
        "겹치는 행": int(dup.sum()),
        "판정": "식별자 가능" if ok else "식별자 불가",
    }


def relation_check(child: pd.DataFrame, fk: str, parent: pd.DataFrame, pk: str,
                   empty_values=("",)) -> dict:
    """자식 fk 값이 부모 pk 에 있는지 — 연결 / 비어 있음(선택 관계) / 끊김(참조 무결성 위반)"""
    v = child[fk].astype(str).str.strip()
    empty = v.isin(empty_values)
    linked = ~empty & v.isin(set(parent[pk].astype(str)))
    broken = ~empty & ~linked
    return {
        "관계": f"{fk} → {pk}",
        "자식 행": len(child),
        "연결": int(linked.sum()),
        "비어 있음": int(empty.sum()),
        "끊김": int(broken.sum()),
    }
