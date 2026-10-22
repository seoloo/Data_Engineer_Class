# -*- coding: utf-8 -*-
"""
profile_data.py — 컬럼 프로파일링 (형식 서명)

값을 하나하나 보는 대신 '모양'으로 묶어 센다.
  숫자 → 9   영문 → A   한글 → 가   나머지(공백·콤마·점·하이픈)는 그대로
  ' 19,800 ' → ' 99,999 '      '2026/03/15' → '9999/99/99'      'GUEST' → 'AAAAA'

한 컬럼에 서명이 여러 개면, 그 컬럼은 표기가 섞여 있다는 뜻이다.

실행 (C:\\dstd 에서)
  .\\.venv\\Scripts\\python.exe src\\profile_data.py                       소형본
  .\\.venv\\Scripts\\python.exe src\\profile_data.py --data data\\raw\\common  확장본
결과는 화면에 찍고, data\\work\\profile_<파일>.csv 로도 저장한다.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rules import load_sources            # noqa: E402  원천 읽는 법은 한 곳에서만 정한다


def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "make_dataset.py").exists():
            return p
    return start


ROOT = find_root(Path(__file__).resolve().parent)
BLANK = "(빈 값)"


def signature(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v) == "":
        return BLANK
    s = re.sub(r"[0-9]", "9", str(v))
    s = re.sub(r"[A-Za-z]", "A", s)
    s = re.sub(r"[가-힣]", "가", s)
    s = re.sub(r"가+", "가…", s)                     # 한글 이름·주소는 길이가 제각각이라 한 덩어리로
    if s != s.strip():                               # 앞뒤 공백은 눈에 보이게
        s = f"'{s}'"
    return s


def profile_frame(df: pd.DataFrame, top: int = 3) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        if col == "원천행":
            continue
        s = df[col].astype(str)
        blank = int((s.str.strip() == "").sum())
        sig = s.map(signature).value_counts()
        shown = " / ".join(f"{k} {v}" for k, v in sig.head(top).items())
        if len(sig) > top:
            shown += f" / 외 {len(sig) - top}종"
        rows.append({
            "컬럼": col,
            "행수": len(s),
            "빈값": blank,
            "고유값": int(s.nunique()),
            "서명수": len(sig),
            "모양수": len([k for k in sig.index if k != BLANK]),   # 빈 값은 결측이지 표기 차이가 아니다
            "서명 분포 (상위)": shown,
        })
    return pd.DataFrame(rows)


def pad(t, w):
    t = str(t)
    return t + " " * max(1, w - sum(2 if ord(c) > 0x1100 else 1 for c in t))


def print_profile(name: str, prof: pd.DataFrame):
    print(f"■ {name}  ({prof['행수'].iat[0]:,}행)")
    print("  " + pad("컬럼", 13) + pad("빈값", 6) + pad("고유", 6) + pad("서명", 5) + "서명 분포")
    for _, r in prof.iterrows():
        mark = "  ←" if r["모양수"] > 1 and r["컬럼"] not in NAME_LIKE else ""
        print("  " + pad(r["컬럼"], 13) + pad(r["빈값"], 6) + pad(r["고유값"], 6)
              + pad(r["서명수"], 5) + r["서명 분포 (상위)"] + mark)
    print()


# 이름·주소처럼 원래 모양이 제각각인 컬럼은 ← 표시에서 뺀다
NAME_LIKE = {"CUST_NM", "goodsName", "categoryName", "EMAIL", "ADDR_SIDO"}


def main(argv=None):
    ap = argparse.ArgumentParser(description="원천 3종 형식 서명 프로파일링")
    ap.add_argument("--data", default=str(ROOT / "data" / "sample"))
    ap.add_argument("--top", type=int, default=3)
    a = ap.parse_args(argv)
    base = Path(a.data)
    if not base.is_absolute():
        base = ROOT / base
    if not (base / "order_2026.csv").exists():
        print(f"원천 파일이 없습니다: {base}")
        return 2
    cust, order, prod = load_sources(base)
    out = ROOT / "data" / "work"
    out.mkdir(parents=True, exist_ok=True)
    for name, df in [("cust_master.csv", cust), ("order_2026.csv", order), ("product_api_response.json", prod)]:
        prof = profile_frame(df, a.top)
        print_profile(name, prof)
        prof.to_csv(out / f"profile_{Path(name).stem}.csv", index=False, encoding="utf-8-sig")
    print("← 표시 = 빈 값을 빼고도 모양이 두 가지 이상. 표기가 섞였는지 사람이 확인할 곳 (전부 문제는 아니다)")
    print("저장: data\\work\\profile_*.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
