# -*- coding: utf-8 -*-
"""
check_rulebook.py — 변환 규칙 정의서 검사기

코드(src\\rules.py)가 쓰는 규칙과 문서(변환규칙정의서.xlsx)에 적힌 규칙이 서로 맞는지 본다.
문서에 없는 규칙으로 데이터를 격리하면, 나중에 아무도 그 격리를 설명하지 못한다.

실행 (C:\\dstd 에서)
  .\\.venv\\Scripts\\python.exe src\\check_rulebook.py                               docs\\변환규칙정의서.xlsx
  .\\.venv\\Scripts\\python.exe src\\check_rulebook.py templates\\변환규칙정의서_템플릿.xlsx
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_dictionary import ROOT, Report, read_sheet, source_columns   # noqa: E402
from rules import RULES                                               # noqa: E402

SHEET = "변환규칙"
HEADER = ["규칙ID", "분류", "대상파일", "대상컬럼", "원천 예시", "처리 방법", "근거"]
KINDS = {"변환", "제거", "제외", "격리", "표시"}
ID_RE = re.compile(r"^[A-Z]\d{2}$")
SECTION_RE = re.compile(r"\d+\.\d+|\d+장")
WHOLE = {"(행 전체)", "(주문 전체)"}
REQUIRED = ["규칙ID", "분류", "대상파일", "대상컬럼", "처리 방법", "근거"]


def check(path: Path, data_dir: Path, rep: Report):
    wb = load_workbook(path, data_only=True)
    rows = read_sheet(wb, SHEET, HEADER, rep)
    cols = source_columns(data_dir)
    seen = {}
    for r in rows:
        n = r["_row"]
        empty = [h for h in REQUIRED if not r[h]]
        if empty:
            rep.err(SHEET, n, "B1", f"빈 칸 — {', '.join(empty)}")
        rid = r["규칙ID"]
        if rid and not ID_RE.match(rid):
            rep.err(SHEET, n, "B2", f"규칙ID '{rid}' — 영문 대문자 1자 + 숫자 2자리 (예: R04)")
        if rid in seen:
            rep.err(SHEET, n, "B3", f"규칙ID '{rid}' — {seen[rid]}행과 중복")
        elif rid:
            seen[rid] = n
        kind = r["분류"]
        if kind and kind not in KINDS:
            rep.err(SHEET, n, "B4", f"분류 '{kind}' — 변환·제거·제외·격리·표시 중 하나")
        elif kind and rid in RULES and RULES[rid][0] != kind:
            rep.err(SHEET, n, "B5", f"{rid} 의 분류가 '{kind}' — 코드(rules.py)는 '{RULES[rid][0]}' 로 처리합니다")
        f = r["대상파일"]
        if f:
            files = [x.strip() for x in f.split(",")]
            for x in files:
                if x not in cols:
                    rep.err(SHEET, n, "B6", f"대상파일 '{x}' — 원천 파일 이름이 아닙니다")
            if r["대상컬럼"] and r["대상컬럼"] not in WHOLE:
                allcols = {c for x in files for c in cols.get(x, [])}
                for c in [x.strip() for x in r["대상컬럼"].split(",")]:
                    if c not in allcols:
                        rep.err(SHEET, n, "B6", f"대상컬럼 '{c}' — {f} 에 없는 컬럼")
        if r["근거"] and not SECTION_RE.search(r["근거"]):
            rep.warn(SHEET, n, "B7", "근거에 업무설명서 절 번호(예: 2.9, 4장)가 없습니다")
        if kind in {"변환", "격리"} and not r["원천 예시"]:
            rep.warn(SHEET, n, "B8", f"{rid} — 원천 예시가 비었습니다. 프로파일링에서 본 실제 값을 적으십시오")
    for rid in RULES:
        if rid not in seen:
            rep.err(SHEET, "-", "B9", f"{rid} — 코드가 쓰는 규칙인데 정의서에 없습니다")
    for rid, n in seen.items():
        if rid not in RULES:
            rep.warn(SHEET, n, "B10", f"{rid} — 정의서에만 있고 코드에는 없는 규칙입니다")
    return {"규칙": len(rows), "코드 규칙": len(RULES)}


def run(path, data_dir=None, quiet=False):
    path = Path(path)
    rep = Report()
    stats = check(path, Path(data_dir) if data_dir else ROOT / "data" / "sample", rep)
    if not quiet:
        print(f"변환 규칙 정의서 검사 — {path.name}")
        print("  " + " / ".join(f"{k} {v}" for k, v in stats.items()))
        print()
        key = lambda x: (1 if x[2] == "-" else 0, 0 if x[2] == "-" else x[2], x[3])
        for sev, sheet, row, rule, msg in sorted(rep.items, key=key):
            rtxt = f"{row}행" if row != "-" else "  - "
            print(f"[{sev}] {rtxt:>5}  {rule:<3}  {msg}")
        if rep.items:
            print()
        print("─" * 60)
        print(f"오류 {rep.n_err}건 · 경고 {rep.n_warn}건"
              + ("   → 통과" if rep.n_err == 0 else "   → 고칠 것이 있습니다"))
    return rep, stats


def main(argv=None):
    ap = argparse.ArgumentParser(description="변환 규칙 정의서 검사기")
    ap.add_argument("rulebook", nargs="?", default=str(ROOT / "docs" / "변환규칙정의서.xlsx"))
    ap.add_argument("--data", help="원천 파일 폴더 (기본: data\\sample)")
    a = ap.parse_args(argv)
    p = Path(a.rulebook)
    if not p.exists():
        print(f"파일이 없습니다: {p}")
        print("Google Sheets 에서 '파일 → 다운로드 → Microsoft Excel(.xlsx)' 로 받아 이 위치에 저장하십시오.")
        return 2
    rep, _ = run(p, a.data)
    return 1 if rep.n_err else 0


if __name__ == "__main__":
    sys.exit(main())
