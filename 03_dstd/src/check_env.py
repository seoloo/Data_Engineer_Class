# -*- coding: utf-8 -*-
"""
check_env.py — 실습 환경 자가 진단

매일 1교시에 한 번 실행한다. 무엇이 준비되었고 무엇이 빠졌는지 한국어로 알려 준다.

실행 (C:\\dstd 에서)
  .\\.venv\\Scripts\\python.exe src\\check_env.py

표준 라이브러리만으로 동작한다. 패키지가 하나도 안 깔려 있어도 진단은 된다.
"""
from __future__ import annotations

import importlib.metadata as md
import re
import socket
import sys
from pathlib import Path

OK, WARN, FAIL, SKIP = "정상", "확인", "실패", "건너뜀"


def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "make_dataset.py").exists():
            return p
    return start


ROOT = find_root(Path(__file__).resolve().parent)
results = []


def pad(s: str, width: int) -> str:
    """한글은 화면에서 두 칸을 차지하므로 그만큼 계산해 맞춘다."""
    w = sum(2 if ord(ch) > 0x1100 else 1 for ch in s)
    return s + " " * max(1, width - w)


def line(status, item, detail="", hint=""):
    results.append(status)
    mark = {OK: "[ 정상 ]", WARN: "[ 확인 ]", FAIL: "[ 실패 ]", SKIP: "[건너뜀]"}[status]
    print(f"{mark} {pad(item, 16)}{detail}")
    if hint:
        print(f"{'':10} → {hint}")


def read_requirements():
    req = {}
    f = ROOT / "requirements.txt"
    if not f.exists():
        return req
    for raw in f.read_text(encoding="utf-8").splitlines():
        s = raw.split("#", 1)[0].strip()
        if not s:
            continue
        m = re.match(r"^([A-Za-z0-9_.\-]+)\s*(==\s*([^\s]+))?", s)
        if m:
            req[m.group(1)] = m.group(3)
    return req


def main():
    print("=" * 64)
    print(" 실습 환경 자가 진단")
    print("=" * 64)

    # 1. Python
    v = sys.version_info
    ver = f"{v.major}.{v.minor}.{v.micro}"
    if (v.major, v.minor) == (3, 13):
        line(OK, "Python", ver)
    elif (v.major, v.minor) >= (3, 11):
        line(WARN, "Python", ver, "수업 기준은 3.13 입니다. 동작은 하지만 출력이 문서와 다를 수 있습니다")
    else:
        line(FAIL, "Python", ver, "3.11 이상이 필요합니다 (pandas 3.x 요구사항)")

    # 2. 가상환경
    in_venv = sys.prefix != sys.base_prefix
    if in_venv:
        line(OK, "가상환경", sys.prefix)
    else:
        line(FAIL, "가상환경", "가상환경 밖에서 실행 중",
             r".\.venv\Scripts\python.exe src\check_env.py 로 실행하십시오")

    # 3. 패키지
    req = read_requirements()
    if not req:
        line(FAIL, "requirements", "requirements.txt 를 찾지 못했습니다", f"{ROOT} 에 있어야 합니다")
    for name, pin in req.items():
        try:
            got = md.version(name)
        except md.PackageNotFoundError:
            line(FAIL, name, "설치 안 됨",
                 r".\.venv\Scripts\python.exe -m pip install -r requirements.txt")
            continue
        if pin is None or got == pin:
            line(OK, name, got)
        else:
            line(WARN, name, f"{got} (기준 {pin})", "동작은 하지만 문서의 출력과 다를 수 있습니다")

    # 4. 프로젝트 루트
    if (ROOT / "src" / "make_dataset.py").exists():
        line(OK, "프로젝트 루트", str(ROOT))
    else:
        line(FAIL, "프로젝트 루트", "src\\make_dataset.py 를 찾지 못했습니다")

    # 5. 원천 데이터
    names = ["cust_master.csv", "order_2026.csv", "product_api_response.json"]
    for sub, when in [("data/sample", "1일차 6교시"), ("data/raw/common", "1일차 6교시")]:
        d = ROOT / sub
        have = [n for n in names if (d / n).exists()]
        if len(have) == 3:
            line(OK, sub, "3/3")
        else:
            size = "sample" if "sample" in sub else "full"
            out = sub.replace("/", "\\")
            line(FAIL, sub, f"{len(have)}/3",
                 rf".\.venv\Scripts\python.exe src\make_dataset.py --variant common --size {size} --out {out}")

    # 6. data 폴더 오염 검사 — MySQL 데이터 디렉터리로 잘못 지정된 경우
    mysql_marks = ["ibdata1", "mysql.ibd", "auto.cnf", "#innodb_redo"]
    found = [m for m in mysql_marks if (ROOT / "data" / m).exists()]
    if found:
        line(FAIL, "data 폴더", f"MySQL 서버 파일이 들어 있습니다: {', '.join(found)}",
             "MySQL 데이터 폴더가 이 프로젝트의 data 로 지정됐습니다. 강사에게 알리십시오")
    else:
        line(OK, "data 폴더", "MySQL 서버 파일 없음")

    # 7. MySQL 포트
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=2):
            line(OK, "MySQL 포트", "localhost:3306 응답 있음")
    except OSError:
        line(WARN, "MySQL 포트", "localhost:3306 응답 없음",
             "MySQL 서비스가 꺼져 있습니다. PowerShell 에서 Get-Service *mysql* 로 확인 (5일차부터 필요)")

    # 8. 표준 사전 (2일차 이후)
    d = ROOT / "docs" / "표준사전.xlsx"
    if d.exists():
        line(OK, "표준 사전", "docs\\표준사전.xlsx 있음")
    else:
        line(SKIP, "표준 사전", "아직 없음 (2일차에 만듭니다)")

    # 9. 매핑표 (2일차) · 변환 규칙 정의서 (3일차)
    for fname, label, day in [("매핑표.xlsx", "매핑표", "2일차"), ("변환규칙정의서.xlsx", "변환 규칙", "3일차")]:
        if (ROOT / "docs" / fname).exists():
            line(OK, label, f"docs\\{fname} 있음")
        else:
            line(SKIP, label, f"아직 없음 ({day}에 만듭니다)")

    # 10. 실습 DB (4일차) — 비밀번호가 필요하므로 여기서는 묻지 않는다
    line(SKIP, "실습 DB", "접속 확인은 src\\check_mysql.py 로 (비밀번호 입력 필요)")

    print("-" * 64)
    n_fail, n_warn = results.count(FAIL), results.count(WARN)
    if n_fail == 0 and n_warn == 0:
        print(" 모두 정상입니다. 수업을 시작해도 됩니다.")
    elif n_fail == 0:
        print(f" 실패 없음 · 확인 {n_warn}건. 수업은 진행할 수 있습니다.")
    else:
        print(f" 실패 {n_fail}건 · 확인 {n_warn}건. 위의 → 안내대로 고친 뒤 다시 실행하십시오.")
    print("=" * 64)
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
