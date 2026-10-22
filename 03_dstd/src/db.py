# -*- coding: utf-8 -*-
"""
db.py — 실습 DB(dstd_lab) 접속 도우미

비밀번호는 코드에 적지 않는다.
  1) 환경변수 DSTD_DB_PW 가 있으면 그것을 쓰고
  2) 없으면 실행할 때 한 번 묻는다 (화면에 보이지 않음). 물어본 값은 이 파이썬 프로세스 안에서만 기억한다.

노트북에서:   from db import connect;  conn = connect()
"""
from __future__ import annotations

import getpass
import os

HOST, PORT, USER, DB = "127.0.0.1", 3306, "root", "dstd_lab"


def password() -> str:
    pw = os.environ.get("DSTD_DB_PW")
    if not pw:
        pw = getpass.getpass(f"MySQL {USER} 비밀번호: ")
        os.environ["DSTD_DB_PW"] = pw        # 이 프로세스 안에서만. 파일에 저장하지 않는다
    return pw


def forget():
    """비밀번호를 잘못 넣었을 때 — 다음 connect() 에서 다시 묻는다."""
    os.environ.pop("DSTD_DB_PW", None)


def connect(database: str | None = DB):
    import pymysql
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=password(),
                           database=database, charset="utf8mb4", connect_timeout=5)


def explain(e: Exception) -> tuple[str, str]:
    """접속 실패를 (원인, 할 일) 한국어로 바꾼다."""
    code = e.args[0] if getattr(e, "args", None) and isinstance(e.args[0], int) else None
    text = str(e)
    if isinstance(e, ModuleNotFoundError):
        return ("PyMySQL 이 설치되지 않았습니다",
                r".\.venv\Scripts\python.exe -m pip install -r requirements.txt")
    if isinstance(e, RuntimeError) and "cryptography" in text:
        return ("cryptography 패키지가 없습니다 (MySQL 8.4 기본 인증에 필요)",
                r".\.venv\Scripts\python.exe -m pip install -r requirements.txt")
    if code == 2003:
        return ("MySQL 서버에 닿지 않습니다 — 서비스가 꺼져 있을 가능성이 큽니다",
                "PowerShell(관리자): Get-Service MySQL84 → Stopped 면 Start-Service MySQL84")
    if code == 1045:
        forget()
        return ("비밀번호가 틀렸습니다",
                "다시 실행하면 다시 묻습니다. 1일차 5교시 Configurator 에서 정한 root 비밀번호")
    if code == 1049:
        return ("dstd_lab 데이터베이스가 없습니다",
                r"Workbench 에서 sql\day04_setup.sql 을 열어 실행(번개 아이콘)")
    if code == 1146:
        return ("hello 테이블이 없습니다",
                r"Workbench 에서 sql\day04_setup.sql 을 다시 실행")
    if code == 1062:
        return ("PK(또는 UNIQUE) 중복 — 같은 키가 이미 있습니다", "원천의 중복을 먼저 정리 (3일차 R01·R02)")
    if code == 1452:
        return ("FK 위반 — 부모 테이블에 없는 값을 가리킵니다", "부모 행이 먼저 있어야 합니다 (3일차 R07·R08)")
    if code == 1451:
        return ("FK 위반 — 자식 행이 가리키고 있어 부모를 지울 수 없습니다", "자식 테이블부터 정리")
    if code == 1292:
        return ("값을 그 자료형으로 바꿀 수 없습니다 (예: 없는 날짜)", "원천 값 확인 (3일차 R04)")
    if code == 1366:
        return ("숫자·날짜 컬럼에 그 형식이 아닌 값을 넣으려 했습니다 (예: '2,500')", "변환 규칙으로 먼저 바꾼 뒤 넣습니다 (3일차 C02)")
    if code == 1048:
        return ("NOT NULL 컬럼에 NULL 을 넣으려 했습니다", "DDL 의 NULL 허용 여부 확인")
    if code == 3780:
        return ("FK 양쪽 컬럼의 자료형이 다릅니다", "부모 PK 와 같은 자료형·길이로")
    if code == 1824:
        return ("FK 가 가리키는 부모 테이블이 없습니다", "부모 테이블을 먼저 만드십시오")
    return (f"알 수 없는 오류 — {type(e).__name__}: {text[:120]}", "강사에게 이 화면을 보여 주십시오")


# ── SQL 파일 실행 (노트북·검증 스크립트용) ─────────────────────
def split_sql(text: str) -> list[str]:
    """';' 로 문장을 나눈다. 따옴표 안의 ';' 와 -- 주석은 무시한다."""
    stmts, cur, q = [], [], None
    for line in text.splitlines():
        s = line
        if q is None and s.strip().startswith("--"):
            continue
        for ch in s:
            if q:
                cur.append(ch)
                if ch == q:
                    q = None
                continue
            if ch in "'\"`":
                q = ch
            if ch == ";":
                st = "".join(cur).strip()
                if st:
                    stmts.append(st)
                cur = []
            else:
                cur.append(ch)
        cur.append("\n")
    st = "".join(cur).strip()
    if st:
        stmts.append(st)
    return stmts


def run_sql(conn, sql: str, show=True, max_rows=20, commit=True):
    """문장 하나 실행 → (열 이름, 행 목록, 경고 목록). 오류는 그대로 던진다."""
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
        cur.execute("SHOW WARNINGS")
        warns = cur.fetchall()
    if commit:
        conn.commit()
    if show:
        print_rows(cols, rows, max_rows)
        if warns:
            print(f"  ⚠ 경고 {len(warns)}건 — 예: {warns[0][1]} {warns[0][2]}")
    return cols, rows, warns


def print_rows(cols, rows, max_rows=20):
    if not cols:
        return
    w = lambda t: sum(2 if ord(c) > 0x1100 else 1 for c in str(t))
    data = [[("NULL" if v is None else str(v)) for v in r] for r in rows[:max_rows]]
    widths = [max([w(c)] + [w(r[i]) for r in data]) for i, c in enumerate(cols)]
    line = lambda vals: "  ".join(str(v) + " " * (wd - w(v)) for v, wd in zip(vals, widths)).rstrip()
    print(line(cols))
    for r in data:
        print(line(r))
    if len(rows) > max_rows:
        print(f"… 외 {len(rows) - max_rows}행")


def run_file(conn, path, max_rows=20, only=None):
    """SQL 파일을 한 문장씩 실행하며 결과·오류를 보여 준다. 오류가 나도 다음 문장으로 간다.
    only: 'SELECT' 처럼 주면 그 종류의 문장 결과만 화면에 보여 준다."""
    from pathlib import Path
    text = Path(path).read_text(encoding="utf-8")
    in_tx = False
    for st in split_sql(text):
        head = " ".join(st.split())
        up = head.upper()
        if up.startswith("START TRANSACTION"):
            in_tx = True
        quiet = only and not up.startswith(only)
        if not quiet:
            print(f"> {head[:96]}")
        try:
            cols, rows, warns = run_sql(conn, st, show=not quiet, max_rows=max_rows, commit=not in_tx)
            if not quiet and not cols:
                print("  (성공)")
        except Exception as e:                   # noqa: BLE001 — 수업에서는 오류도 결과다
            code = e.args[0] if getattr(e, "args", None) else ""
            why, todo = explain(e)
            print(f"  ✖ 오류 {code}: {str(e.args[1] if len(e.args) > 1 else e)[:110]}")
            print(f"    → {why}")
        if up.startswith(("ROLLBACK", "COMMIT")):
            in_tx = False
        if not quiet:
            print()
