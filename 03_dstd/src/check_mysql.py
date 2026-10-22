# -*- coding: utf-8 -*-
"""
check_mysql.py — Python 에서 실습 DB 로 접속되는지 확인한다

실행 (C:\\dstd 에서)
  .\\.venv\\Scripts\\python.exe src\\check_mysql.py
비밀번호를 물으면 root 비밀번호를 입력합니다 (화면에 보이지 않습니다).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db   # noqa: E402


def run() -> bool:
    print("=" * 60)
    print(" 실습 DB 접속 확인")
    print("=" * 60)
    print(f"접속 대상  {db.USER}@{db.HOST}:{db.PORT} / {db.DB}")
    try:
        conn = db.connect()
    except Exception as e:                       # noqa: BLE001 — 원인을 한국어로 바꿔 보여 주는 것이 목적
        why, todo = db.explain(e)
        print(f"[ 실패 ] {why}")
        print(f"         → {todo}")
        return False
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION(), DATABASE(), @@character_set_database")
            ver, name, cs = cur.fetchone()
            print(f"[ 정상 ] 서버 버전   {ver}")
            print(f"[ 정상 ] 데이터베이스 {name}  (문자셋 {cs})")
            try:
                cur.execute("SELECT msg FROM hello")
                rows = cur.fetchall()
            except Exception as e:               # noqa: BLE001
                why, todo = db.explain(e)
                print(f"[ 실패 ] {why}")
                print(f"         → {todo}")
                return False
            msg = rows[0][0] if rows else "(비어 있음)"
            ok = msg == "안녕하세요 dstd_lab"
            print(f"[ {'정상' if ok else '확인'} ] 한글 왕복    {msg}")
            if not ok:
                print("         → Workbench 에서 넣은 한글이 그대로 오지 않았습니다. 강사에게 알리십시오")
    print("-" * 60)
    print(" Python → MySQL 접속 성공. 5일차 준비 끝.")
    print("=" * 60)
    return True


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
