# %% [markdown]
# # 5일차 — 개념 모델을 MySQL 테이블로
#
# `# %%` 가 셀 구분선입니다. 셀마다 Shift+Enter 로, 위에서부터 순서대로 실행하십시오.
# 처음 MySQL 에 접속하는 셀에서 위쪽에 비밀번호 칸이 뜹니다 (root 비밀번호).
# SQL 은 Workbench 에서 쳐도 되고, 이 노트북의 셀로 실행해도 결과는 같습니다.

# %%
# [셀 1] 준비 — 접속 확인
import sys, os
from pathlib import Path


def find_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "src" / "make_dataset.py").exists():
            return p
    return start


try:
    HERE = Path(__file__).resolve().parent
except NameError:
    HERE = Path.cwd()
ROOT = find_root(HERE)
os.chdir(ROOT)
sys.path.insert(0, str(ROOT / "src"))
SQL = ROOT / "sql"

import db
from check_mysql import run as check_mysql

check_mysql()
conn = db.connect()

# %%
# [셀 2] 2교시 — 스테이징: 원천을 있는 그대로 MySQL 에 (sql\day05_stage.sql + src\load_stage.py)
import load_stage

db.run_file(conn, SQL / "day05_stage.sql")
load_stage.load(conn, load_stage.prepare(ROOT / "data" / "raw" / "common"))

# %%
# [셀 3] 3교시 — SQL 보충: 4일차의 함정을 SQL 로 (sql\day05_query.sql)
db.run_file(conn, SQL / "day05_query.sql")

# %%
# [셀 4] 5교시 — 원천에 PK·FK 를 걸어 보면? (sql\day05_constraint.sql — 실험용 복사본 exp_ 에서만)
db.run_file(conn, SQL / "day05_constraint.sql")

# %%
# [셀 5] 6교시 — 내 DDL 검사 (MySQL 에 넣기 전에, 파일만 읽어서)
from check_ddl import run as check_ddl

my_ddl = ROOT / "docs" / "ddl.sql"
if my_ddl.exists():
    rep, _ = check_ddl(my_ddl)
else:
    print("docs\\ddl.sql 이 없습니다 — templates\\ddl_템플릿.sql 을 복사해 작성하십시오")

# %%
# [셀 6] 7교시 — 내 DDL 을 MySQL 에 실행하고, 손으로 넣어 보기
if my_ddl.exists():
    db.run_file(conn, my_ddl)
    db.run_file(conn, SQL / "day05_insert_test.sql")

# %%
# [셀 7] 8교시 — 3일차 규칙으로 정리한 데이터 적재 (src\load_tobe.py)
import load_tobe

mapping = ROOT / "docs" / "매핑표.xlsx"
data, report = load_tobe.prepare(ROOT / "data" / "raw" / "common", mapping)
load_tobe.print_report(data, report)
try:
    print("적재:", "완료" if load_tobe.load(conn, data) else "건너뜀")
except Exception as e:
    why, todo = db.explain(e)
    print(f"[ 실패 ] {why}\n         → {todo}")
    conn.rollback()

# %%
# [셀 8] 8교시 — JOIN 건수 판정 (sql\day05_join.sql)
db.run_file(conn, SQL / "day05_join.sql")
