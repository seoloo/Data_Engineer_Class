-- ============================================================
-- ⚠ 실습 DB(dstd_lab) 전용 — TO-BE 테이블 4개를 지운다
-- DDL 을 고쳐 다시 만들어야 할 때만 쓴다. 다른 DB 에서 실행하지 말 것
-- 자식 테이블부터 지운다 (FK 때문에 부모를 먼저 지울 수 없다)
-- 지운 뒤 docs\ddl.sql 을 다시 실행하고, load_tobe.py 로 다시 적재한다
-- ============================================================
USE dstd_lab;
DROP TABLE IF EXISTS ORD_ITEM;
DROP TABLE IF EXISTS ORD;
DROP TABLE IF EXISTS PRD;
DROP TABLE IF EXISTS CUST;
SHOW TABLES;
