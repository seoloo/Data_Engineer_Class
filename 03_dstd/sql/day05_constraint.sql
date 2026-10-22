-- ============================================================
-- 5일차 5교시 — 원천(스테이징)에 PK·FK 를 걸어 보면?
-- 원본 stg_ 는 건드리지 않고 복사본 exp_ 에서만 실험한다
-- ⚠ exp_order · exp_cust 는 실험용 — 이 파일 첫 줄이 매번 지우고 다시 만든다 (실습 DB 안에서만)
-- Workbench 에서 한 문장씩 Ctrl+Enter. 오류가 나는 게 정상인 문장이 있다
-- ============================================================
USE dstd_lab;

DROP TABLE IF EXISTS exp_order;
DROP TABLE IF EXISTS exp_cust;
CREATE TABLE exp_order LIKE stg_order;
INSERT INTO exp_order SELECT * FROM stg_order;
CREATE TABLE exp_cust LIKE stg_cust;
INSERT INTO exp_cust SELECT * FROM stg_cust;

-- 실험 1. 주문 파일에 PK (ORD_NO, ORD_SEQ) — 오류가 난다
ALTER TABLE exp_order ADD PRIMARY KEY (ORD_NO, ORD_SEQ);

-- 실험 2. 고객 파일에 PK (CUST_NO) — 성공한다
ALTER TABLE exp_cust ADD PRIMARY KEY (CUST_NO);

-- 실험 3. 주문 → 고객 FK — 오류가 난다
ALTER TABLE exp_order ADD FOREIGN KEY (CUSTNO) REFERENCES exp_cust (CUST_NO);

-- 실험 4. FK 를 막는 행은 무엇인가
SELECT o.CUSTNO, COUNT(*) AS 행수
FROM exp_order o LEFT JOIN exp_cust c ON o.CUSTNO = c.CUST_NO
WHERE c.CUST_NO IS NULL
GROUP BY o.CUSTNO ORDER BY 행수 DESC LIMIT 5;
