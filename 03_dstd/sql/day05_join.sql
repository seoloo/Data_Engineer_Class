-- ============================================================
-- 5일차 8교시 — JOIN 건수 판정: 테이블을 잘 나눠도 JOIN 을 잘못 쓰면
-- load_tobe.py 로 적재한 뒤 한 문장씩 Ctrl+Enter
-- ============================================================
USE dstd_lab;

-- J1. 테이블별 행 수
SELECT 'CUST' AS 테이블, COUNT(*) AS 행수 FROM CUST
UNION ALL SELECT 'PRD', COUNT(*) FROM PRD
UNION ALL SELECT 'ORD', COUNT(*) FROM ORD
UNION ALL SELECT 'ORD_ITEM', COUNT(*) FROM ORD_ITEM;

-- J2. 주문과 고객 — INNER JOIN 이면 몇 건?
SELECT COUNT(*) AS 주문수 FROM ORD o JOIN CUST c ON o.CUST_NO = c.CUST_NO;

-- J3. 주문과 고객 — LEFT JOIN 이면 몇 건?
SELECT COUNT(*) AS 주문수 FROM ORD o LEFT JOIN CUST c ON o.CUST_NO = c.CUST_NO;

-- J4. 매출 합계 — INNER JOIN 고객으로 구하면
SELECT SUM(i.SALE_AMT) AS 매출
FROM ORD o JOIN CUST c ON o.CUST_NO = c.CUST_NO JOIN ORD_ITEM i ON i.ORD_NO = o.ORD_NO;

-- J5. 매출 합계 — 고객 없이 구하면
SELECT SUM(SALE_AMT) AS 매출 FROM ORD_ITEM;

-- J6. 배송비 합계 — 주문 테이블에서
SELECT SUM(DLV_FEE_AMT) AS 배송비 FROM ORD;

-- J7. 배송비 합계 — 주문과 항목을 JOIN 한 뒤
SELECT SUM(o.DLV_FEE_AMT) AS 배송비 FROM ORD o JOIN ORD_ITEM i ON i.ORD_NO = o.ORD_NO;

-- J8. 고객별 구매 횟수 — COUNT(*) vs COUNT(DISTINCT)
SELECT o.CUST_NO, COUNT(*) AS 틀린_횟수, COUNT(DISTINCT o.ORD_NO) AS 주문_횟수
FROM ORD o JOIN ORD_ITEM i ON i.ORD_NO = o.ORD_NO
WHERE o.CUST_NO IS NOT NULL
GROUP BY o.CUST_NO ORDER BY 주문_횟수 DESC, o.CUST_NO LIMIT 5;
