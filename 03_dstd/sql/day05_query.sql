-- ============================================================
-- 5일차 3교시 — SQL 보충: 4일차에 pandas 로 본 함정을 SQL 로 다시
-- Workbench 에서 문장 하나에 커서를 두고 Ctrl+Enter (한 문장씩 실행)
-- ============================================================
USE dstd_lab;

-- Q1. 주문 건수?
SELECT COUNT(*) AS 행수 FROM stg_order;

-- Q2. 진짜 주문 건수
SELECT COUNT(DISTINCT ORD_NO) AS 주문수 FROM stg_order;

-- Q3. 상태 표기가 몇 가지?
SELECT ORD_ST, COUNT(*) AS 행수 FROM stg_order GROUP BY ORD_ST ORDER BY 행수 DESC;

-- Q4. 비회원 행
SELECT COUNT(*) AS 비회원행 FROM stg_order WHERE CUSTNO = '' OR CUSTNO = 'GUEST';

-- Q5. 배송비 합계 — 행마다 더하면
SELECT SUM(DLV_FEE) AS 배송비_행합계 FROM stg_order;

-- Q6. 배송비 합계 — 주문마다 한 번
SELECT SUM(fee) AS 배송비_주문합계
FROM (SELECT ORD_NO, MIN(DLV_FEE + 0) AS fee FROM stg_order GROUP BY ORD_NO) t;

-- Q7. 판매금액 합계 — 문자열 금액을 그냥 SUM 하면? (결과 아래 경고를 보십시오)
SELECT SUM(AMT) AS 판매금액_합계 FROM stg_order;

-- Q8. 7월 주문 행 — 날짜가 문자열이면?
SELECT COUNT(*) AS 칠월_행 FROM stg_order WHERE ORD_DT LIKE '2026-07%';

-- Q9. 고객 마스터에 없는 주문 (비회원 포함)
SELECT COUNT(*) AS 고객없음
FROM stg_order o LEFT JOIN stg_cust c ON o.CUSTNO = c.CUST_NO
WHERE c.CUST_NO IS NULL;

-- Q10. 주문을 가장 많이 한 고객 5명
SELECT CUSTNO, COUNT(DISTINCT ORD_NO) AS 주문수, COUNT(*) AS 항목수
FROM stg_order
WHERE CUSTNO NOT IN ('', 'GUEST')
GROUP BY CUSTNO ORDER BY 주문수 DESC, CUSTNO LIMIT 5;
