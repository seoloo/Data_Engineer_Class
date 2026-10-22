-- ============================================================
-- 5일차 7교시 — 손으로 넣어 보기: MySQL 이 무엇을 막는가
-- ddl.sql 로 테이블 4개를 만든 뒤, 한 문장씩 Ctrl+Enter
-- 맨 끝 ROLLBACK 으로 전부 취소한다 — 다음 교시 적재에 흔적이 남지 않는다
-- ============================================================
USE dstd_lab;
START TRANSACTION;

-- ① 정상 고객 한 명
INSERT INTO CUST VALUES ('9000001', '테스트', '010-0000-0000', NULL, '2026-01-02', 'G01', '서울특별시', 'N');

-- ② 같은 고객번호 한 번 더 — PK 가 막는다
INSERT INTO CUST VALUES ('9000001', '테스트2', '010-0000-0001', NULL, '2026-01-02', 'G01', '서울특별시', 'N');

-- ③ 선행 0 — '0000123' 과 123 은 다르다 (VARCHAR 라서 그대로 저장)
INSERT INTO CUST VALUES ('0000123', '선행영', '010-0000-0002', NULL, '2026-01-02', 'G01', '부산광역시', 'N');
SELECT CUST_NO, LENGTH(CUST_NO) AS 길이 FROM CUST WHERE CUST_NM = '선행영';

-- ④ 없는 고객의 주문 — FK 가 막는다 (3일차 R07)
INSERT INTO ORD VALUES ('ORD99999991', '0009999', '2026-03-01', 'CARD', 0, 'PAID', NULL);

-- ⑤ 비회원 주문 — CUST_NO 가 NULL 이면 들어간다 (4일차 |o)
INSERT INTO ORD VALUES ('ORD99999992', NULL, '2026-03-01', 'CARD', 0, 'PAID', NULL);

-- ⑥ 없는 날짜 — DATE 가 막는다 (3일차 R04)
INSERT INTO ORD VALUES ('ORD99999993', '9000001', '2026-02-30', 'CARD', 0, 'PAID', NULL);

-- ⑦ 없는 주문의 항목 — FK 가 막는다
INSERT INTO ORD_ITEM VALUES ('ORD00000000', 1, '0001', 1, 1000);

-- ⑧ 배송비에 문자 — 숫자 컬럼이 막는다
INSERT INTO ORD VALUES ('ORD99999994', NULL, '2026-03-01', 'CARD', '2,500', 'PAID', NULL);

ROLLBACK;
SELECT COUNT(*) AS 남은_테스트행 FROM ORD WHERE ORD_NO LIKE 'ORD9999999%';
