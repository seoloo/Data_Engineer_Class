-- ============================================================
-- TO-BE 테이블 정의 — 이름: (이름)
-- 4일차 docs\모델_TOBE.md 의 엔터티 하나 = 테이블 하나
-- 테이블 이름: 고객 CUST · 상품 PRD · 주문 ORD · 주문항목 ORD_ITEM
-- 컬럼 이름: 표준 사전 「용어」 물리명 / 자료형·길이: 「도메인」 시트
-- NULL 을 허용할지는 "비어 있는 행이 실제로 있는가 + 업무상 비어도 되는가" 로 정한다
-- 부모 테이블(FK 가 가리키는 쪽)을 먼저 만든다: CUST, PRD → ORD → ORD_ITEM
-- ============================================================
USE dstd_lab;

CREATE TABLE IF NOT EXISTS CUST (
  CUST_NO      VARCHAR(7)    NOT NULL COMMENT '고객번호',
  CUST_NM      VARCHAR(100)  NOT NULL COMMENT '고객명',
  -- 나머지 고객 컬럼을 채우십시오 (HP_NO, EMAIL_ADDR, JOIN_DT, CUST_GRD_CD, SIDO_NM, WTHD_YN)
  PRIMARY KEY (CUST_NO)
) COMMENT = '고객';

-- PRD, ORD, ORD_ITEM 을 이어서 작성하십시오
