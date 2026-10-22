-- ============================================================
-- 5일차 2교시 — 스테이징 테이블: 원천을 "있는 그대로" MySQL 에 올린다
-- 모든 컬럼이 문자열(VARCHAR). 고치지도, 거르지도 않는다 — AS-IS 모델 그대로
-- 여러 번 실행해도 안전 (IF NOT EXISTS). 적재는 src\load_stage.py 가 한다
-- ============================================================
USE dstd_lab;

CREATE TABLE IF NOT EXISTS stg_cust (
  SRC_ROW    INT           NOT NULL,
  CUST_NO    VARCHAR(100),
  CUST_NM    VARCHAR(100),
  HP         VARCHAR(100),
  EMAIL      VARCHAR(100),
  JOIN_DT    VARCHAR(100),
  GRD_CD     VARCHAR(100),
  ADDR_SIDO  VARCHAR(100),
  DEL_YN     VARCHAR(100)
) COMMENT = '원천 cust_master.csv 그대로';

CREATE TABLE IF NOT EXISTS stg_order (
  SRC_ROW    INT           NOT NULL,
  ORD_NO     VARCHAR(100),
  ORD_SEQ    VARCHAR(100),
  CUSTNO     VARCHAR(100),
  GOODS_CD   VARCHAR(100),
  ORD_DT     VARCHAR(100),
  QTY        VARCHAR(100),
  AMT        VARCHAR(100),
  ORD_ST     VARCHAR(100),
  PAY_MTHD   VARCHAR(100),
  DLV_FEE    VARCHAR(100),
  CANCEL_DT  VARCHAR(100)
) COMMENT = '원천 order_2026.csv 그대로';

CREATE TABLE IF NOT EXISTS stg_prod (
  SRC_ROW       INT          NOT NULL,
  goodsCode     VARCHAR(100),
  goodsName     VARCHAR(100),
  categoryName  VARCHAR(100),
  unitPrice     VARCHAR(100),
  currency      VARCHAR(100),
  taxType       VARCHAR(100),
  useYn         VARCHAR(100),
  regDate       VARCHAR(100)
) COMMENT = '원천 product_api_response.json 그대로';

SHOW TABLES;
