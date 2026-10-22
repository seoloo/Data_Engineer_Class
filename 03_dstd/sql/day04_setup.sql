-- 4일차 8교시 — 실습 DB 만들기
-- MySQL Workbench 에서 root 로 접속한 뒤, 이 파일을 열고 번개 아이콘(Execute)으로 실행합니다.
-- 여러 번 실행해도 안전합니다. 기존 DB 를 지우는 명령은 없습니다.

CREATE DATABASE IF NOT EXISTS dstd_lab
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE dstd_lab;

-- 접속 확인용 한 줄 테이블. 5일차에 지워도 되는 것입니다.
CREATE TABLE IF NOT EXISTS hello (
  msg VARCHAR(50) NOT NULL
);
INSERT INTO hello (msg)
SELECT '안녕하세요 dstd_lab' FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM hello);

SELECT DATABASE() AS 현재_DB, VERSION() AS 서버_버전, msg FROM hello;
