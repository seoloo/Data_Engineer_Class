# AS-IS 모델 — 원천 3종을 있는 그대로

작성자: (이름) · 2026-10-08

- 엔터티 이름 = 원천 파일 이름(확장자 뺌), 속성 이름 = 원천 컬럼 이름 **그대로**.
- 설명("...") 칸에는 셀 3~6 에서 확인한 사실을 짧게 적는다. 예: "36행 빈 값", "표기 12종"
- 원천끼리 값으로 이어지는 곳은 **점선(..)** 으로. 셀 7 결과를 설명에.
- 그림 보기: VS Code 탭 오른쪽 위 "측면에서 미리 보기 열기" 아이콘 (Ctrl+K → V). 안 되면 https://mermaid.live 에 블록을 붙여 넣기

```mermaid
erDiagram
    cust_master {
        string CUST_NO PK "고객번호 7자리 선행0"
        string CUST_NM "이름 - 유일해도 식별자는 아님"
        string HP "휴대폰 번호 - 12행 겹침"
        string EMAIL "36행 빈 값"
        string JOIN_DT "날짜 형식"
        string GRD_CD "코드 G01~G04"
        string ADDR_SIDO "시도명"
        string DEL_YN "탈퇴여부 / Y=탈퇴"
    }
    
    order_2026{
        string ORD_NO "주문번호 — 혼자서는 식별 못 함"
        string ORD_SEQ "ORD_NO + ORD_SEQ 도 중복 50행"
        string CUSTNO "공란·GUEST = 비회원"
        string GOODS_CD
        string ORD_DT "주문 단위인데 항목마다 반복"
        string QTY
        string AMT "표기 12종"
        string ORD_ST "주문 단위 · 표기 18종"
        string PAY_MTHD "주문 단위"
        string DLV_FEE "주문 단위인데 항목마다 반복"
        string CANCEL_DT "주문 단위"
    }
    product_api_response {
        string goodsCode PK "상품코드 · 4자리 선행0"
        string goodsName
        string categoryName "6종 반복"
        string unitPrice "KRW 정수 또는 USD 소수"
        string currency
        string taxType "Z = 정의 없음"
        string useYn
        string regDate
    }

    order_2026 }o..o| cust_master : "CUSTNO = CUST_NO"
    order_2026 }o..o| product_api_response : "GOODS_CD = goodsCode"

```

## AS-IS 문제점

| # | 문제 | 근거 (셀 · 숫자) | TO-BE 에서 |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |
