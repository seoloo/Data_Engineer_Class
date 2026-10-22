# 1교시 — 파이썬 딕셔너리와 JSON 문자열

# %% [1-A] 주문 한 건을 딕셔너리로
# > [1-1] 주문 한 건을 딕셔너리로 만들고, 값 세 개를 꺼내 보세요
order = {
    "order_id" : "O001",
    "city":"서울",
    "paid":True,
    "memo":None,
    "items":["노트","펜"],
}
print(type(order))
print(order["city"])
print(order["items"][1])


# %% [1-B] str() 로 글자로 바꿔 보기 — 틀린 방법
# > [1-2] str()로 바꾼 결과를 보고, 따옴표·True·None 을 눈여겨보세요
text1 = str(order)
print(text1)
print(type(text1))

# %% [1-C] json.dumps — 딕셔너리를 JSON 문자열로
# > [1-3] json.dumps 결과를 [1-2]와 한 글자씩 비교해 보세요
import json
text2 = json.dumps(order)
print(text2)
print(type(text2))

# %% [1-D] 한글을 그대로, 보기 좋게
# > [1-4] ensure_ascii=False 와 indent=2 를 하나씩 넣어 차이를 확인하세요
print(json.dumps(order,ensure_ascii=False))
print(json.dumps(order,ensure_ascii=False, indent=2))


# %% [1-E] 문자열은 딕셔너리가 아니다
# > [1-5] 겉모양이 같아 보여도 text2 는 글자입니다. 키로 꺼내 보세요
print(text2[0])
print(text2[1:11])
# 그 무엇보다도 우선적이여야 하는것은 type !!


# %% [1-F] json.loads — JSON 문자열을 딕셔너리로
# > [1-6] 거꾸로 돌려서 원래 딕셔너리와 같은지 확인하세요
back = json.loads(text2)
print(back)
print(type(back))
print(back == order)

# %% [1-G] 파이썬식 글자는 JSON 이 아니다
# > [1-7] [1-2]에서 만든 text1 을 json.loads 에 넣어 보세요
back2 = json.loads(text1)
print(back2)


# %% [1-H] (혼자) 내 주문 만들기
# > [1-8] 본인이 최근 산 물건으로 주문 딕셔너리 my_order 를 만드세요
# >       키: order_id, city, paid, memo, items(2개 이상)
# > [1-9] json.dumps(ensure_ascii=False, indent=2) 로 출력하고,
# >       json.loads 로 되돌려서 원래와 같은지(==) 확인하세요
