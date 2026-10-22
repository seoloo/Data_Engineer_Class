# 2교시 — JSON 을 파일로 저장하고 다시 읽기

# %% [2-A] 지금 파이썬이 서 있는 폴더
# > [2-1] 작업 폴더를 확인하세요. 파일 이름만 쓰면 이 폴더 기준입니다
import json
from pathlib import Path
print(Path.cwd()) # pwd
print(Path("data").exists())


# %% [2-B] 저장해 보기 — 폴더가 없으면?
# > [2-2] 1교시 주문을 data/my_order.json 으로 저장해 보세요 (첫 시도는 실패합니다)
order = {"order_id": "O001", "city": "서울", "paid": True, "memo": None, "items": ["노트", "펜"]}



# %% [2-C] 폴더를 만들고 다시 저장
# > [2-3] data 폴더를 만든 뒤 같은 코드를 다시 실행하세요
Path("data").mkdir(exist_ok=True)
with open("data/my_order.json","w",encoding="utf-8") as f:
    json.dump(order,f,ensure_ascii=False, indent=2)
print(Path("data/my_order.json").exists())
print(f.closed)


# %% [2-D] 파일을 글자 그대로 열어 보기
# > [2-4] VS Code 왼쪽 탐색기에서 data/my_order.json 을 열어 보고, 코드로도 읽어 보세요
text = Path("data/my_order.json").read_text(encoding="utf-8")
print(text)
print(type(text))


# %% [2-E] json.load — 파일에서 바로 딕셔너리로
# > [2-5] 이번에는 json.load 로 읽어 딕셔너리로 받으세요
with open("data/my_order.json","r",encoding="utf-8") as f:
    loaded = json.load(f)
print(loaded)
print(type(loaded))
print(loaded["items"])


# %% [2-F] s 하나 차이 — loads 에 파일을 넣으면
# > [2-6] load 대신 loads 를 써 보세요



# %% [2-G] encoding 을 안 맞추면 — 윈도우 기본값 cp949 재현
# > [2-7] cp949 로 저장한 파일을 utf-8 로 읽어 보세요



# %% [2-H] 실습 데이터 받기 — 터미널에서 python make_data.py
# > [2-8] VS Code 터미널(Ctrl+`)에서  python make_data.py  를 실행한 뒤 이 셀을 실행하세요



# %% [2-I] 주문 5건 읽기
# > [2-9] data/orders.json 을 읽어 자료형과 건수를 확인하세요



# %% [2-J] (혼자) 주문 두 건을 리스트로 저장
# > [2-10] 1교시에 만든 my_order 와 order 를 리스트 하나에 담아 data/my_orders.json 으로 저장하세요
# > [2-11] 다시 읽어서 len() 이 2 인지, 두 번째 주문의 city 가 무엇인지 출력하세요
