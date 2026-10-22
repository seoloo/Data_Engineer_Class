# 0교시 — 환경 확인.  셀 안에 커서를 두고 Shift+Enter

# %% [0-A] 파이썬과 패키지 확인
# > 셀 전체를 그대로 실행하세요 (이 셀은 미리 채워져 있습니다)
import sys
print(sys.version.split()[0])
import pandas as pd
import numpy as np
import matplotlib
print(pd.__version__, np.__version__, matplotlib.__version__)



# %% [0-B] 작업 폴더 확인
# > 이 셀도 그대로 실행하세요. 끝이 jsonpd 로 끝나야 합니다
from pathlib import Path
print(Path.cwd())
print(Path("make_data.py").exists())

# %%
