from pathlib import Path
from typing import Callable
import re

import pandas as pd

def composite_func(*f: Callable) -> Callable:
    if len(f) > 1:
        return lambda *x: f[-1](composite_func(*f[:-1])(*x))
    else:
        return f[0]

def extract_digit(s: str) -> float:
    return float(re.search('[0-9.]+', s)[0])

def is_digit_in(s: str) -> bool:
    return True if re.search('[0-9]+', s) else False

def load_all_csv(path: Path) -> pd.DataFrame:
    dfs = []
    for p in path.iterdir():
        df = pd.read_csv(p)
        df['district'] = p.stem.replace('suumo_', '')
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)