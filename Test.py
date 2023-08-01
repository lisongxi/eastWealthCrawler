from enum import Enum


class AA(Enum):
    aa = "aaa"
    bb = "bbb"


class BB:
    cc = "ccc"
    dd = "ddd"


print(type(AA.aa.value))
