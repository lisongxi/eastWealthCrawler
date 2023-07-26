# -*- coding: utf-8 -*-

from enum import Enum
import pipe
from functools import reduce
import pandas as pd


def trace_error(err: Exception) -> str:
    err_name = (
            err.__class__.mro()[:-2] |
            pipe.map(lambda x: str(x)) |
            pipe.map(lambda x: x.lstrip("<class '").rstrip("'>")) |
            pipe.map(lambda x: x.split('.')[-1]) |
            pipe.Pipe(lambda x: reduce(lambda a, b: a + ' <- ' + b, x))
    )
    return err_name


try:
    k = 3
    while k > -2:
        i = 10 / k
        k -= 1
except Exception as err:
    print(err.__class__.mro()[:-2])
    print("-" * 50)
    print(trace_error(err))

