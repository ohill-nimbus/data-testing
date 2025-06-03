"""Typing definitions for the project."""

from decimal import Decimal
from datetime import date, datetime, time, timedelta
from time import struct_time
from typing import Sequence, Union

SQLParam = Union[
    Decimal,
    bytes,
    date,
    datetime,
    float,
    int,
    str,
    time,
    timedelta,
    None,
    bool,
    struct_time,
]

SQLParams = Union[
    Sequence[SQLParam],
    dict[str, SQLParam],
]
