#!/usr/bin/env python3
"""Average fare -- REDUCER: adds up (sum, count) pairs and divides only at the very end."""
import sys

current_key, total, count = None, 0.0, 0


def emit():
    print(f"{current_key}\t{total / count:.2f}\t{count}")


for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("\t")
    s, _, c = value.partition(",")
    if key == current_key:
        total, count = total + float(s), count + int(c)
    else:
        if current_key is not None:
            emit()
        current_key, total, count = key, float(s), int(c)

if current_key is not None:
    emit()
