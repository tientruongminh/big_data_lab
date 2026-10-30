#!/usr/bin/env python3
"""Average fare -- COMBINER: adds up (sum, count) pairs per key. Output has the same shape as its
input, so it may run 0, 1 or many times without changing the final result."""
import sys

current_key, total, count = None, 0.0, 0

for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("\t")
    s, _, c = value.partition(",")
    if key == current_key:
        total, count = total + float(s), count + int(c)
    else:
        if current_key is not None:
            print(f"{current_key}\t{total},{count}")
        current_key, total, count = key, float(s), int(c)

if current_key is not None:
    print(f"{current_key}\t{total},{count}")
