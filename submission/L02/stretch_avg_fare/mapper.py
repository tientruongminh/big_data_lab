#!/usr/bin/env python3
"""Average fare per pickup zone -- MAPPER. Emits "<zone>\t<fare_sum>,<count>" with count = 1."""
import csv
import os
import sys

PU_COL, FARE_COL = 7, 10

zones = {}
if os.path.exists("taxi_zone_lookup.csv"):
    with open("taxi_zone_lookup.csv", newline="") as f:
        for row in csv.DictReader(f):
            zones[row["LocationID"]] = f"{row['Borough']}/{row['Zone']}"

for line in sys.stdin:
    fields = line.rstrip("\n").split(",")
    try:
        pu, fare = fields[PU_COL].strip(), float(fields[FARE_COL])
    except (IndexError, ValueError):
        sys.stderr.write("reporter:counter:TaxiLab,BAD_RECORDS,1\n")
        continue
    print(f"{zones.get(pu, pu)}\t{fare},1")
