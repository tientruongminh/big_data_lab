#!/usr/bin/env python3
"""Pull the REPORT.md text a subagent returned between BEGIN/END markers out of its JSONL transcript."""
import json, re, sys
src, dst = sys.argv[1], sys.argv[2]
best = None


def strings(obj):
    """Every string inside a JSON value (text blocks, tool_use inputs, ...)."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from strings(v)


for line in open(src, encoding="utf-8"):
    try:
        rec = json.loads(line)
    except ValueError:
        continue
    for t in strings(rec):
        m = (re.search(r"={3,}\s*BEGIN REPORT\.md\s*={3,}\n(.*?)\n={3,}\s*END REPORT\.md\s*={3,}", t, re.S)
             or re.search(r"^````(?:markdown|md)\n(# .*?)\n````\s*$", t, re.S | re.M))
        if m:
            best = m.group(1)
if best is None:
    sys.exit("markers not found")
open(dst, "w", encoding="utf-8").write(best.rstrip() + "\n")
print(dst, len(best.splitlines()), "lines")
