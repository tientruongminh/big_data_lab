#!/usr/bin/env python3
"""Prepare submission/LXX/REPORT.md files for pandoc -> LaTeX.

Rows of a markdown table that hold a screenshot (| caption | ![x](img/..) |)
become standalone figures, since images inside LaTeX table cells overflow.
Usage: md2tex.py SRC.md DST.md [PREFIX]   (PREFIX rewrites img/ -> PREFIX/img/)
"""
import re, sys

src, dst = sys.argv[1], sys.argv[2]
prefix = sys.argv[3] if len(sys.argv) > 3 else ""
out, lines = [], open(src, encoding="utf-8").read().splitlines()
row_img = re.compile(r"^\|(.*)\|\s*!\[[^\]]*\]\(([^)]+)\)\s*\|\s*$")
i = 0
while i < len(lines):
    ln = lines[i]
    # a table whose body rows carry images: header, separator, then rows
    if ln.startswith("|") and i + 2 < len(lines) and re.match(r"^\|[\s|:-]+\|$", lines[i + 1]) \
            and row_img.match(lines[i + 2]):
        i += 2
        while i < len(lines) and lines[i].startswith("|"):
            m = row_img.match(lines[i])
            if m:
                cap = m.group(1).strip().strip("|").strip()
                out += [f"![{cap}]({m.group(2)})", ""]
            else:
                out += [lines[i].strip("| "), ""]
            i += 1
        continue
    out.append(ln)
    i += 1
text = "\n".join(out).replace("≫", r"$\gg$") + "\n"  # DejaVu Serif Bold lacks U+226B
if prefix:
    text = re.sub(r"\]\((img/|[^)/]+\.(?:md|txt|csv|py|sh|sql)\))", lambda m: "](" + prefix + "/" + m.group(1), text)
open(dst, "w", encoding="utf-8").write(text)
