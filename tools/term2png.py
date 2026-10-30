"""Render terminal output as a terminal-window PNG for lab reports.

usage: python3 term2png.py IN.txt OUT.png [--title "$ command"] [--hl REGEX] [--width COLS]
       (IN.txt may be "-" for stdin)
Lines matching --hl are drawn in yellow, lines starting with '$ ' or '###' in green.
"""
import argparse
import re
import sys
import textwrap

from PIL import Image, ImageDraw, ImageFont

FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
FONT_B = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
BG, BAR, FG = (30, 30, 30), (55, 55, 55), (220, 220, 220)
HL, CMD, DIM = (255, 214, 102), (120, 220, 120), (150, 150, 150)

p = argparse.ArgumentParser()
p.add_argument("src")
p.add_argument("out")
p.add_argument("--title", default="terminal")
p.add_argument("--hl", default=None)
p.add_argument("--width", type=int, default=150)
p.add_argument("--size", type=int, default=15)
a = p.parse_args()

text = sys.stdin.read() if a.src == "-" else open(a.src, encoding="utf-8", errors="replace").read()
text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text).expandtabs(8)
hl = re.compile(a.hl) if a.hl else None

lines = []  # (text, colour, bold)
for raw in text.rstrip("\n").split("\n"):
    if hl and hl.search(raw):
        colour, bold = HL, True
    elif raw.startswith("$ ") or raw.startswith("###"):
        colour, bold = CMD, True
    else:
        colour, bold = FG, False
    for i, part in enumerate(textwrap.wrap(raw, a.width, drop_whitespace=False) or [""]):
        lines.append(("  " + part if i else part, colour, bold))

font, font_b = ImageFont.truetype(FONT, a.size), ImageFont.truetype(FONT_B, a.size)
cw = font.getbbox("M")[2]
lh = int(a.size * 1.45)
pad, bar = 16, 34
width = max(len(t) for t, _, _ in lines) * cw + 2 * pad
width = max(width, 700)
height = bar + 2 * pad + lh * len(lines)

img = Image.new("RGB", (width, height), BG)
d = ImageDraw.Draw(img)
d.rectangle([0, 0, width, bar], fill=BAR)
for i, c in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
    d.ellipse([12 + i * 22, 11, 24 + i * 22, 23], fill=c)
d.text((90, 8), a.title[: (width - 100) // cw], font=font, fill=DIM)
y = bar + pad
for t, colour, bold in lines:
    d.text((pad, y), t, font=font_b if bold else font, fill=colour)
    y += lh
img.save(a.out)
print("saved", a.out)
