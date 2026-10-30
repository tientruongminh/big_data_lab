#!/usr/bin/env bash
# Build submission/LXX/REPORT.{tex,pdf} for each lab plus submission/BaoCao_L01-L06.{tex,pdf}.
# Cover-page details come from submission/meta.yaml. Needs docker image pandoc/extra:3.5
# (pandoc + pandoc-crossref + TeX Live); fonts are taken from the host.
set -euo pipefail
TOOLS=$(cd "$(dirname "$0")" && pwd)
SUB=${1:-$(dirname "$TOOLS")/submission}
WORK=$(mktemp -d -p "${TMPDIR:-/tmp}")
LABS=(L01 L02 L03 L04 L05 L06)

for l in "${LABS[@]}"; do
  # per-lab file: the H1 becomes the cover title, ## headings become numbered sections
  python3 "$TOOLS/md2tex.py" "$SUB/$l/REPORT.md" "$WORK/$l.full.md"
  python3 - "$WORK/$l.full.md" "$WORK/$l.md" "$WORK/$l.title" <<'PY'
import re, sys
src, dst, tfile = sys.argv[1:]
lines = open(src, encoding="utf-8").read().splitlines()
i = next(k for k, ln in enumerate(lines) if ln.startswith("# "))
open(tfile, "w", encoding="utf-8").write(re.sub(r"\s*\{#[^}]*\}\s*$", "", lines[i][2:]).strip())
open(dst, "w", encoding="utf-8").write("\n".join(lines[:i] + lines[i + 1:]) + "\n")
PY
  python3 "$TOOLS/md2tex.py" "$SUB/$l/REPORT.md" "$WORK/all_$l.md" "$l"
done
for l in "${LABS[@]}"; do cat "$WORK/all_$l.md"; printf '\n'; done > "$WORK/all.md"
cp "$TOOLS"/{latex-header.tex,titlepage.tpl,crossref.yaml,breakcode.lua} "$SUB/meta.yaml" "$WORK/"
: > "$WORK/empty.md"

FONTS=(-v /usr/share/fonts:/usr/share/fonts/host:ro)
MAIN="DejaVu Serif"
if [ -f /mnt/c/Windows/Fonts/times.ttf ]; then FONTS+=(-v /mnt/c/Windows/Fonts:/usr/share/fonts/win:ro); MAIN="Times New Roman"; fi

docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e MAIN="$MAIN" \
  -v "$SUB:/data" -v "$WORK:/work" "${FONTS[@]}" \
  --entrypoint sh pandoc/extra:3.5 -c '
set -e
COMMON="--filter pandoc-crossref --lua-filter=/work/breakcode.lua --metadata-file=/work/crossref.yaml --number-sections --toc
  -V lof -V lot -V lang=vi -V fontsize=12pt -V papersize=a4
  -V geometry:top=2.5cm,bottom=2.5cm,left=3cm,right=2cm
  -V monofont=Liberation\ Mono -V monofontoptions=Scale=0.85
  -V colorlinks=true -V linkcolor=black -V urlcolor=blue -V toccolor=black
  -H /work/latex-header.tex"
cover() { # $1 out, $2 title, $3 subtitle
  pandoc /work/empty.md -t latex --metadata-file=/work/meta.yaml -M title="$2" -M subtitle="$3" \
    --template=/work/titlepage.tpl -o "$1"
}
compile() { # $1 dir, $2 base
  mkdir -p /work/b_$2; cd "$1"
  for i in 1 2 3; do
    xelatex -interaction=nonstopmode -halt-on-error -output-directory=/work/b_$2 $2.tex \
      > /work/b_$2/out.txt 2>&1 || { tail -25 /work/b_$2/out.txt; exit 1; }
  done
  cp /work/b_$2/$2.pdf "$1/"; cd /; echo "built $1/$2.pdf"
}
for l in L01 L02 L03 L04 L05 L06; do
  cover /work/cover_$l.tex "$(cat /work/$l.title)" "Bài thực hành $l"
  eval pandoc /work/$l.md -s -o /data/$l/REPORT.tex $COMMON -V mainfont=\"\$MAIN\" \
    -V documentclass=article --shift-heading-level-by=-1 --include-before-body=/work/cover_$l.tex
  compile /data/$l REPORT
done
cover /work/cover_all.tex "Tổng hợp các bài thực hành L01–L06" ""
eval pandoc /work/all.md -s -o /data/BaoCao_L01-L06.tex $COMMON -V mainfont=\"\$MAIN\" \
  -V documentclass=report --top-level-division=chapter -M chapters=true \
  --include-before-body=/work/cover_all.tex
compile /data BaoCao_L01-L06
'
grep -h "Missing character" "$WORK"/b_*/*.log | sort | uniq -c || true
