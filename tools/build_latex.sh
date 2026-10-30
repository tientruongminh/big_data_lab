#!/usr/bin/env bash
# Build REPORT.tex/REPORT.pdf for each submission/LXX plus a combined BaoCao_L01-L06.{tex,pdf}.
set -euo pipefail
SUB=${1:-/home/tiencd/big_data_lab/data_engineering_labs/submission}
TOOLS=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d -p "${TMPDIR:-/tmp}")
LABS=(L01 L02 L03 L04 L05 L06)
for l in "${LABS[@]}"; do
  python3 "$TOOLS/md2tex.py" "$SUB/$l/REPORT.md" "$WORK/$l.md"
  python3 "$TOOLS/md2tex.py" "$SUB/$l/REPORT.md" "$WORK/all_$l.md" "$l"
done
{ printf -- '---\ntitle: "Báo cáo thực hành Data Engineering — Lab L01–L06"\nauthor: "Trương Minh Tiến"\ndate: "%s"\n---\n\n' "$(date +%d/%m/%Y)"
  for l in "${LABS[@]}"; do cat "$WORK/all_$l.md"; printf '\n\\newpage\n\n'; done; } > "$WORK/all.md"
cp "$TOOLS/latex-header.tex" "$WORK/"
OPTS='--pdf-engine=xelatex -V mainfont="DejaVu Serif" -V sansfont="DejaVu Sans" -V monofont="Liberation Mono" -V monofontoptions=Scale=0.85 -V fontsize=11pt -V geometry:margin=2cm -V lang=vi -V colorlinks=true -H /work/latex-header.tex'
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$SUB:/data" -v "$WORK:/work" -v /usr/share/fonts:/usr/share/fonts/host:ro \
  --entrypoint sh pandoc/extra:3.5 -c "
set -e
build() { # \$1 md, \$2 out dir, \$3 base name, extra pandoc args...
  md=\$1; dir=\$2; base=\$3; shift 3
  pandoc \"\$md\" -s -o \"\$dir/\$base.tex\" $OPTS \"\$@\"
  cd \"\$dir\"; mkdir -p /work/b_\$base
  for i in 1 2; do xelatex -interaction=nonstopmode -halt-on-error -output-directory=/work/b_\$base \$base.tex > /work/b_\$base/log.txt 2>&1 || { tail -30 /work/b_\$base/log.txt; exit 1; }; done
  cp /work/b_\$base/\$base.pdf \"\$dir/\"; cd /
  echo \"built \$dir/\$base.pdf\"
}
for l in ${LABS[*]}; do build /work/\$l.md /data/\$l REPORT -M title=\"\" ; done
build /work/all.md /data BaoCao_L01-L06 --toc -V toc-title='Mục lục'
"
