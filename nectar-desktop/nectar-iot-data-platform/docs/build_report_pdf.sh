#!/usr/bin/env bash
# Render the report to a 5-page A4 PDF (the challenge caps it at 5 pages).
#   bash docs/build_report_pdf.sh
set -euo pipefail
cd "$(dirname "$0")/.."
pandoc docs/REPORT.md -o docs/REPORT.pdf \
  --standalone --css=docs/report.css \
  --pdf-engine=wkhtmltopdf \
  --pdf-engine-opt=--enable-local-file-access \
  --pdf-engine-opt=--disable-smart-shrinking
pages=$(pdfinfo docs/REPORT.pdf | awk '/^Pages/{print $2}')
echo "docs/REPORT.pdf -> ${pages} pages"
[ "$pages" -le 5 ] || { echo "OVER the 5-page limit"; exit 1; }
