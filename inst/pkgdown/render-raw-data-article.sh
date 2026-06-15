#!/usr/bin/env bash
set -euo pipefail

uv sync
uv run python -m ipykernel install \
  --user \
  --name ffiec-pq-docs \
  --display-name "ffiec.pq docs"

QUARTO_PYTHON=.venv/bin/python \
  uv run quarto render vignettes/getting-raw-data.qmd -M execute.eval=true
