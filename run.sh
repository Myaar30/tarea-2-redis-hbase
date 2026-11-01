#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
.venv/bin/python -m pip install --disable-pip-version-check -r requirements.txt
docker compose up -d --build --wait
.venv/bin/python -m tarea.run "$@"
