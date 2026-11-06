#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.lock
bash scripts/generate.sh
./gradlew --no-daemon test installDist
.venv/bin/pip check
