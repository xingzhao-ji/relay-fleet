#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew --no-daemon --max-workers=2 test installDist --console=plain
.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_spool.py tests/test_agent.py tests/test_transport.py
.venv/bin/pip check
