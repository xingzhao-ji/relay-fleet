#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence
bash scripts/generate.sh
./gradlew --no-daemon test installDist --rerun-tasks --console=plain | tee evidence/final-java.log
.venv/bin/python -m pytest -v --junitxml=evidence/pytest.xml | tee evidence/final-python.log
.venv/bin/python -m scripts.experiment | tee evidence/experiment-run.log
.venv/bin/pip check | tee evidence/pip-check.log
