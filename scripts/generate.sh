#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p agent/generated
.venv/bin/python -m grpc_tools.protoc -I proto --python_out=agent/generated --grpc_python_out=agent/generated proto/fleet.proto
# protoc generates a sibling absolute import; package it without altering the wire API.
.venv/bin/python - <<'PY'
from pathlib import Path
p=Path('agent/generated/fleet_pb2_grpc.py')
p.write_text(p.read_text().replace('import fleet_pb2 as fleet__pb2','from . import fleet_pb2 as fleet__pb2'))
Path('agent/generated/__init__.py').touch()
PY
