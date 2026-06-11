#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

export DATABASE_URL="${DATABASE_URL:-postgresql://fagnerdossgoncalves@127.0.0.1:5432/eleicoes}"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate

pip install -q --disable-pip-version-check -r requirements.txt

exec streamlit run app.py "$@"
