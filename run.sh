#!/bin/bash
set -e
cd "$(dirname "$0")"
source .venv/bin/activate
python3 build.py
python3 readme.py
cp out/db.sqlite out/db.bak.sqlite
python3 concurso.py