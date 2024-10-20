#!/bin/bash -i

cd "$(dirname "$0")"

schemaspy db.sqlite --out db.svg -I "TIPO|AREA|TITULARIDAD|JORNADA"