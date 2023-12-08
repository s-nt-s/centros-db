from core.dblite import DBLite
from core.api import Api
import argparse
from textwrap import dedent

parser = argparse.ArgumentParser(
    description='Reescribe README.md',
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)
parser.add_argument(
    '--md', type=str, default="README.md"
)

ARG = parser.parse_args()


def read(file):
    with open(file, "r") as f:
        return f.read().strip()


urls = [
    ("Buscador de centros", Api.URL)
]
with DBLite(ARG.db, readonly=True) as db:
    urls.extend(
        db.to_tuple("select txt, url from concurso")
    )

content = "\n# FAQ\n\n" + read(ARG.md).split("# FAQ")[1].strip()
with open(ARG.md, "w") as f:
    f.write("Crea una base de datos `sqlite` a partir del:\n\n")
    for txt, url in urls:
        f.write(f"* [{txt}]({url})\n")
    f.write(dedent('''
        El resultado actual se puede consular en [`db.sql`](out/db.sql).

        ![diagrama](out/db.svg)
    '''))
    f.write(content)
