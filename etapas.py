from core.dblite import DBLite
import argparse
from os.path import isfile
import sys
from build import insert_etapas

parser = argparse.ArgumentParser(
    description='Rellena las etapas que faltan'
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)

ARG = parser.parse_args()

if not isfile(ARG.db):
    sys.exit(f"No existe {ARG.db}")


def get_min(db: DBLite):
    etp = db.to_tuple("select id from etapa")
    if len(etp) == 0:
        return 0
    etp = max(map(lambda x: len(x.split("/")), etp))
    return etp + 1


with DBLite(ARG.db) as db:
    etp = get_min(db)
    insert_etapas(db, etp, -1)

DBLite.do_sql_backup(ARG.db)
