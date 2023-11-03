from core.api import Api
from core.dblite import DBLite, dict_factory
from typing import Tuple
from core.types import ParamValueText, QueryCentros, CsvRow
import shutil
import argparse
import logging

parser = argparse.ArgumentParser(
    description='Crea db a partir de '+Api.URL,
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)
parser.add_argument(
    '--etapas', type=int, default=0,
    help="Profundidad del árbol de etapas (0 = sin limite)"
)

ARG = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

logger = logging.getLogger(__name__)


def get_if_one(arr):
    arr = set(arr)
    if len(arr) != 1:
        return None
    if len(arr) > 1:
        logger.error("More than one: "+", ".join(sorted(arr)))
    return arr.pop()


def read_file(file: str, *args, **kwargs):
    with open(file, "r") as f:
        txt = f.read().strip()
        txt = txt.format(*args, **kwargs)
        return txt


def walk_fix_query():
    sql = read_file("sql/fix/query.sql")
    while True:
        qr: str = db.one(sql)
        if qr is None:
            break
        qr = qr.split("&")
        qr = dict(kv.split("=") for kv in qr)
        yield qr


def walk_etapas(api: Api, max_steps=0):
    ko = set()
    etapas: Tuple[ParamValueText] = None
    for etapas in api.iter_etapas():
        if max_steps > 0 and len(etapas) > max_steps:
            continue
        idet = []
        text = []
        idqr = []
        data = {}
        for e in etapas:
            idet.append(e.value)
            text.append(e.text)
            idqr.append(f'{e.name}={e.value}')
            data[e.name] = e.value
        if tuple(idqr[:-1]) in ko:
            ko.add(tuple(idqr))
            continue
        rows = api.get_ids(**data)
        if len(rows) == 0:
            ko.add(tuple(idqr))
            continue
        yield QueryCentros(
            centros=rows,
            id="/".join(idet),
            qr="&".join(idqr),
            txt=" -> ".join(text)
        )


def build_db(db: DBLite, api: Api):
    db.execute("sql/schema.sql")

    to_id = {
        "area": dict(db.to_tuple("select txt, id from area")),
        "titularidad": dict(db.to_tuple("select txt, id from titularidad")),
        "tipo": dict(),
    }

    def asdict(obj: CsvRow):
        obj = obj._asdict()
        for k, v in list(obj.items()):
            obj[k] = to_id.get(k, {}).get(v, v)
        return obj

    for k, v in sorted(api.get_form()['cdGenerico'].items()):
        rows = api.get_csv(cdGenerico=k)
        abr = get_if_one((x.tipo for x in rows))
        db.insert("TIPO", id=k, txt=v, abr=abr)
        to_id["tipo"][abr] = k
        for row in rows:
            db.insert("CENTRO", **asdict(row))

    def jump_me(name: str):
        if name in ('cdGenerico', 'comboMunicipios', 'comboDistritos', 'cdTramoEdu'):
            return True
        return name.startswith("checkSubdir")

    for name, obj in api.get_form().items():
        if jump_me(name):
            continue
        for val, txt in sorted(obj.items()):
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for id in api.get_ids(**{name: val}):
                db.insert("QUERY_CENTRO", query=id_query, centro=id)

    for e in walk_etapas(api, max_steps=ARG.etapas):
        db.insert("ETAPA", id=e.id, txt=e.txt)
        for id in e.centros:
            db.insert("ETAPA_CENTRO", etapa=e.id, centro=id)

    for data in walk_fix_query():
        for row in api.get_csv(**data):
            if db.one("select id from CENTRO where id=?", row.id):
                continue
            db.insert("CENTRO", **asdict(row))

    for abr in db.to_tuple(read_file("sql/fix/tipo.sql")):
        db.insert("TIPO", id=abr, txt=abr, abr=abr)

    bad_tit = db.to_tuple(
        read_file("sql/fix/titularidad.sql"),
        row_factory=dict_factory
    )
    if len(bad_tit) == 0:
        db.execute('''
            PRAGMA foreign_keys = ON;
            delete from query where id like 'titularidad%=S'
        ''')
    for e in bad_tit:
        logger.critical("BAD {id}: {titularidad} <> {query}".format(**e))

    if 0 == db.one(
        read_file("sql/fix/iscol.sql", "'checkOrdinaria=O', 'checkContinua=C'")
    ):
        db.execute(read_file("sql/fix/jornada.sql"))


with DBLite(ARG.db, reload=True) as db:
    build_db(db, Api())

with DBLite(ARG.db, readonly=True) as db:
    db.sql_backup(ARG.db.rsplit(".", 1)[0]+".sql")


for tit in ("pub", "pri", "con"):
    db_name, ext = ARG.db.rsplit(".", 1)
    db_name = f"{db_name}_{tit}.{ext}"
    shutil.copyfile(ARG.db, db_name)
    with DBLite(db_name) as db:
        db.execute(read_file("sql/drop/titularidad.sql", tit=tit.upper()))
