from core.api import Api
from core.dblite import DBLite
from typing import Tuple
from core.types import ParamValueText, QueryCentros
import shutil
import sys

import logging

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


def walk_fk_query():
    fix_fk = read_file("sql/fk/query.sql")
    while True:
        qr: str = db.one(fix_fk)
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

    for k, v in sorted(api.get_form()['cdGenerico'].items()):
        rows = api.get_csv(cdGenerico=k)
        abbr = get_if_one((x.tipo for x in rows))
        db.insert("TIPO", id=k, txt=v, abbr=abbr)
        for row in rows:
            db.insert("CENTRO", _or="ignore", **row._asdict())

    for name, obj in api.get_form().items():
        for val, txt in sorted(obj.items()):
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for id in api.get_ids(**{name: val}):
                db.insert("QUERY_CENTRO", query=id_query, centro=id)

    for e in walk_etapas(api, max_steps=1):
        db.insert("ETAPA", id=e.id, txt=e.txt)
        for id in e.centros:
            db.insert("ETAPA_CENTRO", etapa=e.id, centro=id)

    for data in walk_fk_query():
        for row in api.get_csv(**data):
            db.insert("CENTRO", _or="ignore", **row._asdict())

    for abbr in db.to_tuple(read_file("sql/fk/tipo.sql")):
        db.insert("TIPO", id=abbr, txt=abbr, abbr=abbr)


DB_ALL = "out/db.sqlite" if len(sys.argv)==1 else sys.argv[1]
DB_TIT = {
    "pub": ('titularidadPublica=S', 'Público'),
    "pri": ('titularidadPrivada=S', 'Privado'),
    "con": ('titularidadPrivadaConc=S', 'Privado Concertado'),
}

with DBLite(DB_ALL, reload=True) as db:
    build_db(db, Api())

with DBLite(DB_ALL, readonly=True) as db:
    db.sql_backup(DB_ALL.rsplit(".", 1)[0]+".sql")


for suffix, (qr, tit) in DB_TIT.items():
    db_name, ext = DB_ALL.rsplit(".", 1)
    db_name = f"{db_name}_{suffix}.{ext}"
    shutil.copyfile(DB_ALL, db_name)
    with DBLite(db_name) as db:
        db.execute(read_file("sql/titularidad.sql", tit=tit, qr=qr))
        if 1 == db.one("select count(distinct titularidad) from CENTRO"):
            db.execute("ALTER TABLE CENTRO DROP COLUMN titularidad")
