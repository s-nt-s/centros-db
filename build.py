from core.api import Api
from core.dblite import DBLite, dict_factory
from typing import Tuple
from core.types import ParamValueText, QueryCentros
from core.util import must_one, read_file
from core.centro import Centro
import shutil
import argparse
import logging
from core.threadme import ThreadMe

parser = argparse.ArgumentParser(
    description='Crea db a partir de '+Api.URL,
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)
parser.add_argument(
    '--etapas', type=int, default=-1,
    help="Profundidad del árbol de etapas (-1 = sin limite)"
)

ARG = parser.parse_args()
API = Api()
KWV = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

logger = logging.getLogger(__name__)


def build_db(db: DBLite):
    db.execute("sql/schema.sql")

    KWV["area"] = dict(db.to_tuple("select txt, id from area"))
    KWV["titularidad"] = dict(db.to_tuple("select txt, id from titularidad"))
    KWV["tipo"] = dict()

    insert_tipos(db)
    insert_queries(db)
    insert_etapas(db, 0, ARG.etapas)
    insert_all(db)
    insert_missing(db)

    fix_tipo(db)

    # Nos fiamos del campo titularida del csv
    # check_titularidad(db)

    execute_if_query_is_col(
        db,
        "sql/fix/jornada.sql",
        'checkOrdinaria=O',
        'checkContinua=C'
    )


def insert_tipos(db: DBLite):
    for k, v in sorted(API.get_form()['cdGenerico'].items()):
        rows = API.search_csv(cdGenerico=k)
        if len(rows) == 0:
            continue
        abr = must_one((x.tipo for x in rows))
        db.insert("TIPO", id=k, txt=v, abr=abr)
        KWV["tipo"][abr] = k
        multi_insert_centro(db, rows)


def insert_queries(db: DBLite):
    def jump_me(name: str):
        if name in (
            'cdGenerico',
            'comboMunicipios',
            'comboDistritos',
            'cdTramoEdu',
            'titularidadPublica',
            'titularidadPrivada',
            'titularidadPrivadaConc'
        ):
            return True
        return name.startswith("checkSubdir")

    for name, obj in API.get_form().items():
        if jump_me(name):
            continue
        for val, txt in sorted(obj.items()):
            ids = API.search_ids(**{name: val})
            if len(ids) == 0:
                continue
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for id in ids:
                db.insert("QUERY_CENTRO", query=id_query, centro=id)


def insert_etapas(db: DBLite, min_etapa, max_etapa):
    for e in walk_etapas(min_etapa, max_etapa):
        db.insert("ETAPA", id=e.id, txt=e.txt)
        for id in e.centros:
            db.insert("ETAPA_CENTRO", etapa=e.id, centro=id)


def insert_all(db: DBLite):
    rows = API.search_csv()
    multi_insert_centro(db, rows, _or="ignore")


def insert_missing(db: DBLite):
    sql = []
    for table in db.tables:
        if "centro" in db.get_cols(table):
            sql.append(f"select centro from {table}")
    missing = db.to_tuple('''
        select distinct centro from ({}) t where not exists (
            select c.id from centro c where t.centro=c.id
        )
    '''.format(" union ".join(sql)))
    rows = API.get_csv(*missing)
    multi_insert_centro(db, rows)


def fix_tipo(db: DBLite):
    for abr in db.to_tuple(read_file("sql/fix/tipo.sql")):
        db.insert("TIPO", id=abr, txt=abr, abr=abr)


def check_titularidad(db: DBLite):
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


def execute_if_query_is_col(db: DBLite, sql_path: str, *query_in: str):
    query_in = ", ".join(map(lambda x: f"'{x}'", query_in))
    sql = read_file("sql/fix/iscol.sql", query_in)
    if 0 == db.one(sql):
        db.execute(read_file(sql_path))


def multi_insert_centro(db: DBLite, rows: Tuple[Centro], **kwargs):
    def to_dict(row: Centro):
        obj = row._asdict()
        for k, v in list(obj.items()):
            obj[k] = KWV.get(k, {}).get(v, v)
        if row.latlon:
            obj['latitud'] = row.latlon.latitude
            obj['longitud'] = row.latlon.longitude
        obj['titular'] = row.titular
        obj['web'] = row.web
        return obj

    for row in rows:
        db.insert(
            "CENTRO",
            **to_dict(row),
            **kwargs
        )

    '''
    tm = ThreadMe(
        max_thread=30
    )
    for obj in tm.run(to_dict, rows):
        db.insert(
            "CENTRO",
            **obj,
            **kwargs
        )
    '''


def walk_etapas(min_etapa, max_etapa):
    etapas: Tuple[ParamValueText] = None
    for etapas in API.iter_etapas():
        if max_etapa >= 0 and len(etapas) > max_etapa:
            continue
        if min_etapa >= 0 and len(etapas) < min_etapa:
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
        rows = API.search_ids(**data)
        if len(rows) == 0:
            continue
        yield QueryCentros(
            centros=rows,
            id="/".join(idet),
            qr="&".join(idqr),
            txt=" -> ".join(text)
        )


if __name__ == "__main__":
    with DBLite(ARG.db, reload=True) as db:
        build_db(db)

    DBLite.do_sql_backup(ARG.db)

    for tit in ("pub", "pri", "con"):
        db_name, ext = ARG.db.rsplit(".", 1)
        db_name = f"{db_name}_{tit}.{ext}"
        shutil.copyfile(ARG.db, db_name)
        with DBLite(db_name) as db:
            db.execute(read_file("sql/drop/titularidad.sql", tit=tit.upper()))
