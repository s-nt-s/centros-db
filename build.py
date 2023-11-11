from core.api import Api, BulkRequestsApi
from core.dblite import DBLite, dict_factory
from typing import Tuple
from core.types import ParamValueText, QueryCentros
from core.util import must_one, read_file
from core.centro import Centro, BulkRequestsCentro
import argparse
import logging
from core.bulkrequests import BulkRequests
from typing import List, Dict

parser = argparse.ArgumentParser(
    description='Crea db a partir de '+Api.URL,
)
parser.add_argument(
    '--tcp-limit', type=int, default=50
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
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


def build_db(db: DBLite, tcp_limit: int):
    db.execute("sql/schema.sql")

    KWV["area"] = dict(db.to_tuple("select txt, id from area"))
    KWV["titularidad"] = dict(db.to_tuple("select txt, id from titularidad"))
    KWV["tipo"] = dict()

    dwn_html(tcp_limit=tcp_limit)
    dwn_search(tcp_limit=tcp_limit)
    insert_tipos(db)
    insert_queries(db)
    insert_etapas(db)
    insert_all(db)
    insert_missing(db)

    fix_tipo(db)
    fix_latlon(db)

    # Nos fiamos del campo titularida del csv
    # check_titularidad(db)

    execute_if_query_is_col(
        db,
        "sql/fix/jornada.sql",
        'checkOrdinaria=O',
        'checkContinua=C'
    )


def dwn_html(tcp_limit: int = 10):
    BulkRequests(
        tcp_limit=tcp_limit
    ).run(*(
        BulkRequestsCentro(c.id) for c in API.search_centros()
    ))


def dwn_search(tcp_limit: int = 10):
    queries: List[Dict[str, str]] = []
    for k in sorted(API.get_form()['cdGenerico'].keys()):
        queries.append(dict(cdGenerico=k))
    for name, obj in API.get_form().items():
        if jump_me(name):
            continue
        for val in sorted(obj.keys()):
            queries.append({name: val})
    for etapas in API.iter_etapas():
        data = {e.name: e.value for e in etapas}
        queries.append(data)
    BulkRequests(
        tcp_limit=tcp_limit
    ).run(*(
        BulkRequestsApi(API, data) for data in queries
    ))


def insert_tipos(db: DBLite):
    for k, v in sorted(API.get_form()['cdGenerico'].items()):
        rows = API.search_centros(cdGenerico=k)
        if len(rows) == 0:
            continue
        abr = must_one((x.tipo for x in rows))
        db.insert("TIPO", id=k, txt=v, abr=abr)
        KWV["tipo"][abr] = k
        multi_insert_centro(db, rows)


def insert_queries(db: DBLite):
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


def insert_etapas(db: DBLite):
    for e in walk_etapas():
        db.insert("ETAPA", id=e.id, txt=e.txt)
        for id in e.centros:
            db.insert("ETAPA_CENTRO", etapa=e.id, centro=id)
    for e in walk_etapas():
        for id in e.centros:
            eid = e.id.split("/")
            while len(eid) > 1:
                eid.pop()
                etp = "/".join(eid)
                txt = " -> ".join(e.txt.split(" -> ")[:len(eid)])
                db.insert("ETAPA", id=etp, txt=txt, _or="ignore")
                db.insert(
                    "ETAPA_CENTRO",
                    centro=id,
                    etapa=etp,
                    inferido=1,
                    _or="ignore"
                )

    for c in API.search_centros():
        for e in c.etapas:
            db.insert("ETAPA_NOMBRE_CENTRO", centro=c.id, **e._asdict())
    for c in API.search_centros():
        for e in c.etapas:
            eid = e.nombre.split(" -> ")
            while len(eid) > 1:
                eid.pop()
                db.insert(
                    "ETAPA_NOMBRE_CENTRO",
                    centro=id,
                    nombre=" -> ".join(eid),
                    inferido=1,
                    _or="ignore"
                )


def insert_all(db: DBLite):
    rows = API.search_centros()
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
    rows = API.get_centros(*missing)
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


def multi_insert_centro(db: DBLite, rows: Tuple[Centro], _or: str = None):
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
            _or=_or
        )
        for dif in row.educacion_diferenciada:
            db.insert(
                "EDUCACION_DIFERENCIADA",
                centro=row.id,
                etapa=dif,
                _or=_or
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


def fix_latlon(db: DBLite):
    def to_where(k, v):
        if v is None:
            return f'{k} is null'
        if isinstance(v, str):
            return f"{k} = '{v}'"
        return f"{k} = {v}"
    for cnt in db.to_tuple('''
        select
            id, domicilio, municipio, distrito, cp
        from
            centro
        where
            latitud is null or longitud is null
    ''', row_factory=dict_factory):
        sql = '''
            select distinct
                latitud, longitud
            from
                centro
            where
                latitud is not null and
                longitud is not null
        '''.rstrip()
        for k, v in cnt.items():
            if k == "id":
                continue
            sql = sql + ' and ' + to_where(k, v)
        latlon = db.to_tuple(sql, row_factory=dict_factory)
        if len(latlon) != 1:
            continue
        latlon = latlon[0]
        logger.info(
            "centro {id} set latitud={latitud} longitud={longitud}".format(**latlon, **cnt)
        )
        db.execute('''
            UPDATE centro SET
                latitud={latitud},
                longitud={longitud}
            where
                id={id}
        '''.format(**latlon, **cnt))


def walk_etapas():
    etapas: Tuple[ParamValueText] = None
    for etapas in API.iter_etapas():
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


if __name__ == "__main__":
    with DBLite(ARG.db, reload=True) as db:
        build_db(db, ARG.tcp_limit)

    DBLite.do_sql_backup(ARG.db)
