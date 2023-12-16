from core.api import Api
from core.dblite import DBLite, dict_factory
from typing import Tuple, Dict
from core.types import ParamValueText, QueryCentros
from core.util import must_one, read_file, tp_join, logme, parse_dir, unupper
from core.centro import Centro, SEP
from core.colegio import Colegio, BulkRequestsColegio
from core.bulkrequests import BulkRequests
from core.filemanager import FM
import argparse
import logging
from core.concurso import Concurso
import re

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
LAST_TUNE = "sql/fix/last"
DONE = set()

open("build.log", "w").close()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("build.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


@logme
def build_db(db: DBLite, tcp_limit: int = 10):
    db.execute("sql/schema.sql")
    API.search_centros()

    KWV["area"] = dict(db.to_tuple("select txt, id from area"))
    KWV["tipo"] = dict()

    insert_tipos(db)
    insert_queries(db)
    insert_etapas(db)
    insert_all(db)
    insert_missing(db)

    fix_tipo(db)
    fix_latlon(db)
    try_complete(db, tcp_limit)

    # Nos fiamos del campo titularida del csv
    # check_titularidad(db)

    execute_if_query_is_col(
        db,
        "sql/fix/jornada.sql",
        'checkOrdinaria=O',
        'checkContinua=C'
    )

    insert_concurso(db)
    auto_fix(db)


@logme
def insert_tipos(db: DBLite):
    for k, v in sorted(API.get_form()['cdGenerico'].items()):
        rows = API.search_centros(cdGenerico=k)
        if len(rows) == 0:
            continue
        abr = must_one((x.tipo for x in rows))
        db.insert("TIPO", id=k, txt=unupper(v, rstrip=". "), abr=abr)
        KWV["tipo"][abr] = k
        multi_insert_centro(db, rows)


@logme
def insert_queries(db: DBLite):
    for name, obj in API.get_form().items():
        if Api.is_redundant_parameter(name):
            continue
        for val, txt in sorted(obj.items()):
            ids = API.search_ids(**{name: val})
            if len(ids) == 0:
                continue
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for id in ids:
                db.insert("QUERY_CENTRO", query=id_query, centro=id)


@logme
def insert_etapas(db: DBLite):
    for e in walk_etapas():
        db.insert("ETAPA", id=e.id, txt=e.txt)
        for c in e.centros:
            db.insert("ETAPA_CENTRO", etapa=e.id, centro=c)
    for e in walk_etapas():
        for c in e.centros:
            eid = e.id.split("/")
            while len(eid) > 1:
                eid.pop()
                etp = "/".join(eid)
                txt = SEP.join(e.txt.split(SEP)[:len(eid)])
                db.insert("ETAPA", id=etp, txt=txt, _or="ignore")
                db.insert(
                    "ETAPA_CENTRO",
                    centro=c,
                    etapa=etp,
                    inferido=1,
                    _or="ignore"
                )

    for c in API.search_centros():
        if c.isBad():
            continue
        for e in c.etapas:
            db.insert("ETAPA_NOMBRE_CENTRO", centro=c.id, **e._asdict())


@logme
def insert_all(db: DBLite):
    rows = API.search_centros()
    multi_insert_centro(db, rows, _or="ignore")


@logme
def insert_missing(db: DBLite):
    tables = [t for t in db.tables if "centro" in db.get_cols(t)]
    missing = db.to_tuple('''
        select distinct centro from ({}) t where not exists (
            select c.id from centro c where t.centro=c.id
        )
    '''.format(" union ".join(map(
        lambda t: f"select centro from {t}",
        tables
    ))))

    def filter_bad(c: Centro):
        if not c.isBad():
            return True
        logger.warning(f'{c.id} descartado por malformado {c.info}')
        for t in tables:
            db.execute(f"delete from {t} where centro={c.id}")
        return False

    rows = tuple(filter(filter_bad, API.get_centros(*missing)))

    multi_insert_centro(db, rows)


@logme
def fix_tipo(db: DBLite):
    for abr in db.to_tuple(read_file("sql/fix/tipo.sql")):
        db.insert("TIPO", id=abr, txt=unupper(abr), abr=abr)


@logme
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


@logme
def execute_if_query_is_col(db: DBLite, sql_path: str, *query_in: str):
    query_in = ", ".join(map(lambda x: f"'{x}'", query_in))
    sql = read_file("sql/fix/iscol.sql", query_in)
    if 0 == db.one(sql):
        db.execute(read_file(sql_path))


def multi_insert_centro(db: DBLite, rows: Tuple[Centro], _or: str = None):
    def to_dict(row: Centro):
        row.fix()
        obj = row._asdict()
        for k, v in list(obj.items()):
            obj[k] = KWV.get(k, {}).get(v, tp_join(v))
        if row.latlon:
            obj['latitud'] = row.latlon.latitude
            obj['longitud'] = row.latlon.longitude
        obj['titular'] = row.titular
        obj['web'] = tp_join(row.web)
        obj['domicilio'] = parse_dir(row.domicilio)
        return obj

    for row in rows:
        if row.id in DONE:
            continue
        DONE.add(row.id)
        if row.isBad():
            continue
        db.insert(
            "CENTRO",
            **to_dict(row),
            _or=_or
        )
        for dif in row.educacion_diferenciada:
            db.insert(
                "EDUCACION_DIFERENCIADA",
                centro=row.id,
                tipo=dif,
                _or=_or
            )
        for p in row.planes:
            db.insert(
                "PLAN",
                centro=row.id,
                nombre=p,
                _or=_or
            )
        for e in row.extraescolares:
            db.insert(
                "EXTRAESCOLAR",
                centro=row.id,
                nombre=e,
                _or=_or
            )
        for e in row.proyectos:
            db.insert(
                "PROYECTO",
                centro=row.id,
                nombre=e,
                _or=_or
            )


@logme
def fix_latlon(db: DBLite):
    changes = 1
    while changes > 0:
        changes = sum([
            fix_centro_col(
                db,
                cols=('cp', ),
                keys=('domicilio', 'municipio', 'distrito')
            ),
            fix_centro_col(
                db,
                cols=('distrito', ),
                keys=('domicilio', 'municipio', 'cp')
            ),
            fix_centro_col(
                db,
                cols=('latitud', 'longitud'),
                keys=('domicilio', 'municipio', 'distrito', 'cp'),
                strong=False
            )
        ])


@logme
def try_complete(db: DBLite, tcp_limit: int = 10):
    def iter_rows(*args: str, andor="and"):
        where = f" {andor} ".join(map(lambda x: f"{x} is null", args))
        ids = db.to_tuple(f"select id from centro where ({where})")
        if len(ids) > 0:
            BulkRequests(
                tcp_limit=tcp_limit
            ).run(
                *map(BulkRequestsColegio, ids),
                label="colegios"
            )
            for id in ids:
                c = Colegio.get(id)
                if c is not None:
                    yield c

    list(iter_rows(
        "web", "telefono", "email", "latitud", "longitud",
        andor="or"
    ))
    for c in iter_rows("web"):
        if c.web:
            update_centro(db, c.id, web=c.web)
    for c in iter_rows("telefono"):
        if c.telefono:
            update_centro(db, c.id, telefono=c.telefono)
    for c in iter_rows("email"):
        if c.email:
            update_centro(db, c.id, email=c.email)
    for c in iter_rows("latitud", "longitud"):
        if c.latlon:
            update_centro(
                db,
                c.id,
                latitud=c.latlon.latitude,
                longitud=c.latlon.longitude
            )


def fix_centro_col(db: DBLite, cols: Tuple[str], keys: Tuple[str], strong=True):
    if len(keys) == 0:
        return 0
    _and_or = " and " if strong else " or "
    sql1 = '''
        select
            id, {0}
        from
            centro
        where (
            ({1}) and
            ({2})
        )
    '''.format(
        ", ".join(keys),
        " and ".join(map(lambda x: x+" is null", cols)),
        _and_or.join(map(lambda x: x+" is not null", keys))
    ).strip()
    sql2 = '''
        select distinct
            {0}
        from
            centro
        where
            {1}
    '''.format(
        ", ".join(cols),
        " and ".join(map(lambda x: x+" is not null", cols))
    ).strip()

    changes = 0
    for cnt, val in find_val_for_null(db, sql1, sql2):
        update_centro(db, cnt['id'], **val)
        changes = changes + 1
    return changes


def find_val_for_null(db: DBLite, sql1: str, sql2: str) -> Tuple[Tuple[Dict, Dict]]:
    arr = []
    for cnt in db.to_tuple(sql1, row_factory=dict_factory):
        kvs = dict(kv for kv in cnt.items() if kv[0] != 'id')
        sql = (sql2 + " and ") + " and ".join(
            map(lambda k: f'{k}=?', kvs.keys())
        )
        value = db.to_tuple(sql, *kvs.values(), row_factory=dict_factory)
        if len(value) == 1:
            arr.append((cnt, value[0]))
    return tuple(arr)


@logme
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
            txt=SEP.join(text)
        )


@logme
def insert_concurso(db: DBLite):
    re_esp_dif = re.compile(r"centros? de especial dificultad", re.IGNORECASE)
    esp_dif = set()
    ok_cent = set(db.to_tuple("select id from centro"))
    for url in (Concurso.MAESTROS, Concurso.PROFESORES):
        con = Concurso(url)
        db.insert(
            "CONCURSO",
            id=con.abr,
            txt=con.titulo,
            url=con.url,
            cuerpo=con.cuerpo
        )
        for anx in con.anexos.values():
            db.insert(
                "CONCURSO_ANEXO",
                concurso=con.abr,
                anexo=anx.num,
                txt=anx.txt,
                url=anx.url
            )
            if re_esp_dif.search(anx.txt):
                esp_dif = esp_dif.union(anx.centros)
                continue
            ok_cent = ok_cent.union(anx.centros)
            for c in anx.centros:
                db.insert(
                    "CONCURSO_ANEXO_CENTRO",
                    centro=c,
                    concurso=con.abr,
                    anexo=anx.num,
                )

    for c in (esp_dif - ok_cent):
        logger.warning(f"{c} no existe?")
        continue
    esp_dif = esp_dif.intersection(ok_cent)
    update_centro(db, *tuple(sorted(esp_dif)), dificultad=1)


def update_centro(db: DBLite, *id: int, **kwargs):
    if len(kwargs) == 0 or len(id) == 0:
        return

    log = ", ".join(map(
        lambda kv: f'{kv[0]}={tp_join(kv[1])}',
        kwargs.items()
    ))
    idwhere = (f"={id[0]}" if len(id) == 1 else f" in {id}")
    logger.info(f"SET[id{idwhere}] " + log)
    sql = " ".join([
        "update centro set ",
        ", ".join(map(lambda k: f'{k}=?', kwargs.keys())),
        "where id"+idwhere
    ])
    db.execute(sql, *map(tp_join, kwargs.values()))


@logme
def auto_fix(db: DBLite):
    sql = []
    for f in sorted(FM.resolve_path(LAST_TUNE).glob("*.sql")):
        content: str = FM.load(f)
        for ln in content.split("\n"):
            ln = ln.strip()
            if len(ln) > 0 and not ln.startswith("--"):
                sql.append(ln)
    if len(sql):
        db.execute("\n".join(sql))

    file = f"{LAST_TUNE}/01-latlon.sql"
    _concat_ = " || ' ' || ".join(map(
        lambda x: f"IFNULL({x}, '')",
        (
            "domicilio",
            "municipio",
            "distrito",
            "cp"
        )
    ))
    field = f"TRIM(REPLACE({_concat_}, '  ', ' '))"
    sql = []
    for up in (FM.load(file, not_exist_ok=True) or '').split("\n"):
        if not up.strip().startswith("--"):
            sql.append(up)
    for dr in db.to_tuple(f"""
        select distinct
            {field}
        from
            centro
        where
            longitud is null and
            latitud is null and
            domicilio is not null and
            municipio is not null and
            length({field})>10
    """):
        dr = dr.replace("'", "''")
        up = f"--UPDATE centro SET latitud=? and longitud=? where latitud is null and longitud is null and {field}='{dr}';"
        if up not in sql:
            sql.append(up)
    FM.dump(file, "\n".join(sql).strip())


if __name__ == "__main__":
    with DBLite(ARG.db, reload=True) as db:
        build_db(db, ARG.tcp_limit)

    DBLite.do_sql_backup(ARG.db)
