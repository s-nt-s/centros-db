import sqlite3
from os.path import isfile
from urllib.request import urlretrieve
from functools import cache
from requests import Session
from core.dwr import DWR



def trim(s: str | None):
    if s is None:
        return None
    s = s.strip()
    if len(s) == 0:
        return None
    return s


S = Session()


@cache
def get_db():
    out = "/tmp/centros_db.sqlite"
    if not isfile(out):
        urlretrieve("https://s-nt-s.github.io/centros/db.sqlite", out)
    con = sqlite3.connect(out, uri=True)
    return con


def select(sql: str, *args):
    cursor = get_db().cursor()
    if len(args):
        cursor.execute(sql, args)
    else:
        cursor.execute(sql)
    for r in cursor.fetchall():
        yield r
    cursor.close()


URLS: dict[str, int] = {}
for c, in select('''
        select id from centro where web is not null and id in (
            select centro from concurso_anexo_centro
        )
    '''):
    d = DWR.get_total_alumnos(c)
    if d:
        print(c, d)
