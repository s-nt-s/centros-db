from core.api import Api
from core.dblite import DBLite
from typing import Tuple
from core.types import ParamValueText

db = DBLite("centros.sqlite", reload=True)


a = Api()


def get_if_one(arr):
    arr = set(arr)
    if len(arr) != 1:
        return None
    return arr.pop()


with DBLite("centros.sqlite", reload=True) as db:
    db.execute("sql/schema.sql")

    for k, v in a.get_form()['cdGenerico'].items():
        rows = a.get_csv(cdGenerico=k)
        abbr = get_if_one((x.tipo for x in rows))
        db.insert("TIPO", cod=k, txt=v, abbr=abbr)
        for row in rows:
            db.insert("CENTRO", _or="ignore", **row._asdict())

    for name, obj in a.get_form().items():
        for val, txt in obj.items():
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for id in a.get_ids(**{name: val}):
                db.insert("QUERY_CENTRO", query=id_query, centro=id)

    ko = set()
    etapas: Tuple[ParamValueText] = None
    for etapas in a.iter_etapas():
        cod = []
        txt = []
        idqr = []
        data = {}
        for e in etapas:
            cod.append(e.value)
            txt.append(e.text)
            idqr.append(f'{e.name}={e.value}')
            data[e.name] = e.value
        if tuple(idqr[:-1]) in ko:
            continue
        rows = a.get_ids(**data)
        if len(rows) == 0:
            ko.add(tuple(idqr))
            continue
        print(" -> ".join(txt))
        db.insert("ETAPA", cod="/".join(cod), txt=" -> ".join(txt))
        for id in rows:
            db.insert("QUERY_CENTRO", query="&".join(idqr), centro=id)
