from core.api import Api
from core.dblite import DBLite

db = DBLite("centros.sqlite", reload=True)


a = Api()

with DBLite("centros.sqlite", reload=True) as db:
    db.execute("sql/schema.sql")
    for name, obj in a.get_form().items():
        for val, txt in obj.items():
            id_query = f'{name}={val}'
            db.insert("QUERY", id=id_query, txt=txt)
            for row in a.get_csv(**{name: val}):
                db.insert("CENTRO", _or="ignore", **row._asdict())
                db.insert("QUERY_CENTRO", query=id_query, centro=row.id)
