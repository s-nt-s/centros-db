from typing import NamedTuple, Tuple
import re

re_sp = re.compile(r"\s+")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_]+\.[\w\-_]+', re.IGNORECASE)


def _safe_int(x):
    if x is None:
        return None
    return int(x)


def _parse(k, v):
    v = re_sp.sub(" ", v).strip()
    if v in ("", "-", "0", 0):
        return None
    x = re_sp.sub(" ", v).lower()
    if k == 'FAX' and x in ("sinfax", "nohayfax", "no", "x"):
        return None
    if k == "cp" and v == "00000":
        return None
    return v


def _find_mails(arr):
    mails = []
    for v in arr:
        for m in re_mail.findall(v):
            if m not in mails:
                mails.append(m)
    return tuple(mails)


def _find_titularidad(arr):
    tit = set()
    for a in arr:
        if a in ('Público', 'Privado Concertado', 'Privado'):
            tit.add(a)
    if len(tit) != 1:
        return None
    return tit.pop()


class CsvRow(NamedTuple):
    area: str
    id: int
    tipo: str
    nombre: str
    domicilio: str
    municipio: str
    distrito: str
    cp: int
    telefono: int
    fax: int
    email: str
    titularidad: str

    @classmethod
    def build(cls, head: Tuple, row: Tuple):
        obj = {h: _parse(h, c) for h, c in zip(head, row)}
        mails = _find_mails(row[head.index("EMAIL"):])
        mails = " ".join(mails) if mails else None
        titularidad = _find_titularidad(row[head.index("EMAIL2")+1:])

        return cls(
            area=obj['AREA TERRITORIAL'],
            id=int(obj['CODIGO CENTRO']),
            tipo=obj['TIPO DE CENTRO'],
            nombre=obj['CENTRO'],
            domicilio=obj['DOMICILIO'],
            municipio=obj['MUNICIPIO'],
            distrito=obj['DISTRITO MUNICIPAL'],
            cp=_safe_int(obj['COD. POSTAL']),
            telefono=obj['TELEFONO'],
            fax=obj['FAX'],
            email=mails,
            titularidad=titularidad
        )


class ParamValueText(NamedTuple):
    name: str
    value: str
    text: str


class QueryCentros(NamedTuple):
    id: str
    qr: str
    txt: str
    centros: Tuple[int]


class QueryResponse(NamedTuple):
    codCentrosExp: str
    frmExportarResultado: str

    def get_ids(self):
        if len(self.codCentrosExp) == 0:
            return tuple()
        ids = self.codCentrosExp.split(";")
        return tuple(sorted(map(int, ids)))


class LatLon(NamedTuple):
    latitude: float
    longitude: float
