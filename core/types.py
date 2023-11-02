from typing import NamedTuple
import re

re_sp = re.compile(r"\s+")


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
    email1: str
    email2: str
    titularidad: str

    @classmethod
    def build(cls, obj: dict):
        def safeint(s):
            if s is None:
                return None
            s = re_sp.sub("", s)
            if not s.isdigit():
                return None
            return int(s)
        for k, v in list(obj.items()):
            v = re_sp.sub("", v).lower()
            if v in ("", "-", 0):
                obj[k] = None
            elif k == 'FAX' and v in ("sinfax", "nohayfax", "no", "x"):
                obj[k] = None
        return cls(
            area=obj['AREA TERRITORIAL'],
            id=int(obj['CODIGO CENTRO']),
            tipo=obj['TIPO DE CENTRO'],
            nombre=obj['CENTRO'],
            domicilio=obj['DOMICILIO'],
            municipio=obj['MUNICIPIO'],
            distrito=obj['DISTRITO MUNICIPAL'],
            cp=obj['COD. POSTAL'],
            telefono=obj['TELEFONO'],
            fax=obj['FAX'],
            email1=obj['EMAIL'],
            email2=obj['EMAIL2'],
            titularidad=obj['TITULARIDAD']
        )
