from typing import NamedTuple, Tuple


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
