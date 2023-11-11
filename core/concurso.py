from functools import cached_property
import re
from typing import Dict
import logging
from dataclasses import dataclass
from os.path import isfile
from .filemanager import FM
from .web import Web
from urllib.request import urlretrieve
from .util import hashint

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")
re_anexo = re.compile(r"^Anexo (\d+)\. (.+)$")
WEB = Web()


@dataclass(frozen=True)
class Anexo():
    num: int
    txt: str
    url: str

    @cached_property
    def content(self):
        file = FM.resolve_path(
            f"cache/pdf/{self.num:02} - {self.txt} - {hashint(self.url)}.pdf"
        )
        if not isfile(file):
            FM.makedirs(file)
            urlretrieve(self.url, file)
        return FM.load(file)

    @cached_property
    def centros(self):
        ids = re.findall(r"\b28\d{6}\b", self.content)
        return tuple(sorted(set(map(int, ids))))


class Concurso:
    MAESTROS = "https://www.comunidad.madrid/servicios/educacion/concurso-traslados-maestros"
    PROFESORES = "https://www.comunidad.madrid/servicios/educacion/concurso-traslados-profesores-secundaria-formacion-profesional-regimen-especial"

    def __init__(self, url):
        self.url = url

    @cached_property
    def home(self):
        return WEB.get(self.url)

    @cached_property
    def titulo(self):
        h1 = self.home.select_one("h1")
        return re_sp.sub(" ", h1.get_text()).strip()

    @cached_property
    def abr(self):
        t = set(self.titulo.lower().split())
        if t.intersection({"maestro", "maestros"}):
            return "MAE"
        if t.intersection({"profesor", "profesores"}):
            return "PRO"
        raise ValueError("Tipo de concurso no reconocido")

    @cached_property
    def anexos(self) -> Dict[int, Anexo]:
        anexos = {}
        for lg in self.home.select("fieldset legend a"):
            if "anexos" not in lg.get_text().lower():
                continue
            fld = lg.find_parent("fieldset")
            for a in fld.select("ul li a"):
                txt = re_sp.sub(" ", a.get_text()).strip()
                m = re_anexo.match(txt)
                if m is None:
                    continue
                url = a.attrs["href"]
                a = Anexo(
                    num=int(m.group(1)),
                    txt=m.group(2).strip(),
                    url=url
                )
                if a.num in anexos:
                    logger.warning(f"Anexo duplicado {a.num} {a.txt} {a.url}")
                    o: Anexo = anexos[a.num]
                    logger.warning(f"Anexo duplicado {o.num} {o.txt} {o.url}")
                    continue
                anexos[a.num] = a
        return anexos


if __name__ == "__main__":
    con = Concurso(Concurso.PROFESORES)
    anx = con.anexos
    for k, v in anx.items():
        print(k, v.txt, v.centros)
