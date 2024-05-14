from functools import cached_property
import re
from typing import Dict
import logging
from dataclasses import dataclass
from os.path import isfile
from .filemanager import FM
from .web import Web
from urllib.request import urlretrieve
from .util import hashme
from abc import ABC, abstractmethod
from bs4 import Tag, BeautifulSoup

MONTH = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')

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
        if self.url.rsplit(".")[-1].lower() not in "pdf":
            return ""
        file = FM.resolve_path(
            f"cache/pdf/{self.num:02} - {self.txt} - {hashme(self.url)}.pdf"
        )
        if not isfile(file):
            FM.makedirs(file)
            urlretrieve(self.url, file)
        return FM.load(file)

    @cached_property
    def centros(self):
        ids = re.findall(r"\b28\d{6}\b", self.content)
        return tuple(sorted(set(map(int, ids))))


class Concurso(ABC):

    @staticmethod
    def build(url):
        if url in (Concursazo.PROFESORES, Concursazo.MAESTROS):
            return Concursazo(url)
        if url in (Concursillo.PROFESORES, Concursillo.MAESTROS):
            return Concursillo(url)
        raise ValueError(f"url de concursazo/illo no reconocida {url}")

    def __init__(self, url):
        self.url = url

    @cached_property
    def home(self):
        return WEB.get(self.url)

    @cached_property
    def convocatoria(self):
        m = re.search(r"\b20\d\d-20\d\d\b", self.titulo)
        if m is None:
            raise ValueError(f"No se encuentra convocatoria en {self.url}")
        return m.group()

    @cached_property
    def titulo(self):
        h1 = self.home.select_one("h1")
        return re_sp.sub(" ", h1.get_text()).strip()

    @cached_property
    def centros(self):
        centros = set()
        for a in self.anexos.values():
            centros = centros.union(a.centros)
        return tuple(sorted(centros))

    @cached_property
    def abr(self) -> str:
        return self._abr()

    @abstractmethod
    def _abr(self) -> str:
        ...

    @cached_property
    def cuerpo(self) -> str:
        return self._cuerpo()

    @abstractmethod
    def _cuerpo(self) -> str:
        ...

    @cached_property
    def anexos(self) -> Dict[int, Anexo]:
        return self._anexos()

    @abstractmethod
    def _anexos(self) -> str:
        ...

    @property
    @abstractmethod
    def tipo(self):
        ...


class Concursazo(Concurso):
    MAESTROS = "https://www.comunidad.madrid/servicios/educacion/concurso-traslados-maestros"
    PROFESORES = "https://www.comunidad.madrid/servicios/educacion/concurso-traslados-profesores-secundaria-formacion-profesional-regimen-especial"
    MAE = "MAE"
    PRO = "PRO"

    @property
    def tipo(self):
        return "concurso"

    def _abr(self):
        t = set(self.titulo.lower().split())
        if t.intersection({"maestro", "maestros"}):
            return Concursazo.MAE
        if t.intersection({"profesor", "profesores"}):
            return Concursazo.PRO
        raise ValueError("Tipo de concurso no reconocido")

    def _cuerpo(self) -> str:
        crp = set()
        for a in self.anexos.values():
            m = re.search(r"\(Cuerpos? [^\)]+", a.txt)
            if m is None:
                continue
            for n in re.findall(r"\d+", m.group()):
                if len(n) == 4:
                    crp.add(n)
        if crp:
            return " ".join(sorted(crp))
        if self.abr == Concursazo.MAE:
            return "0597"

    def _anexos(self) -> Dict[int, Anexo]:
        anexos = {}
        bocm = self.home.find(
            "a",
            href=re.compile(r"^https?://.*/BOCM-\d+-\d+\.PDF$", re.IGNORECASE)
        )
        if bocm:
            pdf = bocm.attrs["href"].rsplit("/")[-1]
            bcm = pdf.rsplit(".")[0].upper()
            anexos[0] = Anexo(
                num=0,
                txt=bcm,
                url="https://www.bocm.es/"+bcm.lower()
            )
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


class Concursillo(Concurso):
    MAESTROS = "https://www.comunidad.madrid/servicios/educacion/maestros-asignacion-destinos-provisionales-inicio-curso"
    PROFESORES = "https://www.comunidad.madrid/servicios/educacion/secundaria-fp-re-asignacion-destinos-provisionales-inicio-curso"
    MAE = "maestrillo"
    PRO = "profesillo"

    @property
    def tipo(self):
        return "concursillo"

    def _abr(self):
        t = set(re.sub(r"[:,]", " ", self.titulo.lower()).split())
        if t.intersection({"maestro", "maestros"}):
            return Concursillo.MAE
        if t.intersection({"profesor", "profesores", "secundaria"}):
            return Concursillo.PRO
        raise ValueError("Tipo de Concursillo no reconocido")

    def _cuerpo(self) -> str:
        crp = set()
        for a in self.anexos.values():
            m = re.search(r"C[oó]digos\s+de\s+especialidades", a.txt)
            if m is None:
                continue
            ids = re.findall(r"\b0\d{3}\b", a.content)
            crp = crp.union(ids)
        if crp:
            return " ".join(sorted(crp))
        if self.abr == Concursillo.MAE:
            return "0597"

    def _anexos(self) -> Dict[int, Anexo]:
        def _txt(n: Tag):
            n = BeautifulSoup(str(n.find_parent("li")), "html.parser")
            for x in n.findAll(["ul", "ol"]):
                x.extract()
            return re_sp.sub(" ", n.get_text()).strip(": ")
        done = set()
        anexos = {}
        resoluciones = {}
        div: Tag = self.home.select_one("#instrucciones-calendario")
        rsl: Tag
        re_reso = re.compile(r"^\s*resoluci[oó]n\s+de\s+(\d+)\s+de\s+(\w+)\s+de\s+(\d+)(.*)$", flags=re.IGNORECASE)
        for rsl in div.findAll("a", string=re_reso):
            txt = re_sp.sub(r" ", rsl.get_text()).lower().strip()
            m = re_reso.match(txt)
            if m is None:
                raise ValueError(txt)
            d, mes, y, tail = m.groups()
            m = MONTH.index(mes[:3])+1
            txt = f"{y}-{m:02d}-{int(d):02d}"
            rsl.string = re_reso.sub("Resolución "+txt+tail, rsl.string)
            resoluciones[txt] = rsl

        for r, rsl in sorted(resoluciones.items()):
            root: Tag = rsl.find_parent("ul")
            while root.find_parent("ul"):
                root = root.find_parent("ul")
            for a in root.select("li a"):
                url = a.attrs["href"]
                if url in done:
                    continue
                done.add(url)
                num = len(anexos)
                anexos[num] = Anexo(
                    num=num,
                    txt=_txt(a),
                    url=url
                )
        return anexos


if __name__ == "__main__":
    for con in map(Concurso.build, (Concursazo.MAESTROS, Concursazo.PROFESORES, Concursillo.MAESTROS, Concursillo.PROFESORES)):
        for a in con.anexos.values():
            print(con.convocatoria, con.abr, a.num, len(a.centros))
