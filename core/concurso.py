from functools import cached_property
import re
from typing import Dict
import logging
from dataclasses import dataclass
from os.path import isfile
from .filemanager import FM, FileManager
from .web import Driver, Web
from .util import hashme
from abc import ABC, abstractmethod
from bs4 import Tag, BeautifulSoup
from time import sleep
from pathlib import Path
from os import environ

MONTH = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")
re_anexo = re.compile(r"^Anexo (\d+)([a-z])?\. (.+)$")


@dataclass(frozen=True)
class Anexo():
    num: int
    txt: str
    url: str
    letter: str = None

    @cached_property
    def local_pdf(self):
        if self.url.rsplit(".")[-1].lower() not in ("pdf",):
            return None
        return FM.resolve_path(
            f"cache/pdf/{self.num:02}{self.letter or ''} - {self.txt[:30]} - {hashme(self.url)}.pdf"
        )

    @cached_property
    def content(self):
        if self.local_pdf is None:
            return ""
        if not isfile(self.local_pdf):
            FM.makedirs(self.local_pdf)
            with Driver(browser="firefox") as WEB:
                WEB.get(self.url)
                sleep(5)
                s = WEB.pass_cookies()
                r = s.get(self.url)
                with open(self.local_pdf, "wb") as f:
                    f.write(r.content)
        txt: str = FM.load(self.local_pdf)
        txt = txt.strip()
        return txt

    def __get_centros(self):
        ids = re.findall(r"\b28\d{6}\b", self.content)
        arr: list[int] = []
        for i in map(int, ids):
            if i not in arr:
                arr.append(i)
        return tuple(arr)

    @cached_property
    def centros(self):
        local_ctr: None | Path = None
        if self.local_pdf is not None and self.local_pdf.with_suffix(FileManager.OCR_SUFFIX).exists():
            local_ctr = self.local_pdf.with_suffix(".ctr.txt")
        if local_ctr is not None and local_ctr.exists():
            lines = FM.load_txt(local_ctr).strip().split()
            return tuple(sorted(map(int, lines)))
        ctr = self.__get_centros()
        if ctr and local_ctr is not None:
            FM.dump_txt(local_ctr, "\n".join(map(str, ctr)))
        return tuple(sorted(ctr))


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
        w = Web()
        proxy = environ.get("SPAIN_PROXY")
        if proxy is not None:
            w.s.proxies = {"http": proxy, "https": proxy}
        return w.get(self.url)

    @cached_property
    def convocatoria(self):
        m = re.search(r"\b20\d\d-20\d\d\b", self.titulo)
        if m is not None:
            return m.group()
        m = re.search(r" \((20\d\d)\)", self.titulo)
        if m is not None:
            y = int(m.group(1))
            return f"{y}-{y+1}"
        raise ValueError(f"No se encuentra convocatoria en {self.url} {self.titulo} -- {self.home.get_text()}")

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

    def _bocm(self):
        set_bocm: set[str] = set()
        for bocm in self.home.find_all(
            "a",
            href=re.compile(r"^https?://.*/BOCM-\d+-\d+\.PDF$", re.IGNORECASE)
        ):
            pdf = bocm.attrs["href"].rsplit("/")[-1]
            bcm = pdf.rsplit(".")[0].upper()
            set_bocm.add(bcm)
        return tuple(sorted(set_bocm))

    def _anexos(self) -> Dict[int, Anexo]:
        anexos = {}
        tp_bocm = self._bocm()
        for i, bcm in enumerate(tp_bocm, start=-(len(tp_bocm)-1)):
            anexos[i] = Anexo(
                num=i,
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
                num, letter, txt = m.groups()
                if isinstance(letter, str):
                    letter = letter.strip()
                    if len(letter) == 0:
                        letter = None
                a = Anexo(
                    num=int(num),
                    letter=letter,
                    txt=txt.strip(),
                    url=url
                )
                if a.num in anexos:
                    logger.warning(f"Anexo duplicado {a.num}{a.letter or ''} {a.txt} {a.url}")
                    o: Anexo = anexos[a.num]
                    logger.warning(f"Anexo duplicado {o.num}{a.letter or ''} {o.txt} {o.url}")
                    continue
                anexos[a.num] = a
        return anexos


class Concursillo(Concurso):
    MAESTROS = "https://www.comunidad.madrid/servicios/educacion/maestros-asignacion-destinos-provisionales-inicio-curso"
    PROFESORES = "https://www.comunidad.madrid/servicios/educacion/secundaria-fp-re-asignacion-destinos-provisionales-inicio-curso"
    MAE = "concursillo-magisterio"
    PRO = "concursillo"

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
        if self.abr == Concursillo.PRO:
            return "0511 0512 0513 0590 0591 0592 0593 0594 0595 0596 0598"
        if self.abr == Concursillo.MAE:
            return "0597"

    def _anexos(self) -> Dict[int, Anexo]:
        def _txt(n: Tag):
            n = BeautifulSoup(str(n.find_parent("li")), "html.parser")
            for x in n.findAll(["ul", "ol"]):
                x.extract()
            txt = re_sp.sub(" ", n.get_text()).strip(": ")
            txt = re.sub(r"\s*\(\d+ de \w+ de 20\d+\s*\)\s*$", "", txt)
            txt = re.sub(r"\s*\(anexo [\dI]+\)\s*$", "", txt)
            txt = re.sub(r"^Corrección de errores: Resolución", "Resolución", txt)
            return txt
        done = set(('https://gestiona.comunidad.madrid/gpic_solicitud', ))
        anexos = {}
        resoluciones = {}
        div: Tag = self.home.select_one("#instrucciones,#instrucciones-calendario")
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

        def _add(a: Tag):
            url = a.attrs["href"]
            if self.url == Concursillo.PROFESORES and url.endswith("_mae.pdf"):
                url = url.rsplit("_", 1)[0] + "_sec.pdf"
            if url in done:
                return
            done.add(url)
            num = len(anexos)
            anexos[num] = Anexo(
                num=num,
                txt=_txt(a),
                url=url
            )

        for r, rsl in sorted(resoluciones.items()):
            root: Tag = rsl.find_parent("li")
            for a in root.select("a"):
                _add(a)

        for r, rsl in sorted(resoluciones.items()):
            root: Tag = rsl.find_parent("ul")
            while root.find_parent("ul"):
                root = root.find_parent("ul")
            for a in root.select("li a"):
                _add(a)

        return anexos


if __name__ == "__main__":
    for con in map(Concurso.build, (Concursazo.PROFESORES, )):#(Concursazo.MAESTROS, Concursazo.PROFESORES, Concursillo.MAESTROS, Concursillo.PROFESORES)):
        for a in con.anexos.values():
            print(con.convocatoria, con.abr, a.num, a.url, len(a.centros))
