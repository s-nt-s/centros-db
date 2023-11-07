from dataclasses import dataclass, asdict
from functools import cached_property
from typing import Tuple, Dict
from .types import LatLon
from .web import Web
from .cache import Cache
from .utm_to_geo import utm_to_geo
from .retry import retry
import re
import logging
from requests.exceptions import ConnectionError
from .bulkrequests import BulkRequestsFileJob

re_sp = re.compile(r"\s+")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_]+\.[\w\-_]+', re.IGNORECASE)
re_coord = re.compile(r"&xIni=([\d\.]+)&yIni=([\d\.]+)")

logger = logging.getLogger(__name__)
WEB = Web()


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


class CentroHtmlCache(Cache):
    def parse_file_name(self, *args, slf: "Centro" = None, **kargv):
        return f"{self.file}/{slf.id}.html"


class CentroException(Exception):
    pass


class DomNotFoundException(BulkRequestsFileJob):
    def __init__(self, selector: str, url: str = None, more_info: str = None):
        msg = f"No se ha encontrado el elemento {selector}"
        if url is not None:
            msg = msg + f' en {url}'
        if more_info:
            msg = msg + f'  ver: {more_info}'
        super().__init__(msg)


class BulkRequestsCentro(BulkRequestsFileJob):
    def __init__(self, id):
        self.centro = Centro(id=id)
        self.html_cache: CentroHtmlCache = getattr(
            self.centro.get_soup, 
            "__cache_obj__"
        )

    @property
    def url(self):
        return self.centro.info

    @property
    def file(self):
        return self.html_cache.parse_file_name(slf=self.centro)


@dataclass(frozen=True)
class Centro:
    id: int
    area: str = None
    tipo: str = None
    nombre: str = None
    domicilio: str = None
    municipio: str = None
    distrito: str = None
    cp: int = None
    telefono: int = None
    fax: int = None
    email: str = None
    titularidad: str = None

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

    def _asdict(self):
        return asdict(self)

    @cached_property
    def info(self):
        return f"https://gestiona.comunidad.madrid/wpad_pub/run/j/MostrarFichaCentro.icm?cdCentro={self.id}"

    @cached_property
    def home(self):
        soup = self.get_soup()
        body = soup.find("body")
        if not body:
            raise DomNotFoundException("body", url=self.info)
        if not body.select_one(":scope *"):
            txt = re_sp.sub(" ", body.get_text()).strip()
            raise DomNotFoundException("body *", url=self.info, more_info=txt)
        return soup

    @CentroHtmlCache(
        file="data/html/",
        maxOld=5,
        kwself="slf",
        loglevel=logging.DEBUG
    )
    @retry(
        times=3,
        sleep=10,
        exceptions=ConnectionError
    )
    def get_soup(self):
        soup = WEB.get(self.info)
        return soup

    @cached_property
    def web(self):
        return self.inputs.get("tlWeb")

    @cached_property
    def inputs(self) -> Dict[str, str]:
        items = self.home.select("div.formularioconTit input")
        if len(items) == 0:
            raise DomNotFoundException("div.formularioconTit input")
        data = {}
        for i in items:
            n = i.attrs.get("name", "").strip()
            v = i.attrs.get("value", "").strip()
            if v == "null" or 0 in (len(n), len(v)):
                continue
            if n not in ("filtroConsultaSer", "salidaCompSerializada", "formularioConsulta"):
                data[n] = v
        return data

    @cached_property
    def latlon(self):
        mapa = self.home.select_one("#Mapa a")
        if mapa is None:
            return None
        href = mapa.attrs["onclick"]
        m = re_coord.search(href)
        UTM_ED50_HUSO_30 = m.group(1) + "," + m.group(2)
        if UTM_ED50_HUSO_30 == "0.0,0.0":
            return None
        utm_split = UTM_ED50_HUSO_30.split(",")
        x, y = tuple(map(float, utm_split))
        lat, lon = utm_to_geo(30, x, y, "ED50")
        return LatLon(
            latitude=lat,
            longitude=lon
        )

    @cached_property
    def titular(self):
        for td in self.home.select("#capaDatIdentContent td"):
            if td.find("td"):
                continue
            txt = re_sp.sub(" ", td.get_text()).strip()
            val = txt.split("Titular:")
            if len(val) > 1:
                return val[-1].strip()
