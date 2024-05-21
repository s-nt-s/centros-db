from dataclasses import dataclass, asdict, field, replace
from functools import cached_property, cache
from typing import Dict, Tuple, NamedTuple, List, Union
from aiohttp import ClientResponse, ClientSession
from bs4 import BeautifulSoup, Tag
from urllib import parse
import math
from .web import Web, buildSoup, select_attr, DomNotFoundException
from .cache import Cache
from .utm_to_geo import utm_to_geo, LatLon
from .retry import retry
import re
import logging
from requests.exceptions import ConnectionError
from .bulkrequests import BulkRequestsFileJob
from itertools import zip_longest
from .util import fix_char

re_sp = re.compile(r"\s+")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_]+\.[\w\-_]+', re.IGNORECASE)
re_coord = re.compile(r"&xIni=([\d\.]+)&yIni=([\d\.]+)")

logger = logging.getLogger(__name__)

WEB = Web()
SEP = " -> "


def _parse(k: str, v: str):
    if v is None:
        return None
    v = re_sp.sub(" ", v).strip()
    lw = v.lower()
    if lw.lower() in ("", "0", "http://", "https://", "http://no") or re.match(r"^-+$", lw):
        return None
    if k == 'FAX' and lw in ("sinfax", "nohayfax", "no", "x"):
        return None
    if k in ('centro_codigo', 'centro_tipo_codigo', 'dat_codigo', 'direccion_codigo_postal', 'distrito_codigo', 'direccion_coor_x', 'direccion_coor_y', 'CODIGO CENTRO', 'COD. POSTAL'):
        return int(v)
    if isinstance(v, str):
        v = fix_char(v)
    return v


def _find_mails(arr):
    mails = []
    for v in arr:
        for m in re_mail.findall(v):
            if m not in mails:
                mails.append(m)
    return tuple(mails)


def _get_telefono(s: str) -> Tuple[int]:
    if s is None:
        return tuple()
    s = s.replace(".", "")
    s = re.sub(r"^\s*(00|\+)34\s*", "", s)
    s = re_sp.sub(" ", s).strip()
    arr = []
    for t in s.split():
        if len(t) > 8 and t not in arr and t.isdigit():
            arr.append(int(t))
    return tuple(arr)


def _get_web(web: str) -> Tuple[str]:
    if web is None:
        return tuple()
    web = re.sub(r",?\s+|\s+[oó]\s+", " ", web).strip()
    arr = []
    for w in web.split():
        w = re.sub(r"^https?://\s*|[/#\?]+$", "", w, flags=re.IGNORECASE)
        w = w.lower()
        if len(w) and w not in arr:
            arr.append(w)
    return tuple(arr)


def _parse_titularidad(t: str):
    if t in ('Privado Concertado', 'Concertado', 'Concertada'):
        return 'CON'
    if t in ('Privado', 'Privada'):
        return 'PRI'
    if t in ('Público', 'Pública'):
        return "PUB"
    return None


def _find_titularidad(arr):
    tit = set()
    for a in arr:
        a = _parse_titularidad(a)
        if a is not None:
            tit.add(a)
    if len(tit) != 1:
        return None
    return tit.pop()


def _parse_query(url: str):
    spl = parse.urlsplit(url)
    return dict(parse.parse_qsl(spl.query))


def get_etapa_level(td: Tag):
    cls = td.attrs["class"]
    if isinstance(cls, str):
        cls = cls.split()
    for cl in cls:
        if cl.startswith("p"):
            cl = cl[1:]
            if cl.isdigit():
                return int(cl)
    return 0


class CentroHtmlCache(Cache):
    def parse_file_name(self, *args, slf: "Centro" = None, **kargv):
        return f"{self.file}/{slf.id}.html"


class CentroException(Exception):
    pass


class BadMapException(CentroException):
    def __init__(self, id: int, *urls: str):
        msg = f"{id}: Mapa incorrecto"
        if urls:
            msg = msg + ": " + SEP.join(urls)
        super().__init__(msg)


@dataclass
class CountSoupCentro:
    sc: "SoupCentro" = None
    ok_full: int = 0
    ok_basic: int = 0
    ok_similar: int = 0

    @property
    def oderkey(self):
        return (self.ok_full, self.ok_basic, self.ok_similar)

    def __lt__(self, other: "CountSoupCentro"):
        return self.oderkey < other.oderkey

    def __eq__(self, other: "CountSoupCentro"):
        return self.oderkey == other.oderkey


class BulkRequestsCentro(BulkRequestsFileJob):
    DIR_MAP = {}

    def __init__(self, id: int):
        self.id = id
        self.centro = Centro(id=id)
        self.html_cache: CentroHtmlCache = getattr(
            self.centro._get_soup,
            "__cache_obj__"
        )
        self.okkomap: Dict[str, bool] = {}
        self.oksoup: Dict[Tuple, CountSoupCentro] = dict()

    def add_ok_before_map(self, spct: "SoupCentro"):
        if spct.as_tuple not in self.oksoup:
            self.oksoup[spct.as_tuple] = CountSoupCentro(sc=spct)
        counter = self.oksoup[spct.as_tuple]
        counter.ok_basic = counter.ok_basic + 1
        counter.ok_similar = 0
        for c in self.oksoup.values():
            if c.sc.similar(spct):
                counter.ok_similar = counter.ok_similar + 1

    def add_ok_after_map(self, spct: "SoupCentro"):
        counter = self.oksoup[spct.as_tuple]
        counter.ok_full = counter.ok_full + 1

    def get_best(self, silent=False) -> "SoupCentro":
        cntbst = self._get_best()
        if cntbst is None:
            return None
        if cntbst.ok_full > 1:
            return cntbst.sc
        if cntbst.ok_basic > 2:
            return cntbst.sc
        if cntbst.ok_similar > 3:
            return cntbst.sc
        if self.countdown == 0:
            if not silent:
                logger.warning(
                    f"{self.id} sopa elegida por última oportunidad ({self.step+1} intentos)"
                )
            return cntbst.sc
        if len(self.oksoup) > 4:
            if not silent:
                logger.warning(
                    f"{self.id} sopa elegida por cansancio ({self.step+1} intentos)"
                )
            return cntbst.sc
        return None

    def _get_best(self) -> CountSoupCentro:
        if len(self.oksoup) == 0:
            return None
        cntbst = list(sorted(self.oksoup.values()))[-1]
        return cntbst

    def save(self, spct: "SoupCentro"):
        self.html_cache.save(self.file, spct.soup)

    @property
    def url(self):
        return self.centro.info

    @cached_property
    def file(self):
        return self.html_cache.parse_file_name(slf=self.centro)

    async def _get_soup(self, url: str, response: ClientResponse):
        content = await response.text()
        return buildSoup(url, content)

    async def do(self, session: ClientSession):
        if self.countdown == 0 or self.get_best(silent=True) is not None:
            return await self.last_do(session)
        return await self.main_do(session)

    async def main_do(self, session: ClientSession):
        async with session.get(self.url) as response:
            spct = SoupCentro(
                self.centro.id,
                await self._get_soup(self.url, response)
            )
            try:
                spct.check_soup(lazy=True)
            except (DomNotFoundException, CentroException):
                return False
            self.add_ok_before_map(spct)
            if not await self.do_map(session, spct):
                return False
            self.add_ok_after_map(spct)
            spct = self.get_best()
            if spct is None:
                return False
            self.save(spct)
            return True

    async def last_do(self, session: ClientSession):
        async with session.get(self.url) as response:
            spct = SoupCentro(
                self.centro.id,
                await self._get_soup(self.url, response)
            )
            exc = None
            try:
                spct.check_soup(lazy=True)
                self.add_ok_before_map(spct)
                if not await self.do_map(session, spct):
                    urlmap = spct.get_url_info_map()
                    raise BadMapException(
                        self.centro.id,
                        urlmap.get_breadcrumbs()
                    )
                self.add_ok_after_map(spct)
            except (DomNotFoundException, CentroException, BadMapException) as e:
                exc = e
            spct = self.get_best()
            if spct is None:
                if exc is not None:
                    logger.warning(str(exc))
                return False
            self.save(spct)
            return True

    async def do_map(self, session: ClientSession, spct: "SoupCentro"):
        direcc = spct.find_direccion()
        urlmap = spct.get_url_info_map()
        if urlmap is None:
            return True
        if len(urlmap.urls) == 0:
            return True
        if direcc and urlmap.popup == BulkRequestsCentro.DIR_MAP.get(direcc):
            return True
        if urlmap.url in self.okkomap:
            return self.okkomap[urlmap.url]
        for url in urlmap.urls:
            if await self._do_map(session, url, spct):
                if direcc:
                    BulkRequestsCentro.DIR_MAP[direcc] = urlmap.popup
                self.okkomap[urlmap.url] = True
                return True
        self.okkomap[urlmap.url] = False
        return False

    async def _do_map(self, session: ClientSession, url: str, spct: "SoupCentro"):
        async with session.get(url) as maprsponse:
            mpsp = await self._get_soup(url, maprsponse)
            return spct._check_info_map(mpsp)


class Etapa(NamedTuple):
    nombre: str
    titularidad: str
    tipo: str
    plazas: str
    nivel: int

    def merge(self, **kwargs):
        return Etapa(**{
            **self._asdict(),
            **kwargs
        })

    def notnull(self):
        return self.merge(
            nombre=(self.nombre or ''),
            titularidad=(self.titularidad or ''),
            tipo=(self.tipo or ''),
            plazas=(self.plazas or ''),
            nivel=(self.nivel or -1),
        )


@dataclass(frozen=True)
class UrlMap:
    id: int
    thumbnail: str
    popup: str

    @cached_property
    def thumbnail_data(self):
        return _parse_query(self.thumbnail)

    @cached_property
    def width(self):
        return int(self.thumbnail_data['WIDTH'])

    @cached_property
    def height(self):
        return int(self.thumbnail_data['HEIGHT'])

    @cached_property
    def url(self):
        return self.get_url(self.width/2, self.height/2)

    @cached_property
    def urls(self):
        arr = []
        points = set()
        for x in range(1, self.width, 15):
            for y in range(1, self.height, 15):
                points.add((x, y))
        cnt = (int(self.width/2), int(self.height/2))
        points = sorted(points, key=lambda xy: abs(math.dist(xy, cnt)))
        for x, y in points:
            arr.append(self.get_url(x, y))
        return tuple(arr)

    def get_url(self, x: int, y: int):
        data = {
            'VIEWPARAMS': '',
            'SERVICE': '',
            'WMS&VERSION': '1.1.1',
            'REQUEST': 'GetFeatureInfo',
            'LAYERS': 'WPAD_V_CENTROS_GIS',
            'QUERY_LAYERS': 'WPAD_V_CENTROS_GIS',
            'FEATURE_COUNT': '30',
            'FORMAT': 'image/png',
            'INFO_FORMAT': 'text/html',
            'SRS': 'EPSG:23030',
            'CQL_FILTER': f'INCLUDE;CD_CENTRO IN ({self.id})',
            'X': str(int(x)),
            'Y': str(int(y))
        }
        for k in ('BBOX', 'HEIGHT', 'WIDTH'):
            data[k] = self.thumbnail_data[k]
        url = 'https://idem.madrid.org/geoserver/wms'
        url = url + '?' + parse.urlencode(data, doseq=False)
        return url

    def get_breadcrumbs(self):
        return SEP.join([
            str(self.id), self.thumbnail, self.popup, self.url
        ])


class SoupCentro:
    def __init__(self, id: int, soup: BeautifulSoup):
        self.id = id
        self.soup = soup

    def __hash__(self):
        return hash(self.as_tuple)

    def __lt__(self, other: "SoupCentro"):
        return self.as_tuple < other.as_tuple

    def __eq__(self, other: "SoupCentro"):
        return self.as_tuple == other.as_tuple

    def similar(self, other: "SoupCentro"):
        def _tp(o:  "SoupCentro"):
            arr = []
            for e in o.as_tuple:
                if not isinstance(e, LatLon):
                    arr.append(e)
            return tuple(arr)
        return _tp(self) == _tp(other)

    @cached_property
    def as_tuple(self):
        return (
            self.web,
            self.latlon or LatLon(latitude=0, longitude=0),
            self.titular or '',
            self.etapas,
            self.educacion_diferenciada
        )

    @cached_property
    def extraescolares(self):
        return self.__get_div_td('capaInstitContent')

    @cached_property
    def planes(self):
        return self.__get_div_td('capaPlanesEstudioContent')

    @cached_property
    def proyectos(self):
        return self.__get_div_td('capaProyPropiosContent')

    def __get_div_td(self, id) -> Tuple[str]:
        arr = []
        for td in self.soup.select(f"#{id} td"):
            txt = re_sp.sub(" ", td.get_text()).strip()
            txt = fix_char(txt)
            if len(txt) and txt not in arr:
                arr.append(txt)
        return tuple(arr)

    @cached_property
    def web(self) -> Tuple[str]:
        web = self.inputs.get("tlWeb")
        if web is None:
            return tuple()
        web = re.sub(r",?\s+|\s+[oó]\s+", " ", web).strip()
        web = fix_char(web)
        return _get_web(web)

    @cached_property
    def email(self) -> Tuple[str]:
        mails = re_mail.findall(
            (self.inputs.get("tlMail") or '')
        )
        for txt in self.__iter_strong_text():
            for m in re_mail.findall(txt):
                if m not in mails:
                    mails.append(m)
        return tuple(mails)

    @cached_property
    def telefono(self) -> Tuple[str]:
        fax = _get_telefono(self.inputs.get("tlFax"))
        tlfs = list(_get_telefono(self.inputs.get("tlTelefono")))
        for txt in self.__iter_strong_text():
            for m in _get_telefono(txt):
                if m not in tlfs and m not in fax:
                    tlfs.append(m)
        return tuple(tlfs)

    def __iter_strong_text(self) -> str:
        for strong in self.soup.select("#capaDatIdentContent strong"):
            if strong.find(["strong", "td", "span"]):
                continue
            for txt in strong.findAll(text=True):
                txt = txt.get_text()
                txt = fix_char(txt)
                yield txt

    @cached_property
    def inputs(self) -> Dict[str, str]:
        selector = 'div.formularioconTit input[type="hidden"]'
        items = self.soup.select(selector)
        if len(items) == 0:
            raise DomNotFoundException(selector)
        data = {}
        for i in items:
            n = i.attrs.get("name", "").strip()
            v = i.attrs.get("value", "").strip()
            if n in ("filtroConsultaSer", "salidaCompSerializada", "formularioConsulta"):
                continue
            if v == "null" or 0 in (len(n), len(v)):
                continue
            if n == "tlWeb" and "." not in v:
                continue
            data[n] = fix_char(v)
        return data

    @cached_property
    def latlon(self) -> LatLon:
        if self.utm_ed50_huso_30_x_y is None:
            return None
        latlon = utm_to_geo("ED50", 30, *self.utm_ed50_huso_30_x_y)
        return latlon.round(7)

    @cached_property
    def utm_ed50_huso_30_x_y(self):
        href = select_attr(self.soup, "#btLupa", "onclick", safe=True)
        if href is None:
            return None
        m = re_coord.search(href)
        xy = tuple(map(float, m.groups()))
        if xy == (0, 0):
            return None
        return xy

    @cached_property
    def titular(self):
        for td in self.soup.select("#capaDatIdentContent td"):
            if td.find("td"):
                continue
            txt = re_sp.sub(" ", td.get_text()).strip()
            val = txt.split("Titular:")
            if len(val) < 2:
                continue
            val = val[-1].strip()
            if val.strip() in ('', 'null'):
                return None
            val = fix_char(val)
            val = {
                'COMUNDAD DE MADRID': 'COMUNIDAD DE MADRID'
            }.get(val, val)
            return val

    @cached_property
    def etapas(self):
        def get_text(n: Tag):
            txt = re_sp.sub(" ", n.get_text()).strip()
            return fix_char(txt) if len(txt) else None

        def find_padre(etapas: List[Etapa], nivel: int):
            for e in reversed(etapas):
                if e.nivel < nivel:
                    return e

        def get_tipo(txt: str):
            if txt is None:
                return None
            txt = txt.strip(" /,")
            if len(txt) == 0:
                return None
            arr = []
            for s in txt.split("/"):
                s = s.strip(" ,")
                if len(s) == 0:
                    continue
                arr.append(", ".join(sorted(s.split(", "))))
            if len(arr) == 0:
                return None
            return " / ".join(arr)

        etapas: List[Etapa] = []
        for tr in self.soup.select("#capaEtapasContent tr"):
            if len(re_sp.sub("", tr.get_text())) == 0:
                continue
            tds = tr.findAll("td")
            txt = tuple(map(get_text, tds))
            if txt[0] in (None, "", "Etapa"):
                continue
            etapa = Etapa(
                nombre=txt[0],
                titularidad=txt[1],
                tipo=get_tipo(txt[2]),
                plazas=txt[3],
                nivel=get_etapa_level(tds[0])
            )
            padre = find_padre(etapas, etapa.nivel)
            if padre is None:
                etapas.append(etapa)
                continue
            etapas.append(etapa.merge(
                nombre=padre.nombre+SEP+etapa.nombre
            ))
        return tuple(sorted(set(etapas), key=lambda e: e.notnull()))

    @cached_property
    def educacion_diferenciada(self) -> Tuple[str]:
        txt = (select_attr(
            self.soup,
            'input[name="tlEdDiferenciada"]',
            "value",
            safe=True
        ) or '')
        re_sp.sub(" ", txt).strip()
        if txt.lower() in ('', 'null'):
            return tuple()
        txt = fix_char(txt)
        arr = set()
        for t in txt.split(", "):
            t = t.strip()
            t = re.sub(r"\s*\(", " (", t).strip()
            if len(t):
                arr.add(t)
        return tuple(sorted(arr))

    def check_soup(self, lazy=False):
        info = Centro(self.id).info
        body = self.soup.find("body")
        if not body:
            raise DomNotFoundException("body", url=info)
        if not body.select_one(":scope *"):
            txt = re_sp.sub(" ", body.get_text()).strip()
            raise DomNotFoundException("body *", url=info, more_info=txt)
        cdCentro = select_attr(self.soup, "#cdCentro", "value")
        if cdCentro != str(self.id):
            raise CentroException(f"cdCentro={cdCentro} en {info}")
        mapa = select_attr(self.soup, "#Mapa img", "src", safe=True)
        if None in (mapa, self.utm_ed50_huso_30_x_y):
            return
        if f'CD_CENTRO%20IN%20%28{self.id}%29' not in mapa:
            raise BadMapException(
                self.id, mapa
            )
        if not lazy and not self.find_and_check_info_map():
            raise BadMapException(
                self.id, mapa
            )

    def find_direccion(self):
        for td in self.soup.select("#capaDatIdentContent td"):
            if td.find("td"):
                continue
            txt = re_sp.sub(" ", td.get_text()).strip()
            txt = fix_char(txt)
            if txt.startswith("Dirección: "):
                txt = txt.split(": ", 1)[-1].strip()
                if len(txt) > 10:
                    return txt
        return None

    def find_and_check_info_map(self):
        urlmap = self.get_url_info_map()
        if urlmap is None:
            return True
        if len(urlmap.urls) == 0:
            return True
        for url in urlmap.urls:
            if self._check_info_map(WEB.get(url)):
                return True
        return False

    def _check_info_map(self, map: BeautifulSoup):
        for label in map.select("label"):
            if label.get_text().strip() == str(self.id):
                return True
        return False

    def get_url_info_map(self) -> UrlMap:
        mapa = select_attr(self.soup, "#Mapa img", "src", safe=True)
        if None in (mapa, self.utm_ed50_huso_30_x_y):
            return
        popup = select_attr(self.soup, "#btLupa", "onclick").split("'")[1]
        qr_mp = _parse_query(mapa)
        data = {
            'VIEWPARAMS': '',
            'SERVICE': '',
            'WMS&VERSION': '1.1.1',
            'REQUEST': 'GetFeatureInfo',
            'LAYERS': 'WPAD_V_CENTROS_GIS',
            'QUERY_LAYERS': 'WPAD_V_CENTROS_GIS',
            'FEATURE_COUNT': '30',
            'FORMAT': 'image/png',
            'INFO_FORMAT': 'text/html',
            'SRS': 'EPSG:23030',
            'CQL_FILTER': f'INCLUDE;CD_CENTRO IN ({self.id})',
        }
        for k in ('BBOX', 'HEIGHT', 'WIDTH'):
            data[k] = qr_mp[k]
        height = int(qr_mp['HEIGHT'])
        width = int(qr_mp['WIDTH'])
        data['Y'] = int(height/2)
        data['X'] = int(width/2)
        url = 'https://idem.madrid.org/geoserver/wms'
        url = url + '?' + parse.urlencode(data, doseq=False)
        return UrlMap(
            id=self.id,
            thumbnail=mapa,
            popup=popup,
        )


class OpenDataCentro(NamedTuple):
    centro_codigo: int
    centro_nombre: str
    centro_tipo_codigo: int
    centro_tipo_desc_abreviada: str
    centro_tipo_descripcion: str
    centro_titularidad: str
    centro_titular: str
    dat_codigo: int
    dat_nombre: str
    direccion_via_tipo: str
    direccion_via_nombre: str
    direccion_numero: str
    direccion_codigo_postal: int
    municipio_codigo: str
    municipio_nombre: str
    distrito_codigo: int
    distrito_nombre: str
    contacto_telefono1: str
    contacto_telefono2: str
    contacto_telefono3: str
    contacto_telefono4: str
    contacto_fax: str
    contacto_web: str
    contacto_email1: str
    direccion_coor_x: int
    direccion_coor_y: int

    @staticmethod
    def build(obj: Dict):
        if obj is None:
            return None
        return OpenDataCentro(**{k: _parse(k, v) for k, v in obj.items()})

    @cache
    def get_latlon(self) -> LatLon:
        if None in (self.direccion_coor_x, self.direccion_coor_y):
            return None
        latlon = utm_to_geo("ED50", 30, self.direccion_coor_x, self.direccion_coor_y)
        return latlon.round(7)


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
    telefono: Tuple[int] = None
    email: Tuple[str] = tuple()
    titularidad: str = None
    fax: Tuple[int] = tuple()
    _latlon: LatLon = field(repr=False, init=False, default=None)
    _webs: Tuple[str] = field(repr=False, init=False, default=tuple())

    @classmethod
    def build(cls, head: Tuple, row: Tuple):
        obj = {h: _parse(h, c) for h, c in zip_longest(head, row)}
        mails = _find_mails(row[head.index("EMAIL"):])
        titularidad = _find_titularidad(row[head.index("EMAIL2")+1:])
        return cls(
            area=obj['AREA TERRITORIAL'],
            id=obj['CODIGO CENTRO'],
            tipo=obj['TIPO DE CENTRO'],
            nombre=obj['CENTRO'],
            domicilio=obj['DOMICILIO'],
            municipio=obj['MUNICIPIO'],
            distrito=obj['DISTRITO MUNICIPAL'],
            cp=obj['COD. POSTAL'],
            telefono=_get_telefono(obj['TELEFONO']),
            email=mails,
            titularidad=titularidad,
            fax=_get_telefono(obj['FAX']),
        )

    def merge(self, o: Union[OpenDataCentro, Dict]):
        if o is None:
            return self
        if isinstance(o, dict):
            o = OpenDataCentro.build(o)
        telefono = list(self.telefono)
        email = list(self.email)
        fax = list(self.fax)
        for txt in (o.contacto_email1 or "", o.contacto_web or ""):
            for m in re_mail.findall(txt):
                if m not in email:
                    email.append(m)
        for t in _get_telefono(o.contacto_fax):
            if t not in fax:
                fax.append(t)
        for tlf in (o.contacto_telefono1, o.contacto_telefono2, o.contacto_telefono3, o.contacto_telefono4):
            for t in _get_telefono(tlf):
                if t not in telefono and t not in fax:
                    telefono.append(t)

        c = replace(self, telefono=tuple(telefono), email=tuple(email), fax=tuple(fax))
        object.__setattr__(c, '_latlon', o.get_latlon())
        object.__setattr__(c, '_web', _get_web(o.contacto_web))
        return c

    def _asdict(self):
        return asdict(self)

    def fix(self):
        self.fix_mail()
        self.fix_telefono()

    def fix_mail(self):
        is_mail = list(self.email)
        for w in (self.home.web + self._webs):
            w = re.sub(r"^www\.?", "", w)
            if "@" in w and w not in is_mail:
                is_mail.append(w)
        for m in self.home.email:
            if m not in is_mail:
                is_mail.append(m)
        is_mail = tuple(is_mail)
        if is_mail != self.email:
            object.__setattr__(self, 'email', is_mail)

    def fix_telefono(self):
        is_telf = list(self.telefono)
        for t in self.home.telefono:
            if t not in is_telf:
                is_telf.append(t)
        is_telf = tuple(is_telf)
        if is_telf != self.telefono:
            object.__setattr__(self, 'telefono', is_telf)

    @cached_property
    def info(self):
        return f"https://gestiona.comunidad.madrid/wpad_pub/run/j/MostrarFichaCentro.icm?cdCentro={self.id}"

    @cached_property
    def home(self):
        return SoupCentro(self.id, self._get_soup())

    @CentroHtmlCache(
        file="cache/html/",
        maxOld=5,
        kwself="slf",
        loglevel=logging.DEBUG
    )
    @retry(
        times=3,
        sleep=10,
        exceptions=(ConnectionError, DomNotFoundException, CentroException)
    )
    def _get_soup(self):
        soup = WEB.get(self.info)
        SoupCentro(self.id, soup).check_soup(lazy=True)
        return soup

    @property
    def web(self):
        webs = list(
            w for w in self.home.web if "@" not in w
        )
        for w in self._webs:
            if w not in webs and "@" not in w:
                webs.append(w)
        return tuple(webs)

    @property
    def latlon(self):
        if self.home.latlon is not None:
            return self.home.latlon
        return self._latlon

    @cached_property
    def titular(self):
        return self.home.titular

    @cached_property
    def etapas(self) -> Tuple[Etapa]:
        def get_tit(cnt: Centro, et: Etapa):
            t = _parse_titularidad(et.titularidad)
            if t is not None:
                return t
            if cnt.titularidad == 'PUB':
                return 'PUB'
            return 'OTR'
        etps: List[Etapa] = []
        for e in self.home.etapas:
            etps.append(e.merge(
                titularidad=get_tit(self, e)
            ))
        etps = list(
            sorted(set(etps), key=lambda x: (-x.count(SEP), -len(x), x.notnull()))
        )
        for i, e in enumerate(etps):
            if e.titularidad != 'OTR':
                continue
            tit = set()
            for x in etps:
                if x.nombre.startswith(e.nombre+SEP):
                    tit.add(x.titularidad)
            if len(tit) == 1:
                etps[i] = e.merge(titularidad=tit.pop())
        for i, e in enumerate(etps):
            if e.tipo is not None:
                continue
            tip = set()
            for x in etps:
                if x.tipo is not None and (x.nombre, x.titularidad) == (e.nombre, e.titularidad):
                    tip.add(x.tipo)
            if len(tip) == 1:
                etps[i] = e.merge(tipo=tip.pop())
        return tuple(sorted(set(etps), key=lambda x: x.notnull()))

    @cached_property
    def educacion_diferenciada(self):
        return self.home.educacion_diferenciada

    @cached_property
    def extraescolares(self):
        return self.home.extraescolares

    @cached_property
    def planes(self):
        return self.home.planes

    @cached_property
    def proyectos(self):
        return self.home.proyectos

    def isBad(self):
        if self.titularidad is None:
            soup = WEB.get(self.info)
            body = re_sp.sub(" ", soup.find("body").get_text()).strip()
            if body == "Pagina de Error: null":
                return True


if __name__ == "__main__":
    import sys
    id = int(sys.argv[1])
    c = Centro(id)
    soup = c.home
    urlmap = c._get_url_info_map(soup)
    print(urlmap.get_breadcrumbs())
    c._check_info_map(WEB.get(urlmap.url))
