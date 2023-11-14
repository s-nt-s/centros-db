from .web import Web, DomNotFoundException, buildSoup
from functools import cached_property, cache
from .cache import Cache
from .utm_to_geo import LatLon
import logging
import re
from bs4 import BeautifulSoup
from .bulkrequests import BulkRequestsFileJob
from aiohttp import ClientResponse, ClientSession
from .util import to_set_tuple
from typing import Tuple


WEB = Web()
logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


class ColegioHtmlCache(Cache):
    def parse_file_name(self, *args, slf: "Colegio" = None, **kargv):
        return f"{self.file}/{slf.id}.html"


class BulkRequestsColegio(BulkRequestsFileJob):
    DONE = set()

    def __init__(self, id: int):
        self.id = id
        self.colegio = Colegio(id=id)
        self.html_cache: ColegioHtmlCache = getattr(
            self.colegio._get_soup,
            "__cache_obj__"
        )

    def save(self, soup: BeautifulSoup):
        self.html_cache.save(self.file, soup)

    @property
    def url(self):
        return self.colegio.info

    @cached_property
    def file(self):
        return self.html_cache.parse_file_name(slf=self.colegio)

    async def _get_soup(self, url: str, response: ClientResponse):
        content = await response.text()
        soup = buildSoup(url, content)
        return soup

    async def do(self, session: ClientSession):
        async with session.get(self.url) as response:
            soup = await self._get_soup(self.url, response)
            BulkRequestsColegio.DONE.add(self.id)
            try:
                self.colegio._check_soup(soup)
            except DomNotFoundException:
                return False
            self.save(soup)
            return True

    def done(self) -> bool:
        if self.id in BulkRequestsColegio.DONE:
            return True
        return super().done()


class Colegio:
    def __init__(self, id: int):
        self.id = id

    @staticmethod
    @cache
    def get(id: int):
        c = Colegio(id)
        try:
            c.home
        except DomNotFoundException:
            return None
        return c

    @cached_property
    def info(self):
        return f"http://www.buscocolegio.com/Colegio/detalles-colegio.action?id={self.id}"

    @cached_property
    def home(self):
        return self._get_soup()

    @ColegioHtmlCache(
        file="cache/html/cg/",
        maxOld=5,
        kwself="slf",
        loglevel=logging.DEBUG
    )
    def _get_soup(self):
        soup = WEB.get(self.info)
        self._check_soup(soup)
        return soup

    def _check_soup(self, soup: BeautifulSoup):
        h3 = soup.find("h3", string="Código")
        if h3 is None:
            raise DomNotFoundException("h3[text()='Código']")
        cod = h3.parent.find("strong").string
        if cod != str(self.id):
            raise DomNotFoundException(f"h3[text()='Código']:parent strong[text()={self.id}]")

    @cached_property
    def latlon(self):
        latitude = self.get_itemprop("latitude")
        longitude = self.get_itemprop("longitude")
        if None in (latitude, longitude):
            return None
        return LatLon(
            latitude=latitude,
            longitude=longitude
        )

    def get_itemprop(self, s: str):
        n = self.home.find("meta", attrs={"itemprop": s})
        if n:
            v = n.attrs["content"].strip()
            if len(v):
                return float(v)

    def __get_h3_div(self, rg: str):
        n = self.home.find(
            "h3",
            string=re.compile(rg, re.MULTILINE | re.IGNORECASE)
        )
        if n is None:
            return
        div = n.findParent("div")
        if div is None:
            return
        div = BeautifulSoup(str(div), "html.parser")
        div.find("h3").extract()
        txt = div.get_text()
        txt = re_sp.sub(" ", txt).strip()
        if txt in ("", "-", "0"):
            return None
        return txt

    @cached_property
    def web(self) -> Tuple[str]:
        web = self.__get_h3_div(r"\s*Página\s+Web\s*")
        if web is None:
            return tuple()
        web = re.sub(r",?\s+|\s+[oó]\s+", " ", web).strip()
        arr = []
        for w in web.split():
            w = re.sub(r"^https?://\s*|[/#\?]+$", "", w, flags=re.IGNORECASE)
            if len(w) and w not in arr:
                arr.append(w)
        return tuple(arr)

    @cached_property
    def email(self):
        return to_set_tuple(self.__get_h3_div(r"\s*Email\s*"))

    @cached_property
    def telefono(self) -> Tuple[int]:
        telefono = self.__get_h3_div(r"\s*Teléfono\s*")
        if telefono is None:
            return tuple()
        telefono = telefono.replace(".", "")
        r = []
        for t in re.findall(r"(\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*\d\s*)", telefono):
            t = re_sp.sub("", t)
            if len(t) and t not in r:
                r.append(int(t))
        if len(r) == 0:
            return tuple()
        return tuple(r)


if __name__ == "__main__":
    import sys
    for id in sys.argv[1:]:
        c = Colegio.get(int(id))
        if c:
            print(c.latlon, c.email, c.web, c.telefono)
        else:
            print(id, "=", None)
