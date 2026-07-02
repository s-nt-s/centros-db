import re
import time
import requests
from textwrap import dedent
from urllib.parse import urlencode
from core.fetcher import Getter, ClientResponse
from urllib.parse import urlparse, parse_qsl
import logging
from typing import NamedTuple

logger = logging.getLogger(__name__)


class Item(NamedTuple):
    year: int
    serie: str
    value: int | float


def url_to_code(url: str):
    params = dict(parse_qsl(urlparse(url).query))
    code = params.get("c0-e1", None)
    if not isinstance(code, str):
        raise ValueError(f"Invalid code: {code} from url: {url}")
    m = re.match(r"^string:(28\d{6})$", code)
    if m is None:
        raise ValueError(f"Invalid code: {code} from url: {url}")
    return int(m.group(1))


async def rq_to_text(r: ClientResponse):
    text = await r.text()
    items: set[Item] = set()
    for s, y, v in _iter_data(text):
        if s not in ("Total", ):
            items.add(Item(
                year=y,
                serie=s,
                value=v
            ))
    return tuple(sorted(items, key=lambda x: (x.year, x.serie, x.value)))


class DwrGetter(Getter):
    def __init__(self, payload: str):
        super().__init__(
            onread=rq_to_text,
            headers={
                "Content-Type": "text/plain",
                "Origin": "https://gestiona.comunidad.madrid",
                "Referer": "https://gestiona.comunidad.madrid/wpad_pub/",
                "Accept": "*/*",
            }
        )
        self.__payload = dedent(payload)

    def __cod_to_url_get(self, code: int):
        params = {}
        payload = self.__payload.format(
            code=code,
            call_id=f"1_{int(time.time() * 1000)}"
        )
        for line in dedent(payload).strip().splitlines():
            line = line.strip()
            if not line:
                continue
            k, v = line.split("=", 1)
            params[k] = v
        url = "https://gestiona.comunidad.madrid/wpad_pub/dwr/exec/GraficasDWRAccion.obtenerGrafica.dwr"
        url = f"{url}?{urlencode(params)}"
        return url

    def __get(self, *codes: int):
        url_cod: dict[str, int] = {}
        for code in codes:
            url = self.__cod_to_url_get(code)
            url_cod[url] = code
        rtn: dict[int, tuple[Item, ...]] = {}
        data: dict[str, tuple[Item, ...]] = super().get(*url_cod.keys())
        for url, d in data.items():
            if not isinstance(d, tuple) or len(d) == 0:
                continue
            rtn[url_cod[url]] = d
        return rtn

    def get(self, *codes: int):
        data = self.__get(*codes)
        while True:
            ko = set(codes) - set(data.keys())
            if len(ko) == 0:
                break
            time.sleep(3)
            aux = self.__get(*ko)
            if len(aux) == 0:
                break
            data.update(aux)
        return data


def _find_val(text: str, rgx: str):
    arr: list[str] = []
    for s in re.findall(rgx+r"=([^;]+)", text):
        m = re.search(r"\b" + re.escape(s) + r'="([^"]+)"', text)
        if m is None:
            raise ValueError(text)
        val = m.group(1)
        val = val.encode("utf-8").decode("unicode_escape")
        arr.append(val)
    return tuple(arr)


def _iter_series(text: str, rgx: str):
    for s in re.findall(rgx+r"=([^;]+)", text):
        arr: list[int | float] = []
        for ss in re.findall(r"\b" + re.escape(s) + r'\[\d+\]=([^;]+)', text):
            for val in re.findall(r"\b" + re.escape(ss) + r'=([\d\.]+)', text):
                f = float(val)
                i = int(f)
                arr.append(i if i == f else f)
        if arr:
            yield tuple(arr)


def _iter_data(text: str):
    yrs = re.findall(r'"(\d{4})-\d{4}"', text)
    if len(yrs):
        years = tuple(sorted(set(map(int, yrs))))
        l_years = len(years)
        i = 0
        series = _find_val(text, "nombreSerie")
        for vals in _iter_series(text, r"s\d+\.serieY"):
            if len(vals) != l_years:
                raise ValueError()
            for y, v in zip(years, vals):
                if v > 0:
                    yield series[i], y, v
            i = i + 1


class Dwr:
    def __init__(self):
        self.__alumnos = DwrGetter("""
            callCount=1
            c0-scriptName=GraficasDWRAccion
            c0-methodName=obtenerGrafica
            c0-id={call_id}
            c0-e1=string:{code}
            c0-e2=string:TODO
            c0-e3=string:1
            c0-e4=string:1
            c0-param0=Object:{{cdCentro:reference:c0-e1, cdnivelEducativo:reference:c0-e2, cdGrafica:reference:c0-e3, tipoGrafica:reference:c0-e4}}
            xml=true
        """)
        self.__titulacion = DwrGetter("""
            callCount=1
            c0-scriptName=GraficasDWRAccion
            c0-methodName=obtenerGrafica
            c0-id={call_id}
            c0-e1=string:{code}
            c0-e2=string:05
            c0-e3=string:5
            c0-e4=string:1
            c0-e5=string:1
            c0-e6=string:0
            c0-param0=Object:{{cdCentro:reference:c0-e1,cdnivelEducativo:reference:c0-e2,cdGrafica:reference:c0-e3,tipoGrafica:reference:c0-e4,tipoTasaTitul:reference:c0-e5,tipoModalidad:reference:c0-e6}}
            xml=true
        """)

    def get_last_alumnos(self, *codes: int):
        rtn: dict[int, tuple[Item, ...]] = {}
        for code, items in self.__alumnos.get(*codes).items():
            if len(items) == 0:
                continue
            last_year = max(i.year for i in items)
            last_items = tuple(i for i in items if i.year == last_year)
            rtn[code] = last_items
        return rtn

    def get_alumnos(self, *codes: int):
        return self.__alumnos.get(*codes)

    def get_titulacion(self, *codes: int):
        return self.__titulacion.get(*codes)


DWR = Dwr()

if __name__ == "__main__":
    centro = 28079357
    alumnos = DWR.get_titulacion(centro)
    print(f"Centro {centro}: {alumnos}")
