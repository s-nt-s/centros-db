import re
from functools import cache, cached_property
from urllib.parse import urljoin
from typing import Any, Coroutine, Tuple, Dict, List
from aiohttp import ClientSession
import requests
from bs4 import BeautifulSoup, Tag
from os.path import dirname, isfile
from glob import glob
import os
import logging
from requests.exceptions import ConnectionError
from itertools import zip_longest

from .web import Web, Driver, buildSoup, select_attr, DomNotFoundException
from .types import ParamValueText, QueryResponse
from .centro import Centro
from .cache import Cache
from .retry import retry
from .bulkrequests import BulkRequestsFileJob
from .util import hashme, fix_char


logger = logging.getLogger(__name__)

re_location = re.compile(r"document.location.href=\s*[\"'](.*.csv)[\"']")
re_csv_br = re.compile(r"\s*\n\s*")
re_csv_fl = re.compile(r"\s*;\s*")
re_sp = re.compile(r"\s+")

JS_TIMEOUT = int(os.environ.get('JS_TIMEOUT', '600'))
WEB = Web()


def trim_null(s: str, is_null=tuple()):
    if s is None:
        return None
    s = s.strip()
    if s in is_null:
        return None
    return s


def data_to_str(*args, **data):
    if len(data) == 0:
        return "ALL"
    query = "&".join(f'{k}={v}' for k, v in data.items() if k[0] != '_')
    return query


class ApiException(Exception):
    pass


class SearchException(Exception):
    pass


class DwnCsvException(ApiException):
    pass


class BadFormException(SearchException):
    def __init__(self, ask_name, ask_val, get_val):
        msg = f"Se pidio {ask_name}={ask_val} pero se obtuvo {get_val}"
        super().__init__(msg)


class NoCsvUrlDownloadException(SearchException):
    def __init__(self):
        msg = "No se ha encontrado la url para descargar el csv"
        super().__init__(msg)


class BadCsvException(DwnCsvException):
    def __init__(self):
        msg = "No se han devuelto los mismos centros que se solicitaron"
        super().__init__(msg)


class CsvCache(Cache):
    def __init__(self, *args, ext="csv", **kwargs):
        super().__init__(*args, **kwargs)
        self.ext = ext

    def parse_file_name(self, *args, **kargv):
        root = self.file.rstrip("/")
        if len(args) > 0:
            name = ";".join(map(str, args))
            name = hashme(name)
            return f"{root}/{name}.{self.ext}"
        if len(kargv) == 0:
            return f"{root}/all.{self.ext}"
        arr = [root]
        for k, v in kargv.items():
            arr.append(f'{k}={v}')
        return "/".join(arr) + f".{self.ext}"


class IdCache(CsvCache):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, ext="txt", **kwargs)

    def read(self, *args, **kargs):
        content: str = super().read(*args, **kargs)
        ids = map(int, set(content.strip().split()))
        return tuple(sorted(ids))

    def save(self, file, data, *args, **kargs):
        data = "\n".join(map(str, sorted(set(data))))
        super().save(file, data.strip(), *args, **kargs)


def csvstr_to_rows(content: str) -> Tuple[Tuple[str]]:
    rows = []
    for row in re_csv_br.split(content.strip()):
        row = row.rstrip(" ;,-")
        if len(row) > 0:
            rows.append(tuple(re_csv_fl.split(row)))
    return tuple(rows)


def csvstr_to_dict(content: str) -> Tuple[Dict[str, Any]]:
    rows = []
    arr = csvstr_to_rows(content)
    head = arr[0]
    for item in arr[1:]:
        c = {}
        for h, i in zip_longest(head, item):
            if isinstance(i, str) and len(i) == 0:
                i = None
            c[h] = i
        rows.append(c)
    return rows


class BulkRequestsApi(BulkRequestsFileJob):
    def __init__(self, api: "Api", data: Dict[str, str]):
        self.data = data
        self.api = api
        self.id_cache: IdCache = getattr(self.api.search_ids, "__cache_obj__")
        self.ids: Dict[Tuple[int], int] = dict()

    def get_best(self) -> Tuple[int]:
        if len(self.ids) == 0:
            return None
        ids, count = list(sorted(
            self.ids.items(),
            key=lambda kv: (kv[1], len(kv[0]), kv[0])
        ))[-1]
        if count > 2:
            return ids
        if len(ids) and count > 1:
            return ids
        if self.countdown == 0:
            logger.warning(
                f"{data_to_str(**self.data)} elegido por última oportunidad ({self.step+1} intentos)"
            )
            return ids
        return None

    @property
    def url(self):
        return Api.URL

    @property
    def file(self):
        return self.id_cache.parse_file_name(**self.data)

    def done(self) -> bool:
        if self.__is_bad_cdGenerico():
            return False
        return isfile(self.file)

    def __is_bad_cdGenerico(self, ids: Tuple[int] = None):
        if tuple(self.data.keys()) != ('cdGenerico', ):
            return False
        if ids is None and isfile(self.file):
            ids = self.id_cache.read(self.file)
        if ids is None or len(ids) == 0:
            return False
        set_ids = set(ids)
        bad_files = set()
        for f in tuple(glob(self.id_cache.parse_file_name(cdGenerico='*'))):
            if f == self.file:
                continue
            ko = tuple(sorted(set_ids.intersection(self.id_cache.read(f))))
            if len(ko) > 0:
                logger.error(f"Conflicto entre {self.file} y {f}: {ko}")
                bad_files.add(f)
        if bad_files:
            for f in filter(isfile, bad_files.union((self.file,))):
                os.remove(f)
            return True
        return False

    async def do(self, session: ClientSession) -> Coroutine[Any, Any, bool]:
        async with session.post(self.url, data=self.data) as response:
            content = await response.text()
            soup = buildSoup(self.url, content)
            try:
                r = self.api._get_search_response(self.data, soup)
            except (ApiException, DomNotFoundException) as e:
                logger.warn(str(e))
                return False
            ids = r.get_ids()
            if self.__is_bad_cdGenerico(ids):
                return False
            self.ids[ids] = self.ids.get(ids, 0) + 1
            ids = self.get_best()
            if ids is None:
                return False
            self.id_cache.save(self.file, ids)
            return True


class Api():
    URL = "https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm"
    DWN = "https://gestiona.comunidad.madrid/wpad_pub/run/j/ConsultaGeneralNPCentrosCSV.icm"
    FORM = "formBusquedaAvanzada"

    def __init__(self):
        self.__centros = {}

    @staticmethod
    def is_redundant_parameter(name: str):
        if name in (
            'cdGenerico',
            'comboMunicipios',
            'comboDistritos',
            'cdTramoEdu',
            'titularidadPublica',
            'titularidadPrivada',
            'titularidadPrivadaConc'
        ):
            return True
        return name.startswith("checkSubdir")

    def get_centros(self, *ids: Tuple[int]):
        ids = list(ids)
        centros: List[Centro] = []
        for i, id in reversed(list(enumerate(ids))):
            if id in self.__centros:
                centros.append(self.__centros[id])
                del ids[i]
        if len(ids) > 0:
            content = self.get_csv_as_str(*ids)
            for c in self.__parse_csv(content):
                self.__centros[c.id] = c
                centros.append(c)
        centros = sorted(centros, key=lambda x: x.id)
        return tuple(centros)

    def search_centros(self, **kargv):
        ids = self.search_ids(**kargv)
        return self.get_centros(*ids)

    @Cache("cache/csv/opendata.csv")
    def get_opendata_csv_as_str(self):
        r = requests.get("https://datos.comunidad.madrid/catalogo/dataset/c750856d-3166-4dac-8e80-d1b824c968b5/resource/28d60557-1d73-4281-ab08-6cfd3b2f5f83/download/centros_educativos.csv")
        return r.text

    @cache
    def get_opendata(self):
        obj = {}
        for i in csvstr_to_dict(self.get_opendata_csv_as_str()):
            obj[int(i['centro_codigo'])] = i
        return obj

    @IdCache("cache/ids/", maxOld=5)
    def search_ids(self, **data) -> Tuple[int]:
        r = self.__do_search(**data)
        return r.get_ids()

    @CsvCache("cache/csv/", maxOld=5)
    def search_csv_as_str(self, **data):
        r = self.__do_search(**data)
        if len(r.get_ids()) == 0:
            return ""
        content = self.__get_csv_as_str(
            *r.get_ids(),
            endpoint=r.frmExportarResultado
        )
        return content

    @retry(
        times=3,
        sleep=10,
        exceptions=(SearchException, ConnectionError, DomNotFoundException),
        prefix=data_to_str
    )
    def __do_search(self, **data):
        soup = WEB.get(Api.URL, **data)
        return self._get_search_response(data, soup)

    def _get_search_response(self, data: dict, soup: BeautifulSoup):
        self._check_inputs(data, soup)
        codCentrosExp = select_attr(
            soup,
            'input[name="codCentrosExp"]',
            "value"
        )
        frmExportarResultado = select_attr(
            soup,
            '#frmExportarResultado',
            "action"
        )
        r = QueryResponse(
            codCentrosExp=codCentrosExp,
            frmExportarResultado=frmExportarResultado
        )
        logger.debug(f'{len(r.get_ids()):4d} = '+data_to_str(**data))
        return r

    @CsvCache("cache/csv/", maxOld=5, loglevel=logging.INFO)
    def get_csv_as_str(self, *ids: int, endpoint: str = None):
        logger.info(f'get_centros({len(ids)} items)')
        return self.__get_csv_as_str(*ids, endpoint=endpoint)

    @retry(
        times=3,
        sleep=10,
        exceptions=(DwnCsvException, ConnectionError),
        prefix=data_to_str
    )
    def __get_csv_as_str(self, *ids: int, endpoint: str = None):
        ids = tuple(sorted(ids))
        if endpoint is None:
            endpoint = Api.DWN
        soup = WEB.get(endpoint, codCentrosExp=";".join(map(str, ids)))
        url = self.__search_csv_url(soup)
        url = urljoin(endpoint, url)
        r = WEB._get(url)
        if r.status_code == 404:
            raise DwnCsvException(f"{url} not found ({r.status_code})")
        content = r.content.decode('iso-8859-1')
        self.__check_csv_content(content, ids)
        return content

    def __search_csv_url(self, soup: BeautifulSoup):
        script = soup.find("script")
        error = soup.select_one("#detalle_error")
        if error is not None and script is None:
            raise DwnCsvException(error.get_text().strip())
        m = re_location.search(script.string)
        if m is None:
            raise NoCsvUrlDownloadException()
        url = m.group(1)
        return url

    def __check_csv_content(self, content: str, ids: tuple[int]):
        def _parse(arr):
            return tuple(sorted(map(int, set(arr))))
        rows = csvstr_to_rows(content)
        cntnt = (r[1] for r in rows if len(r) > 2 and r[1].isdigit())
        if _parse(cntnt) != _parse(ids):
            raise BadCsvException()

    def _check_inputs(self, data: dict, soup: BeautifulSoup):
        frm = soup.select_one("#"+Api.FORM)
        if frm is None:
            raise DomNotFoundException(data, "#"+Api.FORM)
        for k, v in data.items():
            if k not in self.get_form():
                continue
            dom = frm.select_one(",".join((
                f'select[name="{k}"] option[selected]:not([value^="-"]):not([value="0"])',
                f'input[name="{k}"]:checked'
            )))
            if dom is not None:
                dom = dom.attrs["value"].strip()
            if v != dom:
                raise BadFormException(k, v, dom)

    def __parse_csv(self, content: str) -> Tuple[Centro]:
        rows = csvstr_to_rows(content)
        if len(rows) <= 1:
            return tuple()
        arr = []
        head = rows[1]
        for row in rows[2:]:
            c = self.__build_centro(head, row)
            arr.append(c)
        arr = tuple(arr)
        return arr

    def __build_centro(self, head: Tuple, row: Tuple):
        c = Centro.build(head, row)
        o = self.get_opendata().get(c.id)
        c = c.merge(o)
        return c

    @cached_property
    @retry(
        times=3,
        sleep=10,
        exceptions=ConnectionError
    )
    def home(self):
        return WEB.get(Api.URL)

    @cache
    @Cache("cache/form.json", loglevel=logging.INFO)
    def get_form(self) -> Dict[str, Dict[str, str]]:
        logger.info("get form inputs")
        form = {}
        for name, val, txt in self.iter_inputs(Api.FORM):
            if name not in form:
                form[name] = {}
            form[name][val] = txt
        return form

    @cache
    @Cache("cache/etapas.json", loglevel=logging.INFO)
    def get_etapas(self) -> Dict[str, Dict]:
        logger.info("get form etapas")
        with Driver(wait=10) as w:
            w.get(Api.URL)
            w.wait("comboTipoEnsenanza", presence=True)
            w.driver.set_script_timeout(JS_TIMEOUT)
            with open(f"{dirname(__file__)}/script.js", "r") as f:
                script = "return "+f.read()
            return w.execute_script(script)

    def iter_etapas(self):
        def _walk(node: Dict[str, Dict]):
            for name, val in sorted(node.items()):
                if name == "_":
                    continue
                for value, obj in sorted(val.items()):
                    arr = [ParamValueText(
                        name=name,
                        value=value,
                        text=obj['_']
                    )]
                    yield tuple(arr)
                    for x in _walk(obj):
                        yield tuple(arr + list(x))

        done = set()
        for arr in _walk(self.get_etapas()):
            for i in range(1, len(arr)+1):
                chunk = arr[:i]
                if chunk in done:
                    continue
                yield chunk
                done.add(chunk)

    def iter_inputs(self, id: str):
        def _get_text(n: Tag):
            txt = n.get_text()
            txt = re_sp.sub(" ", txt).strip()
            return fix_char(txt)

        frm = self.home.select_one(f'#{id}')
        for n in frm.select("select"):
            name = trim_null(n.attrs.get("name"), is_null=("", ))
            if name is None:
                continue
            for o in n.select("option"):
                val = trim_null(o.attrs.get("value"), is_null=("", "0", "-1"))
                if val is None:
                    continue
                txt = _get_text(o)
                yield name, val, txt
        for n in frm.select('input[type="checkbox"]'):
            name = trim_null(n.attrs.get("name"), is_null=("", ))
            if name is None:
                continue
            val = trim_null(n.attrs.get("value"))
            if val is None:
                continue
            txt = _get_text(n.find_parent("td").find("a"))
            yield name, val, txt


if __name__ == "__main__":
    a = Api()
    a.get_form()
    a.get_etapas()
