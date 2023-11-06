import re
from functools import cache, cached_property
from urllib.parse import urljoin
from typing import Tuple, Dict
from bs4 import BeautifulSoup
from os.path import dirname
import os
import logging

from .web import Web, Driver
from .types import ParamValueText, QueryResponse
from .centro import Centro
from .cache import Cache
from .retry import retry

logger = logging.getLogger(__name__)

re_location = re.compile(r"document.location.href=\s*[\"'](.*.csv)[\"']")
re_csv_br = re.compile(r"\s*\n\s*")
re_csv_fl = re.compile(r"\s*;\s*")
re_sp = re.compile(r"\s+")

JS_TIMEOUT = int(os.environ.get('JS_TIMEOUT', '120'))


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


class DomNotFoundException(SearchException):
    def __init__(self, selector: str):
        msg = f"No se ha encontrado el elemento {selector}"
        super().__init__(msg)


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


class Api():
    URL = "https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm"
    DWN = "https://gestiona.comunidad.madrid/wpad_pub/run/j/ConsultaGeneralNPCentrosCSV.icm"
    FORM = "formBusquedaAvanzada"

    def get_csv(self, *ids: Tuple[int]):
        if len(ids) == 0:
            return tuple()
        content = self.get_csv_as_str(*ids)
        return self.__parse_csv(content)

    def search_csv(self, **kargv):
        content = self.search_csv_as_str(**kargv)
        return self.__parse_csv(content)

    @IdCache("data/ids/", maxOld=5)
    def search_ids(self, **data) -> Tuple[int]:
        r = self.__do_search(**data)
        return r.get_ids()

    @CsvCache("data/csv/", maxOld=5)
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
        exceptions=SearchException,
        prefix=data_to_str
    )
    def __do_search(self, **data):
        w = Web()
        soup = w.get(Api.URL, **data)
        self.__check_inputs(data, soup)
        codCentrosExp = self.__select_one(
            soup,
            'input[name="codCentrosExp"]',
            "value"
        )
        frmExportarResultado = self.__select_one(
            soup,
            '#frmExportarResultado',
            "action"
        )
        r = QueryResponse(
            codCentrosExp=codCentrosExp,
            frmExportarResultado=frmExportarResultado
        )
        logger.info(f'{len(r.get_ids()):4d} = '+data_to_str(**data))
        return r

    @CsvCache("data/csv/", maxOld=5, loglevel=logging.INFO)
    def get_csv_as_str(self, *ids: int, endpoint: str = None):
        logger.info('get_csv(' + ", ".join(map(str, ids))+')')
        return self.__get_csv_as_str(*ids, endpoint=endpoint)

    @retry(
        times=3,
        sleep=10,
        exceptions=DwnCsvException,
        prefix=data_to_str
    )
    def __get_csv_as_str(self, *ids: int, endpoint: str = None):
        ids = tuple(sorted(ids))
        if endpoint is None:
            endpoint = Api.DWN
        w = Web()
        soup = w.get(endpoint, codCentrosExp=";".join(map(str, ids)))
        url = self.__search_csv_url(soup)
        url = urljoin(endpoint, url)
        r = w._get(url)
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

    def __check_inputs(self, data: dict, soup: BeautifulSoup):
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
                raise BadFormException(data, k, v, dom)

    def __select_one(self, soup: BeautifulSoup, selector: str, attr: str):
        node = soup.select_one(selector)
        if node is None:
            raise DomNotFoundException(selector)
        value = node.attrs[attr]
        return value.strip()

    def __parse_csv(self, content: str) -> Tuple[Centro]:
        rows = csvstr_to_rows(content)
        if len(rows) <= 1:
            return tuple()
        arr = []
        head = rows[1]
        for row in rows[2:]:
            arr.append(Centro.build(head, row))
        arr = tuple(arr)
        return arr

    @cached_property
    def home(self):
        return Web().get(Api.URL)

    @cache
    @Cache("data/form.json", loglevel=logging.INFO)
    def get_form(self) -> Dict[str, Dict[str, str]]:
        logger.info("get form inputs")
        form = {}
        for name, val, txt in self.iter_inputs(Api.FORM):
            if name not in form:
                form[name] = {}
            form[name][val] = txt
        return form

    @cache
    @Cache("data/etapas.json", loglevel=logging.INFO)
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
        frm = self.home.select_one(f'#{id}')
        for n in frm.select("select"):
            name = trim_null(n.attrs.get("name"), is_null=("", ))
            if name is None:
                continue
            for o in n.select("option"):
                val = trim_null(o.attrs.get("value"), is_null=("", "0", "-1"))
                if val is None:
                    continue
                txt = o.get_text().strip()
                yield name, val, txt
        for n in frm.select('input[type="checkbox"]'):
            name = trim_null(n.attrs.get("name"), is_null=("", ))
            if name is None:
                continue
            val = trim_null(n.attrs.get("value"))
            if val is None:
                continue
            txt = n.find_parent("td").find("a").get_text().strip()
            yield name, val, txt


if __name__ == "__main__":
    a = Api()
    a.get_form()
    a.get_etapas()
