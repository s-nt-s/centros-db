import re
from functools import cache, cached_property
from urllib.parse import urljoin
from typing import Tuple, Dict
from bs4 import BeautifulSoup
from os.path import dirname
import logging

from .web import Web, Driver
from .types import CsvRow, ParamValueText
from .cache import Cache
from .retry import retry

logger = logging.getLogger(__name__)

re_location = re.compile(r"document.location.href=\s*[\"'](.*.csv)[\"']")
re_csv_br = re.compile(r"\s*\n\s*")
re_csv_fl = re.compile(r"\s*;\s*")
re_sp = re.compile(r"\s+")


def trim_null(s: str, is_null=tuple()):
    if s is None:
        return None
    s = s.strip()
    if s in is_null:
        return None
    return s


def data_to_str(*args, **data):
    query = "&".join(f'{k}={v}' for k, v in data.items() if k[0] != '_')
    return query


class ApiException(Exception):
    pass


class DwnCsvException(ApiException):
    pass


class DomNotFoundException(ApiException):
    def __init__(self, data: dict, selector: str):
        msg = f"No se ha encontrado el elemento {selector}"
        super().__init__(msg)


class BadFormException(DwnCsvException):
    def __init__(self, data: dict, ask_name, ask_val, get_val):
        msg = f"Se pidio {ask_name}={ask_val} pero se obtuvo {get_val}"
        super().__init__(msg)


class NoCsvUrlDownloadException(DwnCsvException):
    def __init__(self, data: dict):
        msg = f"No se ha encontrado la url para descargar el csv"
        super().__init__(msg)


class BadCsvException(DwnCsvException):
    def __init__(self, data: dict):
        msg = f"No se han devuelto los mismos centros que se solicitaron"
        super().__init__(msg)


class CsvCache(Cache):
    def __init__(self, *args, ext="csv", **kwargs):
        super().__init__(*args, **kwargs)
        self.ext = ext

    def parse_file_name(self, *args, **kargv):
        if len(kargv) == 0:
            raise ApiException("No se puede calcular la ruta local para el " + self.ext)
        arr = [self.file.rstrip("/")]
        for k, v in kargv.items():
            arr.append(f'{k}={v}')
        return "/".join(arr) + "." + self.ext


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

    def __init__(self):
        self.id_form = "formBusquedaAvanzada"

    def get_csv(self, **kargv) -> Tuple[CsvRow]:
        content = self.get_csv_as_str(**kargv)
        rows = csvstr_to_rows(content)
        if len(rows) <= 1:
            return tuple()
        arr = []
        head = rows[1]
        for row in rows[2:]:
            arr.append(CsvRow.build(head, row))
        arr = tuple(arr)
        logger.info(f'{len(arr):4d} = '+data_to_str(**kargv))
        return arr

    @IdCache("data/ids/", maxOld=2)
    def get_ids(self, **data) -> Tuple[int]:
        ids = set()
        for row in self.get_csv(_avoid_save=True, **data):
            ids.add(row.id)
        return tuple(sorted(ids))

    @CsvCache("data/csv/", maxOld=2)
    @retry(
        times=3,
        sleep=10,
        exceptions=DwnCsvException,
        prefix=data_to_str
    )
    def get_csv_as_str(self, **data):
        w = Web()
        soup = w.get(Api.URL, **data)
        self.__check_inputs(data, soup)
        codCentrosExp = self.__select_one(
            soup,
            'input[name="codCentrosExp"]',
            "value"
        )
        url = self.__select_one(
            soup,
            '#frmExportarResultado',
            "action"
        )
        if len(codCentrosExp) == 0:
            return ""
        content = self.__get_content(w, url, codCentrosExp=codCentrosExp)
        self.__check_csv_content(content, codCentrosExp.split(";"))

        return content

    def __check_inputs(self, data: dict, soup: BeautifulSoup):
        frm = soup.select_one("#"+self.id_form)
        if frm is None:
            raise DomNotFoundException(data, "#"+self.id_form)
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

    def __get_csv_url(self, soup: BeautifulSoup):
        script = soup.find("script")
        error = soup.select_one("#detalle_error")
        if error is not None and script is None:
            raise DwnCsvException(error.get_text().strip())
        m = re_location.search(script.string)
        if m is None:
            raise NoCsvUrlDownloadException()
        url = m.group(1)
        return url

    def __get_content(self,  w: Web, url: str, **kwargs):
        soup = w.get(url, **kwargs)
        curl = self.__get_csv_url(soup)
        url = urljoin(url, curl)
        r = w._get(url)
        if r.status_code == 404:
            raise DwnCsvException(f"{url} not found ({r.status_code})")
        content = r.content.decode('iso-8859-1')
        return content

    def __check_csv_content(self, content, ids):
        rows = csvstr_to_rows(content)
        ids = (r[1] for r in rows if len(r) > 2 and r[1].isdigit())
        ids = tuple(sorted(set(ids)))
        if ids != tuple(sorted(set(ids))):
            raise BadCsvException()

    @cached_property
    def home(self):
        return Web().get(Api.URL)

    @cache
    @Cache("data/form.json")
    def get_form(self) -> Dict[str, Dict[str, str]]:
        def is_needed(name: str):
            if name in ('comboMunicipios', 'comboDistritos'):
                return False
            return not name.startswith("checkSubdir")
        logger.info("get form inputs")
        form = {}
        for name, val, txt in self.iter_inputs(self.id_form):
            if not is_needed(name):
                continue
            if name not in form:
                form[name] = {}
            form[name][val] = txt
        return form

    @cache
    @Cache("data/etapas.json")
    def get_etapas(self) -> Dict[str, Dict]:
        logger.info("get form etapas")
        with Driver(wait=10) as w:
            w.get(Api.URL)
            w.wait("comboTipoEnsenanza", presence=True)
            w.driver.set_script_timeout(120)
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


class ApiTitularidadPublica(Api):
    def get_csv_as_str(self, **data):
        for k, v in list(data.items()):
            if k.startswith("titularidad") and k != "titularidadPublica":
                return ""
        data["titularidadPublica"] = "S"
        return super().get_csv_as_str(**data)


if __name__ == "__main__":
    a = Api()
    a.get_form()
    a.get_etapas()
