import re
from functools import cache, cached_property
from urllib.parse import urljoin
from typing import Tuple, Dict

from .web import Web
from .types import CsvRow
from .cache import Cache
from .retry import retry

re_bocm = re.compile(r".*(BOCM-[\d\-]+).PDF", re.IGNORECASE)
re_location = re.compile(r"document.location.href=\s*[\"'](.*.csv)[\"']")
re_sp = re.compile(r"\s+", re.MULTILINE | re.UNICODE)
re_centro = re.compile(r"\b(28\d\d\d\d\d\d)\b")
re_pdfs = re.compile(r".*\bapplication%2Fpdf\b.*")

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


class ApiException(Exception):
    pass


class DwnCsvException(ApiException):
    pass


class BadTypeException(DwnCsvException):
    def __init__(self, ask_type, get_type):
        msg = f"Se pidio cdGenerico={ask_type} pero se obtuvo {get_type}"
        super().__init__(msg)


class NoneCodCentrosExpException(DwnCsvException):
    def __init__(self):
        msg = "Falta codCentrosExp (None)"
        super().__init__(msg)


class EmptyCodCentrosExpException(DwnCsvException):
    def __init__(self):
        msg = "Falta codCentrosExp (vacio)"
        super().__init__(msg)


class NoCsvUrlDownloadException(DwnCsvException):
    def __init__(self):
        msg = "No se ha encontrado la url para descargar el csv"
        super().__init__(msg)


class BadCsvException(DwnCsvException):
    def __init__(self):
        msg = "No se han devuelto los mismos centros que se solicitaron"
        super().__init__(msg)


class CsvCache(Cache):
    def parse_file_name(self, *args, **kargv):
        if len(kargv) == 0:
            raise ApiException("No se puede calcular la ruta local para el csv")
        arr = [self.file.rstrip("/")]
        for k, v in kargv.items():
            arr.append(f'{k}={v}')
        return "/".join(arr) + ".csv"


def csvstr_to_rows(content: str):
    rows = content.strip()
    rows = re_csv_br.split(rows)
    rows = [re_csv_fl.split(r.rstrip(" ;,")) for r in rows]
    return tuple(rows)


class Api():
    URL = "https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm"

    def __init__(self):
        pass

    def get_csv(self, **kargv) -> Tuple[CsvRow]:
        content = self.get_csv_as_str(**kargv)
        rows = csvstr_to_rows(content)
        if len(rows) <= 1:
            return tuple()
        arr = []
        head = rows[1]
        for row in rows[2:]:
            while len(row) < len(head):
                row.append(None)
            if len(row) > len(head):
                row[len(head)-1] = row[-1]
            o = {h: c for h, c in zip(head, row)}
            arr.append(CsvRow.build(o))
        return tuple(arr)

    @CsvCache("data/csv/", maxOld=1)
    @retry(
        times=3,
        exceptions=DwnCsvException,
        sleep=15,
        exc_to_return={
            EmptyCodCentrosExpException: ""
        }
    )
    def get_csv_as_str(self, **data):
        w = Web()
        # data["titularidadPublica"] = "S"
        tipo_centro = data.get("cdGenerico")
        soup = w.get(Api.URL, **data)
        if tipo_centro is not None:
            tp = soup.select("#comboGenericos option[selected]")
            if tp and tp[-1].attrs["value"] != tipo_centro:
                tp = tp[-1].attrs["value"]
                raise BadTypeException(tipo_centro, tp)
        codCentrosExp = soup.find(
            "input", attrs={"name": "codCentrosExp"}
        )
        if codCentrosExp is None:
            raise NoneCodCentrosExpException()
        codCentrosExp = codCentrosExp.attrs["value"].strip()
        if len(codCentrosExp) == 0:
            raise EmptyCodCentrosExpException()
        url = soup.find(
            "form", attrs={"id": "frmExportarResultado"}
        )
        url = url.attrs["action"]
        soup = w.get(url, codCentrosExp=codCentrosExp)
        script = soup.find("script")
        error = soup.select_one("#detalle_error")
        if error is not None and script is None:
            raise DwnCsvException(error.get_text().strip())
        m = re_location.search(script.string)
        if m is None:
            raise NoCsvUrlDownloadException()
        script = m.group(1)
        url = urljoin(url, script)
        r = w._get(url)
        if r.status_code == 404:
            raise DwnCsvException(f"{url} not found ({r.status_code})")
        content = r.content.decode('iso-8859-1')
        rows = csvstr_to_rows(content)
        ids = (r[1] for r in rows if len(r) > 2 and r[1].isdigit())
        ids = tuple(sorted(set(ids)))
        if ids != tuple(sorted(set(codCentrosExp.split(";")))):
            raise BadCsvException()
        return content

    @cached_property
    def home(self):
        return Web().get(Api.URL)

    @cache
    @Cache("data/form.json", reload=True)
    def get_form(self) -> Dict[str, Dict[str, str]]:
        form = {}
        for name, val, txt in self.iter_inputs("formBusquedaAvanzada"):
            if name not in form:
                form[name] = {}
            form[name][val] = txt
        return form

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
    for name, obj in a.get_form().items():
        for val, txt in obj.items():
            a.get_csv(**{name: val})
