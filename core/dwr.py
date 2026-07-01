import re
import time
import requests
from textwrap import dedent


def _find_val(text: str, rgx: str):
    arr: list[str] = []
    for s in re.findall(rgx+r"=([^;]+)", text):
        m = re.search(r"\b" + re.escape(s) + r'="([^"]+)"', text)
        if m is None:
            raise ValueError(text)
        arr.append(m.group(1))
    return tuple(arr)


def _iter_series(text: str, rgx: str):
    for s in re.findall(rgx+r"=([^;]+)", text):
        arr: list[int|float] = []
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
            data: dict[int, int] = {}
            for k, v in zip(years, vals):
                if v > 0:
                    data[k] = v
            yield series[i], data
            i = i + 1


class Dwr:
    def __init__(self):
        self.__s = requests.Session()
        self.__s.headers = {
            "Content-Type": "text/plain",
            "Origin": "https://gestiona.comunidad.madrid",
            "Referer": "https://gestiona.comunidad.madrid/wpad_pub/",
            "Accept": "*/*",
        }

    def __get(self, url: str, payload: str):
        r = self.__s.post(url, data=dedent(payload).strip())
        r.raise_for_status()
        return r.text.strip()
    
    def get_total_alumnos(self, cod: int):
        r = self.get_alumnos(cod)
        if not r:
            return None
        max_year = 0
        for d in r.values():
            max_year = max(max_year, *d.keys())
        if max_year == 0:
            return None
        total = 0
        for d in r.values():
            total = total + d.get(max_year, 0)
        return total

    def get_alumnos(self, cod: int):
        call_id = f"1_{int(time.time() * 1000)}"
        text = self.__get(
            "https://gestiona.comunidad.madrid/wpad_pub/dwr/exec/GraficasDWRAccion.obtenerGrafica.dwr",
            f"""
                callCount=1
                c0-scriptName=GraficasDWRAccion
                c0-methodName=obtenerGrafica
                c0-id={call_id}
                c0-e1=string:{cod}
                c0-e2=string:TODO
                c0-e3=string:1
                c0-e4=string:1
                c0-param0=Object:{{cdCentro:reference:c0-e1, cdnivelEducativo:reference:c0-e2, cdGrafica:reference:c0-e3, tipoGrafica:reference:c0-e4}}
                xml=true
            """
        )
        data: dict[str, dict[int, int]] = {}
        for k, vals in _iter_data(text):
            if k not in ("Total", ):
                data[k] = vals
        return data

    def get_titulacion(self, cod: int):
        call_id = f"1_{int(time.time() * 1000)}"
        text = self.__get(
            "https://gestiona.comunidad.madrid/wpad_pub/dwr/exec/GraficasDWRAccion.obtenerGrafica.dwr",
            f"""
            callCount=1
            c0-scriptName=GraficasDWRAccion
            c0-methodName=obtenerGrafica
            c0-id={call_id}
            c0-e1=string:{cod}
            c0-e2=string:05
            c0-e3=string:5
            c0-e4=string:1
            c0-e5=string:1
            c0-e6=string:0
            c0-param0=Object:{{cdCentro:reference:c0-e1,cdnivelEducativo:reference:c0-e2,cdGrafica:reference:c0-e3,tipoGrafica:reference:c0-e4,tipoTasaTitul:reference:c0-e5,tipoModalidad:reference:c0-e6}}
            xml=true
            """
        )
        data = {}
        for k, vals in _iter_data(text):
            if k not in ("Total", ):
                data[k] = vals
        return data

DWR = Dwr()

if __name__ == "__main__":
    centro = 28079357
    alumnos = DWR.get_titulacion(centro)
    print(f"Centro {centro}: {alumnos}")