from requests import Session
from .cache import Cache
import logging
import csv
from io import StringIO
from collections import defaultdict
from typing import NamedTuple, Union
import re
from unidecode import unidecode
from core.utm_to_geo import UTM_TO_GEO, LatLon
from core.util import mk_dict_1_1, mk_dict_n_1
from types import UnionType, MappingProxyType
import typing
from functools import cached_property, cache
from core.checker import MChecker, UChecker


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


def to_path(*args):
    arr = []
    for a in reversed(args):
        if a is not None and a not in arr:
            arr.append(a)
    return " <-- ".join(arr)


def can_be_none(tipo) -> bool:
    if tipo is type(None):
        return True
    origin = typing.get_origin(tipo)
    if origin is typing.Union:
        return type(None) in typing.get_args(tipo)
    if origin is UnionType:
        return type(None) in typing.get_args(tipo)
    return False


def validate(obj: NamedTuple):
    cls = obj.__class__
    hints = typing.get_type_hints(obj.__class__)

    error: list[str] = []
    for f in cls._fields:
        val = getattr(obj, f)
        tip = hints.get(f)
        if val is None and tip is not None and not can_be_none(tip):
            e = str(f)  #f"{f} ({tip})"
            if e not in error:
                error.append(e)
    if error:
        raise ValueError(f"{cls.__name__} requiere {', '.join(error)}")


def _none_if(s, *arg):
    if s in arg:
        return None
    return s


def _trim(s):
    if not isinstance(s, str):
        return s
    s = re_sp.sub(" ", s)
    s = s.strip()
    if len(s) == 0:
        return None
    return s


def _number(s: str | None):
    if s is None:
        return None
    f = float(s)
    i = int(f)
    if f == i:
        return i
    return f


def _tlf(*args: str | None):
    arr: list[int] = []
    for ori in args:
        if ori is None:
            continue
        s = str(ori)
        s = re.sub(r"\.+", "", s).lower()
        s = re.sub(r"(extensi.n(es)?|ext) (\d{3,4}[;\s$]*)+", "", s)
        s = re.sub(r"\b91-(\d{7})\b", r"91\1", s)
        s = re_sp.sub(r" ", s).strip()

        s = re.sub(r"&amp;#13;", " ", s)
        s = re.sub(re.escape("(+34)"), " ", s)
        spl = r"(\b|^)(?:" + "|".join((
            r"de \d+ a \d+",
            r"\d{1,2}:\d\d",
            r"prefijo 34 si",
            r"servicio \d+\s*h\w*",
            r"de \d+ a.os",
            r"365 d.as",
            r"24 h\w*",
            r"de \d+ a \d+ h(oras?)?\b",
            r"de \w a \w de \d+ a h(oras?)?\b"
        )) + r")(\b|$)"
        s = re.sub(spl, " ", s)
        s = re.sub(r"^[^\d\+]+", "", s)
        s = re.sub(r"([a-záéíóúñń\s]+)", " ", s)

        def _expand(m: re.Match[str]):
            a, b, c = m.groups()
            a = re_sp.sub("",  a)
            b = re_sp.sub("",  b)
            return a + " / " + a[:-len(b)] + b + c

        #s = re.sub(r"\b(91(?:\s*\d){7})\s*/\s*([0-8]\d{2,4})\b(\s*[\D])", _expand, s)

        def _find(m: re.Match[str]):
            t1 = re_sp.sub("", m.group(1))
            arr.append(int(t1))
            t2 = re_sp.sub("", m.group(2) or '/')[1:]
            if len(t2) == 0:
                return ""
            if len(t2) == 9:
                arr.append(int(t2))
                return ""
            if len(t2) < 5:
                arr.append(int(t1[:-len(t2)] + t2))
                return ""
            return m.group(2)

        s = re.sub(r"(\b91\d \d{3} \d{3}\b)(\s*[\-/]\s*[\d\s]+)?", _find, s)
        s = re.sub(r"(\b91 \d{3} \d{2} \d{2}\b)(\s*[\-/]\s*[\d\s]+)?", _find, s)
        s = re.sub(r"(\b91 \d{3} \d{4}\b)(\s*[\-/]\s*[\d\s]+)?", _find, s)
        s = re.sub(r"(\b91 \d{7}\b)(\s*[\-/]\s*[\d\s]+)?", _find, s)

        for x in re.split(r"\s*([;,\-/:\(\\?)])\s*", s):
            no_sp = re_sp.sub(r"", x)
            if len(no_sp) == 0 or (len(no_sp) == 1 and not no_sp.isdecimal()):
                continue
            if no_sp in (
                "+34",
                "1111",
                "112",
                "092",
                "010",
                "012",
                "020",
                "060",
                "0"
            ) or (len(no_sp) > 3 and re.match(r"^\D+$", x)):
                continue
            no_sp = re.sub(r"^\s*(00|\+)34\s*", "", no_sp)
            if re.search(r"\D", no_sp):
                logger.warning(f"Teléfono mal formado {to_path(ori, s, x, no_sp)}")
                continue
            arr_no_sp = [no_sp]
            if len(no_sp) == 18:
                arr_no_sp = [
                    no_sp[:9],
                    no_sp[9:]
                ]
            for no_sp in arr_no_sp:
                if len(no_sp) < 9:
                    logger.warning(f"Teléfono demasiado corto {to_path(ori, s, x, no_sp)}")
                    continue
                if len(no_sp) > 9:
                    logger.warning(f"Teléfono demasiado largo {to_path(ori, s, x, no_sp)}")
                    continue
                if not no_sp.startswith(("9", "6", "7", "8")):
                    logger.warning(f"Prefijo desconocido: {to_path(ori, s, x, no_sp)}")
                    continue
                if no_sp.startswith(("901", "902", "903")):
                    logger.warning(f"Tarificación especial: {to_path(ori, s, x, no_sp)}")
                    continue
                t = int(no_sp)
                arr.append(t)

    return tuple(dict.fromkeys(arr))


def _tipo(s: str | None):
    if s is None:
        return None
    if not s.startswith("/contenido/entidadesYorganismos/"):
        logger.warning(f"Tipo inesperado: {s}")
        return s
    s = s[32:]
    return s


def _codigo_postal(s: str | None):
    if s is None:
        return None
    if re.match(r"^0+$", s):
        return None
    if not re.match(r"^28\d{3}$", s):
        logger.warning(f"Codigo postal mal formado {s}")
        return None
    return int(s)


def get_latlon(x: int | float, y: int | float) -> LatLon:
    if None in (x, y):
        return None
    if isinstance(x, float) or isinstance(y, float):
        latlon = LatLon(x, y)
    else:
        latlon = UTM_TO_GEO.to_geo(x, y)
    if latlon is not None:
        return latlon.round(7)


class CodTxt(NamedTuple):
    cod: str
    txt: str

    def is_empty(self):
        return self.cod is None or self.txt is None

    def is_incomplete(self):
        return (self.cod is None) != (self.txt is None)

    def is_complete(self):
        return self.cod is not None and self.txt is not None

    @classmethod
    def create_dict(cls, *args: Union["CodTxt", None]):
        def _gK(a: CodTxt | None):
            if a is None:
                return None
            return a.cod

        def _gV(a: CodTxt | None):
            if a is None:
                return None
            return a.txt

        return mk_dict_1_1(*args, get_k=_gK, get_v=_gV)

    @classmethod
    def fix_codes(cls, *args: dict):
        k_codes: dict[str, set["CodTxt"]] = defaultdict(set)
        k_dicts: dict[str, dict[str, str]] = {}
        for a in args:
            for k, v in a.items():
                if isinstance(v, cls):
                    k_codes[k].add(v)
        for k, codes, in k_codes.items():
            k_dict = cls.create_dict(*codes)
            if len(k_dict):
                k_dicts[k] = k_dict

        arr = list(args)
        for a in arr:
            for f, k_v in k_dicts.items():
                v_k = {v: k for k, v in k_v.items()}
                v = a.get(f)
                if isinstance(v, cls) and None in (v.cod, v.txt):
                    a[f] = v._replace(
                        cod=v_k.get(v.txt, v.cod),
                        txt=k_v.get(v.cod, v.txt)
                    )
        for a in arr:
            for k, v in a.items():
                if isinstance(v, cls):
                    if v.is_empty():
                        a[k] = None
                    elif v.is_incomplete():
                        raise ValueError(f"Código incompleto {v} en campo {k} en {a}")
        return tuple(arr)


class MunCentro(NamedTuple):
    pk: int
    nombre: str
    url: str
    accesibilidad: tuple[int, ...]
    telefono: tuple[int, ...]
    fax: tuple[int, ...]
    email: tuple[str, ...]
    descripcion_entidad: str | None
    provincia: str | None
    horario: str | None
    equipamiento: str | None
    transporte: str | None
    descripcion: str | None
    via_nombre: str | None
    via_clase: str | None
    via_num_tipo: str | None
    via_num: str | None
    via_planta: str | None
    via_puerta: str | None
    via_escaleras: str | None
    via_orientacion: str | None
    via_localidad: str | None
    codigo_postal: int | None
    barrio: CodTxt | None
    distrito: CodTxt | None
    latlon: LatLon | None
    tipo: str | None


class CamCentro(NamedTuple):
    codigo: int
    dat: CodTxt
    municipio: CodTxt
    telefono: tuple[int, ...]
    fax: tuple[int, ...]
    web: tuple[str, ...]
    email: tuple[str, ...]
    situacion: str
    centro: str | None
    tipo_cod: str
    tipo_abr: str | None
    tipo_ext: str
    titularidad: str
    titular: str | None
    titular_nif: str | None
    centro_nif: str | None
    via_tipo: str | None
    via_domicilio: str | None
    via_num: str | None
    codigo_postal: str | None
    distrito: CodTxt | None
    latlon: LatLon | None
    fecha: str | None


def _join(name: str, *tps: tuple[dict], pk: str = None):
    tps = tuple((t for t in tps if len(t) > 0))
    if len(tps) == 0:
        return tuple()
    k = tuple(tps[0][0].keys())
    for t in tps[1:]:
        if k != tuple(t[0].keys()):
            raise ValueError(f'No coinciden cabeceras de {name}')
    if pk and pk not in k:
        raise ValueError(f'Columna clave {pk} no aparece en {name}')
    rows: list[tuple] = []
    for t in tps:
        for r in t:
            tp = tuple(r.items())
            if tp not in rows:
                rows.append(tp)
    data = tuple(map(dict, rows))
    if pk:
        pk_tup: dict[str, set[tuple]] = defaultdict(set)
        for d in data:
            pk_tup[d[pk]].add(tuple(d.items()))
        dup_pk: set[str] = set()
        for k, st in pk_tup.items():
            if len(st) > 1:
                dup_pk.add(k)
        if dup_pk:
            raise ValueError(f"{pk}={', '.join(sorted(dup_pk))} duplicada con distintos valores {name}")
    logger.info(f"{name} = {len(data)}")
    return data


class OpenData():
    # Centros educativos de la comunidad de Madrid
    # https://datos.comunidad.madrid/dataset/centros_educativos/resource/28d60557-1d73-4281-ab08-6cfd3b2f5f83
    # =
    # https://datos.comunidad.madrid/catalogos/#/dataset/centros_educativos/?view=info
    # =
    # https://datos.comunidad.madrid/dataset/c750856d-3166-4dac-8e80-d1b824c968b5/resource/28d60557-1d73-4281-ab08-6cfd3b2f5f83/download/centros_educativos.csv
    CAM_CENTROS = "https://datos.comunidad.madrid/catalogo/dataset/c750856d-3166-4dac-8e80-d1b824c968b5/resource/28d60557-1d73-4281-ab08-6cfd3b2f5f83/download/centros_educativos.csv"
    # Centros educativos en Madrid 
    # https://datos.madrid.es/dataset/300614-0-centros-educativos
    MUN_CENTROS = "https://datos.madrid.es/dataset/300614-0-centros-educativos/resource/300614-1-centros-educativos-csv/download/300614-1-centros-educativos-csv.csv"
    # Sedes. Centros de Enseñanza
    # https://datos.madrid.es/dataset/212904-0-centros-ensenanza/information
    MUN_SEDES_1 = "https://datos.madrid.es/dataset/212904-0-centros-ensenanza/resource/212904-4-centros-ensenanza-csv/download/212904-4-centros-ensenanza-csv.csv"
    # Sedes. Centros de Educación 
    # https://datos.madrid.es/dataset/212790-0-centros-educacion/information
    MUN_SEDES_2 = "https://datos.madrid.es/dataset/212790-0-centros-educacion/resource/212790-5-centros-educacion-csv/download/212790-5-centros-educacion-csv.csv"
    # Centros municipales de enseñanzas artísticas 
    # https://datos.madrid.es/dataset/203868-0-ceramica-danza-musica-dramatico
    MUN_ARTES = "https://datos.madrid.es/dataset/203868-0-ceramica-danza-musica-dramatico/resource/203868-2-ceramica-danza-musica-dramatico-csv/download/203868-2-ceramica-danza-musica-dramatico-csv.csv"
    # Centros de la Escuela Oficial de Idiomas en Madrid 
    # https://datos.madrid.es/dataset/207037-0-idiomas-oficial
    MUN_EOI = "https://datos.madrid.es/dataset/207037-0-idiomas-oficial/resource/207037-5-idiomas-oficial-csv/download/207037-5-idiomas-oficial-csv.csv"
    # Instalaciones accesibles municipales
    # https://datos.madrid.es/dataset/202162-0-instalaciones-accesibles-municip
    MUN_ACCESIBLE = "https://datos.madrid.es/dataset/202162-0-instalaciones-accesibles-municip/resource/202162-0-instalaciones-accesibles-municip-csv/download/202162-0-instalaciones-accesibles-municip-csv.csv"
    # Instalaciones accesibles no municipales
    # https://datos.madrid.es/dataset/202180-0-instalaciones-accesibles-no-muni
    NO_MUN_ACCESIBLE = "https://datos.madrid.es/dataset/202180-0-instalaciones-accesibles-no-muni/resource/202180-5-instalaciones-accesibles-no-muni-csv/download/202180-5-instalaciones-accesibles-no-muni-csv.csv"

    ACC = MappingProxyType({
        0: "Instalación no accesible para personas con movilidad reducida",
        1: "Instalación accesible para personas con movilidad reducida",
        2: "Instalación parcialmente accesible para personas con movilidad reducida",
        3: "Sin información sobre accesibilidad para personas con movilidad reducida",
        4: "Lengua de signos",
        5: "Señalización podotáctil",
        6: "Bucle de inducción magnético"
    })

    def __init__(self):
        self.__s = Session()

    @Cache("cache/opendata/{0}")
    def __get_text(self, name: str, url: str, encoding: str = None):
        logger.info(f"Descargando {name}")
        r = self.__s.get(url)
        if encoding:
            r.encoding = encoding
        return r.text

    def __read_csv(self, name: str, url: str, encoding: str = None):
        content = self.__get_text(name, url, encoding=encoding)
        with StringIO(content) as f:
            reader = csv.DictReader(f, delimiter=";")
            lst_rows: list[dict] = []
            for r in reader:
                r = {unidecode(k): _trim(v) for k, v in r.items() if k is not None}
                if all((v is None for v in r.values())):
                    continue
                lst_rows.append(r)
            rows = tuple(lst_rows)
        size = len(rows)
        if size == 0:
            logger.info(f"{name} = {size} rows")
        else:
            logger.info(f"{name} = {size} rows")
        return rows

    @cached_property
    def cam_centros(self):
        return MappingProxyType({c.codigo: c for c in self.get_cam_centros()})

    def get_cam_centros(self):
        centros: set[CamCentro] = set()
        for r in self.__read_csv("cam_centros.csv", OpenData.CAM_CENTROS):
            c = CamCentro(
                codigo=int(r['CODIGO']),
                centro=r['CENTRO'],
                tipo_cod=r['COD_TIPO'],
                tipo_abr=r['TIPO_ABRV'],
                tipo_ext=r['TIPO_EXT'],
                titularidad=r['TITULARIDAD'],
                titular=r['TITULAR'],
                titular_nif=r['NIF_TITULAR'],
                centro_nif=r['NIF_CENTRO'],
                dat=CodTxt(
                    cod=r['COD_DAT'],
                    txt=r['DAT']
                ),
                via_tipo=r['CDTPVIA'],
                via_domicilio=r['DOMICILIO'],
                via_num=r['NMVIAL'],
                codigo_postal=_codigo_postal(r['CDPOSTAL']),
                municipio=CodTxt(
                    cod=r['CDMUNI'],
                    txt=r['MUNICIPIO']
                ),
                distrito=CodTxt(
                    cod=r['CDDISTRITO'],
                    txt=r['DISTRITO']
                ),
                telefono=_tlf(r['TELEFONO'], r['TELEFONO2'], r['TELEFONO3'], r['TELEFONO4']),
                fax=_tlf(r['FAX']),
                web=UChecker.find_urls(r['WEB']),
                email=MChecker.find_email(r['WEB'], r['E_MAIL'], r['E_MAIL2']),
                latlon=get_latlon(
                    _number(r['UTM_X']),
                    _number(r['UTM_Y'])
                ),
                situacion=r['SITUACION'],
                fecha=r['FECHA CONSTITUCION'],
            )
            centros.add(c)
        centros = set(CamCentro(**d) for d in CodTxt.fix_codes(*(c._asdict() for c in centros)))
        for c in centros:
            if None in (c.municipio, c.dat):
                raise ValueError(f"Falta municipio o dat en {c}")

        tuple(map(validate, centros))
        return tuple(sorted(centros, key=lambda c: c.codigo))

    @cache
    def get_mun_centros(self):
        centros: set[MunCentro] = set()
        for r in _join(
            "num_*.csv",
            self.__read_csv("mun_centros.csv", OpenData.MUN_CENTROS, encoding="windows-1250"),
            self.__read_csv("mun_artes.csv", OpenData.MUN_ARTES, encoding="windows-1250"),
            self.__read_csv("mun_eoi.csv", OpenData.MUN_EOI, encoding="windows-1250"),
            self.__read_csv("mun_sedes_1.csv", OpenData.MUN_SEDES_1, encoding="windows-1250"),
            self.__read_csv("mun_sedes_2.csv", OpenData.MUN_SEDES_2, encoding="windows-1250"),
            self.__read_csv("mun_accesible.csv", OpenData.MUN_ACCESIBLE, encoding="windows-1250"),
            self.__read_csv("no_mun_accesible.csv", OpenData.NO_MUN_ACCESIBLE, encoding="windows-1250"),
            pk='PK'
        ):
            tipo = _tipo(r['TIPO'])
            c = MunCentro(
                pk=_number(r['PK']),
                nombre=r['NOMBRE'],
                descripcion_entidad=r['DESCRIPCION-ENTIDAD'],
                horario=r['HORARIO'],
                equipamiento=r['EQUIPAMIENTO'],
                transporte=r['TRANSPORTE'],
                descripcion=r['DESCRIPCION'],
                accesibilidad=tuple(sorted(map(int, r['ACCESIBILIDAD'].split(",")))),
                url=r['CONTENT-URL'].split("://", 1)[-1],
                via_nombre=r['NOMBRE-VIA'],
                via_clase=r['CLASE-VIAL'],
                via_num_tipo=r['TIPO-NUM'],
                via_num=r['NUM'],
                via_planta=r['PLANTA'],
                via_puerta=r['PUERTA'],
                via_escaleras=r['ESCALERAS'],
                via_orientacion=r['ORIENTACION'],
                via_localidad=r['LOCALIDAD'],
                provincia=r['PROVINCIA'],
                codigo_postal=_codigo_postal(r['CODIGO-POSTAL']),
                barrio=CodTxt(
                    cod=r['COD-BARRIO'],
                    txt=r['BARRIO']
                ),
                distrito=CodTxt(
                    cod=r['COD-DISTRITO'],
                    txt=_none_if(r['DISTRITO'], 'DISTRITO'),
                ),
                latlon=get_latlon(
                    _number(r['LATITUD']),
                    _number(r['LONGITUD'])
                ) or get_latlon(
                    _number(r['COORDENADA-X']),
                    _number(r['COORDENADA-Y'])
                ),
                telefono=_tlf(r['TELEFONO']),
                fax=_tlf(r['FAX']),
                email=MChecker.find_email(r['EMAIL']),
                tipo=tipo,
            )
            centros.add(c)
        centros = set(MunCentro(**d) for d in CodTxt.fix_codes(*(c._asdict() for c in centros)))
        bar_dis: dict[CodTxt, set[CodTxt]] = defaultdict(set)
        ll_dis: dict[LatLon, set[CodTxt]] = defaultdict(set)
        ll_bar: dict[LatLon, set[CodTxt]] = defaultdict(set)
        for c in centros:
            if c.accesibilidad and any(a not in OpenData.ACC for a in c.accesibilidad):
                raise ValueError(f"Accesibilidad desconocida {c.accesibilidad} en {c}")
            for cod in (c.barrio, c.distrito):
                if cod is not None and cod.is_incomplete():
                    raise ValueError(f"Código incompleto en {c}")
            if c.barrio and c.distrito:
                bar_dis[c.barrio].add(c.distrito)
            if c.barrio and c.latlon:
                ll_bar[c.latlon].add(c.barrio)
            if c.distrito and c.latlon:
                ll_dis[c.latlon].add(c.distrito)
        for c in list(centros):
            centros.remove(c)
            bar = ll_bar.get(c.latlon)
            if c.barrio is None and bar and len(bar) == 1:
                c = c._replace(barrio=tuple(bar)[0])
            dis = ll_dis.get(c.latlon)
            if dis is None or len(dis) != 1:
                dis = bar_dis.get(c.barrio)
            if c.distrito is None and dis and len(dis) == 1:
                c = c._replace(distrito=tuple(dis)[0])
            centros.add(c)

        tuple(map(validate, centros))
        return tuple(sorted(centros, key=lambda c: c.pk))

    def find_mun_centro(self, *args: str | int | None):
        ks = set(a for a in args if a is not None)
        if len(ks) == 0:
            return None

        def _getK(c: CamCentro | MunCentro):
            return ks.intersection({*c.telefono, *c.fax, *c.email})

        k_mun: dict[str | int, MunCentro] = mk_dict_n_1(
            *self.get_mun_centros(),
            get_ks=_getK,
        )
        ok = set(k_mun.values())
        if len(ok) == 1:
            return ok.pop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    o = OpenData()
    o.find_mun_centro(
        913651271,
        "ies.sanisidro.madrid@educa.madrid.org"
    )
    o.get_cam_centros()
