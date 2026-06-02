from requests import Session
from .cache import Cache
import logging
import csv
from io import StringIO
from collections import defaultdict
from typing import NamedTuple, Union
import re
from unidecode import unidecode
from core.utm_to_geo import utm_to_geo, LatLon
from types import MappingProxyType

logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_\.]+\.[\w\-_]+', re.IGNORECASE)


def _email(*args: str | None):
    arr: list[str] = []
    for a in args:
        for e in re_mail.findall(a or ''):
            if e not in arr:
                arr.append(e)
    return tuple(arr)


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
    for s in args:
        if s is None:
            continue
        s = re.sub(r"\.+", "", s).lower()
        s = re.sub(r"extensi.n(es)? (\d{3}\b[;\s]*)+", "", s)
        s = re.sub(r"^[^\d\+]+", "", s)
        s = re.sub(r"\b91-(\d{7})\b", r"91\1", s)
        s = re_sp.sub(r" ", s).strip()

        val = {
            "914 698 614 - 17": tuple(range(914698614, 914698617+1)),
            "913 980 300 - 345": tuple(range(913980300, 913980345+1))
        }.get(s)

        if val is not None:
            for v in val:
                if v not in arr:
                    arr.append(v)
            continue

        for x in re.split(r"\s*([;\-/:\(\\?)]|&amp;#13;|\b(?:secretar.a|extensi.n(?:es)?|emergencias?)\b)\s*", s):
            if re.match(r"^("+'|'.join((
                r"de \d+ a \d+ horas?",
                r"de \w a \w de \d+ a \d+ horas?"
            ))+")$", x):
                continue
            no_sp = re_sp.sub(r"", x)
            if len(no_sp) == 0 or (len(no_sp) == 1 and not no_sp.isdecimal()):
                continue
            if no_sp in ("&amp;#13;", "012", "060", "0") or (len(no_sp)>3 and re.match(r"^\D+$", x)):
                continue
            no_sp = re.sub(r"^\s*(00|\+)34\s*", "", no_sp)
            if re.search(r"\D", no_sp):
                logger.warning(f"Telefono mal formado {no_sp} <-- {x} <-- {s}")
                continue
            if len(no_sp) < 9:
                logger.warning(f"Telefono demasiado corto {no_sp} <-- {x} <-- {s}")
                continue
            t = int(no_sp)
            if t not in arr:
                arr.append(t)
    return tuple(arr)


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


def _web(web: str):
    if web is None:
        return tuple()
    web = re.sub(r",?\s+|\s+[oó]\s+", " ", web).strip()
    arr = []
    for w in map(str.lower, web.split()):
        w = re.sub(r"^https?://\s*|[/#\?]+$", "", w)
        if len(w) == 0:
            continue
        if w not in arr:
            arr.append(w)
    return tuple(arr)


def get_latlon(x: int|float, y: int|float) -> LatLon:
    if None in (x, y):
        return None
    if isinstance(x, float) or isinstance(y, float):
        latlon = LatLon(x, y)
    else:
        latlon = utm_to_geo("ED50", 30, x, y)
    if latlon is not None:
        return latlon.round(7)


class CodTxt(NamedTuple):
    cod: str
    txt: str

    @classmethod
    def create_dict(cls, *args: Union["CodTxt", None]):
        k_v: dict[str, set[str]] = defaultdict(set)
        v_k: dict[str, set[str]] = defaultdict(set)
        for a in args:
            if a is None or a.cod is None or a.txt is None:
                continue
            k_v[a.cod].add(a.txt)
            v_k[a.txt].add(a.cod)
        data: dict[str, str] = {}
        for k, vv in k_v.items():
            if len(vv) != 1:
                continue
            v = vv.pop()
            if len(v_k[v]) != 1:
                continue
            data[k] = v
        return MappingProxyType(data)
    
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
        for i, a in enumerate(arr):
            for f, k_v in k_dicts.items():
                v_k = {v: k for k, v in k_v.items()}
                v = a.get(f)
                if isinstance(v, cls) and None in (v.cod, v.txt):
                    a[f] = v._replace(
                        cod=v_k.get(v.txt, v.cod),
                        txt=k_v.get(v.cod, v.txt)
                    )
                arr[i] = a
        return tuple(arr)


class MunCentro(NamedTuple):
    pk: int
    nombre: str
    descripcion_entidad: str
    horario: str
    equipamiento: str
    transporte: str
    descripcion: str
    accesibilidad: tuple[int]
    url: str
    via_nombre: str
    via_clase: str
    via_num_tipo: str
    via_num: str
    via_planta: str
    via_puerta: str
    via_escaleras: str
    via_orientacion: str
    via_localidad: str
    via_provincia: str
    via_codigo_postal: int | None
    via_barrio: CodTxt
    via_distrito: CodTxt
    latlon: LatLon | None
    telefono: tuple[int]
    fax: tuple[int]
    mail: tuple[str]
    tipo: str


class CamCentro(NamedTuple):
    codigo: int
    centro: str
    tipo_cod: str
    tipo_abr: str
    tipo_ext: str
    titularidad: str
    titular: str
    titular_nif: str
    centro_nif: str
    dat: CodTxt
    via_tipo: str
    via_domicilio: str
    via_num: str
    via_codigo_postal: str
    municipio: CodTxt
    distrito: CodTxt
    telefono: tuple[int]
    fax: tuple[int]
    web: tuple[str]
    mail:tuple[str]
    latlon: LatLon | None
    situacion: str
    fecha: str


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
    # Instalaciones accesibles no municipales
    # https://datos.madrid.es/dataset/202180-0-instalaciones-accesibles-no-muni
    MUN_ACCESIBLE = "https://datos.madrid.es/dataset/202180-0-instalaciones-accesibles-no-muni/resource/202180-5-instalaciones-accesibles-no-muni-csv/download/202180-5-instalaciones-accesibles-no-muni-csv.csv"

    def __init__(self):
        self.__s = Session()

    @Cache("cache/opendata/{0}")
    def __get_text(self, name: str, url: str):
        logger.info(f"Descargando {name}")
        r = self.__s.get(url)
        r.encoding = r.apparent_encoding
        return r.text

    def __read_csv(self, name: str, url: str):
        content = self.__get_text(name, url)
        with StringIO(content) as f:
            reader = csv.DictReader(f, delimiter=";")
            lst_rows: list[dict] = []
            for r in reader:
                r = {unidecode(k): _trim(v) for k, v in r.items()}
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
                via_codigo_postal=_codigo_postal(r['CDPOSTAL']),
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
                web=_web(r['WEB']),
                mail=_email(r['E_MAIL'], r['E_MAIL2']),
                latlon=get_latlon(
                    _number(r['UTM_X']),
                    _number(r['UTM_Y'])
                ),
                situacion=r['SITUACION'],
                fecha=r['FECHA CONSTITUCION'],
            )
            centros.add(c)
        return tuple(sorted(centros, key=lambda c: c.codigo))

    def get_mun_accesible(self):
        self.__read_csv("mun_accesible.csv", OpenData.MUN_ACCESIBLE)

    def get_mun_centros(self):
        centros: set[MunCentro] = set()
        for r in _join(
            "num_*.csv",
            self.__read_csv("mun_centros.csv", OpenData.MUN_CENTROS),
            self.__read_csv("mun_artes.csv", OpenData.MUN_ARTES),
            self.__read_csv("mun_eoi.csv", OpenData.MUN_EOI),
            self.__read_csv("mun_sedes_1.csv", OpenData.MUN_SEDES_1),
            self.__read_csv("mun_sedes_2.csv", OpenData.MUN_SEDES_2),
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
                accesibilidad=tuple(map(int, r['ACCESIBILIDAD'].split(","))),
                url=r['CONTENT-URL'],
                via_nombre=r['NOMBRE-VIA'],
                via_clase=r['CLASE-VIAL'],
                via_num_tipo=r['TIPO-NUM'],
                via_num=r['NUM'],
                via_planta=r['PLANTA'],
                via_puerta=r['PUERTA'],
                via_escaleras=r['ESCALERAS'],
                via_orientacion=r['ORIENTACION'],
                via_localidad=r['LOCALIDAD'],
                via_provincia=r['PROVINCIA'],
                via_codigo_postal=_codigo_postal(r['CODIGO-POSTAL']),
                via_barrio=CodTxt(
                    cod=r['COD-BARRIO'],
                    txt=r['BARRIO']
                ),
                via_distrito=CodTxt(
                    cod=r['COD-DISTRITO'],
                    txt=r['DISTRITO'],
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
                mail=_email(r['EMAIL']),
                tipo=tipo,
            )
            centros.add(c)
        centros = set(MunCentro(**d) for d in CodTxt.fix_codes(*(c._asdict() for c in centros)))
        bar_dis: dict[CodTxt, set[CodTxt]] = defaultdict(set)
        for c in centros:
            if c.via_barrio is None or None in (c.via_barrio.cod, c.via_barrio.txt):
                continue
            if c.via_distrito is None or None in (c.via_distrito.cod, c.via_distrito.txt):
                continue
            bar_dis[c.via_barrio].add(c.via_distrito)
        for c in list(centros):
            if c.via_distrito is None or None in (c.via_distrito.cod, c.via_distrito.txt):
                dis = bar_dis.get(c.via_barrio)
                if dis and len(dis) == 1:
                    centros.remove(c)
                    centros.add(c._replace(via_distrito=tuple(dis)[0]))
        return tuple(sorted(centros, key=lambda c: c.pk))

    def get_centros(self):
        cam = self.get_cam_centros()
        mun = self.get_mun_centros()
        print(*sorted(set(c.tipo or '-' for c in mun)), sep="\n")
        for c in mun:
            if c.via_distrito is not None and None in (c.via_distrito.cod, c.via_distrito.txt):
                print(c)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    o = OpenData()
    o.get_centros()