from hashlib import sha1
from typing import Union, Tuple, Any
import functools
import logging
import re

from collections import defaultdict
from types import MappingProxyType
from typing import TypeVar, Callable, Mapping, Optional
from requests import Session, RequestException
import urllib3
urllib3.disable_warnings()


logger = logging.getLogger(__name__)


def hashme(s):
    return sha1(str(s).encode("utf-8")).hexdigest()


def must_one(arr, log_prefix=None):
    arr = set(arr)
    msg = f"{log_prefix or ''} Must one but is".strip()
    if len(arr) == 0:
        raise ValueError(f"{msg} empty")
    if len(arr) > 1:
        raise ValueError(f"{msg} more: "+", ".join(sorted(arr)))
    val = arr.pop()
    if val is None:
        raise ValueError(f"{msg} None")
    return val


def read_file(file: str, *args, **kwargs):
    with open(file, "r") as f:
        txt = f.read().strip()
        txt = txt.format(*args, **kwargs)
        return txt


def to_set_tuple(s: Union[str, list]) -> Tuple[str]:
    if s is None:
        return tuple()
    if isinstance(s, str):
        s = s.split()
    arr = []
    for i in s:
        if i not in arr:
            arr.append(i)
    return tuple(arr)


def tp_join(t: Union[Any, Tuple]):
    if not isinstance(t, tuple):
        return t
    if len(t) == 0:
        return None
    return " ".join(map(str, t))


def fix_char(txt: str):
    if txt is None:
        return None
    for k, v in (
        "ńñ",
        "įá",
        "şª",
        "ŗº",
        "ˇ¡",
        "ķí"
    ):
        txt = txt.replace(k, v)
        txt = txt.replace(k.upper(), v.upper())
    return txt


def logme(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"{func.__name__}()")
        return func(*args, **kwargs)
    return wrapper


def get_abr_dir(w1: str):
    w1 = w1.lower()
    if w1 == "avenida":
        return "Av."
    if w1 == "bulevar":
        return "Blvr."
    if w1 == "calle":
        return "C/"
    if w1 == "callejon":
        return None
    if w1 == "camino":
        return None
    if w1 == "carrera":
        return None
    if w1 == "carretera":
        return "Ctra."
    if w1 == "paraje":
        return None
    if w1 == "parcela":
        return None
    if w1 == "pasaje":
        return None
    if w1 == "paseo":
        return None
    if w1 == "plaza":
        return "Pl."
    if w1 == "ronda":
        return "Rda."
    if w1 == "senda":
        return None
    if w1 == "urbanizacion":
        return "Urb."
    return None


def parse_dir(dire: str):
    if dire is None:
        return None
    dire = dire.strip()
    if len(dire) == 0:
        return None
    rst = dire.split()
    for i, w in enumerate(rst):
        w = w.lower()
        if w in ("de", "del", "la", "el", "lo", "los"):
            rst[i] = w
    rst[0] = get_abr_dir(rst[0]) or rst[0]
    dire = " ".join(rst)
    dire = re.sub(
        r"\b(s/n|c/v)\b",
        lambda x: x.group().upper(),
        dire,
        flags=re.IGNORECASE
    )
    return dire


def unupper(s: str, rstrip=None):
    if s is None:
        return None
    if rstrip:
        s = s.rstrip(rstrip)
    s = s.strip()
    if len(s) == 0:
        return None
    if s.upper() != s:
        return s
    s = s[0] + s[1:].lower()
    s = re.sub(r"españa\b", "España", s)
    s = re.sub(
        r"psicopedagógica\. ([a-z])",
        lambda x: 'psicopedagógica: '+x.group(1).upper(),
        s,
        flags=re.IGNORECASE
    )
    return s


T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


def mk_dict_1_1(
    *args: T,
    get_k: Callable[[T], K | None],
    get_v: Callable[[T], V | None],
) -> Mapping[K, V]:
    k_v: dict[K, set[V]] = defaultdict(set)
    v_k: dict[V, set[K]] = defaultdict(set)

    for a in args:
        k = get_k(a)
        v = get_v(a)
        if k is None or v is None:
            continue
        k_v[k].add(v)
        v_k[v].add(k)

    d: dict[K, V] = {}
    for k, vv in k_v.items():
        if len(vv) != 1:
            continue
        v = vv.pop()
        if len(v_k[v]) != 1:
            continue
        d[k] = v
    return MappingProxyType(d)


def mk_dict_n_1(
    *args: T,
    get_ks: Callable[[T], Tuple[K, ...] | None],
    get_v: Optional[Callable[[T], V | None]] = None,
) -> Mapping[K, V]:
    k_v: dict[K, set[V]] = defaultdict(set)

    for a in args:
        ks = get_ks(a)
        v = get_v(a) if get_v is not None else a
        if ks is None or len(ks) == 0 or v is None:
            continue
        for k in ks:
            k_v[k].add(v)

    d: dict[K, V] = {}
    for k, vv in k_v.items():
        if len(vv) != 1:
            continue
        v = vv.pop()
        d[k] = v
    return MappingProxyType(d)



def find_webs(ori: str):
    if ori is None:
        return tuple()
    web = ori.lower()
    web = re.sub(r",?\s+|\s+[oó]\s+", " ", web).strip()
    web = re.sub(r"^\.+|\.+$", "", web)
    web = re.sub(r"\s+\.com\b", ".com", web)
    web = re.sub(r"\b(https?://www)\s+", r"\1", web)
    if web in ("", "no tenemos", "http://no", "en proceso"):
        return tuple()
    arr = []
    for w in web.split():
        w = re.sub(r"/+index\.html?$", "", w)
        w = re.sub(r"^https?://\s*|[/#\?]+$", "", w)
        if len(w) == 0:
            continue
        if "." not in w:
            logger.warning(f"Web mal formada {w} <-- {web}")
            continue
        url = redirect_if_needed(w)
        if url:
            logger.info(f"{w} redirige a {url}")
            w = url
        if w not in arr:
            arr.append(w)
    return tuple(arr)


def redirect_if_needed(w: str):
    if w not in (
        "iesjuandelacierva.es",
        "www.vmagerit.com",
    ):
        return None
    url = resolve_url(f"https://{w}")
    if url is None:
        return None
    url = url.split("://", 1)[-1]
    url = url.rstrip("/")
    if url != w:
        return url


@functools.cache
def resolve_url(url: str, timeout: float = 10) -> str:
    try:
        with Session() as s:
            r = s.head(
                url,
                verify=False,
                allow_redirects=True,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0"
                },
            )
            return r.url
    except RequestException as e:
        print(e)
        return None
    
if __name__ == "__main__":
    for w in map(str.strip, '''
        site.educa.madrid.org/ies.anafrank.madrid
        site.educa.madrid.org/ies.juanramonjimenez.madrid
        site.educa.madrid.org/ies.becquer.moraleja
        site.educa.madrid.org/cpm.joaquinturina.madrid
        site.educa.madrid.org/cp.cristobalcolon.madrid
        site.educa.madrid.org/cp.honduras.madrid
        site.educa.madrid.org/cp.leopoldoalas.madrid
        iesjuandelacierva.es
        ies.garciamorato.madrid.educa.madrid.org
        www.iessanfernando.com
        www.vmagerit.com
        www.resad.es
        www.ceipciudaddezaragoza.org
        site.educa.madrid.org/ies.vallecasuno.madrid
        www.educa2.madrid.org/web/centro.cp.antoniodenebrija.madrid
        www.educa2.madrid.org/web/centro.ies.juanadecastilla.madrid
        www.educa2.madrid.org/web/centro.cepa.fuencarral.madrid
        www.educa2.madrid.org/web/centro.cp.pinardesanjose.madrid
        www.educa2.madrid.org/web/centro.eei.losgirasoles.madrid
        www.educa2.madrid.org/web/ceip.larioja/inicio
        www.educa2.madrid.org/web/colegio_felipe_2
        www.educa2.madrid.org/web/centro.eoi.embajadores.madrid/portada
    '''.strip().split()):
        url = redirect_if_needed(w)
        if url is not None:
            print(w)
            print(url)
            print("")