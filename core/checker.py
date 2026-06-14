import smtplib
from dns.resolver import resolve, NoAnswer, NXDOMAIN, LifetimeTimeout, NoNameservers
from dns.exception import Timeout
from core.filemanager import FM
import re
import logging
from functools import cache
from urllib.parse import urlsplit, urlunsplit
from requests import Session, RequestException
import urllib3
from typing import Optional
urllib3.disable_warnings()


logger = logging.getLogger(__name__)

PLAIN_CHAR = {
    "á": "a",
    "é": "e",
    "í": "i",
    "ó": "o",
    "ú": "u"
}

MAIL_KO = (
    'ceiplosjarales@telefonica.net',
    'antonio.martinez.villar@mardid.org',
    'cpmadrid_sur@yahoo.es',
    'eitristras@clece.es',
    'direccion.eilaalameda@grupo5.net',
    'ceipsodali@gmail.com',
    'einaranjo@clece.es',
    'eei.elarquelin@gmail.com',
    'direccion.eieltrebol@grupo5.net',
    'alfar_sec@terra.es',
    'eeilospinos@gmail.com',
    'eeicarricoche@telefonica.net',
    'cpno29@eresmas.com',
    'centro9@centros6.pntic.mec.es',
    'colegiodefresnedillas@hotmail.com',
    'cp.dulcechacon.fuenlabrada@madrid.org',
    'pedro.hernandezgarcia@madrid.org',
    'escuela@escuelaeltomillar.com',
    'eiabetos@clece.es',
    'cpguernica@yahoo.es',
    'almudena.bote@madrid.org',
    'mariasira.delrio@madrid.org',
    'ei.zarabanda@grupo5.net',
    'raquel.victorio@madrid.org',
    'pilar.rodriguez.jimenez@madrid.org',
    'cpdoctora@yahoo.es',
    'sesenayb@centros2.cnice.mecd.es',
    'cp.concepcion.madrid@madrid.org',
    'beatrizgalindo@telefonica.net',
    'rchacel@yahoo.es',
    'fernan11@centros2.cnice.mecd.es',
    'cppiobaroja@cppiobaroja.com',
    'ies.el.pinar@centros5.cnice.mecd.es',
    'amaniel@conservatorioamaniel.com',
    'eeigerardogil@yahoo.es',
    'tirsodemolina@telefonica.net',
    'claudiov@centrosi.cnice.mecd.es',
    'rcajal33@teleline.es',
    'cepa.getafe@madrid.org',
    'herminia.indiano@madrid.org',
    #'amelia.angosto@madrid.org'
    #'cpvirgendelapaz@hotmail.com'
    #'fregacedos@telefonica.net'
    #'jcp97@madrid.org'
)

URL_KO = tuple(map(str.strip, '''
    escueainfantil-lamimosa.com
    www.colegiojarama.com
    www.eduda2.madrid.org/web/centro.cpee.poncedeleon.madrid
    site.educa.madrid.org/ies.anafrank.madrid
    site.educa.madrid.org/ies.juanramonjimenez.madrid
    site.educa.madrid.org/ies.becquer.moraleja
    site.educa.madrid.org/cpm.joaquinturina.madrid
    site.educa.madrid.org/cp.cristobalcolon.madrid
    site.educa.madrid.org/cp.honduras.madrid
    site.educa.madrid.org/cp.leopoldoalas.madrid
    iesjuandelacierva.es
    ies.garciamorato.madrid.educa.madrid.org
    cp.principeasturias.navacerrada.educa.madrid.org
    www.ieshumanejos.com
    cp.machado.colmenarviejo.educa.madrid.org
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
    site.educa.madrid.org/eoi.embajadores.madrid
    www.educa2.madrid.org/web/centro.cp.antoniomorenoro.madrid
    www.educa2.madrid.org/web/centro.cp.colombia.madrid
'''.strip().split()))

MAIL_FIX = {
    'cepa.torrelaguna@educa.madr': 'cepa.torrelaguna@educa.madrid.org',
    'cepa.sansebastian.@educa.madrid.org': 'cepa.sansebastian@educa.madrid.org',
    'cepa.colmenarviejo@educa.ma': 'cepa.colmenarviejo@educa.madrid.org',
}

URL_FIX = {
    "www.educa2.madrid.org/web/ceip.larioja/inicio": "www.educa2.madrid.org/web/ceip.larioja",
    "www.educa2.madrid.org/web/centro.eoi.embajadores.madrid/portada": "www.educa2.madrid.org/web/centro.eoi.embajadores.madrid",
}

re_plain = re.compile(r"[" + "|".join(PLAIN_CHAR.keys()) + "]")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_\.]+\.[\w\-_]+', re.I)


class FileCache:
    def __init__(self, path: str, auto_dump: Optional[int] = None):
        self.__file = FM.resolve_path(path)
        self._data = self.__load()
        self.__auto_dump = auto_dump

    def __load(self) -> dict[str, tuple[str, ...]]:
        if not self.__file.is_file():
            return {}
        data: dict[str, tuple[str, ...]] = {}
        for ln in FM.load_txt(self.__file).strip().split("\n"):
            ln = ln.strip()
            wd = ln.split('\t')
            data[wd[0]] = tuple(wd[1:])
        return data

    def dump(self):
        lines: list[str] = []
        for k, vs in self._data.items():
            lines.append(f"{k}\t" + "\t".join(vs))
        FM.dump(self.__file, "\n".join(lines))

    def __contains__(self, k: str):
        return k in self._data

    def __setitem__(self, k: str, v: tuple[str, ...]):
        self._data[k] = v
        if self.__auto_dump and (len(self._data) % self.__auto_dump == 0):
            self.dump()

    def __getitem__(self, k: str):
        return self._data[k]


class FileKeyCache(FileCache):
    def __setitem__(self, k: str, v: str):
        self._data[k] = (v, )
        self.dump()

    def __getitem__(self, k: str):
        return self._data[k][0]


class MailChecker:
    def __init__(self):
        self.__features = FileCache("cache/mx/features.txt", auto_dump=10)
        self.__mx = FileCache("cache/mx/mx.txt", auto_dump=10)

    @classmethod
    def plain_address(self, a: str):
        return re_plain.sub(lambda m: PLAIN_CHAR[m.group()], a)

    def get_mx_hosts(self, domain: str):
        if domain not in self.__mx:
            self.__mx[domain] = self.__get_mx_hosts(domain)
        return self.__mx[domain]

    def __get_mx_hosts(self, domain: str):
        try:
            answers = resolve(domain, 'MX')
        except (NoAnswer, NXDOMAIN, LifetimeTimeout, NoNameservers) as e:
            logger.critical(f"{domain} {e}")
            return tuple()

        hosts: list[str] = []
        for r in answers:
            host = str(r.exchange).rstrip('.')
            if host and host not in hosts:
                hosts.append(host)
        logger.info(f'{domain}: {", ".join(hosts)}')
        return tuple(hosts)

    def get_features(self, email_or_domain: str):
        try:
            return self.__get_features(email_or_domain)
        except NoAnswer:
            clean = self.plain_address(email_or_domain)
            if clean == email_or_domain:
                raise
            return self.__get_features(clean)

    def __get_features(self, email_or_domain: str):
        domain = email_or_domain.split("@", 1)[-1]
        if domain not in self.__features:
            features: list[str] = []
            mx_hosts = self.get_mx_hosts(domain)
            for mx in mx_hosts:
                for f in self.__ehlo_features(mx):
                    if f not in features:
                        features.append(f)
            self.__features[domain] = tuple(features)
        return self.__features[domain]

    def __ehlo_features(self, mx: str):
        try:
            with smtplib.SMTP(mx, 25, timeout=10) as smtp:
                smtp.ehlo()
                features = tuple(k for k in smtp.esmtp_features.keys() if k != "size")
                logger.info(f'{mx}: {", ".join(features)}')
                return features
        except smtplib.SMTPConnectError as e:
            logger.critical(f"{mx} {e}")
        return tuple()

    def hasSmtpUtf8(self, email_or_domain: str):
        return "smtputf8" in self.get_features(email_or_domain)

    def find_email(self, *args: str | None):
        arr: list[str] = []
        for a in args:
            for e in map(str.lower, re_mail.findall(a or '')):
                clean_e = self.plain_address(e)
                if clean_e != e and not self.hasSmtpUtf8(e):
                    e = clean_e
                e = MAIL_FIX.get(e, e)
                if e not in arr:
                    arr.append(e)
        return tuple(arr)

    def isOk(self, mail: str):
        domain = mail.split("@", 1)[-1]
        mxs = self.get_mx_hosts(domain)
        if len(mxs) == 0:
            return False
        return True


MChecker = MailChecker()


class UrlChecker:
    def __init__(self):
        self.__resolve = FileKeyCache("cache/urls/resolve.txt", auto_dump=1000)

    @cache
    def __is_ok_host(self, host: str):
        for t in ("A", "AAAA", "CNAME", "SOA"):
            try:
                resolve(host, t)
                return True
            except (Timeout, NoAnswer, NoNameservers):
                continue
            except NXDOMAIN:
                return False
        return False

    def plain_url(self, ori: str | None):
        if ori is None:
            return None
        url = ori.strip()
        url = re.sub(r"[/#\?]+$", "", url)
        url = re.sub(r"^(https?:/)(www)", r"\1/\2", url)
        url = re.sub(r"^(https?://)?www\.eduda2\.madrid\.org", r"\1www.educa2.madrid.org", url)
        if len(url) == 0:
            return None
        spl = url.split("://", 1)
        if len(spl) == 1:
            url = f"http://{url}"

        parsed = urlsplit(url)

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        if scheme not in ("http", "https") or not netloc:
            return None
        if scheme == "http" and netloc.endswith(":80"):
            netloc = netloc[:-3]
        elif scheme == "https" and netloc.endswith(":443"):
            netloc = netloc[:-4]
        path = parsed.path
        path = re.sub(r"//+", "/", path)
        path = re.sub(r"/+index\.html?$", "", path)
        path = path.rstrip("/")

        normalized = urlunsplit((scheme, netloc, path, '', ''))
        prefix, name = normalized.split("://", 1)
        normalized = prefix + "://" + URL_FIX.get(name, name)
        return normalized

    def __domain_redirect(self, ori: str):
        parsed = urlsplit(ori)
        if self.__is_ok_host(parsed.netloc):
            return ori
        m = re.match(r"https?://([a-z\.]+)\.educa\.madrid\.org$", ori)
        if m:
            try_url = f"https://www.educa2.madrid.org/web/centro.{m.group(1)}"
            new_url = self.resolve_url(try_url)
            if new_url is not None:
                logger.info(f"{ori} remplazado por a {new_url}")
                return new_url
        return None

    def get_real_target(self, ori: str):
        if ori not in self.__resolve:
            self.__resolve[ori] = self.__get_real_target(ori)
        return self.__resolve[ori]

    @cache
    def __get_real_target(self, ori: str):
        new_url = self.__domain_redirect(ori)
        if new_url is None:
            logger.critical(f"DOMINIO no encontrado para {ori}")
            return ori
        w = new_url.split("://", 1)[-1]
        if w not in (
            "iesjuandelacierva.es",
            "www.vmagerit.com",
            "www.educa2.madrid.org/web/centro.eoi.embajadores.madrid"
        ):
            return new_url
        url = self.resolve_url(new_url)
        if url is None:
            return new_url
        return url

    def find_urls(self, ori: str) -> tuple[str, ...]:
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
            w = self.plain_url(w)
            if w is None:
                continue
            new_url = self.get_real_target(w) or w
            w = re.sub(r"^https?://|/+$", "", w)
            new_url = re.sub(r"^https?://|/+$", "", new_url)
            if new_url != w:
                logger.info(f"{w} redirige a {new_url}")
                w = new_url
            if w not in arr:
                arr.append(w)
        return tuple(arr)

    def __rqs(self, session: Session, method: str, url: str, timeout: float = 10):
        return session.request(
            method=method,
            url=url,
            verify=False,
            allow_redirects=True,
            timeout=timeout,
        )

    @cache
    def resolve_url(self, url: str) -> str | None:
        done: set[str] = set()
        name = url.split("://", 1)[-1].rstrip("/")
        checkBody = name in (
            "www.educa2.madrid.org/web/centro.eoi.embajadores.madrid",
        )
        method = "GET" if checkBody else "HEAD"
        try:
            with Session() as s:
                done.add(url)
                r = self.__rqs(
                    session=s,
                    method=method,
                    url=url
                )
                if r.status_code == 404:
                    return None
                done.add(r.url)
                new_url = r.url
                if checkBody:
                    m = re.search(
                        r'<script type="text/javascript">\s*window\.location\.href\s*=\s*"(https?://[^"]+)"\s*;\s*</script>',
                        r.text
                    )
                    if m:
                        new_url = m.group(1)
                if not isinstance(new_url, str):
                    return None
                if new_url not in done:
                    new_url = self.resolve_url(new_url) or new_url
                return new_url
        except RequestException as e:
            logger.critical(str(e), exc_info=e)
            return None


UChecker = UrlChecker()

if __name__ == "__main__":
    import sys
    import logging
    logging.basicConfig(level=logging.INFO)
    KO = (k for k in sys.argv[1:] if k not in URL_KO)
    for m in (sys.argv[1:] or URL_KO):
        for u in UChecker.find_urls(m):
            pass
