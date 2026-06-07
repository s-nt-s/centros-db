import smtplib
from dns.resolver import resolve, NoAnswer, NXDOMAIN, LifetimeTimeout, NoNameservers
from core.filemanager import FM
import re
import logging

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
MAIL_FIX = {
    'cepa.torrelaguna@educa.madr': 'cepa.torrelaguna@educa.madrid.org',
    'cepa.sansebastian.@educa.madrid.org': 'cepa.sansebastian@educa.madrid.org',
}

re_plain = re.compile(r"[" + "|".join(PLAIN_CHAR.keys()) + "]")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_\.]+\.[\w\-_]+', re.I)


class FileCache:
    def __init__(self, path: str):
        self.__file = FM.resolve_path(path)
        self.__data = self.__load()

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
        for k, vs in self.__data.items():
            lines.append(f"{k}\t" + "\t".join(vs))
        FM.dump(self.__file, "\n".join(lines))

    def __contains__(self, k: str):
        return k in self.__data

    def __setitem__(self, k: str, v: tuple[str, ...]):
        self.__data[k] = v
        self.dump()

    def __getitem__(self, k: str):
        return self.__data[k]


class MailChecker:
    def __init__(self):
        self.__features = FileCache("cache/mx/features.txt")
        self.__mx = FileCache("cache/mx/mx.txt")

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


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    for m in MAIL_KO:
        if MChecker.isOk(m) is False:
            print(m)
