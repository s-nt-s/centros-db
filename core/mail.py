import smtplib
from dns.resolver import resolve, NoAnswer
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

re_plain = re.compile(r"[" + "|".join(PLAIN_CHAR.keys()) + "]")
re_mail = re.compile(r'[\w\.\-_]+@[\w\-_\.]+\.[\w\-_]+', re.I)


class MailChecker:
    def __init__(self):
        self.__file_cache = FM.resolve_path("cache/mx/features.txt")
        self.__cache: dict[str, tuple[str, ...]] = self.__load()

    @classmethod
    def plain_address(self, a: str):
        return re_plain.sub(lambda m: PLAIN_CHAR[m.group()], a)

    def __load(self):
        if not self.__file_cache.is_file():
            return {}
        data: dict[str, tuple[str, ...]] = {}
        for ln in FM.load_txt(self.__file_cache).strip().split("\n"):
            ln = ln.strip()
            wd = ln.split('\t')
            data[wd[0]] = tuple(wd[1:])
        return data

    def save(self):
        lines: list[str] = []
        for k, vs in self.__cache.items():
            lines.append(f"{k}\t" + "\t".join(vs))
        FM.dump(self.__file_cache, "\n".join(lines))

    def get_mx_hosts(self, domain):
        answers = resolve(domain, 'MX')

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
        if domain not in self.__cache:
            features: list[str] = []
            mx_hosts = self.get_mx_hosts(domain)
            for mx in mx_hosts:
                for f in self.__ehlo_features(mx):
                    if f not in features:
                        features.append(f)
            self.__cache[domain] = tuple(features)
            self.save()
        return self.__cache[domain]

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
                if e not in arr:
                    arr.append(e)
        return tuple(arr)


MChecker = MailChecker()
