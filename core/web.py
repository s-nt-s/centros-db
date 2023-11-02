import os
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup, Tag
import logging

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")
re_emb = re.compile(r"^image/[^;]+;base64,.*", re.IGNORECASE)
is_s5h = os.environ.get('http_proxy', "").startswith("socks5h://")
if is_s5h:
    proxy_ip, proxy_port = os.environ['http_proxy'].split(
        "//", 1)[-1].split(":")
    proxy_port = int(proxy_port)

default_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Expires": "Thu, 01 Jan 1970 00:00:00 GMT",
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


def get_query(url):
    q = urlsplit(url)
    q = parse_qsl(q.query)
    q = dict(q)
    return q


def iterhref(soup: Tag):
    """Recorre los atriburos href o src de los tags"""
    n: Tag
    for n in soup.findAll(["img", "form", "a", "iframe", "frame", "link", "script", "input"]):
        attr = "href" if n.name in ("a", "link") else "src"
        if n.name == "form":
            attr = "action"
        val = n.attrs.get(attr)
        if val is None or re_emb.search(val):
            continue
        if not (val.startswith("#") or val.startswith("javascript:")):
            yield n, attr, val


def buildSoup(root: str, source: str, parser="lxml"):
    soup = BeautifulSoup(source, parser)
    for n, attr, val in iterhref(soup):
        val = urljoin(root, val)
        n.attrs[attr] = val
    return soup


def get_text(node: Tag, default: str = None):
    if node is None:
        return default
    txt = None
    if node.name == "input":
        txt = node.attrs.get("value")
        if txt is None:
            txt = node.attrs.get("src")
    else:
        txt = node.get_text()
    if txt is None:
        return default
    txt = re_sp.sub(" ", txt).strip()
    if len(txt) == 0:
        return default
    return txt


class Web:
    def __init__(self, refer=None, verify=True):
        self.s = requests.Session()
        self.s.headers = default_headers
        self.response = None
        self.soup = None
        self.form = None
        self.refer = refer
        self.verify = verify

    def _get(self, url, allow_redirects=True, auth=None, **kvargs):
        if kvargs:
            return self.s.post(url, data=kvargs, allow_redirects=allow_redirects, verify=self.verify, auth=auth)
        return self.s.get(url, allow_redirects=allow_redirects, verify=self.verify, auth=auth)

    def get(self, url, auth=None, parser="lxml", **kvargs):
        if self.refer:
            self.s.headers.update({'referer': self.refer})
        self.response = self._get(url, auth=auth, **kvargs)
        self.refer = self.response.url
        self.soup = buildSoup(url, self.response.content, parser=parser)
        return self.soup

    def prepare_submit(self, slc, silent_in_fail=False, **kvargs):
        data = {}
        self.form = self.soup.select_one(slc)
        if silent_in_fail and self.form is None:
            return None, None
        for i in self.form.select("input[name]"):
            name = i.attrs["name"]
            data[name] = i.attrs.get("value")
        for i in self.form.select("select[name]"):
            name = i.attrs["name"]
            slc = i.select_one("option[selected]")
            slc = slc.attrs.get("value") if slc else None
            data[name] = slc
        data = {**data, **kvargs}
        action = self.form.attrs.get("action")
        action = action.rstrip() if action else None
        if action is None:
            action = self.response.url
        return action, data

    def submit(self, slc, silent_in_fail=False, **kvargs):
        action, data = self.prepare_submit(
            slc, silent_in_fail=silent_in_fail, **kvargs)
        if silent_in_fail and not action:
            return None
        return self.get(action, **data)

    def val(self, slc):
        n = self.soup.select_one(slc)
        if n is None:
            return None
        v = n.attrs.get("value", n.get_text())
        v = v.strip()
        return v if v else None

    @property
    def url(self):
        if self.response is None:
            return None
        return self.response.url

    def json(self, url, **kvargs):
        r = self._get(url, **kvargs)
        return r.json()

    def resolve(self, url, **kvargs):
        if self.refer:
            self.s.headers.update({'referer': self.refer})
        r = self._get(url, allow_redirects=False, **kvargs)
        if r.status_code in (302, 301):
            return r.headers['location']

