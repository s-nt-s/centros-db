import sqlite3
from os.path import isfile
from urllib.request import urlretrieve
from functools import cache
import re
from core.filemanager import FM
from requests import Session
from typing import NamedTuple
from textwrap import dedent
import hashlib
from time import sleep


def hash(s: str):
    e = s.encode("utf-8")
    return hashlib.sha256(e).hexdigest()


def trim(s: str | None):
    if s is None:
        return None
    s = s.strip()
    if len(s) == 0:
        return None
    return s


S = Session()


class Info(NamedTuple):
    url: str
    text: str
    error: str
    timestamp: str
    content_type: str
    status_code: int
    links: tuple[str, ...]

    @classmethod
    def build(cls, obj: dict):
        obj = {k: obj.get(k) for k in cls._fields}
        obj['links'] = tuple(obj['links'] or [])
        obj['text'] = trim(obj['text'])
        return cls(**obj)


@cache
def get_db():
    out = "/tmp/centros_db.sqlite"
    if not isfile(out):
        urlretrieve("https://s-nt-s.github.io/centros/db.sqlite", out)
    con = sqlite3.connect(out, uri=True)
    return con


def select(sql: str, *args):
    cursor = get_db().cursor()
    if len(args):
        cursor.execute(sql, args)
    else:
        cursor.execute(sql)
    for r in cursor.fetchall():
        yield r
    cursor.close()


URLS: dict[str, int] = {}
for c, ws in select('''
        select id, web from centro where web is not null and id in (
            select centro from concurso_anexo_centro
        )
    '''):
    ws = re.sub(r"\bwww\.madrid\.es\S+", "", ws).strip()
    if len(ws) == 0:
        continue
    for w in ws.split():
        w = re.sub(r"/+index\.html?$", "", w)
        w = f"https://{w}"
        i = URLS.get(w)
        if i not in (c, None):
            raise ValueError()
        URLS[w] = c


def fetch(*urls):
    urls = list(sorted(urls))
    if len(urls) == 0:
        return None
    data = S.post(
        "http://127.0.0.1:8000/txt",
        json={"url": urls}
    ).json()
    return {k: Info.build(v) for k, v in data.items()}


ROOT = set(URLS.keys())
TO_DO = set(URLS.keys())
DONE: set[str] = set()


def is_ok(link: str):
    if link in DONE:
        return False
    if re.search(r"\.(js|json|css|png|jpe?g|gif)\b", link):
        return False
    if "/wp-json/" in link:
        return False
    link = link.split("://", 1)[-1]
    for u in ROOT:
        u = u.split("://", 1)[-1]
        if len(link) > len(u) and link.startswith(u):
            return True
    return False


def iter_chunks(*args: str, size: int = 100):
    arr: list[str] = []
    for a in sorted(args):
        arr.append(a)
        if len(arr) == size:
            yield tuple(arr)
            arr = []
    if arr:
        yield tuple(arr)


while TO_DO:
    TO_DO = TO_DO.difference(DONE)
    if len(TO_DO) == 0:
        continue
    for chunk in iter_chunks(*TO_DO):
        sleep(5)
        data = fetch(*chunk)
        DONE = DONE.union(data.keys())
        links: set[str] = set()
        for u, i in data.items():
            if i.url in DONE:
                continue
            if i.error:
                print(i.error)
            if not (200 <= i.status_code <= 299):
                continue
            links = links.union(i.links)
            DONE.add(i.url)
            c = URLS[u]
            if u in ROOT:
                ROOT.add(i.url)
            URLS[i.url] = c
            for lk in i.links:
                URLS[lk] = c
            if i.text:
                FM.dump(
                    f"dwn/websites/{c}/{hash(i.url)}.md",
                    dedent(f'''
                    ---
                    centro: {c}
                    url: {i.url}
                    date: {i.timestamp}
                    status: {i.status_code}
                    content_type: {i.content_type}
                    ---
                    ''').strip()+"\n\n"+i.text
                )
            else:
                print(f"[KO] {u}")
        for link in links:
            if is_ok(link):
                print(f"[++] {link}")
                TO_DO.add(link)
