from core.api import Api, BulkRequestsApi
from core.centro import BulkRequestsCentro
import argparse
import logging
from core.bulkrequests import BulkRequests
from typing import List, Dict

parser = argparse.ArgumentParser(
    description='Descarga ficheros para la cache',
)
parser.add_argument(
    '--tcp-limit', type=int, default=50
)
parser.add_argument(
    '--centros', action='store_true', help="Descarga las fichas de los centros"
)
parser.add_argument(
    '--busquedas', action='store_true', help="Descarga el resultado de las busquedas"
)

open("dwn.log", "w").close()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("dwn.log"),
        logging.StreamHandler()
    ]
)

ARG = parser.parse_args()
ARG.todo = not (ARG.centros or ARG.busquedas)
API = Api()


def dwn_html(tcp_limit: int = 10):
    BulkRequests(
        tcp_limit=tcp_limit,
        tries=10,
        tolerance=5
    ).run(*(
        BulkRequestsCentro(c.id) for c in API.search_centros()
    ), label="centros")


def dwn_search(tcp_limit: int = 10):
    queries: List[Dict[str, str]] = []
    for k in sorted(API.get_form()['cdGenerico'].keys()):
        queries.append(dict(cdGenerico=k))
    for name, obj in API.get_form().items():
        if Api.is_redundant_parameter(name):
            continue
        for val in sorted(obj.keys()):
            queries.append({name: val})
    for etapas in API.iter_etapas():
        data = {e.name: e.value for e in etapas}
        queries.append(data)
    BulkRequests(
        tcp_limit=tcp_limit,
        tolerance=5
    ).run(*(
        BulkRequestsApi(API, data) for data in queries
    ), label="busquedas")


if ARG.todo or ARG.centros:
    dwn_html(tcp_limit=ARG.tcp_limit)

if ARG.todo or ARG.busquedas:
    dwn_search(tcp_limit=ARG.tcp_limit)