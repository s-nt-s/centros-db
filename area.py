from core.dblite import DBLite
import argparse
import logging
import re
from typing import Dict
import json
from collections import defaultdict
from types import MappingProxyType
from shapely import to_geojson
from core.geo import GEO, Municipio
from shapely.geometry import Polygon, MultiPolygon
from core.filemanager import FM
from shapely.ops import unary_union



parser = argparse.ArgumentParser(
    description='Crear ficheros GeoJSON',
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)

ARG = parser.parse_args()

re_sp = re.compile(r"\s+")

open("areas.log", "w").close()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("etapas.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_points():
    done: set[Municipio] = set()
    points: Dict[str, list[Polygon | MultiPolygon]] = defaultdict(list)
    with DBLite(ARG.db, readonly=True) as db:
        for area in db.to_tuple("select distinct area from centro"):
            for municipio, in db.select(
                """
                select
                    distinct municipio
                    from centro where
                    area = ? and
                    municipio is not null
                """,
                area
            ):
                mun = Municipio.search(municipio)
                pol = GEO.municipios[mun]
                points[area].append(pol)
                done.add(mun)
    data: Dict[str, Polygon | MultiPolygon] = {}
    for area, pts in points.items():
        data[area] = unary_union(pts)

    def _find_area(pol: Polygon | MultiPolygon):
        obj: Dict[float, str] = dict()
        for area, p in data.items():
            if p.contains(pol):
                return None
            d = p.distance(pol)
            if d == 0:
                return area
            obj[p.distance(pol)] = area
        return obj[min(obj.keys())]

    for m in Municipio:
        if m not in done:
            pol = GEO.municipios[m]
            area = _find_area(pol)
            if area:
                data[area] = unary_union([data[area], pol])
    return MappingProxyType(data)


POINTS = get_points()
COLORS = {
    "C": "#ffb469",
    "N": "#e41a1c",
    "S": "#377eb8",
    "E": "#4daf4a",
    "O": "#984ea3"
}
NAMES = {
    "C": "Centro",
    "N": "Norte",
    "S": "Sur",
    "E": "Este",
    "O": "Oeste"
}

features = []
for k, pols in POINTS.items():
    # Feature del polígono
    features.append({
        "type": "Feature",
        "geometry": json.loads(to_geojson(pols)),
        "properties": {
            "name": NAMES[k],
            "fill": COLORS[k],
            "stroke": COLORS[k],
        }
    })

geo = {
    "type": "FeatureCollection",
    "features": features
}
FM.dump("out/areas.geojson", geo, indent=0, separators=(',', ':'))
