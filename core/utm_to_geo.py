import sys
import pyproj
from typing import NamedTuple
from core.filemanager import FM
import re

ELIPSOIDE = "WGS84"


class LatLon(NamedTuple):
    latitude: float
    longitude: float

    def round(self, x: int):
        return LatLon(
            latitude=round(self.latitude, x),
            longitude=round(self.longitude, x)
        )


def get_epsg(datum, huso):
    if huso is None:
        return None
    if datum == "ED50":
        if huso >= 28 and huso <= 38:
            return 23000+huso
        return None
    if datum == "ETRS89":
        if huso >= 28 and huso <= 38:
            return 25800 + huso
        if huso == 27:
            return 4082  # REGCAN95 27
        return None
    if datum == "WGS84":
        if huso == 27:
            return 32627
        if huso == 28:
            return 32628
        if huso == 29:
            return 32629
        if huso == 30:
            return 32630
        if huso == 31:
            return 32631
        return None
    if datum == "REGCAN95":
        if huso == 27:
            return 4082
        if huso == 28:
            return 4083
        return None
    return None


class UtmToGeo:
    def __init__(self, datum: str, huso: int):
        self.__datum = datum
        self.__huso = huso
        self.__file = FM.resolve_path(f"cache/utm_to_geo_{self.__datum}_{self.__huso}.txt")
        self.__cache: dict[tuple[int, int], tuple[float, float]] = {}
        self.__load()

    def __load(self):
        self.__cache: dict[tuple[int, int], tuple[float, float]] = {}
        if self.__file.is_file():
            with open(self.__file, "r") as f:
                for line in f:
                    x, y, lat, lon = re.sub(r"\.0\b", "", line.strip()).split("\t")
                    self.__cache[(int(x), int(y))] = (float(lat), float(lon))

    def save(self):
        with open(self.__file, "w") as f:
            for (x, y), (lat, lon) in self.__cache.items():
                f.write(f"{x}\t{y}\t{lat}\t{lon}\n")

    def __file(self):
        return FM.resolve_path(f"cache/utm_to_geo_{self.__datum}_{self.__huso}.txt")
    
    def to_geo(self, utm_x: int, utm_y: int) -> LatLon:
        if (utm_x, utm_y) in self.__cache:
            lat, lon = self.__cache[(utm_x, utm_y)]
            return LatLon(latitude=lat, longitude=lon)
        epsg = get_epsg(self.__datum, self.__huso)
        if epsg is None:
            return None
        transformer = pyproj.Transformer.from_crs('epsg:' + str(epsg), 'epsg:4326')
        lat, lon = transformer.transform(utm_x, utm_y)
        self.__cache[(utm_x, utm_y)] = (lat, lon)
        if len(self.__cache) % 100 == 0:
            self.save()
        return LatLon(latitude=lat, longitude=lon)

UTM_TO_GEO = UtmToGeo("ED50", 30)


if __name__ == "__main__":
    # ej: ED50 30 469656 4481719 = 40.4837353 -3.3593097
    argv = [int(a) if a.isdigit() else a for a in sys.argv[1:]]
    latlon = UTM_TO_GEO.to_geo(*argv).round(7)
    print(*argv)
    print("=")
    print(latlon)
    print(f"https://www.google.com/maps?q={latlon.latitude},{latlon.longitude}")
