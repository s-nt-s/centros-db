import sys
import pyproj
from typing import NamedTuple


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


def utm_to_geo(DATUM, HUSO, UTM_X, UTM_Y):
    if HUSO is None or DATUM is None or UTM_X is None or UTM_Y is None:
        return (None, None)
    epsg = get_epsg(DATUM, HUSO)
    if epsg is None:
        return (None, None)
    transformer = pyproj.Transformer.from_crs('epsg:' + str(epsg), 'epsg:4326')
    lat, lon = transformer.transform(UTM_X, UTM_Y)
    return LatLon(
        latitude=lat,
        longitude=lon
    )


if __name__ == "__main__":
    # ej: ED50 30 469656 4481719 = 40.4837353 -3.3593097
    argv = [int(a) if a.isdigit() else a for a in sys.argv[1:]]
    latlon = utm_to_geo(*argv).round(7)
    print(*argv)
    print("=")
    print(latlon)
    print(f"https://www.google.com/maps?q={latlon.latitude},{latlon.longitude}")
