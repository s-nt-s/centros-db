from requests import Session
from core.cache import Cache
from requests.exceptions import JSONDecodeError
import logging

logger = logging.getLogger(__name__)


class Geo:
    def __init__(self):
        self.__s = Session()

    def __get_json(self, url: str):
        resp = self.__s.get(url)
        try:
            return resp.json()
        except JSONDecodeError:
            logger.critical(f"[{resp.status_code}] Invalid JSON from {url} {resp.text}")
            raise

    def __get_dict(self, url: str):
        data = self.__get_json(url)
        if not isinstance(data, dict):
            raise ValueError("{url} = {data}")
        return data

    @Cache("cache/geo/reverse/{:.5f},{:.5f}.json")
    def get_reverse(self, lat: float, lon: float) -> dict:
        return self.__get_dict(f"https://www.cartociudad.es/geocoder/api/geocoder/reverseGeocode?lon={lon}&lat={lat}")

    def get_cp(self, lat: float, lon: float) -> str:
        data = self.get_reverse(lat, lon)
        cp = data.get("postalCode")
        if isinstance(cp, int):
            return cp
        if not isinstance(cp, str) or not cp.isdecimal():
            raise ValueError(f"Invalid postal code: {cp} from {data}")
        return int(cp)

GEO = Geo()
