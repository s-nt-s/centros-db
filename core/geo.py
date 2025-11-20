from requests import Session
from core.cache import Cache
from requests.exceptions import JSONDecodeError
import logging
from shapely.geometry import shape, Polygon, MultiPolygon, Point
from functools import cached_property, cache
from types import MappingProxyType
from enum import Enum
from core.utm_to_geo import LatLon
from shapely.ops import unary_union
from os import environ
from urllib.parse import quote


logger = logging.getLogger(__name__)
MAX_DIFF_CP = 1


class EmptyResponse(Exception):
    pass


SPAIN_PROXY = environ.get("SPAIN_PROXY")


class Distrito(Enum):
    CENTRO = ("Centro",)
    ARGANZUELA = ("Arganzuela")
    RETIRO = ("Retiro",)
    SALAMANCA = ("Salamanca",)
    CHAMARTIN = ("Chamartín",)
    TETUAN = ("Tetuán",)
    CHAMBERI = ("Chamberí",)
    FUENCARRAL = ("Fuencarral - El Pardo", 'Fuencarral-El Pardo')
    MONCLIA = ("Moncloa - Aravaca", 'Moncloa-Aravaca')
    LATINA = ("Latina",)
    CARABANCHEL = ("Carabanchel",)
    USERA = ("Usera",)
    PUESTE_VALLECAS = ("Puente de Vallecas",)
    MORATALAZ = ("Moratalaz",)
    CIUDAD_LINEAL = ("Ciudad Lineal",)
    HORTALEZA = ("Hortaleza",)
    VILLAVERDE = ("Villaverde",)
    VILLA_VALLECAS = ("Villa de Vallecas",)
    VICALVARO = ("Vicálvaro",)
    SAN_BLAS = ("San Blas - Canillejas", 'San Blas-Canillejas')
    BARAJAS = ("Barajas",)

    @classmethod
    def search(cls, name: str):
        if name is None:
            return None
        for m in cls:
            if name in m.value:
                return m
        raise ValueError(f"Distrito not found for name={name}")


class Municipio(Enum):
    AJALVIR = ("Ajalvir", )
    ALAMO_EL = ("Álamo, El", 'El Álamo')
    ALCALA_DE_HENARES = ("Alcalá de Henares", )
    ALCOBENDAS = ("Alcobendas", )
    ALCORCON = ("Alcorcón", )
    ALDEA_DEL_FRESNO = ("Aldea del Fresno", )
    ALGETE = ("Algete", )
    ALPEDRETE = ("Alpedrete", )
    ARANJUEZ = ("Aranjuez", )
    TRES_CANTOS = ("Tres Cantos", )
    ARGANDA_DEL_REY = ("Arganda del Rey", )
    ARROYOMOLINOS = ("Arroyomolinos", )
    BECERRIL_DE_LA_SIERRA = ("Becerril de la Sierra", )
    BELMONTE_DE_TAJO = ("Belmonte de Tajo", )
    BOADILLA_DEL_MONTE = ("Boadilla del Monte", )
    BOALO_EL = ("Boalo, El", 'El Boalo')
    BRUNETE = ("Brunete", )
    BUITRAGO_DEL_LOZOYA = ("Buitrago del Lozoya", )
    BUSTARVIEJO = ("Bustarviejo", )
    CABRERA_LA = ("Cabrera, La", 'La Cabrera')
    CADALSO_DE_LOS_VIDRIOS = ("Cadalso de los Vidrios", )
    CAMARMA_DE_ESTERUELAS = ("Camarma de Esteruelas", )
    CAMPO_REAL = ("Campo Real", )
    CASARRUBUELOS = ("Casarrubuelos", )
    CENICIENTOS = ("Cenicientos", )
    CERCEDILLA = ("Cercedilla", )
    CIEMPOZUELOS = ("Ciempozuelos", )
    COBENA = ("Cobeña", )
    COLMENAR_DE_OREJA = ("Colmenar de Oreja", )
    COLMENAREJO = ("Colmenarejo", )
    COLMENAR_VIEJO = ("Colmenar Viejo", )
    COLLADO_MEDIANO = ("Collado Mediano", )
    COLLADO_VILLALBA = ("Collado Villalba", )
    COSLADA = ("Coslada", )
    CUBAS_DE_LA_SAGRA = ("Cubas de la Sagra", )
    CHINCHÓN = ("Chinchón", )
    DAGANZO_DE_ARRIBA = ("Daganzo de Arriba", )
    ESCORIAL_EL = ("Escorial, El", 'El Escorial')
    ESTREMERA = ("Estremera", )
    FUENLABRADA = ("Fuenlabrada", )
    FUENTE_EL_SAZ_DE_JARAMA = ("Fuente el Saz de Jarama", )
    FUENTIDUEÑA_DE_TAJO = ("Fuentidueña de Tajo", )
    GALAPAGAR = ("Galapagar", )
    GETAFE = ("Getafe", )
    GRINON = ("Griñón", )
    GUADALIX_DE_LA_SIERRA = ("Guadalix de la Sierra", )
    GUADARRAMA = ("Guadarrama", )
    HOYO_DE_MANZANARES = ("Hoyo de Manzanares", )
    HUMANES_DE_MADRID = ("Humanes de Madrid", )
    LEGANES = ("Leganés", )
    LOECHES = ("Loeches", )
    MADRID = ("Madrid", )
    MORALZARZAL = ("Moralzarzal", )
    MAJADAHONDA = ("Majadahonda", )
    MANZANARES_EL_REAL = ("Manzanares el Real", )
    MECO = ("Meco", )
    MEJORADA_DEL_CAMPO = ("Mejorada del Campo", )
    MIRAFLORES_DE_LA_SIERRA = ("Miraflores de la Sierra", )
    MOLAR_EL = ("Molar, El", 'El Molar')
    MOLINOS_LOS = ("Molinos, Los", 'Los Molinos')
    MORALEJA_DE_ENMEDIO = ("Moraleja de Enmedio", )
    MORATA_DE_TAJUÑA = ("Morata de Tajuña", )
    MOSTOLES = ("Móstoles", )
    NAVACERRADA = ("Navacerrada", )
    VALDEMANCO = ("Valdemanco", )
    VALDEQUEMADA = ("Valdemaqueda", )
    VALDEMORILLO = ("Valdemorillo", )
    VALDEMORO = ("Valdemoro", )
    VALDEOLMOS_ALALPARDO = ('Valdeolmos-Alalpardo', )
    VALDILECHA = ('Valdilecha', )
    VALVERDE_DE_ALCALÁ = ('Valverde de Alcalá', )
    VENTURADA = ('Venturada', )
    LOS_BALDIOS = ('Los Baldios', )
    EL_REDEGÜELO = ('El Redegüelo', )
    LA_ACEBEDA = ('La Acebeda', )
    ALAMEDA_DEL_VALLE = ('Alameda del Valle', )
    AMBITE = ('Ambite', )
    ANCHUELO = ('Anchuelo', )
    BERZOSA_DEL_LOZOYA = ('Berzosa del Lozoya', )
    EL_BERRUECO = ('El Berrueco', 'Berrueco, El')
    CABANILLAS_DE_LA_SIERRA = ('Cabanillas de la Sierra', )
    CERVERA_DE_BUITRAGO = ('Cervera de Buitrago', )
    COLMENAR_DEL_ARROYO = ('Colmenar del Arroyo', )
    CORPA = ('Corpa', )
    EL_ATAZAR = ('El Atazar', )
    BATRES = ('Batres', )
    BRAOJOS = ('Braojos', )
    CHAPINERÍA = ('Chapinería', )
    BREA_DE_TAJO = ('Brea de Tajo', )
    FRESNEDILLAS_DE_LA_OLIVA = ('Fresnedillas de la Oliva', )
    FRESNO_DE_TOROTE = ('Fresno de Torote', )
    CANENCIA = ('Canencia', )
    CARABAÑA = ('Carabaña', )
    ORUSCO_DE_TAJUÑA = ('Orusco de Tajuña', )
    PARACUELLOS_DE_JARAMA = ('Paracuellos de Jarama', )
    PARLA = ('Parla', )
    PATONES = ('Patones', )
    PEDREZUELA = ('Pedrezuela', )
    PELAYOS_DE_LA_PRESA = ('Pelayos de la Presa', )
    GARGANTA_DE_LOS_MONTES = ('Garganta de los Montes', )
    GARGANTILLA_DEL_LOZOYA_Y_PINILLA_DE_BUITRAGO = ('Gargantilla del Lozoya y Pinilla de Buitrago', )
    GASCONES = ('Gascones', )
    LA_HIRUELA = ('La Hiruela', )
    HORCAJO_DE_LA_SIERRA_AOSLOS = ('Horcajo de la Sierra-Aoslos', )
    HORCAJUELO_DE_LA_SIERRA = ('Horcajuelo de la Sierra', )
    LOZOYA = ('Lozoya', )
    MADARCOS = ('Madarcos', )
    MONTEJO_DE_LA_SIERRA = ('Montejo de la Sierra', )
    SANTORCAZ = ('Santorcaz', )
    LOS_SANTOS_DE_LA_HUMOSA = ('Los Santos de la Humosa', 'Santos de la Humosa, Los')
    LA_SERNA_DEL_MONTE = ('La Serna del Monte', )
    SERRANILLOS_DEL_VALLE = ('Serranillos del Valle', )
    NAVALAFUENTE = ('Navalafuente', )
    NAVALAGAMELLA = ('Navalagamella', )
    NAVALCARNERO = ('Navalcarnero', )
    NAVARREDONDA_Y_SAN_MAMÉS = ('Navarredonda y San Mamés', )
    NAVAS_DEL_REY = ('Navas del Rey', )
    NUEVO_BAZTÁN = ('Nuevo Baztán', )
    OLMEDA_DE_LAS_FUENTES = ('Olmeda de las Fuentes', )
    PERALES_DE_TAJUÑA = ('Perales de Tajuña', )
    PEZUELA_DE_LAS_TORRES = ('Pezuela de las Torres', )
    PINILLA_DEL_VALLE = ('Pinilla del Valle', )
    PINTO = ('Pinto', )
    PIÑUÉCAR_GANDULLAS = ('Piñuécar-Gandullas', )
    POZUELO_DE_ALARCÓN = ('Pozuelo de Alarcón', )
    POZUELO_DEL_REY = ('Pozuelo del Rey', )
    PRÁDENA_DEL_RINCÓN = ('Prádena del Rincón', )
    PUEBLA_DE_LA_SIERRA = ('Puebla de la Sierra', )
    QUIJORNA = ('Quijorna', )
    RASCAFRÍA = ('Rascafría', )
    REDUEÑA = ('Redueña', )
    RIBATEJADA = ('Ribatejada', )
    RIVAS_VACIAMADRID = ('Rivas-Vaciamadrid', )
    ROBLEDILLO_DE_LA_JARA = ('Robledillo de la Jara', )
    ROBLEDO_DE_CHAVELA = ('Robledo de Chavela', )
    ROBREGORDO = ('Robregordo', )
    LAS_ROZAS_DE_MADRID = ('Las Rozas de Madrid', 'Rozas de Madrid, Las')
    ROZAS_DE_PUERTO_REAL = ('Rozas de Puerto Real', )
    SAN_AGUSTÍN_DEL_GUADALIX = ('San Agustín del Guadalix', )
    SAN_FERNANDO_DE_HENARES = ('San Fernando de Henares', )
    SAN_LORENZO_DE_EL_ESCORIAL = ('San Lorenzo de El Escorial', )
    SAN_MARTÍN_DE_LA_VEGA = ('San Martín de la Vega', )
    SAN_MARTÍN_DE_VALDEIGLESIAS = ('San Martín de Valdeiglesias', )
    SAN_SEBASTIÁN_DE_LOS_REYES = ('San Sebastián de los Reyes', )
    SANTA_MARÍA_DE_LA_ALAMEDA = ('Santa María de la Alameda', )
    SEVILLA_LA_NUEVA = ('Sevilla la Nueva', )
    SOMOSIERRA = ('Somosierra', )
    SOTO_DEL_REAL = ('Soto del Real', )
    TALAMANCA_DE_JARAMA = ('Talamanca de Jarama', )
    TIELMES = ('Tielmes', )
    TITULCIA = ('Titulcia', )
    VALDARACETE = ('Valdaracete', )
    TORREJÓN_DE_ARDOZ = ('Torrejón de Ardoz', )
    TORREJÓN_DE_LA_CALZADA = ('Torrejón de la Calzada', )
    TORREJÓN_DE_VELASCO = ('Torrejón de Velasco', )
    TORRELAGUNA = ('Torrelaguna', )
    TORRELODONES = ('Torrelodones', )
    TORREMOCHA_DE_JARAMA = ('Torremocha de Jarama', )
    TORRES_DE_LA_ALAMEDA = ('Torres de la Alameda', )
    VALDEAVERO = ('Valdeavero', )
    VALDELAGUNA = ('Valdelaguna', )
    VALDEPIÉLAGOS = ('Valdepiélagos', )
    VALDETORRES_DE_JARAMA = ('Valdetorres de Jarama', )
    VELILLA_DE_SAN_ANTONIO = ('Velilla de San Antonio', )
    EL_VELLÓN = ('El Vellón', 'Vellón, El')
    VILLACONEJOS = ('Villaconejos', )
    VILLA_DEL_PRADO = ('Villa del Prado', )
    VILLALBILLA = ('Villalbilla', )
    VILLAMANRIQUE_DE_TAJO = ('Villamanrique de Tajo', )
    VILLAMANTA = ('Villamanta', )
    VILLAMANTILLA = ('Villamantilla', )
    VILLANUEVA_DE_LA_CAÑADA = ('Villanueva de la Cañada', )
    VILLANUEVA_DEL_PARDILLO = ('Villanueva del Pardillo', )
    VILLANUEVA_DE_PERALES = ('Villanueva de Perales', )
    VILLAR_DEL_OLMO = ('Villar del Olmo', )
    VILLAREJO_DE_SALVANÉS = ('Villarejo de Salvanés', )
    VILLAVICIOSA_DE_ODÓN = ('Villaviciosa de Odón', )
    VILLAVIEJA_DEL_LOZOYA = ('Villavieja del Lozoya', )
    ZARZALEJO = ('Zarzalejo', )
    LOZOYUELA_NAVAS_SIETEIGLESIAS = ('Lozoyuela-Navas-Sieteiglesias', )
    PUENTES_VIEJAS = ('Puentes Viejas', )

    @classmethod
    def search(cls, name: str):
        if name is None:
            return None
        for m in cls:
            if name in m.value:
                return m
        raise ValueError(f"Municipio not found for name={name}")


class Geo:
    def __init__(self):
        self.__s = Session()

    @cached_property
    def distritos(self):
        distritos: dict[Distrito, Polygon | MultiPolygon] = {}
        for name, pol in self.__get_poligons_by(
            "https://sigma.madrid.es/hosted/rest/services/CARTOGRAFIA/LIMITES_ADMINISTRATIVOS/MapServer/3/query?where=1%3D1&outFields=*&f=geojson",
            "NOMBRE"
        ).items():
            mun = Distrito.search(name)
            distritos[mun] = pol
        return MappingProxyType(distritos)

    @cached_property
    def municipios(self):
        municipios: dict[Municipio, Polygon | MultiPolygon] = {}
        for name, pol in self.__get_poligons_by(
            "https://sigma.madrid.es/hosted/rest/services/CARTOGRAFIA/LIMITES_ADMINISTRATIVOS/MapServer/4/query?where=1%3D1&outFields=*&f=geojson",
            "NAMEUNIT"
        ).items():
            mun = Municipio.search(name)
            municipios[mun] = pol

        if Municipio.MADRID not in municipios:
            pols = self.distritos.values()
            municipios[Municipio.MADRID] = unary_union(list(pols))
        return MappingProxyType(municipios)

    def __get_json(self, url: str):
        proxies = None
        if url.startswith("https://www.cartociudad.es") and SPAIN_PROXY:
            proxies = {"http": SPAIN_PROXY, "https": SPAIN_PROXY}
        resp = self.__s.get(url, proxies=proxies)
        try:
            return resp.json()
        except JSONDecodeError:
            text = resp.text.strip()
            if len(text) == 0:
                msg = f"[{resp.status_code}] Empty response from {url}"
                logger.critical(msg)
                raise EmptyResponse(msg)
            logger.critical(f"[{resp.status_code}] Invalid JSON from {url} {text}")
            raise

    def __get_dict(self, url: str):
        data = self.__get_json(url)
        if not isinstance(data, dict):
            raise ValueError("{url} = {data}")
        return data

    @cache
    def __get_poligons_by(self, url: str, k: str):
        geo = self.__get_dict(url)
        limits: dict[str, Polygon | MultiPolygon] = {}

        features = geo.get("features")
        if not isinstance(features, list) or len(features) == 0:
            raise ValueError(f"Invalid features={features} from {geo}")

        for feature in features:
            if not isinstance(feature, dict):
                raise ValueError(f"Invalid feature={feature} from {features}")
            props = feature.get("properties")
            if not isinstance(props, dict):
                raise ValueError(f"Invalid properties={props} from {feature}")
            name = props.get(k)
            if not isinstance(name, str):
                raise ValueError(f"Invalid {k}={name} from {props}")
            geometry = feature.get("geometry")
            if not isinstance(geometry, dict):
                raise ValueError(f"Invalid geometry={geometry} from {feature}")
            geom = shape(geometry)
            if not isinstance(geom, (Polygon, MultiPolygon)):
                raise ValueError(f"Invalid shape={geom} from {geometry}")
            if name in limits:
                raise ValueError(f"{k}={name} duplicate")
            limits[name] = geom
        return MappingProxyType(limits)

    @Cache("cache/geo/reverse/{:.5f},{:.5f}.json")
    def get_reverse(self, lat: float, lon: float) -> dict:
        return self.__get_dict(f"https://www.cartociudad.es/geocoder/api/geocoder/reverseGeocode?lon={lon}&lat={lat}")

    def get_cp(self, latlon: LatLon) -> str:
        try:
            data = self.get_reverse(latlon.latitude, latlon.longitude)
        except EmptyResponse:
            return None
        cp = data.get("postalCode")
        if isinstance(cp, str) and cp.isdecimal():
            cp = int(cp)
        if not isinstance(cp, int):
            raise ValueError(f"Invalid postal code: {cp} from {data}")
        return cp

    def is_in(self, latlon: LatLon, cp: str, municipio: str, distrito: str) -> bool:
        if latlon in (None, LatLon(0, 0)):
            return False
        point = Point(latlon.longitude, latlon.latitude)
        dis = Distrito.search(distrito)
        if dis:
            geom = self.distritos[dis]
            return geom.contains(point)
        mun = Municipio.search(municipio)
        if mun:
            geom = self.municipios[mun]
            return geom.contains(point)
        if cp:
            cp_found = self.get_cp(latlon)
            if cp_found:
                if abs(cp_found-cp) > MAX_DIFF_CP:
                    return False
                if cp_found != cp and (municipio, distrito) == (None, None):
                    return False
        return True

    @cache
    def find(self, address: str, cp: int, municipio: str, distrito: str):
        url = "https://www.cartociudad.es/geocoder/api/geocoder/find?q="+quote(address)
        try:
            obj = self.__get_dict(url)
        except EmptyResponse:
            return None
        lat = obj.get("lat")
        lng = obj.get("lng")
        if not isinstance(lat, float) or not isinstance(lng, float) or 0 in (lat, lng):
            raise ValueError(f"lat={lat}, lng={lng} in {obj}")
        latlon = LatLon(lat, lng)
        point = Point(lat, lng)
        dis = Distrito.search(distrito)
        if dis:
            geom = self.distritos[dis]
            if geom.contains(point):
                return latlon
            raise ValueError(f"{latlon} != {dis}")
        mun = Municipio.search(municipio)
        if mun:
            geom = self.municipios[mun]
            if geom.contains(point):
                return latlon
            raise ValueError(f"{latlon} != {mun}")
        muniCode = obj.get('muniCode')
        if isinstance(muniCode, str) and muniCode.isdecimal():
            muniCode = int(muniCode)
        if not isinstance(muniCode, int):
            raise ValueError(f"muniCode={muniCode} in {obj}")
        if abs(muniCode - cp) > MAX_DIFF_CP:
            raise ValueError(f"{muniCode} != {cp}")
        return latlon

    @cache
    def safe_find(self, address: str, cp: int, municipio: str, distrito: str):
        try:
            return self.find(address, cp, municipio, distrito)
        except ValueError:
            return None


GEO = Geo()


if __name__ == "__main__":
    list(GEO.municipios)
