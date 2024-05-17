from core.api import Api
from core.dblite import DBLite, dict_factory
import argparse
import logging
from core.concurso import Concursazo, Concursillo
from typing import Dict, Tuple

parser = argparse.ArgumentParser(
    description='Añade la información sobre los cuerpos',
)
parser.add_argument(
    '--db', type=str, default="out/db.sqlite"
)

ARG = parser.parse_args()
API = Api()
KWV = {}
LAST_TUNE = "sql/fix/last"
DONE = set()

open("cuerpo.log", "w").close()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("cuerpo.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

ILLO_YEAR = '2023-2024'
ILLO = {
    (Concursillo.MAE, 'Maestros: Asignación de destinos provisionales en inicio de curso (2023-2024)'): 'https://www.comunidad.madrid/servicios/educacion/maestros-asignacion-destinos-provisionales-inicio-curso',
    (Concursillo.PRO, 'Secundaria, FP y RE: Asignación de destinos provisionales en inicio de curso (2023-2024)'): 'https://www.comunidad.madrid/servicios/educacion/secundaria-fp-re-asignacion-destinos-provisionales-inicio-curso'
}

CON_YEAR = '2023-2024'
CON_MAE = 'https://www.comunidad.madrid/servicios/educacion/concurso-traslados-maestros'
CON_PRO = 'https://www.comunidad.madrid/servicios/educacion/concurso-traslados-profesores-secundaria-formacion-profesional-regimen-especial'

CON = {
    (Concursazo.MAE, 'Concurso de traslados de Maestros 2023-2024'): CON_MAE,
    (Concursazo.PRO, 'Concurso de traslados de Profesores de Secundaria, Formación Profesional y Régimen Especial 2023-2024'): CON_PRO
}

ANX = {
    (Concursazo.MAE,  3): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo03_ceip.pdf',
    (Concursazo.MAE,  4): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo04_eso.pdf',
    (Concursazo.MAE,  5): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo05_british.pdf',
    (Concursazo.MAE,  6): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo06_bil.pdf',
    (Concursazo.MAE,  7): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo07_cepa.pdf',
    (Concursazo.MAE,  8): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo08_cepacp.pdf',
    (Concursazo.MAE,  9): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo09_eeii.pdf',
    (Concursazo.MAE, 10): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo10.pdf',
    (Concursazo.MAE, 11): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo11_tgd.pdf',
    (Concursazo.MAE, 12): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo12_educ_esp.pdf',
    (Concursazo.MAE, 13): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo13_compensatoria.pdf',
    (Concursazo.PRO, 15): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_15_anexo.pdf',
    (Concursazo.PRO, 16): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_16_anexo.pdf',
    (Concursazo.PRO, 17): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_17_anexo.pdf',
    (Concursazo.PRO, 18): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_18_anexo.pdf',
    (Concursazo.PRO, 19): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_19_anexo.pdf',
    (Concursazo.PRO, 20): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_20_anexo.pdf',
    (Concursazo.PRO, 21): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_21_anexo.pdf',
    (Concursazo.PRO, 22): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_22_anexo.pdf',
    (Concursazo.PRO, 23): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_23_anexo.pdf',
    (Concursazo.PRO, 24): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_24_anexo.pdf',
    (Concursazo.PRO, 25): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_25_anexo.pdf',
    (Concursazo.PRO, 26): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_26_anexo.pdf',
    (Concursazo.PRO, 27): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_27_anexo.pdf',
    (Concursazo.PRO, 28): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_28_anexo.pdf',
    (Concursazo.PRO, 29): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_29_anexo.pdf',
    (Concursazo.PRO, 30): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_30_anexo.pdf',
    (Concursazo.PRO, 31): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_31_anexo.pdf'
}


def check_data(db: DBLite, data, sql):
    for (con, aux), url in data.items():
        if 1 != db.one('select count(*) from '+sql, con, aux, url):
            raise Exception("El concurso de traslados necesita ser revisado")


def get_pro_anx(db: DBLite) -> Dict[str, Tuple[int]]:
    CA = {
        'secundaria': (15, 16, 17, 18, 19, 20, 21, 25),
        'fp': (15, 18, 21, 26),
        'eoi': (22, 27),
        'musica': (23, 28, 29),
        'diseno': (24, 30, 31),
    }
    OTR_ANX = set(db.to_tuple(
        f"select anexo from CONCURSO_ANEXO where concurso='{Concursazo.PRO}'"
    ))
    for anx in CA.values():
        OTR_ANX.difference_update(anx)
    for c, a in list(CA.items()):
        CA[c] = tuple(sorted(OTR_ANX.union(a)))
    return CA


def set_cuerpo(db: DBLite):
    check_data(db, CON, f"CONCURSO where id=? and txt=? and url=? and convocatoria='{CON_YEAR}'")
    check_data(db, ANX, 'CONCURSO_ANEXO where concurso=? and anexo=? and url=?')
    check_data(db, ILLO, f"CONCURSO where id=? and txt=? and url=? and convocatoria='{ILLO_YEAR}'")

    db.execute(f'''
INSERT INTO CONCURSO (convocatoria, tipo, url, cuerpo, id, txt) VALUES
('{CON_YEAR}', 'concurso', '{CON_MAE}', '0597',           'magisterio', 'Magisterio'),
('{CON_YEAR}', 'concurso', '{CON_PRO}', '0590 0511',      'secundaria', 'Secundaria'),
('{CON_YEAR}', 'concurso', '{CON_PRO}', '0598 0591',      'fp',         'Formación Profesional'),
('{CON_YEAR}', 'concurso', '{CON_PRO}', '0592 0512',      'eoi',        'Escuelas Oficiales de Idiomas'),
('{CON_YEAR}', 'concurso', '{CON_PRO}', '0594 0593',      'musica',     'Música y Artes Escénica'),
('{CON_YEAR}', 'concurso', '{CON_PRO}', '0596 0595 0513', 'diseno',     'Artes Plásticas y Diseño')
;
    ''')
    db.execute(f'''
        INSERT INTO CONCURSO_ANEXO (concurso, anexo, txt, url)
        select 'magisterio' concurso, anexo, txt, url
        from CONCURSO_ANEXO where concurso='{Concursazo.MAE}';
        UPDATE CONCURSO_ANEXO_CENTRO SET concurso='magisterio' where concurso='{Concursazo.MAE}';
        DELETE FROM CONCURSO_ANEXO where concurso='{Concursazo.MAE}';
        DELETE FROM CONCURSO where id='{Concursazo.MAE}';
    ''')
    sql_delete = []
    for con, anx in get_pro_anx(db).items():
        db.execute(f'''
            INSERT INTO CONCURSO_ANEXO (concurso, anexo, txt, url)
            select '{con}' concurso, anexo, txt, url
            from CONCURSO_ANEXO where
                concurso='{Concursazo.PRO}' and
                anexo in {anx};
            INSERT INTO CONCURSO_ANEXO_CENTRO (concurso, anexo, centro)
            select '{con}' concurso, anexo, centro
            from CONCURSO_ANEXO_CENTRO where
                concurso='{Concursazo.PRO}' and
                anexo in {anx};
        ''')
        sql_delete.append(f'''
            DELETE FROM CONCURSO_ANEXO_CENTRO where
                concurso='{Concursazo.PRO}' and
                anexo in {anx};
            DELETE FROM CONCURSO_ANEXO where
                concurso='{Concursazo.PRO}' and
                anexo in {anx};
        ''')
    sql_delete.append(f"DELETE FROM CONCURSO where id='{Concursazo.PRO}';")
    db.execute("\n".join(sql_delete))

    db.execute(f'''
        UPDATE CONCURSO SET txt='Magisterio' where id='{Concursillo.MAE}';
        UPDATE CONCURSO SET txt='Secundaria y FP' where id='{Concursillo.PRO}';
    ''')

    rm_cuerpos = set()
    rm_centros = set()
    CRP = {
        "diseno": ("Artes Plásticas y Diseño", '0596 0595 0513', ('103', '120', '106'), ()),
        "eoi": ("Escuelas Oficiales de Idiomas", '0592 0512', ('080', '081'), ()),
        "musica": ("Música y Artes Escénica", '0594 0593', ("152", "132", "171", "180"), ("016", "017", "151", "131"))
    }
    url, cuerpos = db.one(f"select url, cuerpo from CONCURSO where id='{Concursillo.PRO}'")
    for con, (txt, cps, rm_tps, tps) in CRP.items():
        cnt = db.to_tuple(f"select id from centro where tipo in {rm_tps+tps} and id in (select centro from CONCURSO_ANEXO_CENTRO where concurso='{Concursillo.PRO}')")
        if len(cnt) == 0:
            continue
        rm_centros = rm_centros.union(db.to_tuple(f"select id from centro where tipo in {rm_tps} and id in (select centro from CONCURSO_ANEXO_CENTRO where concurso='{Concursillo.PRO}')"))
        rm_cuerpos = rm_cuerpos.union(cps.split())
        cid = f'concursillo-{con}'
        db.execute(f'''
            INSERT INTO CONCURSO (convocatoria, tipo, url, cuerpo, id, txt) VALUES
            ('{ILLO_YEAR}', 'concursillo', '{url}', '{cps}', '{cid}', '{txt}')
            ;
        ''')
        db.execute(f'''
            INSERT INTO CONCURSO_ANEXO (concurso, anexo, txt, url)
            select '{cid}' concurso, anexo, txt, url
            from CONCURSO_ANEXO where concurso='{Concursillo.PRO}';
            INSERT INTO CONCURSO_ANEXO_CENTRO (concurso, anexo, centro)
            select '{cid}' concurso, anexo, centro
            from CONCURSO_ANEXO_CENTRO where
                concurso='{Concursillo.PRO}' and
                centro in {cnt};
            ''')
    if rm_cuerpos:
        db.execute(f'''
            DELETE FROM CONCURSO_ANEXO_CENTRO where
                concurso='{Concursillo.PRO}' and
                centro in {tuple(sorted(rm_cuerpos))};
        ''')
    cuerpos = " ".join(sorted(set(cuerpos.split()).difference(rm_cuerpos)))
    db.execute(f"UPDATE CONCURSO set cuerpo='{cuerpos}' where id='{Concursillo.PRO}'")


if __name__ == "__main__":
    with DBLite(ARG.db) as db:
        set_cuerpo(db)

    DBLite.do_sql_backup(ARG.db)
