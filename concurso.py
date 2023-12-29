from core.api import Api
from core.dblite import DBLite
import argparse
import logging

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

YEAR = '2023-2024'
CON_MAE = 'https://www.comunidad.madrid/servicios/educacion/concurso-traslados-maestros'
CON_PRO = 'https://www.comunidad.madrid/servicios/educacion/concurso-traslados-profesores-secundaria-formacion-profesional-regimen-especial'

CON = {
    ('MAE', 'Concurso de traslados de Maestros 2023-2024'): CON_MAE,
    ('PRO', 'Concurso de traslados de Profesores de Secundaria, Formación Profesional y Régimen Especial 2023-2024'): CON_PRO
}

ANX = {
    ('MAE',  3): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo03_ceip.pdf',
    ('MAE',  4): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo04_eso.pdf',
    ('MAE',  5): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo05_british.pdf',
    ('MAE',  6): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo06_bil.pdf',
    ('MAE',  7): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo07_cepa.pdf',
    ('MAE',  8): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo08_cepacp.pdf',
    ('MAE',  9): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo09_eeii.pdf',
    ('MAE', 10): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo10.pdf',
    ('MAE', 11): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo11_tgd.pdf',
    ('MAE', 12): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo12_educ_esp.pdf',
    ('MAE', 13): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_mae_anexo13_compensatoria.pdf',
    ('PRO', 15): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_15_anexo.pdf',
    ('PRO', 16): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_16_anexo.pdf',
    ('PRO', 17): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_17_anexo.pdf',
    ('PRO', 18): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_18_anexo.pdf',
    ('PRO', 19): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_19_anexo.pdf',
    ('PRO', 20): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_20_anexo.pdf',
    ('PRO', 21): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_21_anexo.pdf',
    ('PRO', 22): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_22_anexo.pdf',
    ('PRO', 23): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_23_anexo.pdf',
    ('PRO', 24): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_24_anexo.pdf',
    ('PRO', 25): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_25_anexo.pdf',
    ('PRO', 26): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_26_anexo.pdf',
    ('PRO', 27): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_27_anexo.pdf',
    ('PRO', 28): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_28_anexo.pdf',
    ('PRO', 29): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_29_anexo.pdf',
    ('PRO', 30): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_30_anexo.pdf',
    ('PRO', 31): 'https://www.comunidad.madrid/sites/default/files/doc/educacion/rh01/rh01_1896_2324_sec_31_anexo.pdf'
}


def check_data(db: DBLite, data, sql):
    for (con, aux), url in data.items():
        if 1 != db.one('select count(*) from '+sql, con, aux, url):
            raise Exception("El concurso de traslados necesita ser revisado")


def get_pro_anx(db: DBLite):
    CA = {
        'secundaria': (15, 16, 17, 18, 19, 20, 21, 25),
        'fp': (15, 18, 21, 26),
        'eoi': (22, 27),
        'musica': (23, 28, 29),
        'diseno': (24, 30, 31),
    }
    OTR_ANX = set(db.to_tuple(
        "select anexo from CONCURSO_ANEXO where concurso='PRO'"
    ))
    for anx in CA.values():
        OTR_ANX.difference_update(anx)
    for c, a in list(CA.items()):
        CA[c] = tuple(sorted(OTR_ANX.union(a)))
    return CA


def set_cuerpo(db: DBLite):
    check_data(db, CON, 'CONCURSO where id=? and txt=? and url=?')
    check_data(db, ANX, 'CONCURSO_ANEXO where concurso=? and anexo=? and url=?')

    db.execute('''
INSERT INTO CONCURSO (url, cuerpo, id, txt) VALUES
('{MAE}', '0597',           'magisterio', '{YEAR} Magisterio'),
('{PRO}', '0590 0511',      'secundaria', '{YEAR} Enseñanza Secundaria'),
('{PRO}', '0598 0591',      'fp',         '{YEAR} Formación Profesional'),
('{PRO}', '0592 0512',      'eoi',        '{YEAR} Escuelas Oficiales de Idiomas'),
('{PRO}', '0594 0593',      'musica',     '{YEAR} Música y Artes Escénica'),
('{PRO}', '0596 0595 0513', 'diseno',     '{YEAR} Artes Plásticas y Diseño')
;
    '''.format(YEAR=YEAR, PRO=CON_PRO, MAE=CON_MAE))
    db.execute('''
        INSERT INTO CONCURSO_ANEXO (concurso, anexo, txt, url)
        select 'magisterio' concurso, anexo, txt, url
        from CONCURSO_ANEXO where concurso='MAE';
        UPDATE CONCURSO_ANEXO_CENTRO SET concurso='magisterio' where concurso='MAE';
        DELETE FROM CONCURSO_ANEXO where concurso='MAE';
        DELETE FROM CONCURSO where id='MAE';
    ''')
    sql_delete = []
    for con, anx in get_pro_anx(db).items():
        db.execute('''
            INSERT INTO CONCURSO_ANEXO (concurso, anexo, txt, url)
            select '{con}' concurso, anexo, txt, url
            from CONCURSO_ANEXO where
                concurso='PRO' and
                anexo in {anx};
            INSERT INTO CONCURSO_ANEXO_CENTRO (concurso, anexo, centro)
            select '{con}' concurso, anexo, centro
            from CONCURSO_ANEXO_CENTRO where
                concurso='PRO' and
                anexo in {anx};
        '''.format(con=con, anx=anx))
        sql_delete.append('''
            DELETE FROM CONCURSO_ANEXO_CENTRO where
                concurso='PRO' and
                anexo in {anx};
            DELETE FROM CONCURSO_ANEXO where
                concurso='PRO' and
                anexo in {anx};
        '''.format(anx=anx))
    sql_delete.append("DELETE FROM CONCURSO where id='PRO';")
    db.execute("\n".join(sql_delete))


if __name__ == "__main__":
    with DBLite(ARG.db) as db:
        set_cuerpo(db)

    DBLite.do_sql_backup(ARG.db)
