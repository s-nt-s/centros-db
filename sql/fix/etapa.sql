UPDATE ETAPA_CENTRO SET hoja=0;
UPDATE ETAPA_NOMBRE_CENTRO SET hoja=0;
UPDATE ETAPA_CENTRO SET hoja=1 where not exists(
    select
        *
    from
        ETAPA_CENTRO e
    where
        e.centro=ETAPA_CENTRO.centro and
        e.etapa like (ETAPA_CENTRO.etapa || '/%')
)
;
UPDATE ETAPA_NOMBRE_CENTRO SET hoja=1 where not exists(
    select
        *
    from
        ETAPA_NOMBRE_CENTRO e
    where
        e.centro=ETAPA_NOMBRE_CENTRO.centro and
        e.nombre like (ETAPA_NOMBRE_CENTRO.nombre || ' -> %')
)
;