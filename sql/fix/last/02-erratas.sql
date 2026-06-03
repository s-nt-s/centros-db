UPDATE CENTRO SET jornada='C' where id in (
    -- https://site.educa.madrid.org/cp.puertadelasierra.venturada/wp-content/uploads/cp.puertadelasierra.venturada/2025/03/VenturadaJE2526_CEIPS-Puerta-de-la-Sierra_50467727.pdf vía 
    -- https://site.educa.madrid.org/cp.puertadelasierra.venturada/index.php/documentos-de-centro/
    28073872,
    -- https://www.educa2.madrid.org/web/centro.cp.navalafuente -> sección calendario escolar
    28082174
)
;
INSERT OR IGNORE INTO QUERY_CENTRO (centro, query) values
-- https://site.educa.madrid.org/ies.calderondelabar.madrid/index.php/oferta-educativa-3/ turnos
(28020961, 'itRegimenNocturno=4')
;
