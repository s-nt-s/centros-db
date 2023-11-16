Convierte [gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm](https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm) a una base de datos `sqlite3`.

El resultado actual se puede consular en [`db.sql`](out/db.sql).

![diagrama](out/db.svg)

# FAQ

## ¿Qué es la tabla `QUERY`?

Una `QUERY` es un filtro que se puede hacer en [el buscador de centros](https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm), y la tabla `QUERY_CENTRO` recoge los centros que aparecen como resultado al hacer esa `query`.

## ¿Qué diferencia hay entre la tabla `ETAPA` y `ETAPA_NOMBRE_CENTRO`?

La diferencia entre `ETAPA` y `ETAPA_NOMBRE_CENTRO` deriva de que en [el buscador de centros](https://gestiona.comunidad.madrid/wpad_pub/run/j/BusquedaAvanzada.icm) hay dos maneras (no siempre coincidentes) de obtener las etapas educativas de un centro.

La 1º es seleccionar una etapa educativa y darle a buscar, lo cual presuntamente nos dará como resultados los centros que ofertan esa etapa educativa.

La 2º es entrar en la ficha de un centro y ver en su descripción las etapas educativas que oferta.

La 1º información es la recogida en la tabla `ETAPA` y `ETAPA_CENTRO`.

La 2º información es la recogida en la tabla `ETAPA_NOMBRE_CENTRO`.

Como ya se ha dicho, ambos caminos no siempre dan el mismo resultado. Además, en las fichas aparecen nombres de etapas educativas que no coinciden con las que puedes seleccionar en el buscador.

## ¿Qué es el campo `inferido` de las tablas de etapas educativas?

El buscador muestra la información de las etapas educativas como un árbol de jerarquía, por ejemplo `Educación Personas Adultas -> Educación Secundaria Obligatoria para personas adultas (LOE-LOMCE) -> A distancia`, y por lo tanto sería de presuponer que si un centro `X` aparece buscando por ese filtro también debería aparecer si busco por `Educación Personas Adultas -> Educación Secundaria Obligatoria para personas adultas (LOE-LOMCE)` (pues este filtro es más general que el anterior) pero no siempre es así.

Por lo tanto, `inferido=1` significa que el buscador no dice explícitamente que el centro tiene esa etapa educativa, pero se sobreentiende que si porque tiene alguna de sus componentes.