# Retos del workshop — Equinoccio Móvil

Los retos obligan a **modificar** lo que se construyó, no solo a ejecutarlo. Ningún
reto bloquea a los siguientes: si uno no termina a tiempo, continúe.

Las pistas están ordenadas de menos a más explícitas. Lea una a la vez.

## Durante el taller

| # | Módulo | Reto | Tiempo |
|---:|---|---|---:|
| 5 | AI/BI Dashboard | Mapa de sitios de Ecuador | 12 min |
| 6 | Genie | Cazar y corregir una respuesta | 7 min |

## Para después del taller

En el taller, los módulos 01 a 06 se ejecutan juntos con `00b_preparar_todo`. Estos
retos le permiten recorrerlos uno por uno con calma. Abra el notebook del módulo,
léalo y luego resuelva el reto.

| # | Módulo | Notebook | Reto |
|---:|---|---|---|
| 1 | Unity Catalog | `01_unity_catalog` | Detective de gobierno |
| 2 | Lakeflow SDP | `03_pipeline` | Endurecer una expectation |
| 3 | DQX | `04_dqx_quality` | De advertencia a error |
| 4 | Metric Views | `06_metric_views` | Una medida nueva |
| 7 | Databricks App | `09_app` | Filtro de región natural |
| 8 | Caso de negocio | — | ¿Dónde invertir este trimestre? |

---

## Reto 1 — Detective de gobierno (Unity Catalog)

Responda usando **solo Catalog Explorer**, sin SQL:

1. ¿Quién es el owner de su schema `z2h_<usuario>`?
2. ¿Qué tipo de volume es `raw_data`: managed o external?
3. Abra `cell_tower_metrics_raw_checkpoint` → **Lineage**. ¿Qué job la creó?

<details><summary>Pista</summary>

El owner y el tipo están en la pestaña **Overview/Details**. En **Lineage** revise
la sección de jobs o notebooks relacionados; aparece cuando el job ya terminó.
</details>

## Reto 2 — Endurecer una expectation (Lakeflow SDP)

El negocio decide que una latencia mayor a **120 ms** ya es una anomalía.

1. En `pipeline/transformations/telco_pipeline.py`, cambie la expectation
   `latency_range` de `cell_tower_metrics_silver` para que acepte solo 0–120 ms.
2. Guarde y vuelva a ejecutar el pipeline desde la interfaz (**Run pipeline**).
3. En el DAG, seleccione `cell_tower_metrics_silver` y compare en **Data quality**
   cuántos registros fallan `latency_range` ahora.
4. Al terminar, revierta el cambio (0–500 ms) y vuelva a ejecutar el pipeline.

**Pregunte:** ¿se eliminaron filas? ¿Por qué sí o por qué no?

<details><summary>Pista 1</summary>

Busque `"latency_range":` en el archivo.
</details>
<details><summary>Pista 2</summary>

Fíjese en el decorador: `@dp.expect_all` registra; `@dp.expect_all_or_drop` elimina.
</details>

## Reto 3 — De advertencia a error (DQX)

1. En `04_dqx_quality`, use los widgets de arriba: `scope=guided` y `min_downlink=50.0`
   (el negocio exige al menos 50 Mbps). Ejecute todo el notebook y anote
   `rows_with_warnings` de `cell_tower_metrics` en la tabla de resumen final.
2. En la celda `CHECKS_BY_DATASET`, dentro de las reglas de `cell_tower_metrics`, cambie
   la `criticality` de `downlink_workshop_sla` de `warn` a `error` y vuelva a ejecutar.
3. Compare cuántas filas tienen ahora `cell_tower_metrics_validated` y
   `cell_tower_metrics_quarantine`.
4. Al terminar, vuelva a `warn`, `min_downlink=1.0` y `scope=all`, y ejecute de nuevo:
   las tablas Gold leen de `validated`.

**Pregunte:** ¿qué pasa con un registro que solo incumple esa regla en cada caso?

## Reto 4 — Una medida nueva (Metric Views)

Operaciones quiere saber **qué porcentaje de las horas** está en estado `Crítico`.

1. En `06_metric_views`, dentro del YAML de `mv_network_quality`, agregue bajo
   `measures:` una medida `Horas críticas` con ese porcentaje. Respete la indentación
   de las otras medidas.
2. Vuelva a ejecutar esa celda (`CREATE OR REPLACE VIEW ... WITH METRICS`).
3. En una celda nueva, consulte la medida por `Región natural`.

**Pregunte:** ¿qué región natural tiene el mayor porcentaje? ¿Coincide con la que
tiene el peor `Cumplimiento de calidad de red`?

<details><summary>Pista 1</summary>

La columna `health_status` de `gold_network_hourly` vale `Saludable`, `Atención` o `Crítico`.
</details>
<details><summary>Pista 2</summary>

`COUNT_IF(condición)` cuenta filas que cumplen una condición.
</details>
<details><summary>Pista 3</summary>

Las medidas se consultan con `MEASURE()`:
``SELECT `Región natural`, MEASURE(`Horas críticas`) FROM mv_network_quality GROUP BY ALL``.
</details>

## Reto 5 — Mapa de sitios de Ecuador (AI/BI Dashboard)

En su dashboard (o en una copia del dashboard de referencia), agregue un **mapa** con un
punto por cada sitio de red, coloreado según su nivel de calidad de red.

1. Cree un dataset con **una fila por sitio** que incluya su provincia, cantón,
   latitud, longitud y su `Cumplimiento de calidad de red` promedio.
2. Agregue una visualización de tipo **Point map** con ese dataset.
3. Coloree los puntos por nivel de calidad: **baja** (< 55 %), **media** (55–65 %) y
   **alta** (> 65 %).

Puede construirlo a mano o pedírselo a Genie Code. Si usa Genie Code, **revise el SQL
del dataset** antes de aceptarlo.

**Pregunte:** ¿en qué ciudades se concentran los sitios de calidad baja? ¿Qué
conversación abre Galápagos sobre conectividad insular?

<details><summary>Pista 1 (los datos)</summary>

Las coordenadas (`latitude`, `longitude`) están en `network_sites_silver`. La calidad de
red diaria de cada sitio está en `gold_site_daily_360` (columna
`network_sla_compliance_pct`). Únalas por `site_id` y agrupe por sitio.
</details>
<details><summary>Pista 2 (el mapa)</summary>

La latitud y la longitud no se escriben: son columnas del dataset. En la visualización
**Point map**, asigne `latitude` al campo **Latitude** y `longitude` al campo
**Longitude**. Agregue el sitio, el cantón y la calidad al **Tooltip**.
</details>
<details><summary>Pista 3 (el color)</summary>

Calcule el nivel en la consulta con `CASE WHEN` sobre el promedio de
`network_sla_compliance_pct` y úselo en el campo **Color**. No use `site_health_status`:
es diario, y un sitio tiene varios estados en el período.
</details>
<details><summary>Pista 4 (si se atasca)</summary>

Complete los `...` y reemplace `<catálogo>` y `<usuario>` por los suyos:

```sql
SELECT s.site_id, s.region AS provincia, s.canton, s.latitude, s.longitude,
       round(avg(g.network_sla_compliance_pct), 1) AS calidad_red_pct,
       CASE WHEN avg(g.network_sla_compliance_pct) < 55 THEN 'Calidad baja'
            WHEN ... THEN 'Calidad media'
            ELSE ... END AS nivel_calidad
FROM <catálogo>.z2h_<usuario>.gold_site_daily_360 g
JOIN <catálogo>.z2h_<usuario>.network_sites_silver s USING (site_id)
GROUP BY ALL
```
</details>

## Reto 6 — Cazar y corregir una respuesta (Genie)

1. Pregunte en modo Chat: *¿Qué provincia de la Costa tiene peor calidad de red en 4G?*
2. Pulse **Show code**. ¿Filtró por `Región natural = 'Costa'` o por otra cosa?
3. Pregunte: *¿Cuánto ingreso está en riesgo en Pichincha por incidentes críticos?*
4. Si alguna respuesta usa una columna equivocada o no entiende un término, agregue
   una regla a la lista `instrucciones_extra` de la sección 4 de `08_genie`, vuelva a
   ejecutar esa celda y repita la pregunta. Una regla por idea, y concreta: nombre el
   término que confundió a Genie y lo que significa.

**Regla de oro:** una respuesta fluida no es una respuesta correcta. Verifique el SQL.

## Reto 7 — Filtro de región natural (Databricks App)

1. En `apps/network-monitor/app.py`, junto al `st.selectbox("Provincia", ...)`,
   agregue un segundo `st.selectbox` con **Región natural**.
2. Modifique `_filter_region` para que aplique ambos filtros en las tablas que tengan
   la columna `natural_region`.
3. Vuelva a ejecutar la sección 2 de `09_app` (redespliega la App) y pruebe el filtro.

**Pregunte:** ¿por qué el KPI de clientes activos no cambia con este filtro?

<details><summary>Pista</summary>

Copie el patrón de `_filter_region`. No todas las tablas Gold tienen
`natural_region`: compruebe con `if "natural_region" in frame`.
</details>

## Reto 8 — Caso de negocio: ¿dónde invertir este trimestre?

> Equinoccio Móvil tiene presupuesto para mejorar la red en **una sola provincia**
> este trimestre. ¿Cuál elegirían y por qué?

Reglas:

- Individual o en equipo. Escriba su recomendación en media página.
- La recomendación debe apoyarse en **un widget del dashboard** y **una respuesta de
  Genie con su SQL verificado**.
- Consideren al menos tres ángulos: calidad de red, ingreso en riesgo y
  clientes con alto riesgo de baja.
- No hay una única respuesta correcta: se evalúa la evidencia, no la provincia.

Criterios de evaluación: evidencia (40 %), uso correcto de los datos (30 %),
claridad (30 %).
