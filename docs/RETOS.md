# Retos del workshop — Equinoccio Móvil

Los retos obligan a **modificar** lo que se construyó, no solo a ejecutarlo. Ningún
reto bloquea a los siguientes: si uno no termina a tiempo, continúe.

Las pistas están ordenadas de menos a más explícitas. Lea una a la vez.

## Durante el taller

| # | Módulo | Reto | Tiempo |
|---:|---|---|---:|
| 5 | AI/BI Dashboard | Página "Ecuador por regiones" (pasos 1 y 3) | 10 min |
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
| 5 | AI/BI Dashboard | `07_dashboard` | Paso 2: mapa de sitios |
| 7 | Databricks App | `09_app` | Filtro de región natural |
| 8 | Caso de negocio | — | ¿Dónde invertir este trimestre? |

---

## Reto 1 — Detective de gobierno (Unity Catalog)

Responda usando **solo Catalog Explorer**, sin SQL:

1. ¿Quién es el owner de su schema `z2h_<usuario>`?
2. ¿Qué tipo de volume es `raw_data`: managed o external?
3. Abra `cell_tower_metrics_raw_checkpoint` → **Lineage**. ¿Qué job la creó?

<details><summary>Pista</summary>

El owner y el tipo están en la pestaña **Overview/Details**. El lineage aparece
después de que el job termina.
</details>

## Reto 2 — Endurecer una expectation (Lakeflow SDP)

El negocio decide que una latencia mayor a **120 ms** ya es una anomalía.

1. En `pipeline/transformations/telco_pipeline.py`, cambie la expectation
   `latency_range` de `cell_tower_metrics_silver` para que acepte solo 0–120 ms.
2. Guarde y vuelva a ejecutar el pipeline desde la interfaz (**Run pipeline**).
3. Compare en **Data quality** cuántos registros fallan ahora.

**Pregunte:** ¿se eliminaron filas? ¿Por qué sí o por qué no?

<details><summary>Pista 1</summary>

Busque `"latency_range":` en el archivo.
</details>
<details><summary>Pista 2</summary>

Fíjese en el decorador: `@dp.expect_all` registra; `@dp.expect_all_or_drop` elimina.
</details>

## Reto 3 — De advertencia a error (DQX)

1. En `04_dqx_quality`, use `scope=guided` y cambie el widget `min_downlink` a `50.0`
   (el negocio exige al menos 50 Mbps). Ejecute y anote `rows_with_warnings` de
   `cell_tower_metrics`.
2. En las reglas YAML, cambie la `criticality` de `downlink_workshop_sla` de `warn`
   a `error` y vuelva a ejecutar.
3. Compare `cell_tower_metrics_validated` y `cell_tower_metrics_quarantine`.

**Pregunte:** ¿qué pasa con un registro que solo incumple esa regla en cada caso?

## Reto 4 — Una medida nueva (Metric Views)

Operaciones quiere saber **qué porcentaje de las horas** está en estado `Crítico`.

1. En `06_metric_views`, agregue a `mv_network_quality` una medida
   `Horas críticas` con ese porcentaje.
2. Vuelva a ejecutar la celda y consúltela por `Región natural`.

**Pregunte:** ¿qué región natural tiene el mayor porcentaje? ¿Coincide con la que
tiene el peor `Cumplimiento SLA de red`?

<details><summary>Pista 1</summary>

La columna `health_status` de `gold_network_hourly` vale `Saludable`, `Atención` o `Crítico`.
</details>
<details><summary>Pista 2</summary>

`COUNT_IF(condición)` cuenta filas que cumplen una condición.
</details>

## Reto 5 — Página "Ecuador por regiones" (AI/BI Dashboard)

Duplique el dashboard de referencia (o use el suyo) y agregue una **página nueva** con:

1. Un KPI y una barra de `Cumplimiento SLA de red` por **Región natural**
   (Costa, Sierra, Amazonía, Insular).
2. *(Después del taller)* Un **mapa de sitios** con punto por sitio, coloreado por
   estado de salud.
3. Un **filtro de Región natural** que actualice todos los widgets de la página.

Durante el taller haga los pasos 1 y 3; el mapa queda para después.

Puede construirlo a mano o pedírselo a Genie Code. Si usa Genie Code, **revise el SQL
de cada dataset** antes de aceptarlo.

**Pregunte:** en el mapa, ¿qué conversación abre Galápagos sobre conectividad insular?

<details><summary>Pista 1</summary>

`Región natural` es una dimensión de `mv_network_quality`.
</details>
<details><summary>Pista 2</summary>

Las coordenadas (`latitude`, `longitude`) están en `network_sites_silver`. Únala por
`site_id` con `gold_site_daily_360`.
</details>

## Reto 6 — Cazar y corregir una respuesta (Genie)

1. Pregunte en modo Chat: *¿Qué provincia de la Costa tiene peor SLA de red en 4G?*
2. Pulse **Show code**. ¿Filtró por `Región natural = 'Costa'` o por otra cosa?
3. Pregunte: *¿Cuánto ingreso está en riesgo en Pichincha por incidentes críticos?*
4. Si alguna respuesta usa una columna equivocada o no entiende un término, agregue
   una instrucción o sinónimo en la sección 4 de `08_genie`, vuelva a ejecutarla y
   repita la pregunta.

**Regla de oro:** una respuesta fluida no es una respuesta correcta. Verifique el SQL.

## Reto 7 — Filtro de región natural (Databricks App)

1. En `apps/network-monitor/app.py`, agregue un segundo `st.selectbox` con
   **Región natural** junto al de Provincia.
2. Haga que filtre las tablas que tengan la columna `natural_region`.
3. Vuelva a ejecutar la sección 2 de `09_app` y pruebe el filtro.

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
- Consideren al menos tres ángulos: calidad de red (SLA), ingreso en riesgo y
  clientes con alto riesgo de baja.
- No hay una única respuesta correcta: se evalúa la evidencia, no la provincia.

Criterios de evaluación: evidencia (40 %), uso correcto de los datos (30 %),
claridad (30 %).
