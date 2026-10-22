"""Configuración por participante y creación idempotente de assets con el SDK.

Los notebooks del workshop usan este módulo para que cada participante tenga su
propio schema, job, pipeline, dashboard, Genie Space y App, sin CLI ni perfiles.
La autenticación es la nativa del notebook: `WorkspaceClient()` sin argumentos.
"""

from __future__ import annotations

import io
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import apps, sql

# Los servicios del SDK se importan dentro de cada función: el entorno serverless por
# defecto puede traer un SDK antiguo sin `apps` o `genie`. Los notebooks que usan esas
# APIs actualizan el SDK con `%pip install --upgrade databricks-sdk`.

PREFIX = "z2h"
GOLD_OBJECTS = (
    "gold_network_hourly",
    "gold_site_daily_360",
    "gold_customer_360",
    "gold_customer_product_daily",
    "gold_incident_impact",
)
APP_FILES = ("app.py", "data_access.py", "requirements.txt")


def slugify(user_name: str) -> str:
    """`ana.perez@empresa.com` → `ana_perez`. Solo minúsculas, números y `_`."""

    local_part = user_name.split("@", 1)[0].lower()
    local_part = unicodedata.normalize("NFKD", local_part).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-z0-9]+", "_", local_part).strip("_")
    return slug or "participante"


def validate_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value or ""):
        raise ValueError(f"{label} no válido: {value!r}. Use letras, números y '_'.")
    return value


@dataclass(frozen=True)
class WorkshopConfig:
    user_name: str
    slug: str
    catalog: str
    schema: str
    warehouse_id: str
    repo_root: str

    @property
    def namespace(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/raw_data"

    @property
    def home(self) -> str:
        return f"/Workspace/Users/{self.user_name}"

    @property
    def job_name(self) -> str:
        return f"{PREFIX} - Preparar datos - {self.slug}"

    @property
    def pipeline_name(self) -> str:
        return f"{PREFIX} - Calidad de red - {self.slug}"

    @property
    def dashboard_name(self) -> str:
        return f"Experiencia móvil 360 (referencia) - {self.slug}"

    @property
    def genie_title(self) -> str:
        return f"Analista 360 de Red y Clientes - {self.slug}"

    @property
    def app_name(self) -> str:
        # Las Apps solo admiten minúsculas, números y guiones, y son únicas por workspace.
        return f"{PREFIX}-{self.slug.replace('_', '-')}"[:30].rstrip("-")

    @property
    def app_deploy_path(self) -> str:
        return f"{self.home}/{PREFIX}_app"


def config_path(user_name: str) -> str:
    return f"/Workspace/Users/{user_name}/.{PREFIX}/config.json"


def find_repo_root(start: Path | None = None) -> Path:
    """Sube desde el directorio del notebook hasta encontrar la raíz del Git folder."""

    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / "notebooks").is_dir():
            return candidate
    raise FileNotFoundError("No se encontró la raíz del repositorio. ¿Abrió el notebook desde el Git folder?")


def save_config(w: WorkspaceClient, config: WorkshopConfig) -> str:
    from databricks.sdk.service.workspace import ImportFormat

    path = config_path(config.user_name)
    w.workspace.mkdirs(path.rsplit("/", 1)[0])
    payload = json.dumps(asdict(config), ensure_ascii=False, indent=2).encode()
    w.workspace.upload(path, io.BytesIO(payload), format=ImportFormat.AUTO, overwrite=True)
    return path


def load_config(w: WorkspaceClient, user_name: str) -> WorkshopConfig:
    path = config_path(user_name)
    try:
        with w.workspace.download(path) as handle:
            return WorkshopConfig(**json.loads(handle.read()))
    except Exception as error:  # noqa: BLE001 - el mensaje guía al participante
        raise RuntimeError(f"No existe {path}. Ejecute primero notebooks/00_configuracion.") from error


def choose_warehouse(w: WorkspaceClient, preferred_id: str = "") -> sql.EndpointInfo:
    """Elige un SQL Warehouse PRO o Serverless; Genie no admite Classic."""

    from databricks.sdk.service import sql

    if preferred_id:
        return w.warehouses.get(preferred_id)
    candidates = [
        warehouse
        for warehouse in w.warehouses.list()
        if warehouse.enable_serverless_compute or warehouse.warehouse_type == sql.EndpointInfoWarehouseType.PRO
    ]
    if not candidates:
        raise RuntimeError("No hay un SQL Warehouse PRO o Serverless con permiso CAN USE.")
    candidates.sort(
        key=lambda item: (
            item.state != sql.State.RUNNING,
            not item.enable_serverless_compute,
            item.name or "",
        )
    )
    return candidates[0]


def workspace_url(w: WorkspaceClient, path: str) -> str:
    return f"{w.config.host.rstrip('/')}/{path.lstrip('/')}"


def _api_path(path: str) -> str:
    """Las APIs de jobs aceptan rutas de workspace sin el prefijo `/Workspace`."""

    return path.removeprefix("/Workspace")


# ---------------------------------------------------------------------------
# Job de preparación de datos
# ---------------------------------------------------------------------------


def upsert_data_job(w: WorkspaceClient, config: WorkshopConfig, seed: int = 42) -> int:
    from databricks.sdk.service import jobs

    notebook_path = f"{config.repo_root}/notebooks/tareas/generar_datos"
    settings = dict(
        name=config.job_name,
        description="Genera las once fuentes sintéticas del workshop en el volume del participante.",
        tags={"workshop": "zero-2-hero", "participante": config.slug},
        max_concurrent_runs=1,
        tasks=[
            jobs.Task(
                task_key="generar_datos_sinteticos",
                description="Crea CSV en el volume y tablas checkpoint de contingencia.",
                notebook_task=jobs.NotebookTask(
                    notebook_path=_api_path(notebook_path),
                    source=jobs.Source.WORKSPACE,
                    base_parameters={
                        "catalog": config.catalog,
                        "schema": config.schema,
                        "seed": str(seed),
                    },
                ),
            )
        ],
    )
    existing = next(iter(w.jobs.list(name=config.job_name)), None)
    if existing:
        w.jobs.reset(job_id=existing.job_id, new_settings=jobs.JobSettings(**settings))
        return existing.job_id
    return w.jobs.create(**settings).job_id


# ---------------------------------------------------------------------------
# Lakeflow Spark Declarative Pipeline
# ---------------------------------------------------------------------------


def find_pipeline_id(w: WorkspaceClient, name: str) -> str | None:
    for pipeline in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if pipeline.name == name:
            return pipeline.pipeline_id
    return None


def upsert_pipeline(w: WorkspaceClient, config: WorkshopConfig) -> str:
    """Crea o actualiza el pipeline. No lo ejecuta: el participante lo inicia desde la UI."""

    from databricks.sdk.service import pipelines

    root_path = f"{config.repo_root}/pipeline"
    spec = dict(
        name=config.pipeline_name,
        catalog=config.catalog,
        schema=config.schema,
        serverless=True,
        development=True,
        continuous=False,
        channel="CURRENT",
        root_path=root_path,
        libraries=[
            pipelines.PipelineLibrary(
                file=pipelines.FileLibrary(path=f"{root_path}/transformations/telco_pipeline.py")
            )
        ],
        configuration={"telco.catalog": config.catalog, "telco.schema": config.schema},
        tags={"workshop": "zero-2-hero", "participante": config.slug},
    )
    pipeline_id = find_pipeline_id(w, config.pipeline_name)
    if pipeline_id:
        w.pipelines.update(pipeline_id=pipeline_id, **spec)
        return pipeline_id
    return w.pipelines.create(**spec).pipeline_id


# ---------------------------------------------------------------------------
# AI/BI Dashboard de referencia
# ---------------------------------------------------------------------------


def upsert_dashboard(w: WorkspaceClient, config: WorkshopConfig, serialized: dict, publish: bool = True) -> str:
    from databricks.sdk.service import dashboards

    body = json.dumps(serialized, ensure_ascii=False, separators=(",", ":"))
    existing = next(
        (item for item in w.lakeview.list() if item.display_name == config.dashboard_name),
        None,
    )
    if existing:
        dashboard_id = existing.dashboard_id
        w.lakeview.update(
            dashboard_id=dashboard_id,
            dashboard=dashboards.Dashboard(
                display_name=config.dashboard_name,
                serialized_dashboard=body,
                warehouse_id=config.warehouse_id,
            ),
            dataset_catalog=config.catalog,
            dataset_schema=config.schema,
        )
    else:
        dashboard_id = w.lakeview.create(
            dashboard=dashboards.Dashboard(
                display_name=config.dashboard_name,
                parent_path=_api_path(config.home),
                serialized_dashboard=body,
                warehouse_id=config.warehouse_id,
            ),
            # Las consultas usan nombres simples; catálogo y schema se resuelven aquí.
            dataset_catalog=config.catalog,
            dataset_schema=config.schema,
        ).dashboard_id
    if publish:
        w.lakeview.publish(dashboard_id=dashboard_id, embed_credentials=True, warehouse_id=config.warehouse_id)
    return dashboard_id


# ---------------------------------------------------------------------------
# Genie Space
# ---------------------------------------------------------------------------


def find_genie_space_id(w: WorkspaceClient, title: str) -> str | None:
    page_token = None
    while True:
        response = w.genie.list_spaces(page_token=page_token)
        for space in response.spaces or []:
            if space.title == title:
                return space.space_id
        page_token = response.next_page_token
        if not page_token:
            return None


def upsert_genie_space(
    w: WorkspaceClient, config: WorkshopConfig, serialized: dict, space_id: str | None = None
) -> str:
    """Crea o actualiza el espacio. Pase `space_id` si lo conoce: `list_spaces` tarda unos
    segundos en incluir un espacio recién creado y la búsqueda por título lo duplicaría."""

    body = json.dumps(serialized, ensure_ascii=False, separators=(",", ":"))
    description = "Análisis sintético y gobernado de red, clientes, productos y experiencia"
    space_id = space_id or find_genie_space_id(w, config.genie_title)
    if space_id:
        w.genie.update_space(
            space_id=space_id,
            serialized_space=body,
            title=config.genie_title,
            description=description,
            warehouse_id=config.warehouse_id,
        )
        return space_id
    return w.genie.create_space(
        warehouse_id=config.warehouse_id,
        serialized_space=body,
        title=config.genie_title,
        description=description,
        parent_path=_api_path(config.home),
    ).space_id


# ---------------------------------------------------------------------------
# Databricks App
# ---------------------------------------------------------------------------


def app_yaml(config: WorkshopConfig) -> str:
    return f"""command:
  - streamlit
  - run
  - app.py
  - --server.address=0.0.0.0
  - --server.port=8000
  - --server.headless=true

# Generado por notebooks/09_app para {config.slug}. Edite el código en el Git folder
# y vuelva a ejecutar el notebook para redesplegar.
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: TELCO_CATALOG
    value: "{config.catalog}"
  - name: TELCO_SCHEMA
    value: "{config.schema}"
"""


def stage_app_source(w: WorkspaceClient, config: WorkshopConfig) -> str:
    """Copia el código de la App a la carpeta personal y genera su `app.yaml`."""

    from databricks.sdk.service.workspace import ImportFormat

    source_dir = Path(config.repo_root) / "apps" / "network-monitor"
    target_dir = config.app_deploy_path
    w.workspace.mkdirs(target_dir)
    for file_name in APP_FILES:
        content = (source_dir / file_name).read_bytes()
        w.workspace.upload(f"{target_dir}/{file_name}", io.BytesIO(content), format=ImportFormat.AUTO, overwrite=True)
    w.workspace.upload(
        f"{target_dir}/app.yaml", io.BytesIO(app_yaml(config).encode()), format=ImportFormat.AUTO, overwrite=True
    )
    return target_dir


def app_resources(config: WorkshopConfig) -> list[apps.AppResource]:
    from databricks.sdk.service import apps

    resources = [
        apps.AppResource(
            name="sql-warehouse",
            sql_warehouse=apps.AppResourceSqlWarehouse(
                id=config.warehouse_id,
                permission=apps.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE,
            ),
        )
    ]
    for object_name in GOLD_OBJECTS:
        resources.append(
            apps.AppResource(
                name=object_name.replace("_", "-"),
                uc_securable=apps.AppResourceUcSecurable(
                    securable_full_name=f"{config.namespace}.{object_name}",
                    securable_type=apps.AppResourceUcSecurableUcSecurableType.TABLE,
                    permission=apps.AppResourceUcSecurableUcSecurablePermission.SELECT,
                ),
            )
        )
    return resources


def upsert_app(w: WorkspaceClient, config: WorkshopConfig) -> apps.App:
    """Crea la App con sus recursos o los actualiza; deja el compute encendido."""

    from databricks.sdk.errors import NotFound
    from databricks.sdk.service import apps

    definition = apps.App(
        name=config.app_name,
        description=f"Monitor de experiencia móvil del workshop - {config.slug}",
        resources=app_resources(config),
    )
    try:
        current = w.apps.get(config.app_name)
    except NotFound:
        return w.apps.create_and_wait(app=definition)

    w.apps.update(name=config.app_name, app=definition)
    if current.compute_status and current.compute_status.state != apps.ComputeState.ACTIVE:
        return w.apps.start_and_wait(name=config.app_name)
    return w.apps.get(config.app_name)


def deploy_app(w: WorkspaceClient, config: WorkshopConfig, source_code_path: str) -> apps.AppDeployment:
    from databricks.sdk.service import apps

    return w.apps.deploy_and_wait(
        app_name=config.app_name,
        app_deployment=apps.AppDeployment(
            source_code_path=source_code_path,
            mode=apps.AppDeploymentMode.SNAPSHOT,
        ),
    )
