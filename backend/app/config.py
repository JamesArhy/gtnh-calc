import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_source: str
    local_data_dir: Path
    recipes_json: Path
    machine_index_json: Path
    default_version: str
    graph_backend: str


def load_settings() -> Settings:
    repo_root = Path(__file__).resolve().parents[2]
    data_source = os.environ.get("DATA_SOURCE", "local")
    local_data_dir = Path(os.environ.get("LOCAL_DATA_DIR", "in/parquet"))
    recipes_json = Path(os.environ.get("RECIPES_JSON", "in/recipes.json"))
    machine_index_json = Path(os.environ.get("MACHINE_INDEX_JSON", "in/machine_index.json"))
    graph_backend = os.environ.get("GRAPH_DB", "ladybugdb")
    if not local_data_dir.is_absolute():
        local_data_dir = repo_root / local_data_dir
    if not recipes_json.is_absolute():
        recipes_json = repo_root / recipes_json
    if not machine_index_json.is_absolute():
        machine_index_json = repo_root / machine_index_json
    default_version = os.environ.get("DEFAULT_VERSION", "local")
    return Settings(
        data_source=data_source,
        local_data_dir=local_data_dir,
        recipes_json=recipes_json,
        machine_index_json=machine_index_json,
        default_version=default_version,
        graph_backend=graph_backend,
    )
