from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import load_settings
from .data_source import LocalDataSource, S3DataSource
from .gtnh import MachineTuning
from .graph import GraphRequest, GraphTarget, build_graph
from .name_index import load_name_index

settings = load_settings()

if settings.data_source == "local":
    data_source = LocalDataSource(settings.local_data_dir, settings.default_version)
elif settings.data_source == "s3":
    data_source = S3DataSource()
else:
    raise RuntimeError(f"Unsupported data source: {settings.data_source}")

name_index = load_name_index(settings.recipes_json)

app = FastAPI(title="GTNH Production Planner API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class GraphTargetModel(BaseModel):
    target_type: str
    target_id: str
    target_meta: int = 0
    target_rate_per_s: float


class GraphRequestModel(BaseModel):
    version: str | None = None
    targets: list[GraphTargetModel] | None = None
    target_type: str = "item"
    target_id: str = ""
    target_meta: int = 0
    target_rate_per_s: float = 0.0
    max_depth: int = 3
    overclock_tiers: int = 0
    parallel: int = 1
    recipe_override: dict[str, str] = {}
    recipe_overclock_tiers: dict[str, int] = {}


@app.get("/api/versions")
def list_versions():
    return {"versions": list(data_source.list_versions())}


@app.get("/api/search/items")
def search_items(q: str, limit: int = 20, version: str | None = None):
    dataset = data_source.open_dataset(version or settings.default_version)
    results = dataset.list_item_matches(q, limit)
    dataset.close()

    if q and len(results) < limit:
        q_lower = q.lower()
        seen = {(item["item_id"], int(item["meta"])) for item in results}
        for (item_id, meta), name in name_index.items.items():
            if q_lower in name.lower() and (item_id, meta) not in seen:
                results.append({"item_id": item_id, "meta": int(meta)})
                seen.add((item_id, meta))
                if len(results) >= limit:
                    break

    enriched = []
    for item in results:
        name = name_index.items.get((item["item_id"], item["meta"]))
        enriched.append({**item, "name": name})
    return {"items": enriched}


@app.get("/api/search/fluids")
def search_fluids(q: str, limit: int = 20, version: str | None = None):
    dataset = data_source.open_dataset(version or settings.default_version)
    results = dataset.list_fluid_matches(q, limit)
    dataset.close()

    if q and len(results) < limit:
        q_lower = q.lower()
        seen = {fluid["fluid_id"] for fluid in results}
        for fluid_id, name in name_index.fluids.items():
            if q_lower in name.lower() and fluid_id not in seen:
                results.append({"fluid_id": fluid_id})
                seen.add(fluid_id)
                if len(results) >= limit:
                    break

    enriched = []
    for fluid in results:
        name = name_index.fluids.get(fluid["fluid_id"])
        enriched.append({**fluid, "name": name})
    return {"fluids": enriched}


@app.get("/api/recipes/by-output")
def recipes_by_output(
    output_type: str,
    item_id: str | None = None,
    meta: int = 0,
    fluid_id: str | None = None,
    machine_id: str | None = None,
    limit: int = 10,
    version: str | None = None,
):
    dataset = data_source.open_dataset(version or settings.default_version)
    if output_type == "item" and item_id:
        if machine_id:
            recipes = dataset.recipes_for_output_item_by_machine(item_id, meta, machine_id, limit)
        else:
            recipes = dataset.recipes_for_output_item(item_id, meta, limit)
    elif output_type == "fluid" and fluid_id:
        if machine_id:
            recipes = dataset.recipes_for_output_fluid_by_machine(fluid_id, machine_id, limit)
        else:
            recipes = dataset.recipes_for_output_fluid(fluid_id, limit)
    else:
        dataset.close()
        raise HTTPException(status_code=400, detail="Invalid output selector")
    enriched = []
    for recipe in recipes:
        info = name_index.recipes.get(recipe["rid"], {})
        inputs = dataset.recipe_inputs(recipe["rid"])
        outputs = dataset.recipe_outputs(recipe["rid"])
        enriched.append(
            {
                **recipe,
                "machine_name": info.get("machine_name") or recipe.get("machine_id"),
                "min_tier": info.get("min_tier"),
                "min_voltage": info.get("min_voltage"),
                "amps": info.get("amps"),
                "item_inputs": [
                    {
                        **item,
                        "name": name_index.items.get((item["item_id"], item["meta"])),
                    }
                    for item in inputs["items"]
                ],
                "fluid_inputs": [
                    {
                        **fluid,
                        "name": name_index.fluids.get(fluid["fluid_id"]),
                    }
                    for fluid in inputs["fluids"]
                ],
                "item_outputs": [
                    {
                        **item,
                        "name": name_index.items.get((item["item_id"], item["meta"])),
                    }
                    for item in outputs["items"]
                ],
                "fluid_outputs": [
                    {
                        **fluid,
                        "name": name_index.fluids.get(fluid["fluid_id"]),
                    }
                    for fluid in outputs["fluids"]
                ],
            }
        )
    dataset.close()
    return {"recipes": enriched}


@app.get("/api/machines/by-output")
def machines_by_output(
    output_type: str,
    item_id: str | None = None,
    meta: int = 0,
    fluid_id: str | None = None,
    limit: int = 200,
    version: str | None = None,
):
    dataset = data_source.open_dataset(version or settings.default_version)
    if output_type == "item" and item_id:
        machine_ids = dataset.machines_for_output_item(item_id, meta, limit)
    elif output_type == "fluid" and fluid_id:
        machine_ids = dataset.machines_for_output_fluid(fluid_id, limit)
    else:
        dataset.close()
        raise HTTPException(status_code=400, detail="Invalid output selector")
    dataset.close()
    machines = [
        {
            "machine_id": machine_id,
            "machine_name": name_index.machine_names.get(machine_id, machine_id),
        }
        for machine_id in machine_ids
    ]
    return {"machines": machines}


@app.get("/api/machines")
def list_machines(version: str | None = None):
    dataset = data_source.open_dataset(version or settings.default_version)
    machine_ids = dataset.list_machines()
    dataset.close()
    machines = []
    for machine_id in machine_ids:
        machines.append(
            {
                "machine_id": machine_id,
                "machine_name": name_index.machine_names.get(machine_id, machine_id),
            }
        )
    return {"machines": machines}


@app.get("/api/recipes/by-machine")
def recipes_by_machine(
    machine_id: str,
    limit: int = 50,
    q: str | None = None,
    version: str | None = None,
):
    dataset = data_source.open_dataset(version or settings.default_version)
    q_lower = q.lower() if q else None
    results = []
    raw_recipes = dataset.recipes_for_machine(machine_id, limit if not q else limit * 5)
    for recipe in raw_recipes:
        info = name_index.recipes.get(recipe["rid"], {})
        inputs = dataset.recipe_inputs(recipe["rid"])
        outputs = dataset.recipe_outputs(recipe["rid"])
        item_outputs = [
            {
                **item,
                "name": name_index.items.get((item["item_id"], item["meta"])),
            }
            for item in outputs["items"]
        ]
        fluid_outputs = [
            {
                **fluid,
                "name": name_index.fluids.get(fluid["fluid_id"]),
            }
            for fluid in outputs["fluids"]
        ]
        if q_lower:
            def _match_output(output: dict, key: str) -> bool:
                name = (output.get("name") or output.get(key) or "").lower()
                return q_lower in name

            if not (
                any(_match_output(item, "item_id") for item in item_outputs)
                or any(_match_output(fluid, "fluid_id") for fluid in fluid_outputs)
            ):
                continue

        results.append(
            {
                **recipe,
                "machine_name": info.get("machine_name") or recipe.get("machine_id"),
                "min_tier": info.get("min_tier"),
                "min_voltage": info.get("min_voltage"),
                "amps": info.get("amps"),
                "item_inputs": [
                    {
                        **item,
                        "name": name_index.items.get((item["item_id"], item["meta"])),
                    }
                    for item in inputs["items"]
                ],
                "fluid_inputs": [
                    {
                        **fluid,
                        "name": name_index.fluids.get(fluid["fluid_id"]),
                    }
                    for fluid in inputs["fluids"]
                ],
                "item_outputs": item_outputs,
                "fluid_outputs": fluid_outputs,
            }
        )
        if len(results) >= limit:
            break
    dataset.close()
    return {"recipes": results}


@app.post("/api/graph")
def graph(req: GraphRequestModel):
    dataset = data_source.open_dataset(req.version or settings.default_version)
    tuning = MachineTuning(overclock_tiers=req.overclock_tiers, parallel=req.parallel)
    if req.targets:
        targets = [
            GraphTarget(
                target_type=target.target_type,
                target_id=target.target_id,
                target_meta=target.target_meta,
                target_rate_per_s=target.target_rate_per_s,
            )
            for target in req.targets
        ]
    else:
        if not req.target_id:
            dataset.close()
            raise HTTPException(status_code=400, detail="Missing target")
        targets = [
            GraphTarget(
                target_type=req.target_type,
                target_id=req.target_id,
                target_meta=req.target_meta,
                target_rate_per_s=req.target_rate_per_s,
            )
        ]
    graph_req = GraphRequest(
        version=req.version or settings.default_version,
        targets=targets,
        max_depth=req.max_depth,
        tuning=tuning,
        recipe_override=req.recipe_override,
        recipe_overclock_tiers=req.recipe_overclock_tiers,
    )
    result = build_graph(dataset, name_index, graph_req)
    dataset.close()
    return result


# Mount built frontend if present.
static_dir = Path("frontend/dist")
if static_dir.exists():
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
