from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from .data_source import DuckDBDataset
from .gtnh import MachineTuning, apply_overclock, rate_per_second
from .name_index import NameIndex



@dataclass
class GraphTarget:
    target_type: str
    target_id: str
    target_meta: int
    target_rate_per_s: float


@dataclass
class GraphRequest:
    version: str
    targets: list[GraphTarget]
    max_depth: int
    tuning: MachineTuning
    recipe_override: Dict[str, str]
    recipe_overclock_tiers: Dict[str, int]


def _item_key(item_id: str, meta: int) -> str:
    return f"item:{item_id}:{meta}"


def _fluid_key(fluid_id: str) -> str:
    return f"fluid:{fluid_id}"


def _recipe_key(rid: str, output_key: str) -> str:
    return f"recipe:{rid}:{output_key}"


def build_graph(dataset: DuckDBDataset, names: NameIndex, req: GraphRequest) -> dict:
    nodes: Dict[str, dict] = {}
    edges: Dict[str, dict] = {}

    def tuning_for_recipe(recipe: dict) -> MachineTuning:
        override = req.recipe_overclock_tiers.get(recipe["rid"])
        if override is None:
            return req.tuning
        return MachineTuning(overclock_tiers=int(override), parallel=req.tuning.parallel)

    def ensure_item_node(item_id: str, meta: int) -> str:
        node_id = _item_key(item_id, meta)
        if node_id not in nodes:
            label = names.items.get((item_id, meta), f"{item_id}:{meta}")
            nodes[node_id] = {
                "id": node_id,
                "type": "item",
                "label": label,
                "item_id": item_id,
                "meta": meta,
            }
        return node_id

    def ensure_fluid_node(fluid_id: str) -> str:
        node_id = _fluid_key(fluid_id)
        if node_id not in nodes:
            label = names.fluids.get(fluid_id, fluid_id)
            nodes[node_id] = {
                "id": node_id,
                "type": "fluid",
                "label": label,
                "fluid_id": fluid_id,
            }
        return node_id

    def ensure_recipe_node(
        recipe: dict, tuning: MachineTuning, target_rate: float, target_output: dict | None, output_key: str
    ) -> str:
        node_id = _recipe_key(recipe["rid"], output_key)
        if node_id not in nodes:
            duration_ticks, eut = apply_overclock(recipe["duration_ticks"], recipe["eut"], tuning)
            recipe_info = names.recipes.get(recipe["rid"], {})
            machine_name = recipe_info.get("machine_name") or recipe["machine_id"]
            nodes[node_id] = {
                "id": node_id,
                "type": "recipe",
                "label": machine_name,
                "rid": recipe["rid"],
                "machine_id": recipe["machine_id"],
                "machine_name": machine_name,
                "min_tier": recipe_info.get("min_tier"),
                "base_duration_ticks": recipe["duration_ticks"],
                "base_eut": recipe["eut"],
                "duration_ticks": duration_ticks,
                "eut": eut,
                "overclock_tiers": tuning.overclock_tiers,
                "parallel": tuning.parallel,
                "machines_required": None,
                "target_rate_per_s": target_rate,
                "target_output": target_output,
            }
        return node_id

    def add_edge(source: str, target: str, data: dict) -> None:
        edge_id = f"{source}->{target}:{data.get('kind', 'link')}"
        if edge_id in edges:
            edges[edge_id]["rate_per_s"] += data.get("rate_per_s", 0)
            return
        edges[edge_id] = {
            "id": edge_id,
            "source": source,
            "target": target,
            **data,
        }

    def expand_item(item_id: str, meta: int, rate_needed: float, depth: int, path: set[str]) -> None:
        if depth > req.max_depth:
            return
        path_key = f"item:{item_id}:{meta}"
        if path_key in path:
            return
        next_path = set(path)
        next_path.add(path_key)

        item_node = ensure_item_node(item_id, meta)

        key = f"item:{item_id}:{meta}"
        override_rid = req.recipe_override.get(key)
        recipes = dataset.recipes_for_output_item(item_id, meta, limit=5)
        if not recipes:
            return
        recipe = None
        if override_rid:
            recipe = dataset.recipe_by_rid(override_rid)
        if recipe is None:
            recipe = recipes[0]

        tuning = tuning_for_recipe(recipe)
        outputs = dataset.recipe_outputs(recipe["rid"])
        duration_ticks, _ = apply_overclock(recipe["duration_ticks"], recipe["eut"], tuning)

        target_output = None
        for out in outputs["items"]:
            if out["item_id"] == item_id and out["meta"] == meta:
                target_output = out
                break
        if not target_output:
            return

        per_machine_rate = rate_per_second(target_output["count"], duration_ticks)
        total_rate_per_machine = per_machine_rate * max(1, tuning.parallel)
        machines_required = rate_needed / total_rate_per_machine if total_rate_per_machine > 0 else 0

        output_key = f"item:{item_id}:{meta}"
        recipe_node = ensure_recipe_node(recipe, tuning, rate_needed, target_output, output_key)
        nodes[recipe_node]["machines_required"] = machines_required
        nodes[recipe_node]["per_machine_rate_per_s"] = per_machine_rate

        add_edge(
            recipe_node,
            item_node,
            {
                "kind": "produces",
                "rate_per_s": rate_needed,
                "count_per_cycle": target_output["count"],
            },
        )

        inputs = dataset.recipe_inputs(recipe["rid"])
        for inp in inputs["items"]:
            inp_node = ensure_item_node(inp["item_id"], inp["meta"])
            req_rate = rate_per_second(inp["count"], duration_ticks) * machines_required
            add_edge(
                inp_node,
                recipe_node,
                {
                    "kind": "consumes",
                    "rate_per_s": req_rate,
                    "count_per_cycle": inp["count"],
                },
            )
            expand_item(inp["item_id"], inp["meta"], req_rate, depth + 1, next_path)

        for inp in inputs["fluids"]:
            inp_node = ensure_fluid_node(inp["fluid_id"])
            req_rate = rate_per_second(inp["mb"], duration_ticks) * machines_required
            add_edge(
                inp_node,
                recipe_node,
                {
                    "kind": "consumes",
                    "rate_per_s": req_rate,
                    "mb_per_cycle": inp["mb"],
                },
            )

        for out in outputs["items"]:
            if out["item_id"] == item_id and out["meta"] == meta:
                continue
            out_node = ensure_item_node(out["item_id"], out["meta"])
            add_edge(
                recipe_node,
                out_node,
                {
                    "kind": "byproduct",
                    "rate_per_s": rate_per_second(out["count"], duration_ticks) * machines_required,
                    "count_per_cycle": out["count"],
                },
            )

        for out in outputs["fluids"]:
            out_node = ensure_fluid_node(out["fluid_id"])
            add_edge(
                recipe_node,
                out_node,
                {
                    "kind": "byproduct",
                    "rate_per_s": rate_per_second(out["mb"], duration_ticks) * machines_required,
                    "mb_per_cycle": out["mb"],
                },
            )

    def expand_fluid(fluid_id: str, rate_needed: float, depth: int, path: set[str]) -> None:
        if depth > req.max_depth:
            return
        path_key = f"fluid:{fluid_id}"
        if path_key in path:
            return
        next_path = set(path)
        next_path.add(path_key)

        fluid_node = ensure_fluid_node(fluid_id)

        key = f"fluid:{fluid_id}"
        override_rid = req.recipe_override.get(key)
        recipes = dataset.recipes_for_output_fluid(fluid_id, limit=5)
        if not recipes:
            return
        recipe = None
        if override_rid:
            recipe = dataset.recipe_by_rid(override_rid)
        if recipe is None:
            recipe = recipes[0]

        tuning = tuning_for_recipe(recipe)
        outputs = dataset.recipe_outputs(recipe["rid"])
        duration_ticks, _ = apply_overclock(recipe["duration_ticks"], recipe["eut"], tuning)

        target_output = None
        for out in outputs["fluids"]:
            if out["fluid_id"] == fluid_id:
                target_output = out
                break
        if not target_output:
            return

        per_machine_rate = rate_per_second(target_output["mb"], duration_ticks)
        total_rate_per_machine = per_machine_rate * max(1, tuning.parallel)
        machines_required = rate_needed / total_rate_per_machine if total_rate_per_machine > 0 else 0

        output_key = f"fluid:{fluid_id}"
        recipe_node = ensure_recipe_node(recipe, tuning, rate_needed, target_output, output_key)
        nodes[recipe_node]["machines_required"] = machines_required
        nodes[recipe_node]["per_machine_rate_per_s"] = per_machine_rate

        add_edge(
            recipe_node,
            fluid_node,
            {
                "kind": "produces",
                "rate_per_s": rate_needed,
                "mb_per_cycle": target_output["mb"],
            },
        )

        inputs = dataset.recipe_inputs(recipe["rid"])
        for inp in inputs["items"]:
            inp_node = ensure_item_node(inp["item_id"], inp["meta"])
            req_rate = rate_per_second(inp["count"], duration_ticks) * machines_required
            add_edge(
                inp_node,
                recipe_node,
                {
                    "kind": "consumes",
                    "rate_per_s": req_rate,
                    "count_per_cycle": inp["count"],
                },
            )
            expand_item(inp["item_id"], inp["meta"], req_rate, depth + 1, next_path)

        for inp in inputs["fluids"]:
            inp_node = ensure_fluid_node(inp["fluid_id"])
            req_rate = rate_per_second(inp["mb"], duration_ticks) * machines_required
            add_edge(
                inp_node,
                recipe_node,
                {
                    "kind": "consumes",
                    "rate_per_s": req_rate,
                    "mb_per_cycle": inp["mb"],
                },
            )
            if inp["fluid_id"] != fluid_id:
                expand_fluid(inp["fluid_id"], req_rate, depth + 1, next_path)

    for target in req.targets:
        if target.target_type == "item":
            expand_item(target.target_id, target.target_meta, target.target_rate_per_s, 0, set())
        else:
            expand_fluid(target.target_id, target.target_rate_per_s, 0, set())

    return {
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
        "meta": {
            "version": req.version,
            "targets": [
                {
                    "type": target.target_type,
                    "id": target.target_id,
                    "meta": target.target_meta,
                    "rate_per_s": target.target_rate_per_s,
                }
                for target in req.targets
            ],
        },
    }
