from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import logging
import threading
import os
import shutil

import duckdb

logger = logging.getLogger("ladybugdb")

SEARCH_ITEMS_CYPHER = """
MATCH (i:Item)
WHERE toLower(i.item_id) CONTAINS $query
RETURN i.item_id AS item_id, i.meta AS meta
LIMIT $limit
""".strip()

SEARCH_FLUIDS_CYPHER = """
MATCH (f:Fluid)
WHERE toLower(f.fluid_id) CONTAINS $query
RETURN f.fluid_id AS fluid_id
LIMIT $limit
""".strip()

MACHINES_BY_OUTPUT_ITEM_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_ITEM]->(i:Item {item_id: $item_id, meta: $meta})
RETURN DISTINCT r.machine_id AS machine_id
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINE_COUNTS_BY_OUTPUT_ITEM_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_ITEM]->(i:Item {item_id: $item_id, meta: $meta})
RETURN r.machine_id AS machine_id, COUNT(r) AS recipe_count
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINES_BY_OUTPUT_FLUID_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_FLUID]->(f:Fluid {fluid_id: $fluid_id})
RETURN DISTINCT r.machine_id AS machine_id
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINE_COUNTS_BY_OUTPUT_FLUID_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_FLUID]->(f:Fluid {fluid_id: $fluid_id})
RETURN r.machine_id AS machine_id, COUNT(r) AS recipe_count
ORDER BY machine_id
LIMIT $limit
""".strip()
RECIPES_BY_OUTPUT_ITEM_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_ITEM]->(i:Item {item_id: $item_id, meta: $meta})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_OUTPUT_ITEM_MACHINE_CYPHER = """
MATCH (r:Recipe {machine_id: $machine_id})-[:OUTPUT_ITEM]->(i:Item {item_id: $item_id, meta: $meta})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_OUTPUT_FLUID_CYPHER = """
MATCH (r:Recipe)-[:OUTPUT_FLUID]->(f:Fluid {fluid_id: $fluid_id})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_OUTPUT_FLUID_MACHINE_CYPHER = """
MATCH (r:Recipe {machine_id: $machine_id})-[:OUTPUT_FLUID]->(f:Fluid {fluid_id: $fluid_id})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

MACHINES_BY_INPUT_ITEM_CYPHER = """
MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe)
RETURN DISTINCT r.machine_id AS machine_id
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINE_COUNTS_BY_INPUT_ITEM_CYPHER = """
MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe)
RETURN r.machine_id AS machine_id, COUNT(r) AS recipe_count
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINES_BY_INPUT_FLUID_CYPHER = """
MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe)
RETURN DISTINCT r.machine_id AS machine_id
ORDER BY machine_id
LIMIT $limit
""".strip()

MACHINE_COUNTS_BY_INPUT_FLUID_CYPHER = """
MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe)
RETURN r.machine_id AS machine_id, COUNT(r) AS recipe_count
ORDER BY machine_id
LIMIT $limit
""".strip()

RECIPES_BY_INPUT_ITEM_CYPHER = """
MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe)
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_INPUT_ITEM_MACHINE_CYPHER = """
MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe {machine_id: $machine_id})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_INPUT_FLUID_CYPHER = """
MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe)
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

RECIPES_BY_INPUT_FLUID_MACHINE_CYPHER = """
MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe {machine_id: $machine_id})
RETURN r.rid AS rid, r.machine_id AS machine_id, r.duration_ticks AS duration_ticks, r.eut AS eut
LIMIT $limit
""".strip()

ItemKey = Tuple[str, int]


@dataclass
class GraphData:
    item_index: List[Tuple[str, str, int]]
    fluid_index: List[Tuple[str, str]]
    recipes: Dict[str, Dict[str, Any]]
    item_input_map: Dict[ItemKey, set[str]]
    fluid_input_map: Dict[str, set[str]]
    item_output_map: Dict[ItemKey, set[str]]
    fluid_output_map: Dict[str, set[str]]


def _require_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset file: {path}")
    return path


def load_graph_data(data_dir: Path) -> GraphData:
    recipes_path = _require_file(data_dir / "recipes.parquet")
    item_inputs_path = _require_file(data_dir / "item_inputs.parquet")
    item_outputs_path = _require_file(data_dir / "item_outputs.parquet")
    fluid_inputs_path = _require_file(data_dir / "fluid_inputs.parquet")
    fluid_outputs_path = _require_file(data_dir / "fluid_outputs.parquet")

    con = duckdb.connect(database=":memory:")
    recipe_rows = con.execute(
        "select rid, machine_id, duration_ticks, eut from read_parquet(?)",
        [str(recipes_path)],
    ).fetchall()
    recipes: Dict[str, Dict[str, Any]] = {}
    for rid, machine_id, duration_ticks, eut in recipe_rows:
        recipes[rid] = {
            "rid": rid,
            "machine_id": machine_id,
            "duration_ticks": int(duration_ticks),
            "eut": int(eut),
        }

    item_input_map: Dict[ItemKey, set[str]] = {}
    item_input_rows = con.execute(
        "select rid, item_id, meta from read_parquet(?)",
        [str(item_inputs_path)],
    ).fetchall()
    for rid, item_id, meta in item_input_rows:
        key = (item_id, int(meta))
        item_input_map.setdefault(key, set()).add(rid)

    fluid_input_map: Dict[str, set[str]] = {}
    fluid_input_rows = con.execute(
        "select rid, fluid_id from read_parquet(?)",
        [str(fluid_inputs_path)],
    ).fetchall()
    for rid, fluid_id in fluid_input_rows:
        fluid_input_map.setdefault(fluid_id, set()).add(rid)

    item_output_map: Dict[ItemKey, set[str]] = {}
    item_output_rows = con.execute(
        "select rid, item_id, meta from read_parquet(?)",
        [str(item_outputs_path)],
    ).fetchall()
    for rid, item_id, meta in item_output_rows:
        key = (item_id, int(meta))
        item_output_map.setdefault(key, set()).add(rid)

    fluid_output_map: Dict[str, set[str]] = {}
    fluid_output_rows = con.execute(
        "select rid, fluid_id from read_parquet(?)",
        [str(fluid_outputs_path)],
    ).fetchall()
    for rid, fluid_id in fluid_output_rows:
        fluid_output_map.setdefault(fluid_id, set()).add(rid)

    item_index_rows = con.execute(
        """
        select distinct item_id, meta
        from (
            select item_id, meta from read_parquet(?)
            union all
            select item_id, meta from read_parquet(?)
        )
        """,
        [str(item_inputs_path), str(item_outputs_path)],
    ).fetchall()
    item_index: List[Tuple[str, str, int]] = []
    for item_id, meta in item_index_rows:
        item_index.append((item_id.lower(), item_id, int(meta)))
    item_index.sort(key=lambda row: (row[0], row[2]))

    fluid_index_rows = con.execute(
        """
        select distinct fluid_id
        from (
            select fluid_id from read_parquet(?)
            union all
            select fluid_id from read_parquet(?)
        )
        """,
        [str(fluid_inputs_path), str(fluid_outputs_path)],
    ).fetchall()
    fluid_index: List[Tuple[str, str]] = []
    for (fluid_id,) in fluid_index_rows:
        fluid_index.append((fluid_id.lower(), fluid_id))
    fluid_index.sort(key=lambda row: row[0])

    con.close()
    return GraphData(
        item_index=item_index,
        fluid_index=fluid_index,
        recipes=recipes,
        item_input_map=item_input_map,
        fluid_input_map=fluid_input_map,
        item_output_map=item_output_map,
        fluid_output_map=fluid_output_map,
    )


def _build_downstream_recipe_cypher(
    start_match: str,
    target_match: str,
    input_rel: str,
    max_depth: int,
    with_machine_filter: bool,
) -> str:
    depth = max(1, int(max_depth))
    recipe_node = "r1:Recipe {machine_id: $machine_id}" if with_machine_filter else "r1:Recipe"
    blocks: List[str] = []
    for step_count in range(1, depth + 1):
        pattern = f"(start)-[:{input_rel}]->({recipe_node})"
        for step in range(1, step_count):
            pattern += (
                f"-[:OUTPUT_ITEM|OUTPUT_FLUID]->(n{step})"
                "-[:INPUT_ITEM|INPUT_FLUID]->"
                f"(r{step + 1}:Recipe)"
            )
        pattern += "-[:OUTPUT_ITEM|OUTPUT_FLUID]->(target)"
        block = (
            f"MATCH {start_match} "
            f"MATCH {target_match} "
            f"MATCH {pattern} "
            "RETURN DISTINCT r1.rid AS rid, r1.machine_id AS machine_id, "
            "r1.duration_ticks AS duration_ticks, r1.eut AS eut "
            "LIMIT $limit"
        )
        blocks.append(block)
    return "\nUNION\n".join(blocks)


class GraphStore:
    def query(self, cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def is_ready(self) -> bool:
        return True

    def warm_up(self) -> None:
        return None

    def wait_until_ready(self) -> None:
        return None

    def search_items(self, query: str, limit: int) -> List[Dict[str, Any]]:
        return self.query(SEARCH_ITEMS_CYPHER, {"query": query.lower(), "limit": int(limit)})

    def search_fluids(self, query: str, limit: int) -> List[Dict[str, Any]]:
        return self.query(SEARCH_FLUIDS_CYPHER, {"query": query.lower(), "limit": int(limit)})

    def machines_by_output_item(self, item_id: str, meta: int, limit: int) -> List[str]:
        rows = self.query(
            MACHINES_BY_OUTPUT_ITEM_CYPHER,
            {"item_id": item_id, "meta": int(meta), "limit": int(limit)},
        )
        return [row["machine_id"] for row in rows]

    def machine_counts_by_output_item(self, item_id: str, meta: int, limit: int) -> List[Dict[str, Any]]:
        return self.query(
            MACHINE_COUNTS_BY_OUTPUT_ITEM_CYPHER,
            {"item_id": item_id, "meta": int(meta), "limit": int(limit)},
        )

    def machines_by_output_fluid(self, fluid_id: str, limit: int) -> List[str]:
        rows = self.query(
            MACHINES_BY_OUTPUT_FLUID_CYPHER,
            {"fluid_id": fluid_id, "limit": int(limit)},
        )
        return [row["machine_id"] for row in rows]

    def machine_counts_by_output_fluid(self, fluid_id: str, limit: int) -> List[Dict[str, Any]]:
        return self.query(
            MACHINE_COUNTS_BY_OUTPUT_FLUID_CYPHER,
            {"fluid_id": fluid_id, "limit": int(limit)},
        )

    def recipes_by_output_item(
        self, item_id: str, meta: int, limit: int, machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cypher = RECIPES_BY_OUTPUT_ITEM_MACHINE_CYPHER if machine_id else RECIPES_BY_OUTPUT_ITEM_CYPHER
        params = {"item_id": item_id, "meta": int(meta), "limit": int(limit)}
        if machine_id:
            params["machine_id"] = machine_id
        return self.query(cypher, params)

    def recipes_by_output_fluid(
        self, fluid_id: str, limit: int, machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cypher = RECIPES_BY_OUTPUT_FLUID_MACHINE_CYPHER if machine_id else RECIPES_BY_OUTPUT_FLUID_CYPHER
        params = {"fluid_id": fluid_id, "limit": int(limit)}
        if machine_id:
            params["machine_id"] = machine_id
        return self.query(cypher, params)

    def machines_by_input_item(self, item_id: str, meta: int, limit: int) -> List[str]:
        rows = self.query(
            MACHINES_BY_INPUT_ITEM_CYPHER,
            {"item_id": item_id, "meta": int(meta), "limit": int(limit)},
        )
        return [row["machine_id"] for row in rows]

    def machine_counts_by_input_item(self, item_id: str, meta: int, limit: int) -> List[Dict[str, Any]]:
        return self.query(
            MACHINE_COUNTS_BY_INPUT_ITEM_CYPHER,
            {"item_id": item_id, "meta": int(meta), "limit": int(limit)},
        )

    def machines_by_input_fluid(self, fluid_id: str, limit: int) -> List[str]:
        rows = self.query(
            MACHINES_BY_INPUT_FLUID_CYPHER,
            {"fluid_id": fluid_id, "limit": int(limit)},
        )
        return [row["machine_id"] for row in rows]

    def machine_counts_by_input_fluid(self, fluid_id: str, limit: int) -> List[Dict[str, Any]]:
        return self.query(
            MACHINE_COUNTS_BY_INPUT_FLUID_CYPHER,
            {"fluid_id": fluid_id, "limit": int(limit)},
        )

    def recipes_by_input_item(
        self, item_id: str, meta: int, limit: int, machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cypher = RECIPES_BY_INPUT_ITEM_MACHINE_CYPHER if machine_id else RECIPES_BY_INPUT_ITEM_CYPHER
        params = {"item_id": item_id, "meta": int(meta), "limit": int(limit)}
        if machine_id:
            params["machine_id"] = machine_id
        return self.query(cypher, params)

    def recipes_by_input_fluid(
        self, fluid_id: str, limit: int, machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cypher = RECIPES_BY_INPUT_FLUID_MACHINE_CYPHER if machine_id else RECIPES_BY_INPUT_FLUID_CYPHER
        params = {"fluid_id": fluid_id, "limit": int(limit)}
        if machine_id:
            params["machine_id"] = machine_id
        return self.query(cypher, params)

    def recipes_by_input_item_downstream(
        self,
        item_id: str,
        meta: int,
        output_type: str,
        output_id: str,
        output_meta: int,
        max_depth: int,
        limit: int,
        machine_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._recipes_by_input_downstream(
            input_type="item",
            input_id=item_id,
            input_meta=int(meta),
            output_type=output_type,
            output_id=output_id,
            output_meta=int(output_meta),
            max_depth=max_depth,
            limit=limit,
            machine_id=machine_id,
        )

    def recipes_by_input_fluid_downstream(
        self,
        fluid_id: str,
        output_type: str,
        output_id: str,
        output_meta: int,
        max_depth: int,
        limit: int,
        machine_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._recipes_by_input_downstream(
            input_type="fluid",
            input_id=fluid_id,
            input_meta=0,
            output_type=output_type,
            output_id=output_id,
            output_meta=int(output_meta),
            max_depth=max_depth,
            limit=limit,
            machine_id=machine_id,
        )

    def _recipes_by_input_downstream(
        self,
        input_type: str,
        input_id: str,
        input_meta: int,
        output_type: str,
        output_id: str,
        output_meta: int,
        max_depth: int,
        limit: int,
        machine_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if output_type not in ("item", "fluid"):
            return []
        if max_depth <= 0 or limit <= 0:
            return []

        output_item_key = f"{output_id}|{int(output_meta)}" if output_type == "item" else None
        output_fluid_id = output_id if output_type == "fluid" else None
        if not output_item_key and not output_fluid_id:
            return []

        output_item_cache: Dict[str, set[str]] = {}
        output_fluid_cache: Dict[str, set[str]] = {}
        input_items_cache: Dict[str, set[str]] = {}
        input_fluids_cache: Dict[str, set[str]] = {}

        def _fill_output_item_cache(item_keys: Iterable[str]) -> None:
            missing = [item_key for item_key in item_keys if item_key not in output_item_cache]
            if not missing:
                return
            for item_key in missing:
                output_item_cache[item_key] = set()
            cypher = (
                "UNWIND $item_keys AS item_key "
                "MATCH (i:Item {item_key: item_key})<-[:OUTPUT_ITEM]-(r:Recipe) "
                "RETURN item_key AS item_key, r.rid AS rid"
            )
            for chunk in self._chunk_values(missing, size=200):
                rows = self.query(cypher, {"item_keys": chunk})
                for row in rows:
                    item_key = row.get("item_key")
                    rid = row.get("rid")
                    if item_key and rid:
                        output_item_cache.setdefault(item_key, set()).add(rid)

        def _fill_output_fluid_cache(fluid_ids: Iterable[str]) -> None:
            missing = [fluid_id for fluid_id in fluid_ids if fluid_id not in output_fluid_cache]
            if not missing:
                return
            for fluid_id in missing:
                output_fluid_cache[fluid_id] = set()
            cypher = (
                "UNWIND $fluid_ids AS fluid_id "
                "MATCH (f:Fluid {fluid_id: fluid_id})<-[:OUTPUT_FLUID]-(r:Recipe) "
                "RETURN fluid_id AS fluid_id, r.rid AS rid"
            )
            for chunk in self._chunk_values(missing, size=200):
                rows = self.query(cypher, {"fluid_ids": chunk})
                for row in rows:
                    fluid_id = row.get("fluid_id")
                    rid = row.get("rid")
                    if fluid_id and rid:
                        output_fluid_cache.setdefault(fluid_id, set()).add(rid)

        def _fill_input_items_cache(rids: Iterable[str]) -> None:
            missing = [rid for rid in rids if rid not in input_items_cache]
            if not missing:
                return
            for rid in missing:
                input_items_cache[rid] = set()
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (r:Recipe {rid: rid})<-[:INPUT_ITEM]-(i:Item) "
                "RETURN rid AS rid, i.item_key AS item_key"
            )
            for chunk in self._chunk_values(missing, size=200):
                rows = self.query(cypher, {"rids": chunk})
                for row in rows:
                    rid = row.get("rid")
                    item_key = row.get("item_key")
                    if rid and item_key:
                        input_items_cache.setdefault(rid, set()).add(item_key)

        def _fill_input_fluids_cache(rids: Iterable[str]) -> None:
            missing = [rid for rid in rids if rid not in input_fluids_cache]
            if not missing:
                return
            for rid in missing:
                input_fluids_cache[rid] = set()
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (r:Recipe {rid: rid})<-[:INPUT_FLUID]-(f:Fluid) "
                "RETURN rid AS rid, f.fluid_id AS fluid_id"
            )
            for chunk in self._chunk_values(missing, size=200):
                rows = self.query(cypher, {"rids": chunk})
                for row in rows:
                    rid = row.get("rid")
                    fluid_id = row.get("fluid_id")
                    if rid and fluid_id:
                        input_fluids_cache.setdefault(rid, set()).add(fluid_id)

        def _recipes_for_output_items(item_keys: Iterable[str]) -> set[str]:
            _fill_output_item_cache(item_keys)
            matched: set[str] = set()
            for item_key in item_keys:
                matched.update(output_item_cache.get(item_key, set()))
            return matched

        def _recipes_for_output_fluids(fluid_ids: Iterable[str]) -> set[str]:
            _fill_output_fluid_cache(fluid_ids)
            matched: set[str] = set()
            for fluid_id in fluid_ids:
                matched.update(output_fluid_cache.get(fluid_id, set()))
            return matched

        def _inputs_for_recipes(rids: Iterable[str]) -> Tuple[set[str], set[str]]:
            _fill_input_items_cache(rids)
            _fill_input_fluids_cache(rids)
            items: set[str] = set()
            fluids: set[str] = set()
            for rid in rids:
                items.update(input_items_cache.get(rid, set()))
                fluids.update(input_fluids_cache.get(rid, set()))
            return items, fluids

        frontier_items = {output_item_key} if output_item_key else set()
        frontier_fluids = {output_fluid_id} if output_fluid_id else set()
        seen_items = set(frontier_items)
        seen_fluids = set(frontier_fluids)
        seen_recipes: set[str] = set()
        reachable_recipes: set[str] = set()

        for _ in range(int(max_depth)):
            if not frontier_items and not frontier_fluids:
                break
            next_recipes: set[str] = set()
            if frontier_items:
                next_recipes.update(_recipes_for_output_items(frontier_items))
            if frontier_fluids:
                next_recipes.update(_recipes_for_output_fluids(frontier_fluids))
            next_recipes -= seen_recipes
            if not next_recipes:
                break
            seen_recipes.update(next_recipes)
            reachable_recipes.update(next_recipes)
            next_items, next_fluids = _inputs_for_recipes(next_recipes)
            frontier_items = next_items - seen_items
            frontier_fluids = next_fluids - seen_fluids
            seen_items.update(frontier_items)
            seen_fluids.update(frontier_fluids)

        if not reachable_recipes:
            return []
        if input_type == "item":
            matched_rids = self._recipes_consuming_item(
                reachable_recipes, input_id, input_meta, machine_id
            )
        else:
            matched_rids = self._recipes_consuming_fluid(reachable_recipes, input_id, machine_id)
        if not matched_rids:
            return []
        return self._recipe_rows_for_rids(matched_rids, limit, machine_id)

    def _chunk_values(self, values: Iterable[str], size: int = 500) -> Iterable[List[str]]:
        chunk: List[str] = []
        for value in values:
            chunk.append(value)
            if len(chunk) >= size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def _recipe_rows_for_rids(
        self, rids: Iterable[str], limit: int, machine_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        remaining = int(limit)
        if remaining <= 0:
            return []
        rows: List[Dict[str, Any]] = []
        if machine_id:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (r:Recipe {rid: rid}) "
                "WHERE r.machine_id = $machine_id "
                "RETURN r.rid AS rid, r.machine_id AS machine_id, "
                "r.duration_ticks AS duration_ticks, r.eut AS eut "
                "LIMIT $limit"
            )
        else:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (r:Recipe {rid: rid}) "
                "RETURN r.rid AS rid, r.machine_id AS machine_id, "
                "r.duration_ticks AS duration_ticks, r.eut AS eut "
                "LIMIT $limit"
            )
        for chunk in self._chunk_values(rids):
            remaining = int(limit) - len(rows)
            if remaining <= 0:
                break
            params: Dict[str, Any] = {"rids": chunk, "limit": remaining}
            if machine_id:
                params["machine_id"] = machine_id
            rows.extend(self.query(cypher, params))
        return rows[: int(limit)]

    def _recipes_by_output_items(self, item_keys: Iterable[str]) -> set[str]:
        rids: set[str] = set()
        cypher = (
            "UNWIND $item_keys AS item_key "
            "MATCH (i:Item {item_key: item_key})<-[:OUTPUT_ITEM]-(r:Recipe) "
            "RETURN DISTINCT r.rid AS rid"
        )
        for chunk in self._chunk_values(item_keys):
            rows = self.query(cypher, {"item_keys": chunk})
            rids.update(row["rid"] for row in rows if row.get("rid"))
        return rids

    def _recipes_by_output_fluids(self, fluid_ids: Iterable[str]) -> set[str]:
        rids: set[str] = set()
        cypher = (
            "UNWIND $fluid_ids AS fluid_id "
            "MATCH (f:Fluid {fluid_id: fluid_id})<-[:OUTPUT_FLUID]-(r:Recipe) "
            "RETURN DISTINCT r.rid AS rid"
        )
        for chunk in self._chunk_values(fluid_ids):
            rows = self.query(cypher, {"fluid_ids": chunk})
            rids.update(row["rid"] for row in rows if row.get("rid"))
        return rids

    def _input_items_for_recipes(self, rids: Iterable[str]) -> set[str]:
        items: set[str] = set()
        cypher = (
            "UNWIND $rids AS rid "
            "MATCH (r:Recipe {rid: rid})<-[:INPUT_ITEM]-(i:Item) "
            "RETURN DISTINCT i.item_key AS item_key"
        )
        for chunk in self._chunk_values(rids):
            rows = self.query(cypher, {"rids": chunk})
            items.update(row["item_key"] for row in rows if row.get("item_key"))
        return items

    def _input_fluids_for_recipes(self, rids: Iterable[str]) -> set[str]:
        fluids: set[str] = set()
        cypher = (
            "UNWIND $rids AS rid "
            "MATCH (r:Recipe {rid: rid})<-[:INPUT_FLUID]-(f:Fluid) "
            "RETURN DISTINCT f.fluid_id AS fluid_id"
        )
        for chunk in self._chunk_values(rids):
            rows = self.query(cypher, {"rids": chunk})
            fluids.update(row["fluid_id"] for row in rows if row.get("fluid_id"))
        return fluids

    def _recipes_consuming_item(
        self, rids: Iterable[str], item_id: str, meta: int, machine_id: Optional[str]
    ) -> set[str]:
        matched: set[str] = set()
        if machine_id:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe {rid: rid}) "
                "WHERE r.machine_id = $machine_id "
                "RETURN DISTINCT r.rid AS rid"
            )
        else:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (i:Item {item_id: $item_id, meta: $meta})-[:INPUT_ITEM]->(r:Recipe {rid: rid}) "
                "RETURN DISTINCT r.rid AS rid"
            )
        params_base = {"item_id": item_id, "meta": int(meta)}
        if machine_id:
            params_base["machine_id"] = machine_id
        for chunk in self._chunk_values(rids):
            rows = self.query(cypher, {**params_base, "rids": chunk})
            matched.update(row["rid"] for row in rows if row.get("rid"))
        return matched

    def _recipes_consuming_fluid(
        self, rids: Iterable[str], fluid_id: str, machine_id: Optional[str]
    ) -> set[str]:
        matched: set[str] = set()
        if machine_id:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe {rid: rid}) "
                "WHERE r.machine_id = $machine_id "
                "RETURN DISTINCT r.rid AS rid"
            )
        else:
            cypher = (
                "UNWIND $rids AS rid "
                "MATCH (f:Fluid {fluid_id: $fluid_id})-[:INPUT_FLUID]->(r:Recipe {rid: rid}) "
                "RETURN DISTINCT r.rid AS rid"
            )
        params_base = {"fluid_id": fluid_id}
        if machine_id:
            params_base["machine_id"] = machine_id
        for chunk in self._chunk_values(rids):
            rows = self.query(cypher, {**params_base, "rids": chunk})
            matched.update(row["rid"] for row in rows if row.get("rid"))
        return matched

    def _recipe_rows_by_rids(self, rids: Iterable[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        cypher = (
            "UNWIND $rids AS rid "
            "MATCH (r:Recipe {rid: rid}) "
            "RETURN r.rid AS rid, r.machine_id AS machine_id, "
            "r.duration_ticks AS duration_ticks, r.eut AS eut"
        )
        for chunk in self._chunk_values(rids):
            rows.extend(self.query(cypher, {"rids": chunk}))
        return rows


class LadybugGraphStore(GraphStore):
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._db = None
        self._conn = None
        self._create_graph()
        self._loaded = False
        self._loading = False
        self._load_error: Optional[Exception] = None
        self._lock = threading.Lock()

    def _create_graph(self) -> None:
        try:
            import real_ladybug as ladybugdb  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "GRAPH_DB=ladybugdb requires the real-ladybug package to be installed."
            ) from exc

        db_path = self.data_dir.parent / "ladybugdb"
        if db_path.exists():
            self._maybe_reset_incomplete_db(db_path)
        max_threads = os.cpu_count() or 0
        self._db = ladybugdb.Database(db_path, max_num_threads=max_threads)
        self._conn = ladybugdb.Connection(self._db)

    def _execute(self, cypher: str, params: Dict[str, Any]) -> Any:
        if self._conn is None:
            raise RuntimeError("LadybugDB connection is not initialized.")
        return self._conn.execute(cypher, params or {})

    def _result_rows(self, result: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if result is None:
            return rows
        if isinstance(result, list):
            for part in result:
                if hasattr(part, "rows_as_dict"):
                    part.rows_as_dict()
                    rows.extend(part.get_all())
            return rows
        if hasattr(result, "rows_as_dict"):
            result.rows_as_dict()
            return result.get_all()
        try:
            return list(result)
        except TypeError:
            return rows

    def _execute_rows(self, cypher: str, rows: List[Dict[str, Any]], per_row_cypher: str) -> None:
        if not rows:
            return
        try:
            self._execute(cypher, {"rows": rows})
            return
        except Exception:
            for row in rows:
                self._execute(per_row_cypher, row)

    def _chunked(self, rows: Iterable[Dict[str, Any]], size: int = 1000) -> Iterable[List[Dict[str, Any]]]:
        chunk: List[Dict[str, Any]] = []
        for row in rows:
            chunk.append(row)
            if len(chunk) >= size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def _load_graph(self, data: GraphData) -> None:
        if self._conn is None:
            raise RuntimeError("LadybugDB connection is not initialized.")
        self._ensure_schema()
        item_rows = [{"item_id": item_id, "meta": meta} for _, item_id, meta in data.item_index]
        for row in item_rows:
            row["item_key"] = f"{row['item_id']}|{row['meta']}"
        for chunk in self._chunked(item_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MERGE (i:Item {item_key: row.item_key}) "
                    "SET i.item_id = row.item_id, i.meta = row.meta"
                ),
                chunk,
                (
                    "MERGE (i:Item {item_key: $item_key}) "
                    "SET i.item_id = $item_id, i.meta = $meta"
                ),
            )

        fluid_rows = [{"fluid_id": fluid_id} for _, fluid_id in data.fluid_index]
        for chunk in self._chunked(fluid_rows):
            self._execute_rows(
                "UNWIND $rows AS row MERGE (f:Fluid {fluid_id: row.fluid_id})",
                chunk,
                "MERGE (f:Fluid {fluid_id: $fluid_id})",
            )

        recipe_rows = [
            {
                "rid": recipe["rid"],
                "machine_id": recipe.get("machine_id"),
                "duration_ticks": recipe.get("duration_ticks"),
                "eut": recipe.get("eut"),
            }
            for recipe in data.recipes.values()
        ]
        for chunk in self._chunked(recipe_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MERGE (r:Recipe {rid: row.rid}) "
                    "SET r.machine_id = row.machine_id, r.duration_ticks = row.duration_ticks, r.eut = row.eut"
                ),
                chunk,
                (
                    "MERGE (r:Recipe {rid: $rid}) "
                    "SET r.machine_id = $machine_id, r.duration_ticks = $duration_ticks, r.eut = $eut"
                ),
            )

        item_input_rows = [
            {"rid": rid, "item_id": item_id, "meta": meta, "item_key": f"{item_id}|{meta}"}
            for (item_id, meta), rids in data.item_input_map.items()
            for rid in rids
        ]
        for chunk in self._chunked(item_input_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MATCH (r:Recipe {rid: row.rid}), (i:Item {item_key: row.item_key}) "
                    "MERGE (i)-[:INPUT_ITEM]->(r)"
                ),
                chunk,
                (
                    "MATCH (r:Recipe {rid: $rid}), (i:Item {item_key: $item_key}) "
                    "MERGE (i)-[:INPUT_ITEM]->(r)"
                ),
            )

        fluid_input_rows = [
            {"rid": rid, "fluid_id": fluid_id}
            for fluid_id, rids in data.fluid_input_map.items()
            for rid in rids
        ]
        for chunk in self._chunked(fluid_input_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MATCH (r:Recipe {rid: row.rid}), (f:Fluid {fluid_id: row.fluid_id}) "
                    "MERGE (f)-[:INPUT_FLUID]->(r)"
                ),
                chunk,
                (
                    "MATCH (r:Recipe {rid: $rid}), (f:Fluid {fluid_id: $fluid_id}) "
                    "MERGE (f)-[:INPUT_FLUID]->(r)"
                ),
            )

        item_output_rows = [
            {"rid": rid, "item_id": item_id, "meta": meta, "item_key": f"{item_id}|{meta}"}
            for (item_id, meta), rids in data.item_output_map.items()
            for rid in rids
        ]
        for chunk in self._chunked(item_output_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MATCH (r:Recipe {rid: row.rid}), (i:Item {item_key: row.item_key}) "
                    "MERGE (r)-[:OUTPUT_ITEM]->(i)"
                ),
                chunk,
                (
                    "MATCH (r:Recipe {rid: $rid}), (i:Item {item_key: $item_key}) "
                    "MERGE (r)-[:OUTPUT_ITEM]->(i)"
                ),
            )

        fluid_output_rows = [
            {"rid": rid, "fluid_id": fluid_id}
            for fluid_id, rids in data.fluid_output_map.items()
            for rid in rids
        ]
        for chunk in self._chunked(fluid_output_rows):
            self._execute_rows(
                (
                    "UNWIND $rows AS row "
                    "MATCH (r:Recipe {rid: row.rid}), (f:Fluid {fluid_id: row.fluid_id}) "
                    "MERGE (r)-[:OUTPUT_FLUID]->(f)"
                ),
                chunk,
                (
                    "MATCH (r:Recipe {rid: $rid}), (f:Fluid {fluid_id: $fluid_id}) "
                    "MERGE (r)-[:OUTPUT_FLUID]->(f)"
                ),
            )

        self._execute(
            "MERGE (m:Meta {key: $key}) SET m.value = $value",
            {"key": "loaded", "value": "true"},
        )

    def _maybe_reset_incomplete_db(self, db_path: Path) -> None:
        try:
            import real_ladybug as ladybugdb  # type: ignore
        except ImportError:
            return
        try:
            db = ladybugdb.Database(db_path, read_only=True)
            conn = ladybugdb.Connection(db)
            result = conn.execute(
                "MATCH (m:Meta {key: $key}) RETURN m.value AS value LIMIT 1",
                {"key": "loaded"},
            )
            rows = self._result_rows(result)
            conn.close()
            db.close()
            if rows:
                return
            logger.warning("ladybugdb incomplete; resetting %s", db_path)
        except Exception:
            logger.warning("ladybugdb incomplete or unreadable; resetting %s", db_path)
        try:
            if db_path.is_dir():
                shutil.rmtree(db_path)
            elif db_path.exists():
                db_path.unlink()
        except Exception as exc:
            logger.warning("ladybugdb reset failed: %s", exc)

    def _bulk_import_parquet(self) -> None:
        if self._conn is None:
            raise RuntimeError("LadybugDB connection is not initialized.")
        self._ensure_schema()

        import duckdb

        stage_dir = self.data_dir / "_ladybug_import"
        if stage_dir.exists():
            shutil.rmtree(stage_dir, ignore_errors=True)
        stage_dir.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(database=":memory:")

        def _path_sql(path: Path) -> str:
            return str(path).replace("\\", "/").replace("'", "''")

        item_nodes_path = _path_sql(stage_dir / "item_nodes.parquet")
        fluid_nodes_path = _path_sql(stage_dir / "fluid_nodes.parquet")
        recipe_nodes_path = _path_sql(stage_dir / "recipe_nodes.parquet")
        input_item_path = _path_sql(stage_dir / "input_item.parquet")
        input_fluid_path = _path_sql(stage_dir / "input_fluid.parquet")
        output_item_path = _path_sql(stage_dir / "output_item.parquet")
        output_fluid_path = _path_sql(stage_dir / "output_fluid.parquet")

        item_inputs = _path_sql(self.data_dir / "item_inputs.parquet")
        item_outputs = _path_sql(self.data_dir / "item_outputs.parquet")
        fluid_inputs = _path_sql(self.data_dir / "fluid_inputs.parquet")
        fluid_outputs = _path_sql(self.data_dir / "fluid_outputs.parquet")
        recipes_path = _path_sql(self.data_dir / "recipes.parquet")

        logger.info("ladybugdb import: staging item nodes")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    item_id || '|' || CAST(meta AS VARCHAR) AS item_key,
                    item_id,
                    CAST(meta AS BIGINT) AS meta
                FROM (
                    SELECT item_id, meta FROM read_parquet('{item_inputs}')
                    UNION ALL
                    SELECT item_id, meta FROM read_parquet('{item_outputs}')
                )
            ) TO '{item_nodes_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging fluid nodes")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT fluid_id
                FROM (
                    SELECT fluid_id FROM read_parquet('{fluid_inputs}')
                    UNION ALL
                    SELECT fluid_id FROM read_parquet('{fluid_outputs}')
                )
            ) TO '{fluid_nodes_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging recipe nodes")
        con.execute(
            f"""
            COPY (
                SELECT
                    rid,
                    MIN(machine_id) AS machine_id,
                    CAST(MIN(duration_ticks) AS BIGINT) AS duration_ticks,
                    CAST(MIN(eut) AS BIGINT) AS eut
                FROM read_parquet('{recipes_path}')
                GROUP BY rid
            ) TO '{recipe_nodes_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging output item rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    o.rid AS "from",
                    item_id || '|' || CAST(meta AS VARCHAR) AS "to"
                FROM read_parquet('{item_outputs}') o
                JOIN read_parquet('{recipes_path}') r
                ON o.rid = r.rid
            ) TO '{output_item_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging input item rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    item_id || '|' || CAST(meta AS VARCHAR) AS "from",
                    i.rid AS "to"
                FROM read_parquet('{item_inputs}') i
                JOIN read_parquet('{recipes_path}') r
                ON i.rid = r.rid
            ) TO '{input_item_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging output fluid rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    o.rid AS "from",
                    fluid_id AS "to"
                FROM read_parquet('{fluid_outputs}') o
                JOIN read_parquet('{recipes_path}') r
                ON o.rid = r.rid
            ) TO '{output_fluid_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging input fluid rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    fluid_id AS "from",
                    i.rid AS "to"
                FROM read_parquet('{fluid_inputs}') i
                JOIN read_parquet('{recipes_path}') r
                ON i.rid = r.rid
            ) TO '{input_fluid_path}' (FORMAT PARQUET)
            """
        )

        con.close()

        logger.info("ladybugdb import: COPY Item")
        self._execute(f"COPY Item FROM '{item_nodes_path}'", {})
        logger.info("ladybugdb import: COPY Fluid")
        self._execute(f"COPY Fluid FROM '{fluid_nodes_path}'", {})
        logger.info("ladybugdb import: COPY Recipe")
        self._execute(f"COPY Recipe FROM '{recipe_nodes_path}'", {})
        logger.info("ladybugdb import: COPY INPUT_ITEM")
        self._execute(f"COPY INPUT_ITEM FROM '{input_item_path}'", {})
        logger.info("ladybugdb import: COPY INPUT_FLUID")
        self._execute(f"COPY INPUT_FLUID FROM '{input_fluid_path}'", {})
        logger.info("ladybugdb import: COPY OUTPUT_ITEM")
        self._execute(f"COPY OUTPUT_ITEM FROM '{output_item_path}'", {})
        logger.info("ladybugdb import: COPY OUTPUT_FLUID")
        self._execute(f"COPY OUTPUT_FLUID FROM '{output_fluid_path}'", {})

        self._execute(
            "MERGE (m:Meta {key: $key}) SET m.value = $value",
            {"key": "loaded", "value": "true"},
        )
        self._execute(
            "MERGE (m:Meta {key: $key}) SET m.value = $value",
            {"key": "input_loaded", "value": "true"},
        )

        try:
            shutil.rmtree(stage_dir)
        except Exception:
            logger.warning("ladybugdb cleanup failed: %s", stage_dir)

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            with self._lock:
                if not self._loaded:
                    self._loading = True
                    try:
                        if not self._has_data():
                            self._bulk_import_parquet()
                        self._loaded = True
                        self._ensure_input_rels()
                    finally:
                        self._loading = False

    def query(self, cypher: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._ensure_loaded()
        logger.info("ladybugdb query: %s", cypher.splitlines()[0])
        result = self._execute(cypher, params)
        return self._result_rows(result)

    def is_ready(self) -> bool:
        return self._loaded

    def warm_up(self) -> None:
        if self._loaded or self._loading:
            return

        def _runner() -> None:
            try:
                self._ensure_loaded()
            except Exception as exc:
                self._load_error = exc
            finally:
                self._loading = False

        self._loading = True
        thread = threading.Thread(target=_runner, name="ladybugdb-warmup", daemon=True)
        thread.start()

    def wait_until_ready(self) -> None:
        self._ensure_loaded()
        if self._load_error:
            raise self._load_error

    def _ensure_schema(self) -> None:
        if self._conn is None:
            raise RuntimeError("LadybugDB connection is not initialized.")
        tables = self._execute("CALL show_tables() RETURN *;", {})
        table_rows = self._result_rows(tables)
        existing = {(row.get("name"), row.get("type")) for row in table_rows}

        if ("Item", "NODE") not in existing:
            self._execute(
                "CREATE NODE TABLE Item(item_key STRING, item_id STRING, meta INT64, PRIMARY KEY (item_key))",
                {},
            )
        if ("Fluid", "NODE") not in existing:
            self._execute(
                "CREATE NODE TABLE Fluid(fluid_id STRING, PRIMARY KEY (fluid_id))",
                {},
            )
        if ("Recipe", "NODE") not in existing:
            self._execute(
                (
                    "CREATE NODE TABLE Recipe("
                    "rid STRING, machine_id STRING, duration_ticks INT64, eut INT64, PRIMARY KEY (rid))"
                ),
                {},
            )
        if ("Meta", "NODE") not in existing:
            self._execute(
                "CREATE NODE TABLE Meta(key STRING, value STRING, PRIMARY KEY (key))",
                {},
            )
        if ("OUTPUT_ITEM", "REL") not in existing:
            self._execute("CREATE REL TABLE OUTPUT_ITEM(FROM Recipe TO Item)", {})
        if ("OUTPUT_FLUID", "REL") not in existing:
            self._execute("CREATE REL TABLE OUTPUT_FLUID(FROM Recipe TO Fluid)", {})
        if ("INPUT_ITEM", "REL") not in existing:
            self._execute("CREATE REL TABLE INPUT_ITEM(FROM Item TO Recipe)", {})
        if ("INPUT_FLUID", "REL") not in existing:
            self._execute("CREATE REL TABLE INPUT_FLUID(FROM Fluid TO Recipe)", {})

    def _has_data(self) -> bool:
        try:
            result = self._execute(
                "MATCH (m:Meta {key: $key}) RETURN m.value AS value LIMIT 1",
                {"key": "loaded"},
            )
            return len(self._result_rows(result)) > 0
        except Exception:
            return False

    def _inputs_loaded(self) -> bool:
        try:
            result = self._execute(
                "MATCH (m:Meta {key: $key}) RETURN m.value AS value LIMIT 1",
                {"key": "input_loaded"},
            )
            return len(self._result_rows(result)) > 0
        except Exception:
            return False

    def _bulk_import_inputs(self) -> None:
        if self._conn is None:
            raise RuntimeError("LadybugDB connection is not initialized.")
        self._ensure_schema()

        import duckdb

        stage_dir = self.data_dir / "_ladybug_import"
        if stage_dir.exists():
            shutil.rmtree(stage_dir, ignore_errors=True)
        stage_dir.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(database=":memory:")

        def _path_sql(path: Path) -> str:
            return str(path).replace("\\", "/").replace("'", "''")

        input_item_path = _path_sql(stage_dir / "input_item.parquet")
        input_fluid_path = _path_sql(stage_dir / "input_fluid.parquet")
        item_inputs = _path_sql(self.data_dir / "item_inputs.parquet")
        fluid_inputs = _path_sql(self.data_dir / "fluid_inputs.parquet")
        recipes_path = _path_sql(self.data_dir / "recipes.parquet")

        logger.info("ladybugdb import: staging input item rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    item_id || '|' || CAST(meta AS VARCHAR) AS "from",
                    i.rid AS "to"
                FROM read_parquet('{item_inputs}') i
                JOIN read_parquet('{recipes_path}') r
                ON i.rid = r.rid
            ) TO '{input_item_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging input fluid rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    fluid_id AS "from",
                    i.rid AS "to"
                FROM read_parquet('{fluid_inputs}') i
                JOIN read_parquet('{recipes_path}') r
                ON i.rid = r.rid
            ) TO '{input_fluid_path}' (FORMAT PARQUET)
            """
        )

        con.close()

        logger.info("ladybugdb import: COPY INPUT_ITEM")
        self._execute(f"COPY INPUT_ITEM FROM '{input_item_path}'", {})
        logger.info("ladybugdb import: COPY INPUT_FLUID")
        self._execute(f"COPY INPUT_FLUID FROM '{input_fluid_path}'", {})

        self._execute(
            "MERGE (m:Meta {key: $key}) SET m.value = $value",
            {"key": "input_loaded", "value": "true"},
        )

        try:
            shutil.rmtree(stage_dir)
        except Exception:
            logger.warning("ladybugdb cleanup failed: %s", stage_dir)

    def _ensure_input_rels(self) -> None:
        if self._inputs_loaded():
            return
        try:
            self._bulk_import_inputs()
        except Exception as exc:
            logger.warning("ladybugdb input import failed: %s", exc)


def create_graph_store(backend: str, data_dir: Path) -> Optional[GraphStore]:
    backend = backend.lower().strip()
    if backend in ("off", "none", "duckdb"):
        return None
    if backend in ("ladybugdb", "real-ladybug", "real_ladybug"):
        return LadybugGraphStore(data_dir)
    if backend == "memory":
        raise ValueError("GRAPH_DB=memory is not supported. Use GRAPH_DB=ladybugdb or GRAPH_DB=off.")
    raise ValueError(f"Unknown graph backend: {backend}")
