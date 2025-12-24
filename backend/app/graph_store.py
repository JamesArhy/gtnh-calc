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


ItemKey = Tuple[str, int]


@dataclass
class GraphData:
    item_index: List[Tuple[str, str, int]]
    fluid_index: List[Tuple[str, str]]
    recipes: Dict[str, Dict[str, Any]]
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
        item_output_map=item_output_map,
        fluid_output_map=fluid_output_map,
    )


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
                    rid AS "from",
                    item_id || '|' || CAST(meta AS VARCHAR) AS "to"
                FROM read_parquet('{item_outputs}')
            ) TO '{output_item_path}' (FORMAT PARQUET)
            """
        )

        logger.info("ladybugdb import: staging output fluid rels")
        con.execute(
            f"""
            COPY (
                SELECT DISTINCT
                    rid AS "from",
                    fluid_id AS "to"
                FROM read_parquet('{fluid_outputs}')
            ) TO '{output_fluid_path}' (FORMAT PARQUET)
            """
        )

        con.close()

        logger.info("ladybugdb import: COPY Item")
        self._execute(f"COPY Item FROM '{item_nodes_path}'", {})
        logger.info("ladybugdb import: COPY Fluid")
        self._execute(f"COPY Fluid FROM '{fluid_nodes_path}'", {})
        logger.info("ladybugdb import: COPY Recipe")
        self._execute(f"COPY Recipe FROM '{recipe_nodes_path}'", {})
        logger.info("ladybugdb import: COPY OUTPUT_ITEM")
        self._execute(f"COPY OUTPUT_ITEM FROM '{output_item_path}'", {})
        logger.info("ladybugdb import: COPY OUTPUT_FLUID")
        self._execute(f"COPY OUTPUT_FLUID FROM '{output_fluid_path}'", {})

        self._execute(
            "MERGE (m:Meta {key: $key}) SET m.value = $value",
            {"key": "loaded", "value": "true"},
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

    def _has_data(self) -> bool:
        try:
            result = self._execute(
                "MATCH (m:Meta {key: $key}) RETURN m.value AS value LIMIT 1",
                {"key": "loaded"},
            )
            return len(self._result_rows(result)) > 0
        except Exception:
            return False


def create_graph_store(backend: str, data_dir: Path) -> Optional[GraphStore]:
    backend = backend.lower().strip()
    if backend in ("off", "none", "duckdb"):
        return None
    if backend in ("ladybugdb", "real-ladybug", "real_ladybug"):
        return LadybugGraphStore(data_dir)
    if backend == "memory":
        raise ValueError("GRAPH_DB=memory is not supported. Use GRAPH_DB=ladybugdb or GRAPH_DB=off.")
    raise ValueError(f"Unknown graph backend: {backend}")
