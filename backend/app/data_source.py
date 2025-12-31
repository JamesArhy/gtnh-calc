from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import duckdb


@dataclass
class DuckDBDataset:
    version: str
    data_dir: Path
    con: duckdb.DuckDBPyConnection
    _item_outputs_has_chance: Optional[bool] = None
    _fluid_outputs_has_chance: Optional[bool] = None

    def close(self) -> None:
        self.con.close()

    def list_item_matches(self, query: str, limit: int) -> list[dict]:
        sql = """
            select distinct item_id, meta
            from (
                select item_id, meta from read_parquet(?)
                union all
                select item_id, meta from read_parquet(?)
            )
            where item_id ilike ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "item_inputs.parquet"),
                str(self.data_dir / "item_outputs.parquet"),
                f"%{query}%",
                int(limit),
            ],
        ).fetchall()
        return [
            {"item_id": row[0], "meta": int(row[1])}
            for row in rows
        ]

    def list_fluid_matches(self, query: str, limit: int) -> list[dict]:
        sql = """
            select distinct fluid_id
            from (
                select fluid_id from read_parquet(?)
                union all
                select fluid_id from read_parquet(?)
            )
            where fluid_id ilike ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "fluid_inputs.parquet"),
                str(self.data_dir / "fluid_outputs.parquet"),
                f"%{query}%",
                int(limit),
            ],
        ).fetchall()
        return [{"fluid_id": row[0]} for row in rows]

    def recipe_by_rid(self, rid: str) -> Optional[dict]:
        sql = """
            select rid, machine_id, recipe_class, duration_ticks, eut
            from read_parquet(?)
            where rid = ?
        """
        row = self.con.execute(sql, [str(self.data_dir / "recipes.parquet"), rid]).fetchone()
        if not row:
            return None
        return {
            "rid": row[0],
            "machine_id": row[1],
            "recipe_class": row[2],
            "duration_ticks": int(row[3]),
            "eut": int(row[4]),
        }

    def recipe_inputs(self, rid: str) -> dict:
        items = self.con.execute(
            """
            select item_id, meta, count
            from read_parquet(?)
            where rid = ?
            """,
            [str(self.data_dir / "item_inputs.parquet"), rid],
        ).fetchall()
        fluids = self.con.execute(
            """
            select fluid_id, mb
            from read_parquet(?)
            where rid = ?
            """,
            [str(self.data_dir / "fluid_inputs.parquet"), rid],
        ).fetchall()
        return {
            "items": [{"item_id": r[0], "meta": int(r[1]), "count": int(r[2])} for r in items],
            "fluids": [{"fluid_id": r[0], "mb": int(r[1])} for r in fluids],
        }

    def recipe_outputs(self, rid: str) -> dict:
        item_outputs_path = str(self.data_dir / "item_outputs.parquet")
        fluid_outputs_path = str(self.data_dir / "fluid_outputs.parquet")
        if self._item_outputs_has_chance is None:
            try:
                self.con.execute("select chance from read_parquet(?) limit 1", [item_outputs_path])
                self._item_outputs_has_chance = True
            except duckdb.Error:
                self._item_outputs_has_chance = False
        if self._fluid_outputs_has_chance is None:
            try:
                self.con.execute("select chance from read_parquet(?) limit 1", [fluid_outputs_path])
                self._fluid_outputs_has_chance = True
            except duckdb.Error:
                self._fluid_outputs_has_chance = False

        if self._item_outputs_has_chance:
            items = self.con.execute(
                """
                select item_id, meta, count, chance
                from read_parquet(?)
                where rid = ?
                """,
                [item_outputs_path, rid],
            ).fetchall()
        else:
            items = self.con.execute(
                """
                select item_id, meta, count
                from read_parquet(?)
                where rid = ?
                """,
                [item_outputs_path, rid],
            ).fetchall()

        if self._fluid_outputs_has_chance:
            fluids = self.con.execute(
                """
                select fluid_id, mb, chance
                from read_parquet(?)
                where rid = ?
                """,
                [fluid_outputs_path, rid],
            ).fetchall()
        else:
            fluids = self.con.execute(
                """
                select fluid_id, mb
                from read_parquet(?)
                where rid = ?
                """,
                [fluid_outputs_path, rid],
            ).fetchall()
        return {
            "items": [
                {
                    "item_id": r[0],
                    "meta": int(r[1]),
                    "count": int(r[2]),
                    "chance": float(r[3]) if self._item_outputs_has_chance and r[3] is not None else None,
                }
                for r in items
            ],
            "fluids": [
                {
                    "fluid_id": r[0],
                    "mb": int(r[1]),
                    "chance": float(r[2]) if self._fluid_outputs_has_chance and r[2] is not None else None,
                }
                for r in fluids
            ],
        }

    def recipes_for_output_item(self, item_id: str, meta: int, limit: int) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.item_id = ? and o.meta = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_outputs.parquet"),
                item_id,
                int(meta),
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_output_item_by_machine(
        self, item_id: str, meta: int, machine_id: str, limit: int
    ) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.item_id = ? and o.meta = ? and r.machine_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_outputs.parquet"),
                item_id,
                int(meta),
                machine_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_output_fluid(self, fluid_id: str, limit: int) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.fluid_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_outputs.parquet"),
                fluid_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_output_fluid_by_machine(
        self, fluid_id: str, machine_id: str, limit: int
    ) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.fluid_id = ? and r.machine_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_outputs.parquet"),
                fluid_id,
                machine_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_input_item(self, item_id: str, meta: int, limit: int) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.item_id = ? and i.meta = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_inputs.parquet"),
                item_id,
                int(meta),
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_input_item_by_machine(
        self, item_id: str, meta: int, machine_id: str, limit: int
    ) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.item_id = ? and i.meta = ? and r.machine_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_inputs.parquet"),
                item_id,
                int(meta),
                machine_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_input_fluid(self, fluid_id: str, limit: int) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.fluid_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_inputs.parquet"),
                fluid_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def recipes_for_input_fluid_by_machine(
        self, fluid_id: str, machine_id: str, limit: int
    ) -> list[dict]:
        sql = """
            select r.rid, r.machine_id, r.duration_ticks, r.eut
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.fluid_id = ? and r.machine_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_inputs.parquet"),
                fluid_id,
                machine_id,
                int(limit),
            ],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]

    def machines_for_output_item(self, item_id: str, meta: int, limit: int = 200) -> list[str]:
        sql = """
            select distinct r.machine_id
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.item_id = ? and o.meta = ?
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_outputs.parquet"),
                item_id,
                int(meta),
                int(limit),
            ],
        ).fetchall()
        return [row[0] for row in rows]

    def machines_for_output_fluid(self, fluid_id: str, limit: int = 200) -> list[str]:
        sql = """
            select distinct r.machine_id
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.fluid_id = ?
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_outputs.parquet"),
                fluid_id,
                int(limit),
            ],
        ).fetchall()
        return [row[0] for row in rows]

    def machine_recipe_counts_for_output_item(
        self, item_id: str, meta: int, limit: int = 200
    ) -> list[dict]:
        sql = """
            select r.machine_id, count(distinct r.rid) as recipe_count
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.item_id = ? and o.meta = ?
            group by r.machine_id
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_outputs.parquet"),
                item_id,
                int(meta),
                int(limit),
            ],
        ).fetchall()
        return [{"machine_id": row[0], "recipe_count": int(row[1])} for row in rows]

    def machine_recipe_counts_for_output_fluid(self, fluid_id: str, limit: int = 200) -> list[dict]:
        sql = """
            select r.machine_id, count(distinct r.rid) as recipe_count
            from read_parquet(?) r
            join read_parquet(?) o
            on r.rid = o.rid
            where o.fluid_id = ?
            group by r.machine_id
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_outputs.parquet"),
                fluid_id,
                int(limit),
            ],
        ).fetchall()
        return [{"machine_id": row[0], "recipe_count": int(row[1])} for row in rows]

    def machine_recipe_counts_for_input_item(
        self, item_id: str, meta: int, limit: int = 200
    ) -> list[dict]:
        sql = """
            select r.machine_id, count(distinct r.rid) as recipe_count
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.item_id = ? and i.meta = ?
            group by r.machine_id
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "item_inputs.parquet"),
                item_id,
                int(meta),
                int(limit),
            ],
        ).fetchall()
        return [{"machine_id": row[0], "recipe_count": int(row[1])} for row in rows]

    def machine_recipe_counts_for_input_fluid(self, fluid_id: str, limit: int = 200) -> list[dict]:
        sql = """
            select r.machine_id, count(distinct r.rid) as recipe_count
            from read_parquet(?) r
            join read_parquet(?) i
            on r.rid = i.rid
            where i.fluid_id = ?
            group by r.machine_id
            order by r.machine_id
            limit ?
        """
        rows = self.con.execute(
            sql,
            [
                str(self.data_dir / "recipes.parquet"),
                str(self.data_dir / "fluid_inputs.parquet"),
                fluid_id,
                int(limit),
            ],
        ).fetchall()
        return [{"machine_id": row[0], "recipe_count": int(row[1])} for row in rows]

    def list_machines(self) -> list[str]:
        sql = """
            select distinct machine_id
            from read_parquet(?)
            order by machine_id
        """
        rows = self.con.execute(sql, [str(self.data_dir / "recipes.parquet")]).fetchall()
        return [row[0] for row in rows]

    def recipes_for_machine(self, machine_id: str, limit: int) -> list[dict]:
        sql = """
            select rid, machine_id, duration_ticks, eut
            from read_parquet(?)
            where machine_id = ?
            limit ?
        """
        rows = self.con.execute(
            sql,
            [str(self.data_dir / "recipes.parquet"), machine_id, int(limit)],
        ).fetchall()
        return [
            {
                "rid": r[0],
                "machine_id": r[1],
                "duration_ticks": int(r[2]),
                "eut": int(r[3]),
            }
            for r in rows
        ]


class DataSource:
    def list_versions(self) -> Iterable[str]:
        raise NotImplementedError

    def open_dataset(self, version: str) -> DuckDBDataset:
        raise NotImplementedError


class LocalDataSource(DataSource):
    def __init__(self, data_dir: Path, default_version: str) -> None:
        self.data_dir = data_dir
        self.default_version = default_version

    def list_versions(self) -> Iterable[str]:
        return [self.default_version]

    def open_dataset(self, version: str) -> DuckDBDataset:
        if version != self.default_version:
            raise ValueError(f"Unknown version: {version}")
        con = duckdb.connect(database=":memory:")
        return DuckDBDataset(version=version, data_dir=self.data_dir, con=con)


class S3DataSource(DataSource):
    def __init__(self, *args, **kwargs) -> None:
        pass

    def list_versions(self) -> Iterable[str]:
        raise NotImplementedError("S3 data source not implemented")

    def open_dataset(self, version: str) -> DuckDBDataset:
        raise NotImplementedError("S3 data source not implemented")
