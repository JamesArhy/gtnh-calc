from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from .gtnh import MachineBonus


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_bonus(value: Any, default: float = 1.0) -> float:
    val = _as_float(value)
    if val is None or val <= 0:
        return default
    return val


def _normalize_efficiency_bonus(value: Any) -> float:
    val = _normalize_bonus(value, 1.0)
    # Efficiency comes through as either a multiplier (0.8) or percent (80).
    if val > 5:
        return val / 100.0
    return val


def _normalize_max_parallel(value: Any) -> Optional[float]:
    val = _as_float(value)
    if val is None or val <= 0:
        return None
    return val


def _entry_bonus(entry: Dict[str, Any]) -> MachineBonus:
    speed_bonus = _normalize_bonus(entry.get("speed_bonus"), 1.0)
    # Coil bonus is treated as an extra speed multiplier when present.
    speed_bonus *= _normalize_bonus(entry.get("coil_bonus"), 1.0)
    parallel_bonus = _normalize_bonus(entry.get("parallel_bonus"), 1.0)
    efficiency_bonus = _normalize_efficiency_bonus(entry.get("efficiency_bonus"))
    max_parallel = _normalize_max_parallel(entry.get("max_parallel"))
    return MachineBonus(
        speed_bonus=speed_bonus,
        efficiency_bonus=efficiency_bonus,
        parallel_bonus=parallel_bonus,
        max_parallel=max_parallel,
    )


def _entry_score(bonus: MachineBonus) -> float:
    parallel = bonus.parallel_bonus
    if bonus.max_parallel:
        parallel = min(parallel, bonus.max_parallel)
    return bonus.speed_bonus * parallel


def _select_best_bonus(entries: List[Dict[str, Any]]) -> Optional[MachineBonus]:
    best: Optional[MachineBonus] = None
    best_score = -1.0
    best_efficiency = 1.0
    for entry in entries:
        bonus = _entry_bonus(entry)
        score = _entry_score(bonus)
        if score > best_score or (score == best_score and bonus.efficiency_bonus < best_efficiency):
            best = bonus
            best_score = score
            best_efficiency = bonus.efficiency_bonus
    return best


def _load_parquet_entries(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    con = duckdb.connect(database=":memory:")
    rows = con.execute(
        (
            "select machine_id, parallel_bonus, max_parallel, speed_bonus, "
            "efficiency_bonus, coil_bonus "
            "from read_parquet(?)"
        ),
        [str(path)],
    ).fetchall()
    con.close()
    return [
        {
            "machine_id": row[0],
            "parallel_bonus": row[1],
            "max_parallel": row[2],
            "speed_bonus": row[3],
            "efficiency_bonus": row[4],
            "coil_bonus": row[5],
        }
        for row in rows
    ]


def _load_json_entries(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    entries = []
    for entry in data.get("machineIndex", []):
        entries.append(
            {
                "machine_id": entry.get("machineId"),
                "parallel_bonus": entry.get("parallelBonus"),
                "max_parallel": entry.get("maxParallel"),
                "speed_bonus": entry.get("speedBonus"),
                "efficiency_bonus": entry.get("efficiencyBonus"),
                "coil_bonus": entry.get("coilBonus"),
            }
        )
    return entries


def load_machine_bonuses(machine_index_json: Path, data_dir: Path) -> Dict[str, MachineBonus]:
    entries = _load_parquet_entries(data_dir / "machine_index.parquet")
    if not entries:
        entries = _load_json_entries(machine_index_json)
    if not entries:
        return {}

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        machine_id = entry.get("machine_id")
        if machine_id:
            grouped[machine_id].append(entry)

    bonuses: Dict[str, MachineBonus] = {}
    for machine_id, machine_entries in grouped.items():
        bonus = _select_best_bonus(machine_entries)
        if bonus:
            bonuses[machine_id] = bonus
    return bonuses
