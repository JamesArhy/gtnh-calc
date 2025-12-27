import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Any
import re

ItemKey = Tuple[str, int]


@dataclass
class NameIndex:
    items: Dict[ItemKey, str]
    fluids: Dict[str, str]
    recipes: Dict[str, Dict[str, Any]]
    machine_names: Dict[str, str]
    machine_names_by_tier: Dict[str, Dict[str, str]]


_TIER_ORDER = [
    "ULV",
    "LV",
    "MV",
    "HV",
    "EV",
    "IV",
    "LuV",
    "ZPM",
    "UV",
    "UHV",
    "UEV",
    "UIV",
    "UMV",
]


def _title_from_recipe_map(name: str) -> str:
    if name.endswith("Recipes"):
        name = name[: -len("Recipes")]
    name = name.replace("_", " ")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    titled = " ".join(word.capitalize() for word in name.split())
    if titled == "Blast Furnace":
        return "Electric Blast Furnace"
    return titled


def _parse_machine_tier(meta_tile_name: str | None) -> str | None:
    if not meta_tile_name:
        return None
    match = re.search(r"tier\.(\d+)", meta_tile_name)
    if not match:
        return None
    tier_num = int(match.group(1))
    if tier_num < 0:
        return None
    if tier_num == 0:
        return "ULV"
    if tier_num < len(_TIER_ORDER):
        return _TIER_ORDER[tier_num]
    return None


def _load_machine_names(machine_index_json: Path) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    if not machine_index_json.exists():
        return {}, {}
    with machine_index_json.open("r", encoding="utf-8") as f:
        data = json.load(f)
    names: Dict[str, str] = {}
    names_by_tier: Dict[str, Dict[str, str]] = {}
    for entry in data.get("machineIndex", []):
        machine_id = entry.get("machineId")
        display_name = entry.get("displayName")
        if not machine_id or not display_name:
            continue
        if machine_id not in names:
            names[machine_id] = display_name
        tier = _parse_machine_tier(entry.get("metaTileName"))
        if tier:
            names_by_tier.setdefault(machine_id, {})[tier] = display_name
    return names, names_by_tier


def load_name_index(recipes_json: Path, machine_index_json: Path | None = None) -> NameIndex:
    items: Dict[ItemKey, str] = {}
    fluids: Dict[str, str] = {}
    recipes: Dict[str, Dict[str, Any]] = {}
    machine_names: Dict[str, str] = {}
    machine_names_by_tier: Dict[str, Dict[str, str]] = {}

    if machine_index_json is not None:
        names, names_by_tier = _load_machine_names(machine_index_json)
        machine_names.update(names)
        machine_names_by_tier.update(names_by_tier)

    if not recipes_json.exists():
        return NameIndex(
            items=items,
            fluids=fluids,
            recipes=recipes,
            machine_names=machine_names,
            machine_names_by_tier=machine_names_by_tier,
        )

    # NOTE: this loads the full JSON; keep it simple for now.
    with recipes_json.open("r", encoding="utf-8") as f:
        data = json.load(f)

    for recipe_map in data.get("recipeMaps", []):
        display_name = recipe_map.get("displayName") or ""
        machine_id = recipe_map.get("machineId")
        pretty_name = _title_from_recipe_map(display_name) if display_name else None
        if machine_id and pretty_name and machine_id not in machine_names:
            machine_names.setdefault(machine_id, pretty_name)
        for recipe in recipe_map.get("recipes", []):
            rid = recipe.get("rid")
            if rid:
                recipes[rid] = {
                    "machine_id": recipe.get("machineId"),
                    "machine_name": machine_names.get(recipe.get("machineId")),
                    "min_tier": recipe.get("minTier"),
                    "min_voltage": recipe.get("minVoltage"),
                    "amps": recipe.get("ampsAtMinTier"),
                    "ebf_temp": recipe.get("ebfTemp"),
                }
            for entry in recipe.get("itemInputs", []) + recipe.get("itemOutputs", []):
                item_id = entry.get("id")
                meta = entry.get("meta", 0)
                name = entry.get("displayName")
                if item_id and name:
                    items.setdefault((item_id, int(meta)), name)
            for entry in recipe.get("fluidInputs", []) + recipe.get("fluidOutputs", []):
                fluid_id = entry.get("id")
                name = entry.get("displayName") or entry.get("localizedName")
                if fluid_id and name:
                    fluids.setdefault(fluid_id, name)

    return NameIndex(
        items=items,
        fluids=fluids,
        recipes=recipes,
        machine_names=machine_names,
        machine_names_by_tier=machine_names_by_tier,
    )
