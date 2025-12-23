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


def _title_from_recipe_map(name: str) -> str:
    if name.endswith("Recipes"):
        name = name[: -len("Recipes")]
    name = name.replace("_", " ")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    titled = " ".join(word.capitalize() for word in name.split())
    if titled == "Blast Furnace":
        return "Electric Blast Furnace"
    return titled


def load_name_index(recipes_json: Path) -> NameIndex:
    items: Dict[ItemKey, str] = {}
    fluids: Dict[str, str] = {}
    recipes: Dict[str, Dict[str, Any]] = {}
    machine_names: Dict[str, str] = {}

    if not recipes_json.exists():
        return NameIndex(items=items, fluids=fluids, recipes=recipes, machine_names=machine_names)

    # NOTE: this loads the full JSON; keep it simple for now.
    with recipes_json.open("r", encoding="utf-8") as f:
        data = json.load(f)

    for recipe_map in data.get("recipeMaps", []):
        display_name = recipe_map.get("displayName") or ""
        machine_id = recipe_map.get("machineId")
        pretty_name = _title_from_recipe_map(display_name) if display_name else None
        if machine_id and pretty_name:
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
    )
