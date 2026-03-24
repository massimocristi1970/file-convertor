from typing import Any, Dict, List, Optional


def apply_template_to_defaults(template: Optional[dict]) -> dict:
    template = template or {}
    merge_keys_by_role = template.get("merge_keys_by_role", {})
    normalised_keys = {}
    for role, keys in merge_keys_by_role.items():
        if isinstance(keys, list):
            normalised_keys[role] = keys
        elif isinstance(keys, str):
            normalised_keys[role] = [part.strip() for part in keys.split(",") if part.strip()]
        else:
            normalised_keys[role] = []

    return {
        "version": int(template.get("version", 2)),
        "join_type": template.get("join_type", "left"),
        "base_role": template.get("base_role", ""),
        "merge_keys_by_role": normalised_keys,
        "duplicate_strategy_by_role": template.get("duplicate_strategy_by_role", {}),
        "output_spec": list(template.get("output_spec", [])),
    }


def build_template_payload(
    join_type: str,
    base_role: str,
    merge_keys_by_role: Dict[str, List[str]],
    duplicate_strategy_by_role: Dict[str, str],
    out_rows: List[Dict[str, Any]],
) -> dict:
    return {
        "version": 2,
        "join_type": join_type,
        "base_role": base_role,
        "merge_keys_by_role": merge_keys_by_role,
        "duplicate_strategy_by_role": duplicate_strategy_by_role,
        "output_spec": out_rows,
    }
