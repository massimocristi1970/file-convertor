import re
from typing import Any, Dict, List, Tuple

import pandas as pd

from transforms import apply_transform


CALLER_AI_REQUIRED_COLUMNS = ["Name", "PhoneNumber", "CardNumber", "DateOfBirth", "PostalCode", "Title", "Surname"]


def _normalise_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _first_matching_column(columns: List[str], aliases: List[str]) -> str:
    normalised = {_normalise_label(column): column for column in columns}
    for alias in aliases:
        match = normalised.get(_normalise_label(alias))
        if match:
            return match
    return ""


def build_caller_ai_output_spec(columns: List[str]) -> List[Dict[str, Any]]:
    name_source = _first_matching_column(columns, ["name", "full name", "customer name", "client name", "contact name"])
    phone_source = _first_matching_column(columns, ["phone", "phone number", "mobile", "mobile number", "telephone", "tel", "contact number"])
    card_source = _first_matching_column(columns, ["card number", "card", "cardnumber", "account number", "account"])
    dob_source = _first_matching_column(columns, ["date of birth", "dob", "birth date", "dateofbirth"])
    postcode_source = _first_matching_column(
        columns,
        ["postcode", "post code", "postal code", "postalcode", "zip", "zip code", "zipcode", "address", "full address", "address line 1"],
    )
    title_source = _first_matching_column(columns, ["title", "salutation", "prefix", "customer title"]) or name_source
    surname_source = _first_matching_column(columns, ["surname", "last name", "family name"]) or name_source

    return [
        {"source": name_source or "(blank)", "transform": "Name: extract first", "params": {}, "output_name": "Name"},
        {"source": phone_source or "(blank)", "transform": "UK mobile -> 44", "params": {}, "output_name": "PhoneNumber"},
        {"source": card_source or "(blank)", "transform": "Digits: keep last N", "params": {"n": 4}, "output_name": "CardNumber"},
        {"source": dob_source or "(blank)", "transform": "None", "params": {}, "output_name": "DateOfBirth"},
        {"source": postcode_source or "(blank)", "transform": "UK Postcode (extract)", "params": {}, "output_name": "PostalCode"},
        {"source": title_source or "(blank)", "transform": "Name: extract title", "params": {}, "output_name": "Title"},
        {"source": surname_source or "(blank)", "transform": "Name: extract surname", "params": {}, "output_name": "Surname"},
    ]


def build_export_dataframe(source_df: pd.DataFrame, out_rows: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[str]]:
    export_df = pd.DataFrame(index=source_df.index)
    missing_sources: List[str] = []

    for row in out_rows:
        source = str(row.get("source", "(blank)") or "(blank)")
        output_name = str(row.get("output_name", "") or "").strip()
        transform = str(row.get("transform", "None") or "None")
        params = dict(row.get("params", {}) or {})

        if not output_name:
            continue
        if source == "(blank)":
            export_df[output_name] = ""
            continue
        if source not in source_df.columns:
            missing_sources.append(f"{output_name} <- {source}")
            continue

        export_df[output_name] = apply_transform(source_df[source], transform, params)

    return export_df.reset_index(drop=True), missing_sources
