import random
import re
from typing import Any, Dict, List, Tuple

import pandas as pd

from transforms import apply_transform


CALLER_AI_REQUIRED_COLUMNS = ["Name", "PhoneNumber", "CardNumber", "DateOfBirth", "PostalCode", "Title", "Surname"]
DUMMY_CARD_OUTPUT_NAMES = {"CardNumber"}


def _dummy_card_numbers(count: int) -> List[str]:
    """Generate a list of random 4-digit numeric strings (zero padded)."""
    return [f"{random.randint(0, 9999):04d}" for _ in range(count)]


def _normalise_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _first_matching_column(columns: List[str], aliases: List[str]) -> str:
    normalised = {_normalise_label(column): column for column in columns}
    for alias in aliases:
        match = normalised.get(_normalise_label(alias))
        if match:
            return match
    return ""


def _matching_columns(columns: List[str], aliases: List[str]) -> List[str]:
    normalised_aliases = [_normalise_label(alias) for alias in aliases]
    matches: List[str] = []
    for column in columns:
        normalised_column = _normalise_label(column)
        if any(alias and (alias in normalised_column or normalised_column in alias) for alias in normalised_aliases):
            matches.append(column)
    return list(dict.fromkeys(matches))


def build_caller_ai_output_spec(columns: List[str]) -> List[Dict[str, Any]]:
    name_source = _first_matching_column(columns, ["name", "full name", "customer name", "client name", "contact name"])
    phone_source = _first_matching_column(columns, ["phone", "phone number", "mobile", "mobile number", "telephone", "tel", "contact number"])
    card_source = _first_matching_column(columns, ["card number", "card", "cardnumber", "account number", "account"])
    dob_source = _first_matching_column(columns, ["date of birth", "dob", "birth date", "dateofbirth"])
    postcode_candidates = _matching_columns(
        columns,
        ["postcode", "post code", "postal code", "postalcode", "zip", "zip code", "zipcode", "address", "full address", "address line 1", "address1", "street"],
    )
    postcode_source = postcode_candidates[0] if postcode_candidates else ""
    title_source = _first_matching_column(columns, ["title", "salutation", "prefix", "customer title"]) or name_source
    surname_source = _first_matching_column(columns, ["surname", "last name", "family name"]) or name_source

    return [
        {"source": name_source or "(blank)", "transform": "Name: extract first", "params": {}, "output_name": "Name"},
        {"source": phone_source or "(blank)", "transform": "UK mobile -> 44", "params": {}, "output_name": "PhoneNumber"},
        {"source": card_source or "(blank)", "transform": "Digits: keep last N", "params": {"n": 4}, "output_name": "CardNumber"},
        {"source": dob_source or "(blank)", "transform": "Date: format", "params": {"format": "%Y-%m-%d"}, "output_name": "DateOfBirth"},
        {
            "source": postcode_source or "(blank)",
            "transform": "UK Postcode (extract)",
            "params": {"fallback_sources": postcode_candidates[1:]},
            "output_name": "PostalCode",
        },
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
            if output_name in DUMMY_CARD_OUTPUT_NAMES:
                export_df[output_name] = _dummy_card_numbers(len(source_df.index))
            else:
                export_df[output_name] = ""
            continue
        fallback_sources = [candidate for candidate in params.get("fallback_sources", []) if isinstance(candidate, str)]
        candidate_sources = [source] + [candidate for candidate in fallback_sources if candidate != source]
        available_sources = [candidate for candidate in candidate_sources if candidate in source_df.columns]
        if not available_sources:
            missing_sources.append(f"{output_name} <- {source}")
            continue
        result = apply_transform(source_df[available_sources[0]], transform, params)
        for candidate in available_sources[1:]:
            fallback_result = apply_transform(source_df[candidate], transform, params)
            result = result.where(result.astype(str).str.strip() != "", fallback_result)
        export_df[output_name] = result

    return export_df.reset_index(drop=True), missing_sources
