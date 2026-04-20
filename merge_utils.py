import io
import zipfile
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from data_io import normalise_merge_key, safe_filename, to_csv_bytes


def parse_merge_key_columns(raw: str) -> List[str]:
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def build_composite_key(df: pd.DataFrame, key_cols: List[str]) -> pd.Series:
    if not key_cols:
        raise RuntimeError("At least one merge key column is required.")
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        raise RuntimeError(f"Merge key columns not found: {', '.join(missing)}")
    parts = [normalise_merge_key(df[col]) for col in key_cols]
    combined = parts[0]
    for part in parts[1:]:
        combined = combined + "||" + part
    return combined


def diagnostics_for_df(df: pd.DataFrame, key_cols: List[str], role: str) -> Dict[str, Any]:
    merge_key = build_composite_key(df, key_cols)
    return {
        "Role": role,
        "Rows": int(len(df)),
        "Columns": int(df.shape[1]),
        "Blank keys": int((merge_key == "").sum()),
        "Duplicate keys": int(merge_key.duplicated().sum()),
        "Distinct keys": int(merge_key.nunique(dropna=False)),
    }


def aggregate_duplicates(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    key_name = "__merge_key__"
    grouped = df.groupby(key_name, dropna=False, sort=False)

    def agg_series(series: pd.Series) -> Any:
        non_blank = [str(v) for v in series if not pd.isna(v) and str(v) != ""]
        if not non_blank:
            return ""
        unique = list(dict.fromkeys(non_blank))
        return unique[0] if len(unique) == 1 else " | ".join(unique)

    out = grouped.agg({col: agg_series for col in df.columns if col != key_name}).reset_index(drop=False)
    split_keys = out[key_name].str.split(r"\|\|")
    for idx, key_col in enumerate(key_cols):
        out[key_col] = split_keys.str[idx]
    return out


def prepare_merge_frame(
    df: pd.DataFrame,
    key_cols: List[str],
    duplicate_strategy: str,
    role: str,
) -> Tuple[pd.DataFrame, List[str]]:
    work = df.copy()
    work["__merge_key__"] = build_composite_key(work, key_cols)
    notes: List[str] = []

    blank_rows = int((work["__merge_key__"] == "").sum())
    if blank_rows > 0:
        notes.append(f"{role}: found {blank_rows:,} rows with blank composite keys")

    dup_count = int(work["__merge_key__"].duplicated().sum())
    if dup_count > 0:
        notes.append(f"{role}: found {dup_count:,} duplicate composite keys")

    if duplicate_strategy == "Error" and dup_count > 0:
        raise RuntimeError(f"{role} has duplicate merge keys and duplicate strategy is set to Error.")
    if duplicate_strategy == "Keep first":
        work = work.drop_duplicates(subset=["__merge_key__"], keep="first")
    elif duplicate_strategy == "Keep last":
        work = work.drop_duplicates(subset=["__merge_key__"], keep="last")
    elif duplicate_strategy == "Aggregate values":
        work = aggregate_duplicates(work, key_cols)

    return work, notes


def _build_unmatched_zip(
    reports: List[Dict[str, Any]],
    delimiter: str,
    encoding: str,
    quoting: int,
    escapechar_enabled: bool,
    date_format: str,
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for report in reports:
            if report["df"].empty:
                continue
            archive.writestr(
                report["name"],
                to_csv_bytes(
                    report["df"],
                    delimiter=delimiter,
                    encoding=encoding,
                    quoting=quoting,
                    escapechar_enabled=escapechar_enabled,
                    date_format=date_format,
                ),
            )
    buf.seek(0)
    return buf.getvalue()


def merge_dataframes(
    file_entries: List[Dict[str, Any]],
    base_role: str,
    join_type: str,
    exclude_unmatched: bool,
    delimiter: str,
    encoding: str,
    quoting: int,
    escapechar_enabled: bool,
    date_format: str,
) -> Dict[str, Any]:
    roles = [entry["role"] for entry in file_entries]
    diagnostics = [diagnostics_for_df(entry["df"], entry["key_cols"], entry["role"]) for entry in file_entries]
    processed: Dict[str, Dict[str, Any]] = {}
    notes: List[str] = []

    for entry in file_entries:
        prepared, prep_notes = prepare_merge_frame(entry["df"], entry["key_cols"], entry["duplicate_strategy"], entry["role"])
        processed[entry["role"]] = {**entry, "df": prepared}
        notes.extend(prep_notes)

    merged = processed[base_role]["df"].copy()
    unmatched_reports: List[Dict[str, Any]] = []
    how = join_type.lower()

    for role in roles:
        if role == base_role:
            continue
        right_entry = processed[role]
        indicator_col = f"__merge__{safe_filename(role, 20)}"
        merged = merged.merge(
            right_entry["df"].copy(),
            on="__merge_key__",
            how=how,
            suffixes=("", f"__{safe_filename(role, 20)}"),
            indicator=indicator_col,
        )

        left_only = merged[merged[indicator_col] == "left_only"].copy()
        right_only = merged[merged[indicator_col] == "right_only"].copy()
        both_count = int((merged[indicator_col] == "both").sum())

        if not left_only.empty:
            unmatched_reports.append(
                {
                    "name": f"{safe_filename(base_role)}_unmatched_against_{safe_filename(role)}.csv",
                    "df": left_only,
                }
            )
            notes.append(f"{role}: {len(left_only):,} base rows had no match")
        if not right_only.empty:
            unmatched_reports.append(
                {
                    "name": f"{safe_filename(role)}_orphans_vs_{safe_filename(base_role)}.csv",
                    "df": right_only,
                }
            )
            notes.append(f"{role}: {len(right_only):,} rows were present only in this file")

        notes.append(f"{role}: matched {both_count:,} rows")

        if exclude_unmatched:
            merged = merged[merged[indicator_col] == "both"].copy()

        merged = merged.drop(columns=[indicator_col], errors="ignore")

    merged = merged.drop(columns=["__merge_key__"], errors="ignore")
    unmatched_zip = _build_unmatched_zip(
        unmatched_reports,
        delimiter=delimiter,
        encoding=encoding,
        quoting=quoting,
        escapechar_enabled=escapechar_enabled,
        date_format=date_format,
    )
    return {
        "merged": merged,
        "diagnostics": pd.DataFrame(diagnostics),
        "notes": notes,
        "unmatched_zip": unmatched_zip,
        "has_unmatched_reports": any(not report["df"].empty for report in unmatched_reports),
    }


def _next_available_column_name(preferred: str, existing: Set[str]) -> str:
    candidate = preferred.strip() or "SourceFile"
    if candidate not in existing:
        return candidate
    suffix = 1
    while f"{candidate}_{suffix}" in existing:
        suffix += 1
    return f"{candidate}_{suffix}"


def combine_dataframes(
    file_entries: List[Dict[str, Any]],
    schema_mode: str,
    add_source_file: bool,
    source_column_name: str,
) -> Dict[str, Any]:
    if not file_entries:
        raise RuntimeError("At least one file is required to combine.")

    diagnostics: List[Dict[str, Any]] = []
    notes: List[str] = []
    ordered_columns: List[str] = []
    ordered_seen: Set[str] = set()

    for entry in file_entries:
        df = entry["df"]
        diagnostics.append(
            {
                "File": entry["name"],
                "Rows": int(len(df)),
                "Columns": int(df.shape[1]),
            }
        )
        for column in df.columns:
            if column not in ordered_seen:
                ordered_seen.add(column)
                ordered_columns.append(column)

    first_columns = list(file_entries[0]["df"].columns)
    combine_columns = ordered_columns if schema_mode == "Union columns" else list(first_columns)

    if schema_mode == "Strict same columns":
        first_column_set = set(first_columns)
        for entry in file_entries[1:]:
            entry_columns = list(entry["df"].columns)
            entry_column_set = set(entry_columns)
            if entry_column_set != first_column_set:
                missing = [column for column in first_columns if column not in entry_column_set]
                extras = [column for column in entry_columns if column not in first_column_set]
                detail_parts: List[str] = []
                if missing:
                    detail_parts.append(f"missing columns: {', '.join(missing)}")
                if extras:
                    detail_parts.append(f"extra columns: {', '.join(extras)}")
                detail = "; ".join(detail_parts) if detail_parts else "columns differ"
                raise RuntimeError(f"{entry['name']} does not match the first file schema ({detail}).")
        notes.append("Schema mode: strict same columns")
    elif schema_mode == "Union columns":
        notes.append("Schema mode: union columns")
    else:
        raise RuntimeError(f"Unsupported schema mode: {schema_mode}")

    final_source_column_name = source_column_name.strip() or "SourceFile"
    if add_source_file:
        final_source_column_name = _next_available_column_name(final_source_column_name, set(combine_columns))
        if final_source_column_name != (source_column_name.strip() or "SourceFile"):
            notes.append(f"Source column renamed to {final_source_column_name} to avoid a name collision")

    combined_frames: List[pd.DataFrame] = []
    for entry in file_entries:
        prepared = entry["df"].reindex(columns=combine_columns)
        if add_source_file:
            prepared = prepared.copy()
            prepared[final_source_column_name] = entry["name"]
        combined_frames.append(prepared)

    combined = pd.concat(combined_frames, ignore_index=True) if combined_frames else pd.DataFrame(columns=combine_columns)
    if add_source_file:
        combine_columns = list(combine_columns) + [final_source_column_name]
        combined = combined.reindex(columns=combine_columns)

    notes.append(f"Combined {len(file_entries):,} files into {len(combined):,} rows")
    return {
        "combined": combined,
        "diagnostics": pd.DataFrame(diagnostics),
        "notes": notes,
        "source_column_name": final_source_column_name if add_source_file else None,
    }
