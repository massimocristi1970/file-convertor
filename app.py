import io
import json
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from data_io import (
    SUPPORTED_INPUT_TYPES,
    SUPPORTED_OUTPUT_TYPES,
    TEXT_LIKE_TYPES,
    detect_file_type,
    delimiter_from_choice,
    force_columns_to_text,
    get_mime_type,
    list_xlsx_sheets,
    looks_like_xml_text,
    parse_csv_columns,
    quoting_from_choice,
    read_uploaded_file_as_df,
    safe_filename,
    to_export_bytes,
)
from merge_utils import merge_dataframes, parse_merge_key_columns
from template_utils import apply_template_to_defaults, build_template_payload
from transforms import TRANSFORM_FUNCS, apply_transform


APP_TITLE = "Data Mapper"


def parse_file_with_ui(
    file_obj: Any,
    header_row_default: int,
    formula_mode: str,
    drop_empty: bool,
    force_text_cols: List[str],
    key_prefix: str,
) -> Dict[str, Any]:
    file_type = detect_file_type(file_obj.name)
    file_bytes = file_obj.getvalue()
    is_xml_text = file_type == "txt" and looks_like_xml_text(file_bytes)
    sheet_name: Optional[str] = None
    header_row = int(header_row_default)
    text_parse_mode = "Delimited"
    text_delimiter: Optional[str] = None

    if file_type == "xlsx":
        sheets = list_xlsx_sheets(file_obj.getvalue(), data_only=(formula_mode == "Cached values (recommended)"))
        col1, col2 = st.columns([2, 1])
        with col1:
            sheet_name = st.selectbox(f"Sheet ({file_obj.name})", sheets, key=f"{key_prefix}_sheet")
        with col2:
            header_row = st.number_input(
                f"Header row ({file_obj.name})",
                min_value=1,
                max_value=100,
                value=int(header_row_default),
                step=1,
                key=f"{key_prefix}_header",
            )
    elif file_type in TEXT_LIKE_TYPES:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            text_parse_mode = st.selectbox(
                f"Text parse mode ({file_obj.name})",
                options=["Delimited", "Fixed width"],
                index=0,
                key=f"{key_prefix}_text_mode",
            )
        with col2:
            delimiter_choice = st.selectbox(
                f"Delimiter ({file_obj.name})",
                options=["Auto-detect", "Comma (,)", "Semicolon (;)", "Tab (\\t)", "Pipe (|)", "Space", "Custom"],
                index=0 if file_type == "txt" else (3 if file_type == "tsv" else 1),
                key=f"{key_prefix}_delimiter_choice",
            )
            custom_delimiter = ""
            if delimiter_choice == "Custom":
                custom_delimiter = st.text_input("Custom delimiter", value="|", key=f"{key_prefix}_custom_delim")
            if delimiter_choice != "Auto-detect":
                text_delimiter = delimiter_from_choice(delimiter_choice, custom_delimiter)
        with col3:
            header_row = st.number_input(
                f"Header row ({file_obj.name})",
                min_value=1,
                max_value=100,
                value=int(header_row_default),
                step=1,
                key=f"{key_prefix}_text_header",
            )

    df = read_uploaded_file_as_df(
        file_obj=file_obj,
        file_type=file_type,
        sheet_name=sheet_name,
        header_row=header_row,
        formula_mode=formula_mode,
        drop_empty=drop_empty,
        text_parse_mode=text_parse_mode,
        text_delimiter=text_delimiter,
    )
    df = force_columns_to_text(df, force_text_cols)
    return {
        "file_type": file_type,
        "is_xml_text": is_xml_text,
        "sheet_name": sheet_name,
        "header_row": header_row,
        "text_parse_mode": text_parse_mode,
        "text_delimiter": text_delimiter,
        "df": df,
        "raw_bytes": file_bytes,
    }


def render_json_options(prefix: str) -> str:
    labels = [
        "Records (list of objects)",
        "Split (columns + index + data)",
        "Index (rows as keys)",
        "Columns (columns as keys)",
    ]
    values = ["records", "split", "index", "columns"]
    idx = st.selectbox(
        f"JSON format ({prefix})",
        range(len(labels)),
        format_func=lambda i: labels[i],
        index=0,
        key=f"{prefix}_json_orient",
    )
    return values[idx]


def render_xml_options(prefix: str) -> Dict[str, str]:
    col1, col2 = st.columns(2)
    with col1:
        root = st.text_input(f"XML root element ({prefix})", value="rows", key=f"{prefix}_xml_root")
    with col2:
        row = st.text_input(f"XML row element ({prefix})", value="row", key=f"{prefix}_xml_row")
    return {"root": root.strip() or "rows", "row": row.strip() or "row"}


def render_download_button(
    df: pd.DataFrame,
    base_name: str,
    output_type: str,
    delimiter: str,
    encoding: str,
    quoting: int,
    escapechar_enabled: bool,
    date_format: str,
    json_orient: str,
    xml_root: str,
    xml_row: str,
    label_prefix: str,
    raw_export_bytes: Optional[bytes] = None,
) -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_name = f"{base_name}_{timestamp}.{output_type}"
    out_bytes = raw_export_bytes if raw_export_bytes is not None else to_export_bytes(
        df=df,
        output_type=output_type,
        delimiter=delimiter,
        encoding=encoding,
        quoting=quoting,
        escapechar_enabled=escapechar_enabled,
        date_format=date_format,
        json_orient=json_orient,
        xml_root=xml_root,
        xml_row=xml_row,
    )
    st.download_button(
        label=f"{label_prefix} {output_type.upper()}",
        data=out_bytes,
        file_name=out_name,
        mime=get_mime_type(output_type),
    )


st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.markdown(
    """
Upload tabular files and either:

- **Convert** a single file between CSV, TSV, TXT, XLSX, JSON, XML, and Parquet
- **Merge + Map + Transform** multiple uploads with diagnostics, composite keys, duplicate strategies, and audit exports
"""
)

with st.sidebar:
    st.header("Mode")
    app_mode = st.radio("Choose workflow", options=["Simple Convert", "Merge + Map + Transform"], index=0)

    st.divider()
    st.header("General options")
    header_row = st.number_input("Default header row", min_value=1, max_value=100, value=1, step=1)
    formula_mode = st.selectbox(
        "Formulas",
        options=["Cached values (recommended)", "Formula strings"],
        index=0,
    )
    drop_empty = st.checkbox("Drop completely empty rows/columns", value=True)

    st.divider()
    st.subheader("Delimited export formatting")
    delim_choice = st.selectbox("Default delimiter", ["Comma (,)", "Semicolon (;)", "Tab (\\t)", "Pipe (|)", "Space", "Custom"], index=0)
    custom_delim = st.text_input("Custom default delimiter", value=",") if delim_choice == "Custom" else ""
    delimiter = delimiter_from_choice(delim_choice, custom_delim)
    encoding_label = st.selectbox("Encoding", ["utf-8", "utf-8-sig (Excel-friendly)", "cp1252"], index=1)
    encoding_map = {"utf-8": "utf-8", "utf-8-sig (Excel-friendly)": "utf-8-sig", "cp1252": "cp1252"}
    quoting_choice = st.selectbox("Quoting", ["Minimal (default)", "All fields", "Non-numeric", "None"], index=0)
    quoting = quoting_from_choice(quoting_choice)
    escapechar_enabled = st.checkbox("Enable escape character (\\)", value=(quoting_choice == "None"))
    date_format = st.text_input("Date format", value="%Y-%m-%d")

    st.divider()
    st.subheader("Data fidelity")
    force_text_raw = st.text_area(
        "Force these columns to TEXT",
        value="",
        placeholder="e.g. AccountNumber, SortCode, Postcode, Mobile",
    )
    force_text_cols = parse_csv_columns(force_text_raw)
    preview_rows = st.slider("Preview rows", min_value=5, max_value=200, value=25, step=5)

    st.divider()
    st.subheader("Mapping template (optional)")
    template_file = st.file_uploader("Load template (.json)", type=["json"], key="template_json")
    template_defaults = apply_template_to_defaults(None)
    if template_file:
        try:
            template_defaults = apply_template_to_defaults(json.loads(template_file.getvalue().decode("utf-8")))
            st.success("Template loaded.")
        except Exception as exc:
            st.error(f"Failed to load template: {exc}")


if app_mode == "Simple Convert":
    uploaded = st.file_uploader(
        "Upload a file",
        type=SUPPORTED_INPUT_TYPES,
        help="Supports .xlsx, .csv, .tsv, .txt, .json, .xml, and .parquet",
    )
    if not uploaded:
        st.info("Upload a file to begin.")
        st.stop()

    try:
        parsed = parse_file_with_ui(
            uploaded,
            header_row_default=int(header_row),
            formula_mode=formula_mode,
            drop_empty=drop_empty,
            force_text_cols=force_text_cols,
            key_prefix="simple",
        )
    except Exception as exc:
        st.error(f"Failed to read '{uploaded.name}'. Error: {exc}")
        st.stop()

    input_type = parsed["file_type"]
    file_bytes = uploaded.getvalue()
    base_name = safe_filename(uploaded.name.rsplit(".", 1)[0])
    output_type = st.selectbox(
        "Output format",
        options=SUPPORTED_OUTPUT_TYPES,
        index=SUPPORTED_OUTPUT_TYPES.index("xml") if input_type == "txt" else 0,
    )
    json_orient = render_json_options("simple") if output_type == "json" else "records"
    xml_options = render_xml_options("simple") if output_type == "xml" else {"root": "rows", "row": "row"}

    if input_type == "xlsx":
        sheets = list_xlsx_sheets(file_bytes, data_only=(formula_mode == "Cached values (recommended)"))
        export_mode = st.radio("Export", options=["Single sheet", "All sheets → ZIP"], horizontal=True)
        if export_mode == "All sheets → ZIP":
            try:
                archive = io.BytesIO()
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
                    for sheet_name in sheets:
                        df_sheet = force_columns_to_text(
                            read_uploaded_file_as_df(
                                file_obj=uploaded,
                                file_type="xlsx",
                                sheet_name=sheet_name,
                                header_row=int(parsed["header_row"]),
                                formula_mode=formula_mode,
                                drop_empty=drop_empty,
                            ),
                            force_text_cols,
                        )
                        zip_handle.writestr(
                            f"{safe_filename(sheet_name)}.{output_type}",
                            to_export_bytes(
                                df=df_sheet,
                                output_type=output_type,
                                delimiter=delimiter,
                                encoding=encoding_map[encoding_label],
                                quoting=quoting,
                                escapechar_enabled=escapechar_enabled,
                                date_format=date_format,
                                json_orient=json_orient,
                                xml_root=xml_options["root"],
                                xml_row=xml_options["row"],
                            ),
                        )
                archive.seek(0)
                st.download_button(
                    "Download ZIP",
                    data=archive.getvalue(),
                    file_name=f"{base_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip",
                )
            except Exception as exc:
                st.error(f"Failed to create ZIP export. Error: {exc}")
            st.subheader("Preview")
            st.dataframe(parsed["df"].head(preview_rows), use_container_width=True)
            st.stop()

    st.subheader("Preview")
    input_label = f"{input_type} (XML content)" if parsed.get("is_xml_text") else input_type
    st.caption(f"Detected input type: `{input_label}`")
    st.dataframe(parsed["df"].head(preview_rows), use_container_width=True)
    st.caption(f"Rows: {len(parsed['df']):,} | Columns: {parsed['df'].shape[1]:,}")

    try:
        render_download_button(
            df=parsed["df"],
            base_name=base_name,
            output_type=output_type,
            delimiter=delimiter if output_type != "tsv" else "\t",
            encoding=encoding_map[encoding_label],
            quoting=quoting,
            escapechar_enabled=escapechar_enabled,
            date_format=date_format,
            json_orient=json_orient,
            xml_root=xml_options["root"],
            xml_row=xml_options["row"],
            label_prefix="Download",
            raw_export_bytes=parsed["raw_bytes"] if parsed.get("is_xml_text") and output_type == "xml" else None,
        )
    except Exception as exc:
        st.error(f"Failed to create export. Error: {exc}")
    st.stop()


st.markdown("### 1) Upload files")
uploaded_files = st.file_uploader(
    "Upload files",
    type=SUPPORTED_INPUT_TYPES,
    accept_multiple_files=True,
    help="Supports .xlsx, .csv, .tsv, .txt, .json, .xml, and .parquet",
)
if not uploaded_files:
    st.info("Upload one or more files to begin.")
    st.stop()

file_entries: List[Dict[str, Any]] = []
for idx, uploaded in enumerate(uploaded_files):
    st.markdown(f"#### File {idx + 1}: `{uploaded.name}`")
    default_role = f"File{idx + 1}"
    role = st.text_input(
        f"Role name ({uploaded.name})",
        value=default_role,
        key=f"role_{idx}",
        help="Give each upload a stable label, for example Applications or Payments.",
    ).strip() or default_role
    try:
        parsed = parse_file_with_ui(
            uploaded,
            header_row_default=int(header_row),
            formula_mode=formula_mode,
            drop_empty=drop_empty,
            force_text_cols=force_text_cols,
            key_prefix=f"merge_{idx}",
        )
    except Exception as exc:
        st.error(f"Failed to read '{uploaded.name}'. Error: {exc}")
        st.stop()

    default_keys = template_defaults.get("merge_keys_by_role", {}).get(role, ["AppID"])
    key_cols_raw = st.text_input(
        f"Merge key columns ({uploaded.name})",
        value=", ".join(default_keys),
        key=f"keys_{idx}",
        help="Use one or more columns separated by commas to create a composite merge key.",
    )
    key_cols = parse_merge_key_columns(key_cols_raw)
    duplicate_strategy = st.selectbox(
        f"Duplicate key strategy ({uploaded.name})",
        options=["Keep first", "Keep last", "Aggregate values", "Error"],
        index=["Keep first", "Keep last", "Aggregate values", "Error"].index(
            template_defaults.get("duplicate_strategy_by_role", {}).get(role, "Keep first")
        ),
        key=f"dupes_{idx}",
    )

    st.caption(f"Rows: {len(parsed['df']):,} | Columns: {parsed['df'].shape[1]:,}")
    st.dataframe(parsed["df"].head(min(preview_rows, 25)), use_container_width=True)

    file_entries.append(
        {
            "name": uploaded.name,
            "role": role,
            "df": parsed["df"],
            "key_cols": key_cols,
            "duplicate_strategy": duplicate_strategy,
        }
    )

roles = [entry["role"] for entry in file_entries]
if len(set(roles)) != len(roles):
    st.error("Role names must be unique.")
    st.stop()
if any(not entry["key_cols"] for entry in file_entries):
    st.error("Every file needs at least one merge key column.")
    st.stop()

st.divider()
st.subheader("2) Merge")
base_role_default = template_defaults.get("base_role", roles[0])
if base_role_default not in roles:
    base_role_default = roles[0]

base_role = st.selectbox("Base dataset role", roles, index=roles.index(base_role_default))
join_type = st.selectbox(
    "Join type",
    options=["left", "inner", "outer"],
    index=["left", "inner", "outer"].index(template_defaults.get("join_type", "left")),
)
exclude_unmatched = st.checkbox(
    "Exclude records not matched across files",
    value=True,
    help="When enabled, only rows matched in every merge step remain in the final output.",
)

try:
    merge_result = merge_dataframes(
        file_entries=file_entries,
        base_role=base_role,
        join_type=join_type,
        exclude_unmatched=exclude_unmatched,
        delimiter=delimiter,
        encoding=encoding_map[encoding_label],
        quoting=quoting,
        escapechar_enabled=escapechar_enabled,
        date_format=date_format,
    )
except Exception as exc:
    st.error(f"Merge failed. Error: {exc}")
    st.stop()

merged = merge_result["merged"]
st.subheader("Diagnostics")
st.dataframe(merge_result["diagnostics"], use_container_width=True)
if merge_result["notes"]:
    st.info("Merge notes:\n\n- " + "\n- ".join(merge_result["notes"]))
if merge_result["has_unmatched_reports"]:
    st.download_button(
        "Download unmatched rows report (ZIP)",
        data=merge_result["unmatched_zip"],
        file_name=f"{safe_filename(base_role)}_unmatched_reports.zip",
        mime="application/zip",
    )

st.success(f"Merged rows: {len(merged):,} | Columns: {merged.shape[1]:,}")
st.dataframe(merged.head(preview_rows), use_container_width=True)

st.divider()
st.subheader("3) Build export columns")

all_cols = list(merged.columns)
template_output_rows = list(template_defaults.get("output_spec", []))
num_out = st.number_input("How many output columns?", min_value=1, max_value=200, value=max(10, len(template_output_rows)), step=1)
out_rows: List[Dict[str, Any]] = []

for idx in range(int(num_out)):
    defaults = template_output_rows[idx] if idx < len(template_output_rows) else {}
    default_src = str(defaults.get("source", "(blank)"))
    default_transform = str(defaults.get("transform", "None"))
    default_name = str(defaults.get("output_name", ""))
    default_params = dict(defaults.get("params", {}) or {})

    col1, col2, col3 = st.columns([3, 2, 3])
    with col1:
        src = st.selectbox(
            f"Source column #{idx + 1}",
            options=["(blank)"] + all_cols,
            index=(["(blank)"] + all_cols).index(default_src) if default_src in (["(blank)"] + all_cols) else 0,
            key=f"src_{idx}",
        )
    with col2:
        tf_name = st.selectbox(
            f"Transform #{idx + 1}",
            options=list(TRANSFORM_FUNCS.keys()),
            index=list(TRANSFORM_FUNCS.keys()).index(default_transform) if default_transform in TRANSFORM_FUNCS else 0,
            key=f"tf_{idx}",
        )
    with col3:
        output_name = st.text_input(
            f"Output column name #{idx + 1}",
            value=default_name if default_name else ("" if src == "(blank)" else str(src)),
            key=f"out_{idx}",
        ).strip()

    params: Dict[str, Any] = {}
    if tf_name == "Digits: keep last N":
        params["n"] = st.number_input(f"N #{idx + 1}", min_value=1, max_value=50, value=int(default_params.get("n", 4)), step=1, key=f"n_{idx}")
    elif tf_name == "Extract by regex":
        params["pattern"] = st.text_input(f"Regex pattern #{idx + 1}", value=str(default_params.get("pattern", r"(\w+)")), key=f"rx_{idx}")
        params["group"] = st.number_input(f"Regex group #{idx + 1}", min_value=0, max_value=20, value=int(default_params.get("group", 1)), step=1, key=f"grp_{idx}")
        params["ignore_case"] = st.checkbox(f"Ignore case #{idx + 1}", value=bool(default_params.get("ignore_case", True)), key=f"ic_{idx}")
    elif tf_name == "Split + take part":
        params["delim"] = st.text_input(f"Delimiter #{idx + 1}", value=str(default_params.get("delim", ",")), key=f"delim_{idx}")
        params["index"] = st.number_input(f"Index #{idx + 1}", min_value=-50, max_value=50, value=int(default_params.get("index", 0)), step=1, key=f"idx_{idx}")
    elif tf_name == "Prefix if missing":
        params["prefix"] = st.text_input(f"Prefix #{idx + 1}", value=str(default_params.get("prefix", "")), key=f"pre_{idx}")
    elif tf_name == "Suffix":
        params["suffix"] = st.text_input(f"Suffix #{idx + 1}", value=str(default_params.get("suffix", "")), key=f"suf_{idx}")
    elif tf_name == "Regex replace":
        params["pattern"] = st.text_input(f"Regex pattern replace #{idx + 1}", value=str(default_params.get("pattern", r"\s+")), key=f"rrx_{idx}")
        params["repl"] = st.text_input(f"Replace with #{idx + 1}", value=str(default_params.get("repl", "")), key=f"rrepl_{idx}")
        params["ignore_case"] = st.checkbox(f"Ignore case replace #{idx + 1}", value=bool(default_params.get("ignore_case", True)), key=f"ric_{idx}")

    out_rows.append({"source": src, "transform": tf_name, "params": params, "output_name": output_name})

named_outputs = [row["output_name"] for row in out_rows if row["output_name"]]
duplicate_outputs = sorted({name for name in named_outputs if named_outputs.count(name) > 1})
if duplicate_outputs:
    st.error("Output column names must be unique: " + ", ".join(duplicate_outputs))
    st.stop()

export_df = pd.DataFrame()
for row in out_rows:
    if row["source"] == "(blank)" or not row["output_name"] or row["source"] not in merged.columns:
        continue
    export_df[row["output_name"]] = apply_transform(merged[row["source"]], row["transform"], row["params"])

st.subheader("Export preview")
st.caption(f"Rows: {len(export_df):,} | Columns: {export_df.shape[1]:,}")
st.dataframe(export_df.head(preview_rows), use_container_width=True)

st.divider()
st.subheader("4) Download")
merged_output_type = st.selectbox("Export format", options=SUPPORTED_OUTPUT_TYPES, index=0)
merged_json_orient = render_json_options("merged") if merged_output_type == "json" else "records"
merged_xml_options = render_xml_options("merged") if merged_output_type == "xml" else {"root": "rows", "row": "row"}
try:
    render_download_button(
        df=export_df,
        base_name=safe_filename(base_role),
        output_type=merged_output_type,
        delimiter=delimiter if merged_output_type != "tsv" else "\t",
        encoding=encoding_map[encoding_label],
        quoting=quoting,
        escapechar_enabled=escapechar_enabled,
        date_format=date_format,
        json_orient=merged_json_orient,
        xml_root=merged_xml_options["root"],
        xml_row=merged_xml_options["row"],
        label_prefix="Download Merged/Transformed",
    )
except Exception as exc:
    st.error(f"Failed to create merged export. Error: {exc}")

st.divider()
st.subheader("5) Save mapping template")
template_payload = build_template_payload(
    join_type=join_type,
    base_role=base_role,
    merge_keys_by_role={entry["role"]: entry["key_cols"] for entry in file_entries},
    duplicate_strategy_by_role={entry["role"]: entry["duplicate_strategy"] for entry in file_entries},
    out_rows=out_rows,
)
st.download_button(
    "Download mapping template (.json)",
    data=json.dumps(template_payload, indent=2).encode("utf-8"),
    file_name=f"{safe_filename(base_role)}_mapping_template.json",
    mime="application/json",
)
