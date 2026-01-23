import io
import re
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    import openpyxl
except ImportError:
    openpyxl = None


APP_TITLE = "XLSX → CSV Converter (Robust)"


# ---------- Helpers ----------
def safe_filename(name: str, max_len: int = 80) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\-. ]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    name = name.strip(" ._")
    if not name:
        name = "sheet"
    return name[:max_len]


def parse_force_text_columns(raw: str) -> List[str]:
    """
    User can enter: "colA, colB, AccountNumber"
    We'll treat them as column names after header row is read.
    """
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def delimiter_from_choice(choice: str, custom: str) -> str:
    mapping = {
        "Comma (,)": ",",
        "Semicolon (;)": ";",
        "Tab (\\t)": "\t",
        "Pipe (|)": "|",
        "Custom": custom if custom else ",",
    }
    return mapping.get(choice, ",")


def quoting_from_choice(choice: str) -> int:
    import csv

    mapping = {
        "Minimal (default)": csv.QUOTE_MINIMAL,
        "All fields": csv.QUOTE_ALL,
        "Non-numeric": csv.QUOTE_NONNUMERIC,
        "None": csv.QUOTE_NONE,
    }
    return mapping.get(choice, csv.QUOTE_MINIMAL)


def load_workbook_bytes(xlsx_bytes: bytes, data_only: bool) -> "openpyxl.Workbook":
    if openpyxl is None:
        raise RuntimeError("openpyxl is not installed. Install it with: pip install openpyxl")
    bio = io.BytesIO(xlsx_bytes)
    # read_only=False to allow pandas to read properly; keep_vba False; data_only controls formulas vs cached values
    return openpyxl.load_workbook(bio, data_only=data_only, read_only=False)


def read_sheet_as_dataframe(
    xlsx_bytes: bytes,
    sheet_name: str,
    header_row: int,
    formula_mode: str,
    drop_empty: bool,
) -> pd.DataFrame:
    """
    formula_mode:
      - "Cached values (recommended)" -> data_only=True
      - "Formula strings"             -> data_only=False
    """
    data_only = (formula_mode == "Cached values (recommended)")
    # Use openpyxl engine explicitly for consistent behaviour
    df = pd.read_excel(
        io.BytesIO(xlsx_bytes),
        sheet_name=sheet_name,
        header=header_row - 1,  # pandas is 0-indexed
        engine="openpyxl",
    )

    if drop_empty:
        # Drop entirely empty rows/cols
        df = df.dropna(how="all").dropna(axis=1, how="all")

    # If user chose formula strings, pandas will typically read computed values (if cached) not formulas.
    # To truly export formula strings we need openpyxl cell inspection.
    if formula_mode == "Formula strings":
        wb = load_workbook_bytes(xlsx_bytes, data_only=False)
        ws = wb[sheet_name]
        # Build a dataframe from worksheet values (including formulas) using header row as column names.
        # This is slower but more faithful.
        values = list(ws.values)
        if not values:
            return pd.DataFrame()

        hdr_idx = header_row - 1
        if hdr_idx >= len(values):
            return pd.DataFrame()

        headers = list(values[hdr_idx])
        data_rows = values[hdr_idx + 1 :]

        # Make unique header names
        clean_headers = []
        seen = {}
        for h in headers:
            h = str(h).strip() if h is not None else ""
            if not h:
                h = "Unnamed"
            if h in seen:
                seen[h] += 1
                h2 = f"{h}.{seen[h]}"
            else:
                seen[h] = 0
                h2 = h
            clean_headers.append(h2)

        df2 = pd.DataFrame(data_rows, columns=clean_headers)

        if drop_empty:
            df2 = df2.dropna(how="all").dropna(axis=1, how="all")

        return df2

    return df


def force_columns_to_text(df: pd.DataFrame, col_names: List[str]) -> pd.DataFrame:
    """
    Forces specific columns (if present) to string and preserves leading zeros.
    """
    if not col_names:
        return df
    df = df.copy()
    for col in col_names:
        if col in df.columns:
            # Convert to string but keep NaN as empty
            df[col] = df[col].map(lambda x: "" if pd.isna(x) else str(x))
    return df


def normalise_dates(df: pd.DataFrame, date_format: str) -> pd.DataFrame:
    """
    Format datetime-like columns to consistent string format for CSV output.
    """
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime(date_format)
    return df


def to_csv_bytes(
    df: pd.DataFrame,
    delimiter: str,
    encoding: str,
    quoting: int,
    escapechar_enabled: bool,
    date_format: str,
) -> bytes:
    import csv

    df_out = normalise_dates(df, date_format=date_format)

    buf = io.StringIO()
    df_out.to_csv(
        buf,
        index=False,
        sep=delimiter,
        encoding=None,  # we encode after
        quoting=quoting,
        escapechar="\\" if escapechar_enabled else None,
        quotechar='"',
        lineterminator="\n",
    )
    return buf.getvalue().encode(encoding, errors="replace")


# ---------- UI ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

st.markdown(
    """
Upload an **.xlsx** file, choose export settings, then download a **CSV** (single sheet)
or a **ZIP** of CSVs (all sheets).
"""
)

uploaded = st.file_uploader("Upload .xlsx", type=["xlsx"])

with st.sidebar:
    st.header("Export options")

    export_mode = st.radio(
        "Export mode",
        options=["Single sheet → CSV", "All sheets → ZIP of CSVs"],
        index=0,
    )

    header_row = st.number_input("Header row (1 = first row)", min_value=1, max_value=100, value=1, step=1)

    formula_mode = st.selectbox(
        "Formulas",
        options=["Cached values (recommended)", "Formula strings"],
        index=0,
        help="Cached values are what Excel last calculated and saved. Formula strings exports '=SUM(...)' etc.",
    )

    drop_empty = st.checkbox("Drop completely empty rows/columns", value=True)

    st.divider()
    st.subheader("CSV formatting")

    delim_choice = st.selectbox("Delimiter", ["Comma (,)", "Semicolon (;)", "Tab (\\t)", "Pipe (|)", "Custom"], index=0)
    custom_delim = st.text_input("Custom delimiter", value=",") if delim_choice == "Custom" else ""
    delimiter = delimiter_from_choice(delim_choice, custom_delim)

    encoding = st.selectbox("Encoding", ["utf-8", "utf-8-sig (Excel-friendly)", "cp1252"], index=1)
    encoding_map = {"utf-8": "utf-8", "utf-8-sig (Excel-friendly)": "utf-8-sig", "cp1252": "cp1252"}

    quoting_choice = st.selectbox("Quoting", ["Minimal (default)", "All fields", "Non-numeric", "None"], index=0)
    quoting = quoting_from_choice(quoting_choice)

    escapechar_enabled = st.checkbox(
        "Enable escape character (\\) (useful if quoting=None)",
        value=(quoting_choice == "None"),
    )

    date_format = st.text_input("Date format", value="%Y-%m-%d", help="Python strftime format")

    st.divider()
    st.subheader("Data fidelity")

    force_text_raw = st.text_area(
        "Force these columns to TEXT (preserve IDs/leading zeros)\nComma-separated column names:",
        value="",
        placeholder="e.g. AccountNumber, SortCode, Postcode",
    )
    force_text_cols = parse_force_text_columns(force_text_raw)

    preview_rows = st.slider("Preview rows", min_value=5, max_value=200, value=25, step=5)

if not uploaded:
    st.info("Upload an .xlsx file to begin.")
    st.stop()

xlsx_bytes = uploaded.getvalue()

# Get sheet names safely
try:
    wb_tmp = load_workbook_bytes(xlsx_bytes, data_only=(formula_mode == "Cached values (recommended)"))
    sheet_names = wb_tmp.sheetnames
except Exception as e:
    st.error(f"Could not read workbook. Error: {e}")
    st.stop()

if not sheet_names:
    st.error("No worksheets found in the uploaded file.")
    st.stop()

col1, col2 = st.columns([1, 1], gap="large")

with col1:
    st.subheader("Workbook")
    st.write(
        {
            "filename": uploaded.name,
            "size_kb": round(len(xlsx_bytes) / 1024, 1),
            "sheets": sheet_names,
        }
    )

with col2:
    st.subheader("Preview")

    if export_mode == "Single sheet → CSV":
        sheet = st.selectbox("Select sheet", sheet_names)
        try:
            df = read_sheet_as_dataframe(
                xlsx_bytes=xlsx_bytes,
                sheet_name=sheet,
                header_row=int(header_row),
                formula_mode=formula_mode,
                drop_empty=drop_empty,
            )
            df = force_columns_to_text(df, force_text_cols)
            st.dataframe(df.head(preview_rows), use_container_width=True)
            st.caption(f"Rows: {len(df):,} | Columns: {df.shape[1]:,}")
        except Exception as e:
            st.error(f"Failed to read sheet '{sheet}'. Error: {e}")
            st.stop()
    else:
        # Preview the first sheet as a sample
        sheet = sheet_names[0]
        try:
            df = read_sheet_as_dataframe(
                xlsx_bytes=xlsx_bytes,
                sheet_name=sheet,
                header_row=int(header_row),
                formula_mode=formula_mode,
                drop_empty=drop_empty,
            )
            df = force_columns_to_text(df, force_text_cols)
            st.dataframe(df.head(preview_rows), use_container_width=True)
            st.caption(f"Previewing first sheet: {sheet} | Rows: {len(df):,} | Columns: {df.shape[1]:,}")
        except Exception as e:
            st.error(f"Failed to read sheet '{sheet}'. Error: {e}")
            st.stop()

st.divider()
st.subheader("Download")

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
base_name = safe_filename(uploaded.name.rsplit(".", 1)[0])

if export_mode == "Single sheet → CSV":
    out_name = f"{base_name}_{safe_filename(sheet)}_{timestamp}.csv"
    try:
        csv_bytes = to_csv_bytes(
            df=df,
            delimiter=delimiter,
            encoding=encoding_map[encoding],
            quoting=quoting,
            escapechar_enabled=escapechar_enabled,
            date_format=date_format,
        )
        st.download_button(
            label=f"Download CSV ({sheet})",
            data=csv_bytes,
            file_name=out_name,
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Failed to create CSV. Error: {e}")
else:
    zip_name = f"{base_name}_{timestamp}_csvs.zip"
    try:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for s in sheet_names:
                df_s = read_sheet_as_dataframe(
                    xlsx_bytes=xlsx_bytes,
                    sheet_name=s,
                    header_row=int(header_row),
                    formula_mode=formula_mode,
                    drop_empty=drop_empty,
                )
                df_s = force_columns_to_text(df_s, force_text_cols)
                csv_s = to_csv_bytes(
                    df=df_s,
                    delimiter=delimiter,
                    encoding=encoding_map[encoding],
                    quoting=quoting,
                    escapechar_enabled=escapechar_enabled,
                    date_format=date_format,
                )
                csv_filename = f"{safe_filename(s)}.csv"
                z.writestr(csv_filename, csv_s)

        zip_buf.seek(0)
        st.download_button(
            label="Download ZIP (all sheets as CSV)",
            data=zip_buf.getvalue(),
            file_name=zip_name,
            mime="application/zip",
        )
    except Exception as e:
        st.error(f"Failed to create ZIP. Error: {e}")
