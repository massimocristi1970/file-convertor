import re
from typing import Any, Dict

import pandas as pd


POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", flags=re.IGNORECASE)


def _as_str(x: Any) -> str:
    return "" if pd.isna(x) else str(x)


def tf_none(x: str, p: Dict[str, Any]) -> str:
    return x


def tf_text_force(x: str, p: Dict[str, Any]) -> str:
    return x


def tf_uk_postcode(x: str, p: Dict[str, Any]) -> str:
    match = POSTCODE_RE.search(x.upper())
    return match.group(1).strip() if match else ""


def tf_first_line(x: str, p: Dict[str, Any]) -> str:
    s = x.strip()
    if not s:
        return ""
    parts = [token.strip() for token in s.split(",")]
    for token in parts:
        if token:
            return token
    return s


def tf_uk_mobile_add44(x: str, p: Dict[str, Any]) -> str:
    s = re.sub(r"\s+", "", x.strip())
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    digits = re.sub(r"\D", "", s)
    if digits.startswith("44"):
        return digits
    if digits.startswith("0"):
        return "44" + digits[1:]
    return "44" + digits


def tf_digits_last_n(x: str, p: Dict[str, Any]) -> str:
    n = int(p.get("n", 4))
    digits = re.findall(r"\d", x)
    return "".join(digits[-n:]) if len(digits) >= n else ""


def tf_extract_regex(x: str, p: Dict[str, Any]) -> str:
    pattern = str(p.get("pattern", "") or "")
    if not pattern:
        return ""
    flags = re.IGNORECASE if bool(p.get("ignore_case", True)) else 0
    match = re.search(pattern, x, flags=flags)
    if not match:
        return ""
    try:
        return (match.group(int(p.get("group", 1))) or "").strip()
    except Exception:
        return ""


def tf_split_take(x: str, p: Dict[str, Any]) -> str:
    delim = str(p.get("delim", ","))
    idx = int(p.get("index", 0))
    parts = [token.strip() for token in x.split(delim)]
    if idx < 0:
        idx = len(parts) + idx
    return parts[idx] if 0 <= idx < len(parts) else ""


def tf_prefix_if_missing(x: str, p: Dict[str, Any]) -> str:
    prefix = str(p.get("prefix", "") or "")
    if not prefix:
        return x
    return x if x.startswith(prefix) else prefix + x


def tf_suffix(x: str, p: Dict[str, Any]) -> str:
    suffix = str(p.get("suffix", "") or "")
    return x + suffix if suffix else x


def tf_regex_replace(x: str, p: Dict[str, Any]) -> str:
    pattern = str(p.get("pattern", "") or "")
    replacement = str(p.get("repl", "") or "")
    if not pattern:
        return x
    flags = re.IGNORECASE if bool(p.get("ignore_case", True)) else 0
    return re.sub(pattern, replacement, x, flags=flags)


TRANSFORM_FUNCS = {
    "None": tf_none,
    "Text (force)": tf_text_force,
    "UK Postcode (extract)": tf_uk_postcode,
    "Address first line (before comma)": tf_first_line,
    "UK mobile → 44": tf_uk_mobile_add44,
    "Digits: keep last N": tf_digits_last_n,
    "Extract by regex": tf_extract_regex,
    "Split + take part": tf_split_take,
    "Prefix if missing": tf_prefix_if_missing,
    "Suffix": tf_suffix,
    "Regex replace": tf_regex_replace,
}


def apply_transform(series: pd.Series, tf_name: str, params: Dict[str, Any]) -> pd.Series:
    fn = TRANSFORM_FUNCS.get(tf_name, tf_none)
    return series.map(lambda value: fn(_as_str(value), params))
