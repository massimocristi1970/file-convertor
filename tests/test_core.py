import unittest

import pandas as pd

from data_io import read_xml_bytes_safely, to_xml_bytes
from merge_utils import build_composite_key, merge_dataframes


class CoreTests(unittest.TestCase):
    def test_xml_round_trip_preserves_columns(self) -> None:
        df = pd.DataFrame([{"Account": "0012", "Name": "Ada"}, {"Account": "0099", "Name": "Grace"}])
        xml_bytes = to_xml_bytes(df, date_format="%Y-%m-%d")
        restored = read_xml_bytes_safely(xml_bytes)
        self.assertEqual(list(restored.columns), ["Account", "Name"])
        self.assertEqual(restored.iloc[0]["Account"], "0012")

    def test_composite_key_normalises_excel_style_numbers(self) -> None:
        df = pd.DataFrame([{"AppID": "123.0", "Postcode": "AB1 2CD"}])
        keys = build_composite_key(df, ["AppID", "Postcode"])
        self.assertEqual(keys.iloc[0], "123||AB1 2CD")

    def test_merge_exclude_unmatched_keeps_only_matches(self) -> None:
        left = pd.DataFrame([{"AppID": "1", "Name": "Ada"}, {"AppID": "2", "Name": "Grace"}])
        right = pd.DataFrame([{"AppID": "1", "Score": "10"}, {"AppID": "3", "Score": "11"}])
        result = merge_dataframes(
            file_entries=[
                {"role": "Applications", "df": left, "key_cols": ["AppID"], "duplicate_strategy": "Keep first"},
                {"role": "Scores", "df": right, "key_cols": ["AppID"], "duplicate_strategy": "Keep first"},
            ],
            base_role="Applications",
            join_type="left",
            exclude_unmatched=True,
            delimiter=",",
            encoding="utf-8",
            quoting=0,
            escapechar_enabled=False,
            date_format="%Y-%m-%d",
        )
        merged = result["merged"]
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged.iloc[0]["AppID"], "1")


if __name__ == "__main__":
    unittest.main()
