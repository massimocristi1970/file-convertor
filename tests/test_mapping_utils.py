import unittest

import pandas as pd

from mapping_utils import CALLER_AI_REQUIRED_COLUMNS, build_caller_ai_output_spec, build_export_dataframe
from transforms import apply_transform


class MappingUtilsTests(unittest.TestCase):
    def test_caller_ai_spec_prefills_required_columns_and_transforms(self) -> None:
        spec = build_caller_ai_output_spec(["Full Name", "Mobile Number", "Card Number", "DOB", "Address", "Title"])
        self.assertEqual([row["output_name"] for row in spec], CALLER_AI_REQUIRED_COLUMNS)
        self.assertEqual(spec[0]["transform"], "Name: extract first")
        self.assertEqual(spec[1]["transform"], "UK mobile -> 44")
        self.assertEqual(spec[2]["transform"], "Digits: keep last N")
        self.assertEqual(spec[2]["params"], {"n": 4})
        self.assertEqual(spec[4]["transform"], "UK Postcode (extract)")
        self.assertEqual(spec[5]["transform"], "Name: extract title")
        self.assertEqual(spec[6]["transform"], "Name: extract surname")

    def test_build_export_dataframe_keeps_blank_columns(self) -> None:
        source = pd.DataFrame([{"Name": "Ada Lovelace"}])
        export_df, missing = build_export_dataframe(
            source,
            [
                {"source": "(blank)", "transform": "None", "params": {}, "output_name": "Title"},
                {"source": "Name", "transform": "Name: extract surname", "params": {}, "output_name": "Surname"},
            ],
        )
        self.assertEqual(missing, [])
        self.assertEqual(list(export_df.columns), ["Title", "Surname"])
        self.assertEqual(export_df.iloc[0].to_dict(), {"Title": "", "Surname": "Lovelace"})

    def test_build_export_dataframe_reports_missing_sources(self) -> None:
        source = pd.DataFrame([{"Name": "Ada Lovelace"}])
        export_df, missing = build_export_dataframe(
            source,
            [{"source": "Phone", "transform": "None", "params": {}, "output_name": "PhoneNumber"}],
        )
        self.assertTrue(export_df.empty)
        self.assertEqual(missing, ["PhoneNumber <- Phone"])

    def test_name_transforms_extract_first_title_and_surname(self) -> None:
        series = pd.Series(["Dr Ada Lovelace"])
        self.assertEqual(apply_transform(series, "Name: extract first", {}).iloc[0], "Ada")
        self.assertEqual(apply_transform(series, "Name: extract title", {}).iloc[0], "Dr")
        self.assertEqual(apply_transform(series, "Name: extract surname", {}).iloc[0], "Lovelace")
        self.assertEqual(apply_transform(pd.Series(["Mrs"]), "Name: extract title", {}).iloc[0], "Mrs")


if __name__ == "__main__":
    unittest.main()
