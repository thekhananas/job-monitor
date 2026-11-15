from pathlib import Path
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout

from job_monitor.sources_csv import check_live_sources, main, parse_sources_csv, render_sources_csv, render_sources_json


class SourcesCsvTests(unittest.TestCase):
    def test_example_csv_matches_example_json(self):
        root = Path(__file__).resolve().parent.parent
        csv_text = (root / "config" / "sources.example.csv").read_text(encoding="utf-8")
        expected = json.loads((root / "config" / "sources.example.json").read_text(encoding="utf-8"))
        self.assertEqual(parse_sources_csv(csv_text), expected)

    def test_parses_enabled_and_baseline_policy_overrides(self):
        csv_text = (
            "provider,board,company,enabled,baseline_policy\n"
            "greenhouse,acme,Acme Corp,true,notify\n"
            "ashby,ameba,Ameba,false,\n"
        )
        config = parse_sources_csv(csv_text)
        self.assertEqual(config, {
            "version": 1,
            "baseline_policy": "suppress",
            "sources": [
                {"provider": "greenhouse", "board": "acme", "company": "Acme Corp", "baseline_policy": "notify"},
                {"provider": "ashby", "board": "ameba", "company": "Ameba", "enabled": False},
            ],
        })

    def test_rejects_invalid_provider_slug_and_duplicates(self):
        with self.assertRaises(ValueError):
            parse_sources_csv("provider,board,company\nworkday,acme,Acme\n")
        with self.assertRaises(ValueError):
            parse_sources_csv("provider,board,company\ngreenhouse,bad/slug,Acme\n")
        with self.assertRaises(ValueError):
            parse_sources_csv(
                "provider,board,company\n"
                "greenhouse,acme,Acme\n"
                "greenhouse,acme,Acme Duplicate\n"
            )

    def test_check_live_sources_reports_failures_only_for_enabled_boards(self):
        config = parse_sources_csv(
            "provider,board,company,enabled\n"
            "greenhouse,good,Good Co,true\n"
            "ashby,ameba,Ameba,true\n"
            "lever,disabled,Disabled Co,false\n"
        )

        class FakeHTTPError(Exception):
            code = 404

        def fake_fetcher(source):
            if source["board"] == "ameba":
                raise FakeHTTPError("Not Found")
            return []

        self.assertEqual(check_live_sources(config, fetcher=fake_fetcher), [("ashby:ameba", "FakeHTTPError HTTP 404")])

    def test_cli_generates_and_checks_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "sources.csv"
            json_path = root / "sources.json"
            csv_path.write_text("provider,board,company\ngreenhouse,anthropic,Anthropic\n", encoding="utf-8")

            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(["--csv", str(csv_path), "--out", str(json_path)]), 0)
            self.assertEqual(json_path.read_text(encoding="utf-8"), render_sources_json(parse_sources_csv(csv_path.read_text())))

            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(["--csv", str(csv_path), "--out", str(json_path), "--check"]), 0)

            csv_path.write_text("provider,board,company\nlever,apolloresearch,Apollo Research\n", encoding="utf-8")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(["--csv", str(csv_path), "--out", str(json_path), "--check"]), 1)

    def test_export_csv_round_trips_example_json(self):
        root = Path(__file__).resolve().parent.parent
        example_json = json.loads((root / "config" / "sources.example.json").read_text(encoding="utf-8"))
        csv_text = render_sources_csv(example_json)
        self.assertEqual(parse_sources_csv(csv_text), example_json)

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sources.csv"
            with redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(["--export-csv", "--csv", str(csv_path), "--out", str(root / "config" / "sources.example.json")]),
                    0,
                )
            self.assertEqual(parse_sources_csv(csv_path.read_text(encoding="utf-8")), example_json)


if __name__ == "__main__":
    unittest.main()
