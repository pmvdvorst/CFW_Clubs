import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")
SCRIPT = ROOT / "scripts" / "match_existing_geodirectory_ids.py"


class MatchExistingGeoDirectoryIdsTest(unittest.TestCase):
    def test_matches_by_website_domain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            staging_csv = tmp / "staging.csv"
            gd_csv = tmp / "gd.csv"
            output_csv = tmp / "matched.csv"

            staging_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,source_url,club_name,website,facebook_url,instagram_url,twitter_url,city,region,country,notes_internal",
                        "wildbettys-com-toronto-ontario,,https://www.wildbettys.com/about,Wild Bettys,https://www.wildbettys.com,,,,Toronto,Ontario,Canada,",
                    ]
                ),
                encoding="utf-8",
            )
            gd_csv.write_text(
                "\n".join(
                    [
                        "ID,post_title,city,region,country,website,facebook,instagram,twitter",
                        "706,Wild Bettys,Toronto,Ontario,Canada,https://wildbettys.com,https://facebook.com/wildbettys,https://instagram.com/wildbettys,",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(staging_csv), str(gd_csv), str(output_csv)],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            with output_csv.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["existing_gd_id"], "706")
            self.assertIn("matched_existing_gd_id=706", rows[0]["notes_internal"])
            self.assertIn("gd_match_method=", rows[0]["notes_internal"])

    def test_matches_by_name_and_location_when_urls_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            staging_csv = tmp / "staging.csv"
            gd_csv = tmp / "gd.csv"
            output_csv = tmp / "matched.csv"

            staging_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,source_url,club_name,website,facebook_url,instagram_url,twitter_url,city,region,country,notes_internal",
                        "lapdogs-toronto,, ,LapDogs Cycling Club,,,,,Toronto,Ontario,Canada,",
                    ]
                ),
                encoding="utf-8",
            )
            gd_csv.write_text(
                "\n".join(
                    [
                        "ID,post_title,city,region,country,website,facebook,instagram,twitter",
                        "732,LapDogs Cycling Club,Toronto,Ontario,Canada,,,,",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(staging_csv), str(gd_csv), str(output_csv)],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            with output_csv.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["existing_gd_id"], "732")
            self.assertIn("gd_match_method=name_location", rows[0]["notes_internal"])

    def test_leaves_ambiguous_rows_blank(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            staging_csv = tmp / "staging.csv"
            gd_csv = tmp / "gd.csv"
            output_csv = tmp / "matched.csv"

            staging_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,source_url,club_name,website,facebook_url,instagram_url,twitter_url,city,region,country,notes_internal",
                        "ambiguous-club,, ,Hamilton Riders,,,,,Hamilton,Ontario,Canada,",
                    ]
                ),
                encoding="utf-8",
            )
            gd_csv.write_text(
                "\n".join(
                    [
                        "ID,post_title,city,region,country,website,facebook,instagram,twitter",
                        "800,Hamilton Riders,Hamilton,Ontario,Canada,,,,",
                        "801,Hamilton Riders,Hamilton,Ontario,Canada,,,,",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(staging_csv), str(gd_csv), str(output_csv)],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            with output_csv.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["existing_gd_id"], "")
            self.assertIn("existing_gd_id ambiguous", rows[0]["notes_internal"])


if __name__ == "__main__":
    unittest.main()
