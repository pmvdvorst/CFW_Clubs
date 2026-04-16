import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")
SCRIPT = ROOT / "scripts" / "generate_location_seeds_from_geonames.py"


class GenerateLocationSeedsTest(unittest.TestCase):
    def test_generator_filters_country_and_admin1(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            geonames = tmp_path / "cities.txt"
            admin1 = tmp_path / "admin1CodesASCII.txt"
            country_info = tmp_path / "countryInfo.txt"
            output_csv = tmp_path / "locations.csv"

            geonames.write_text(
                "\n".join(
                    [
                        "6167865\tToronto\tToronto\t\t43.70011\t-79.4163\tP\tPPLA\tCA\t\t08\t\t\t\t2731571\t\t\tAmerica/Toronto\t2024-01-01",
                        "6094817\tOttawa\tOttawa\t\t45.41117\t-75.69812\tP\tPPLC\tCA\t\t08\t\t\t\t812129\t\t\tAmerica/Toronto\t2024-01-01",
                        "6173331\tVancouver\tVancouver\t\t49.24966\t-123.11934\tP\tPPLA\tCA\t\t02\t\t\t\t600000\t\t\tAmerica/Vancouver\t2024-01-01",
                    ]
                ),
                encoding="utf-8",
            )
            admin1.write_text(
                "\n".join(
                    [
                        "CA.08\tOntario\tOntario\t6093943",
                        "CA.02\tBritish Columbia\tBritish Columbia\t5909050",
                    ]
                ),
                encoding="utf-8",
            )
            country_info.write_text(
                "CA\tCAN\t124\tCA\tCanada\tOttawa\t9984670\t37058856\tNA\t.ca\tCAD\tDollar\t1\t\t\t\ten-CA,fr-CA\t6251999\tUS\t\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(geonames),
                    str(output_csv),
                    "--country-code",
                    "CA",
                    "--admin1",
                    "Ontario",
                    "--admin1-codes-file",
                    str(admin1),
                    "--country-info-file",
                    str(country_info),
                    "--min-population",
                    "500000",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            with output_csv.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual([row["city"] for row in rows], ["Toronto", "Ottawa"])
            self.assertEqual(rows[0]["region"], "Ontario")
            self.assertEqual(rows[0]["country"], "Canada")
            self.assertEqual(rows[0]["query_hint"], "road gravel mtb")


if __name__ == "__main__":
    unittest.main()
