import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.backfill_summary_final import summary_from_row


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")
SCRIPT = ROOT / "scripts" / "backfill_summary_final.py"


class BackfillSummaryFinalTest(unittest.TestCase):
    def test_summary_from_row_uses_history_and_disciplines(self) -> None:
        row = {
            "club_name": "Brampton Cycling Club",
            "city": "Brampton",
            "audience": "",
            "disciplines_csv": "Road",
            "discipline_primary": "Road",
            "summary_raw": "We are a non-profit organization that was founded in 1972.",
            "notes_internal": "classification=club",
        }
        summary = summary_from_row(row)
        self.assertIn("Brampton Cycling Club", summary)
        self.assertIn("Founded in 1972", summary)
        self.assertIn("road rides", summary)

    def test_summary_from_row_handles_women_only_program(self) -> None:
        row = {
            "club_name": "New Hope Community Bikes",
            "city": "Hamilton",
            "audience": "Women Only",
            "disciplines_csv": "MTB,Road",
            "discipline_primary": "MTB",
            "summary_raw": "Tour de Cafe is a novice group ride held every other Saturday and Tour de MTB is a beginner women’s mountain bike ride.",
            "notes_internal": "classification=program",
        }
        summary = summary_from_row(row)
        self.assertIn("women-first cycling program", summary)
        self.assertIn("mountain bike and road rides", summary)
        self.assertIn("newer riders", summary)

    def test_summary_from_row_does_not_treat_program_creation_as_club_founding(self) -> None:
        row = {
            "club_name": "Ottawa Bicycle Club",
            "city": "Ottawa",
            "audience": "Mixed Gender",
            "disciplines_csv": "Road,Gravel",
            "discipline_primary": "Road",
            "summary_raw": "The women’s time trial series was created in 1989 as a way of encouraging the development of female cyclists.",
            "notes_internal": "classification=club",
        }
        summary = summary_from_row(row)
        self.assertIn("time trial series dates back to 1989", summary)
        self.assertNotIn("Founded in 1989", summary)

    def test_script_writes_summary_final(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            input_csv = tmp / "staging.csv"
            output_csv = tmp / "staging-out.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,summary_final,club_name,city,audience,disciplines_csv,discipline_primary,summary_raw,notes_internal",
                        "club-1,,Example Club,Toronto,Women Only,Road,Road,Weekly road rides for women,classification=ride-group",
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(input_csv), str(output_csv)],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            with output_csv.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(rows[0]["summary_final"])


if __name__ == "__main__":
    unittest.main()
