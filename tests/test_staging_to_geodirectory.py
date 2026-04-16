import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")
SCRIPT = ROOT / "scripts" / "staging_to_geodirectory.py"


class StagingToGeoDirectoryTest(unittest.TestCase):
    def test_converter_exports_only_approved_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "approved-club,,approved,Approved Club,Raw summary,Final summary,Women Only,Road,\"Road,Gravel\",approved-club.example,https://facebook.com/approvedclub,https://instagram.com/approvedclub,https://twitter.com/approvedclub,approvedclub,,,hello@example.com,123 Main,,Toronto,Ontario,Canada,M5V,,43.0,-79.0,https://example.com/logo.png,official-site,https://example.com/group-ride-cover.jpg,https://example.com/women-ride-1.jpg::https://example.com/team-photo-2.jpg,licensed,Recreational",
                        "pending-club,,pending,Pending Club,Raw summary,Final summary,Mixed Gender,MTB,MTB,https://pending.example,,,,,,,,,,Waterloo,Ontario,Canada,,PLUS,44.0,-80.0,https://example.com/logo2.png,official-site,https://example.com/cover2.jpg,,official-site,",
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

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["post_title"], "Approved Club")
            self.assertEqual(row["post_tags"], "Women Only")
            self.assertEqual(row["post_category"], ",42,43,")
            self.assertEqual(row["default_category"], "42")
            self.assertEqual(row["logo"], "https://example.com/logo.png|||")
            self.assertEqual(
                row["post_images"],
                "https://example.com/group-ride-cover.jpg|||::https://example.com/women-ride-1.jpg|||::https://example.com/team-photo-2.jpg|||",
            )
            self.assertEqual(row["website"], "https://approved-club.example")
            self.assertEqual(row["twitterusername"], "approvedclub")

    def test_converter_falls_back_to_social_for_missing_disciplines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "social-club,,approved,Social Club,Raw summary,,Women Only,,,https://social.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,,,,,,",
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

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["post_title"], "Social Club")
            self.assertEqual(row["post_category"], ",57,")
            self.assertEqual(row["default_category"], "57")

    def test_converter_falls_back_to_mixed_gender_for_missing_audience(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "audience-fallback,,approved,Audience Fallback,Raw summary,, ,Road,Road,https://fallback.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,,,,,,",
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

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["post_title"], "Audience Fallback")
            self.assertEqual(row["post_tags"], "Mixed Gender")

    def test_converter_excludes_logo_style_images_from_post_images(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "image-cleanup,,approved,Image Cleanup,Raw summary,,Women Only,Road,Road,https://cleanup.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,https://cleanup.example/assets/club-logo.svg,official-site,https://cleanup.example/assets/club-logo.svg,https://cleanup.example/images/group-ride.jpg::https://cleanup.example/images/google-play-badge.png::https://cleanup.example/images/women-ride.jpg,official-site,",
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

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                rows[0]["post_images"],
                "https://cleanup.example/images/group-ride.jpg|||::https://cleanup.example/images/women-ride.jpg|||",
            )

    def test_converter_excludes_sponsor_graphics_and_keeps_cover_plus_five_extras(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "gallery-limit,,approved,Gallery Limit,Raw summary,,Women Only,Road,Road,https://gallery.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,https://gallery.example/assets/club-logo.png,official-site,https://gallery.example/images/homepage.jpg,https://gallery.example/sponsors/roadkit.jpg::https://gallery.example/images/ride-1.jpg::https://gallery.example/images/ride-2.jpg::https://gallery.example/images/ride-3.jpg::https://gallery.example/images/ride-4.jpg::https://gallery.example/images/ride-5.jpg::https://gallery.example/images/ride-6.jpg,official-site,",
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

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                rows[0]["post_images"],
                "::".join(
                    [
                        "https://gallery.example/images/homepage.jpg|||",
                        "https://gallery.example/images/ride-1.jpg|||",
                        "https://gallery.example/images/ride-2.jpg|||",
                        "https://gallery.example/images/ride-3.jpg|||",
                        "https://gallery.example/images/ride-4.jpg|||",
                        "https://gallery.example/images/ride-5.jpg|||",
                    ]
                ),
            )

    def test_converter_prefers_landscape_images_in_export_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_csv = tmp_path / "staging.csv"
            output_csv = tmp_path / "gd.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "external_id,existing_gd_id,review_status,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,focus",
                        "landscape-order,,approved,Landscape Order,Raw summary,,Women Only,Road,Road,https://landscape.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,https://landscape.example/logo.png,official-site,https://landscape.example/images/portrait.jpg?w=800&h=1200,https://landscape.example/images/landscape.jpg?w=1600&h=900::https://landscape.example/images/square.jpg?w=900&h=900,official-site,",
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

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                rows[0]["post_images"],
                "::".join(
                    [
                        "https://landscape.example/images/landscape.jpg?w=1600&h=900|||",
                        "https://landscape.example/images/portrait.jpg?w=800&h=1200|||",
                        "https://landscape.example/images/square.jpg?w=900&h=900|||",
                    ]
                ),
            )


if __name__ == "__main__":
    unittest.main()
