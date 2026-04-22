import tempfile
import unittest
from pathlib import Path

from scripts.staging_service import (
    StagingFileLockedError,
    StagingLockRegistry,
    build_row_media_preview,
    filter_rows,
    load_staging_file,
    update_review_status_for_rows,
    update_row_fields,
)


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")


def write_staging_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "external_id,existing_gd_id,source_type,source_name,source_url,discovered_at,last_checked,review_status,confidence_score,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,image_notes,focus,notes_internal",
                "approved-club,,openai-web-search,seed,https://approved.example,2026-04-16 10:00:00,2026-04-16 10:00:00,approved,0.97,Approved Club,Raw summary,Final summary,Women Only,Road,\"Road,Gravel\",https://approved.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,https://approved.example/logo.png,official-site,https://approved.example/cover.jpg,,official-site,,,\"score=0.97 | location=Toronto\"",
                "mismatch-club,555,openai-web-search,seed,https://mismatch.example,2026-04-16 10:00:00,2026-04-16 10:00:00,pending,0.93,Mismatch Club,Raw summary,,Mixed Gender,Road,Road,https://mismatch.example,,,,,,,,,,Ottawa,Ontario,US,,,45.0,-75.0,,,,official-site,,,\"score=0.93 | location=Ottawa | audience needs review\"",
                "research-club,,openai-web-search,seed,https://research.example,2026-04-16 10:00:00,2026-04-16 10:00:00,needs_research,0.61,Research Club,Raw summary,, ,,,https://research.example,,,,,,,,,,Hamilton,Ontario,Canada,,,,,,,,,,\"score=0.61 | discipline needs review | location uses seed city\"",
            ]
        ),
        encoding="utf-8",
    )


class StagingServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(dir=ROOT / "data")
        self.addCleanup(self.tempdir.cleanup)
        self.staging_csv = Path(self.tempdir.name) / "club-staging-review-test.csv"
        write_staging_csv(self.staging_csv)

    def test_filter_rows_supports_flags_and_confidence(self) -> None:
        _, rows = load_staging_file(self.staging_csv)
        filtered = filter_rows(
            rows,
            {
                "flag": "suspicious_country_mismatch",
                "min_confidence": "0.90",
                "review_status": "pending",
            },
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["club_name"], "Mismatch Club")
        self.assertIn("suspicious country mismatch", filtered[0]["_flag_labels"])
        self.assertIn("existing GD ID already present", filtered[0]["_flag_labels"])

    def test_update_row_fields_and_bulk_status_persist(self) -> None:
        updated = update_row_fields(
            self.staging_csv,
            "research-club",
            {
                "summary_final": "Polished summary",
                "discipline_primary": "Road",
                "disciplines_csv": "Road,Gravel",
            },
        )
        self.assertEqual(updated["summary_final"], "Polished summary")
        self.assertEqual(updated["discipline_primary"], "Road")

        count = update_review_status_for_rows(self.staging_csv, ["research-club"], "approved")
        self.assertEqual(count, 1)

        _, rows = load_staging_file(self.staging_csv)
        research_row = next(row for row in rows if row["external_id"] == "research-club")
        self.assertEqual(research_row["review_status"], "approved")
        self.assertEqual(research_row["summary_final"], "Polished summary")

    def test_build_row_media_preview_orders_and_deduplicates_images(self) -> None:
        preview = build_row_media_preview(
            {
                "logo_source_url": " https://club.example/logo.png ",
                "cover_source_url": "https://club.example/cover.jpg",
                "gallery_source_urls": "https://club.example/cover.jpg::https://club.example/gallery-1.jpg::::https://club.example/gallery-2.jpg::https://club.example/gallery-1.jpg",
            }
        )

        self.assertEqual(preview["logo_url"], "https://club.example/logo.png")
        self.assertEqual(
            preview["photo_urls"],
            [
                "https://club.example/cover.jpg",
                "https://club.example/gallery-1.jpg",
                "https://club.example/gallery-2.jpg",
            ],
        )
        self.assertEqual(
            preview["photo_items"],
            [
                {"url": "https://club.example/cover.jpg", "kind": "cover", "label": "Cover"},
                {
                    "url": "https://club.example/gallery-1.jpg",
                    "kind": "gallery",
                    "label": "Picture",
                    "index": 0,
                },
                {
                    "url": "https://club.example/gallery-2.jpg",
                    "kind": "gallery",
                    "label": "Picture",
                    "index": 1,
                },
            ],
        )

    def test_lock_registry_blocks_updates(self) -> None:
        lock_registry = StagingLockRegistry()
        lock_registry.acquire(self.staging_csv, owner="test-lock")
        self.addCleanup(lock_registry.release, self.staging_csv)
        with self.assertRaises(StagingFileLockedError):
            update_row_fields(
                self.staging_csv,
                "approved-club",
                {"summary_final": "Updated"},
                lock_registry=lock_registry,
            )


if __name__ == "__main__":
    unittest.main()
