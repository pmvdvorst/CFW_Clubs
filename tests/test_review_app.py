import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.review_app import create_app, workspace_relative
from scripts.staging_service import StagingLockRegistry, load_staging_file


ROOT = Path("/Users/paul/Documents/VDM/CFW_Clubs")


def write_staging_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "external_id,existing_gd_id,source_type,source_name,source_url,discovered_at,last_checked,review_status,confidence_score,club_name,summary_raw,summary_final,audience,discipline_primary,disciplines_csv,website,facebook_url,instagram_url,twitter_url,twitter_handle,youtube_url,strava_url,email,street,street2,city,region,country,postal_code,plus_code,latitude,longitude,logo_source_url,logo_rights_status,cover_source_url,gallery_source_urls,image_rights_status,image_notes,focus,notes_internal",
                "approved-club,,openai-web-search,seed,https://approved.example,2026-04-16 10:00:00,2026-04-16 10:00:00,approved,0.97,Approved Club,Raw summary,Final summary,Women Only,Road,Road,https://approved.example,,,,,,,,,,Toronto,Ontario,Canada,,,43.0,-79.0,https://approved.example/logo.png,official-site,https://approved.example/cover.jpg,https://approved.example/gallery-1.jpg::https://approved.example/gallery-2.jpg,official-site,,,\"score=0.97 | location=Toronto\"",
                "pending-club,,openai-web-search,seed,https://pending.example,2026-04-16 10:00:00,2026-04-16 10:00:00,pending,0.72,Pending Club,Raw summary,,Mixed Gender,Road,Road,https://pending.example,,,,,,,,,,Waterloo,Ontario,Canada,,,44.0,-80.0,,,,official-site,,,\"score=0.72 | location=Waterloo\"",
            ]
        ),
        encoding="utf-8",
    )


class FakeDiscoveryManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def start(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return None

    def snapshot(self, staging_path):  # type: ignore[no-untyped-def]
        return {
            "staging_csv": workspace_relative(staging_path),
            "status": "idle",
            "running": False,
            "return_code": None,
            "started_at": "",
            "finished_at": "",
            "command": [],
            "log": "",
        }


class ReviewAppTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(dir=ROOT / "data")
        self.addCleanup(self.tempdir.cleanup)
        self.staging_csv = Path(self.tempdir.name) / "club-staging-app-test.csv"
        write_staging_csv(self.staging_csv)
        self.seed_csv = ROOT / "examples" / "location-seeds-sample.csv"
        self.geonames = Path(self.tempdir.name) / "cities.txt"
        self.admin1 = Path(self.tempdir.name) / "admin1CodesASCII.txt"
        self.country_info = Path(self.tempdir.name) / "countryInfo.txt"
        self.generated_seed_csv = Path(self.tempdir.name) / "generated-location-seeds.csv"
        self.geonames.write_text(
            "\n".join(
                [
                    "6167865\tToronto\tToronto\t\t43.70011\t-79.4163\tP\tPPLA\tCA\t\t08\t\t\t\t2731571\t\t\tAmerica/Toronto\t2024-01-01",
                    "6094817\tOttawa\tOttawa\t\t45.41117\t-75.69812\tP\tPPLC\tCA\t\t08\t\t\t\t812129\t\t\tAmerica/Toronto\t2024-01-01",
                    "6173331\tVancouver\tVancouver\t\t49.24966\t-123.11934\tP\tPPLA\tCA\t\t02\t\t\t\t600000\t\t\tAmerica/Vancouver\t2024-01-01",
                ]
            ),
            encoding="utf-8",
        )
        self.admin1.write_text(
            "\n".join(
                [
                    "CA.08\tOntario\tOntario\t6093943",
                    "CA.02\tBritish Columbia\tBritish Columbia\t5909050",
                ]
            ),
            encoding="utf-8",
        )
        self.country_info.write_text(
            "CA\tCAN\t124\tCA\tCanada\tOttawa\t9984670\t37058856\tNA\t.ca\tCAD\tDollar\t1\t\t\t\ten-CA,fr-CA\t6251999\tUS\t\n",
            encoding="utf-8",
        )
        self.lock_registry = StagingLockRegistry()
        self.discovery_manager = FakeDiscoveryManager()
        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test",
                "LOCK_REGISTRY": self.lock_registry,
                "DISCOVERY_MANAGER": self.discovery_manager,
                "SESSION_STATE": {},
                "DEFAULT_STAGING_CSV": workspace_relative(self.staging_csv),
                "DEFAULT_SEED_CSV": workspace_relative(self.seed_csv),
            }
        )
        self.client = self.app.test_client()

    def test_seed_preview_and_save_flow(self) -> None:
        preview_response = self.client.post(
            "/seed",
            data={
                "seed_action": "preview",
                "geonames_file": workspace_relative(self.geonames),
                "output_csv": workspace_relative(self.generated_seed_csv),
                "country_code": "CA",
                "admin1": "Ontario",
                "admin1_code": "",
                "country_info_file": workspace_relative(self.country_info),
                "admin1_codes_file": workspace_relative(self.admin1),
                "feature_codes": "PPLC,PPLA",
                "min_population": "500000",
                "max_locations": "10",
                "query_hint": "road gravel",
            },
        )
        preview_body = preview_response.get_data(as_text=True)
        self.assertEqual(preview_response.status_code, 200)
        self.assertIn("Preview shows 2 location seeds for Canada / Ontario.", preview_body)
        self.assertIn("Map-style grouping", preview_body)
        self.assertIn("Toronto", preview_body)
        self.assertIn("Toronto", preview_body)
        self.assertIn("Ottawa", preview_body)
        self.assertIn("seed-cluster-bubble", preview_body)

        save_response = self.client.post(
            "/seed",
            data={
                "seed_action": "save",
                "geonames_file": workspace_relative(self.geonames),
                "output_csv": workspace_relative(self.generated_seed_csv),
                "country_code": "CA",
                "admin1": "Ontario",
                "admin1_code": "",
                "country_info_file": workspace_relative(self.country_info),
                "admin1_codes_file": workspace_relative(self.admin1),
                "feature_codes": "PPLC,PPLA",
                "min_population": "500000",
                "max_locations": "10",
                "query_hint": "road gravel",
            },
        )
        save_body = save_response.get_data(as_text=True)
        self.assertEqual(save_response.status_code, 200)
        self.assertTrue(self.generated_seed_csv.exists())
        self.assertIn("Use this seed CSV in Discovery", save_body)

    def test_discovery_run_maps_form_values_to_command(self) -> None:
        response = self.client.post(
            "/discover/run",
            data={
                "seed_csv": workspace_relative(self.seed_csv),
                "staging_csv": workspace_relative(self.staging_csv),
                "openai_api_key": "sk-test-session-key",
                "discovery_model": "gpt-5.4",
                "verification_model": "gpt-5.4-mini",
                "reasoning_effort": "medium",
                "max_locations": "5",
                "min_confidence": "0.88",
                "max_candidates_per_location": "4",
                "discovery_max_output_tokens": "1400",
                "verification_max_output_tokens": "500",
                "max_retries": "6",
                "retry_buffer_seconds": "1.5",
                "timeout": "45",
                "pause_seconds": "0.2",
                "skip_verification": "1",
                "replace_existing": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.discovery_manager.calls), 1)
        command = self.discovery_manager.calls[0]["command"]
        env = self.discovery_manager.calls[0]["env"]
        self.assertIn("--skip-verification", command)
        self.assertIn("--replace-existing", command)
        self.assertIn("0.88", command)
        self.assertIn("1400", command)
        self.assertIn(str(self.staging_csv), command)
        self.assertNotIn("sk-test-session-key", command)
        self.assertEqual(env["OPENAI_API_KEY"], "sk-test-session-key")

    def test_discovery_run_requires_api_key_when_none_is_available(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            response = self.client.post(
                "/discover/run",
                data={
                    "seed_csv": workspace_relative(self.seed_csv),
                    "staging_csv": workspace_relative(self.staging_csv),
                },
                follow_redirects=True,
            )
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Discovery needs an OpenAI API key.", body)
        self.assertEqual(len(self.discovery_manager.calls), 0)

    def test_discovery_run_reuses_saved_session_api_key(self) -> None:
        first_response = self.client.post(
            "/discover/run",
            data={
                "seed_csv": workspace_relative(self.seed_csv),
                "staging_csv": workspace_relative(self.staging_csv),
                "openai_api_key": "sk-saved-session-key",
            },
        )
        self.assertEqual(first_response.status_code, 302)
        self.assertEqual(self.discovery_manager.calls[0]["env"]["OPENAI_API_KEY"], "sk-saved-session-key")

        second_response = self.client.post(
            "/discover/run",
            data={
                "seed_csv": workspace_relative(self.seed_csv),
                "staging_csv": workspace_relative(self.staging_csv),
            },
        )
        self.assertEqual(second_response.status_code, 302)
        self.assertEqual(len(self.discovery_manager.calls), 2)
        self.assertEqual(self.discovery_manager.calls[1]["env"]["OPENAI_API_KEY"], "sk-saved-session-key")

    def test_discovery_page_shows_environment_key_status(self) -> None:
        with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-env-test-key"}, clear=True):
            response = self.client.get(f"/discover?staging_csv={workspace_relative(self.staging_csv)}")
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("OpenAI: Inherited from OPENAI_API_KEY", body)
        self.assertIn("Discovery can use the key that was already present in the review app environment.", body)

    def test_bulk_review_action_updates_status(self) -> None:
        response = self.client.post(
            "/review/bulk",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "action": "approve",
                "row_ids": "pending-club",
                "next": "/review",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        _, rows = load_staging_file(self.staging_csv)
        pending_row = next(row for row in rows if row["external_id"] == "pending-club")
        self.assertEqual(pending_row["review_status"], "approved")

    def test_review_page_renders_media_columns_and_detail_below_table(self) -> None:
        response = self.client.get(f"/review?staging_csv={workspace_relative(self.staging_csv)}")
        body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("<th>Team</th>", body)
        self.assertIn("<th>URL</th>", body)
        self.assertIn("<th>Logo</th>", body)
        self.assertIn("<th>Photos</th>", body)
        self.assertIn("<th>Description (Final)</th>", body)
        self.assertIn('data-inline-row-editor', body)
        self.assertIn('name="club_name"', body)
        self.assertIn('name="website"', body)
        self.assertIn('name="audience"', body)
        self.assertIn('name="disciplines_csv"', body)
        self.assertIn('name="summary_final"', body)
        self.assertIn('data-preview-image="1"', body)
        self.assertIn('data-image-size', body)
        self.assertIn("https://approved.example/logo.png", body)
        self.assertIn("https://approved.example/cover.jpg", body)
        self.assertIn("https://approved.example/gallery-1.jpg", body)
        self.assertIn('data-table-delete-logo', body)
        self.assertIn('data-table-delete-photo-kind="cover"', body)
        self.assertIn('data-table-delete-photo-kind="gallery"', body)
        self.assertIn('data-table-add-photo', body)
        self.assertIn('data-table-add-logo', body)
        self.assertIn('data-media-editor', body)
        self.assertIn('data-delete-logo', body)
        self.assertIn('data-delete-photo-kind="cover"', body)
        self.assertIn('data-delete-photo-kind="gallery"', body)
        self.assertIn('data-add-photo', body)
        self.assertLess(body.index('class="panel table-panel"'), body.index("<h3>Row Detail</h3>"))

    def test_review_page_shows_add_logo_when_selected_row_has_no_logo(self) -> None:
        response = self.client.get(
            f"/review?staging_csv={workspace_relative(self.staging_csv)}&row_id=pending-club"
        )
        body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("No logo yet.", body)
        self.assertIn('data-add-logo', body)

    def test_api_row_update_persists_quick_edit(self) -> None:
        response = self.client.post(
            "/api/rows/pending-club",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "club_name": "Pending Club Updated",
                "website": "https://pending.example/club",
                "audience": "Women Only",
                "disciplines_csv": "Road,Gravel",
                "summary_final": "Ready for editorial review.",
                "review_status": "needs_research",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])

        _, rows = load_staging_file(self.staging_csv)
        pending_row = next(row for row in rows if row["external_id"] == "pending-club")
        self.assertEqual(pending_row["club_name"], "Pending Club Updated")
        self.assertEqual(pending_row["website"], "https://pending.example/club")
        self.assertEqual(pending_row["audience"], "Women Only")
        self.assertEqual(pending_row["disciplines_csv"], "Road,Gravel")
        self.assertEqual(pending_row["summary_final"], "Ready for editorial review.")
        self.assertEqual(pending_row["review_status"], "needs_research")

    def test_export_preview_uses_selected_approved_rows_only(self) -> None:
        self.client.post(
            "/api/selection",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "row_id": "approved-club",
                "selected": "1",
            },
        )
        self.client.post(
            "/api/selection",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "row_id": "pending-club",
                "selected": "1",
            },
        )

        response = self.client.get(
            f"/export?staging_csv={workspace_relative(self.staging_csv)}",
        )
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("2 selected rows", body)
        self.assertIn("1 valid GeoDirectory rows", body)
        self.assertIn("Approved Club", body)

    def test_export_generate_is_blocked_while_locked(self) -> None:
        self.client.post(
            "/api/selection",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "row_id": "approved-club",
                "selected": "1",
            },
        )
        self.lock_registry.acquire(self.staging_csv, owner="openai-discovery")
        self.addCleanup(self.lock_registry.release, self.staging_csv)

        response = self.client.post(
            "/export/generate",
            data={
                "staging_csv": workspace_relative(self.staging_csv),
                "output_csv": workspace_relative(Path(self.tempdir.name) / "gd-out.csv"),
            },
            follow_redirects=True,
        )
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Export is blocked while discovery is writing to this staging file.", body)

    def test_tooltip_icons_render_on_forms(self) -> None:
        seed_response = self.client.get("/seed")
        seed_body = seed_response.get_data(as_text=True)
        self.assertEqual(seed_response.status_code, 200)
        self.assertIn('class="help-icon"', seed_body)
        self.assertIn("GeoNames city dump or zip file", seed_body)

        discover_response = self.client.get(f"/discover?staging_csv={workspace_relative(self.staging_csv)}")
        discover_body = discover_response.get_data(as_text=True)
        self.assertEqual(discover_response.status_code, 200)
        self.assertIn("The verified location seed CSV", discover_body)


if __name__ == "__main__":
    unittest.main()
