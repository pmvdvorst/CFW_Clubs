import json
import socket
import unittest
from urllib.error import URLError

from scripts.discover_clubs import LocationSeed
from scripts.discover_clubs_with_openai import (
    Candidate,
    OpenAIResponseError,
    RetryableOpenAIResponseError,
    RetryableOpenAITimeoutError,
    apply_verification,
    build_discovery_payload,
    build_discovery_prompt,
    build_verification_payload,
    build_verification_prompt,
    candidate_is_acceptable,
    candidate_to_search_result,
    clean_summary_text,
    collect_source_urls,
    discovery_user_location,
    drop_conflicting_rows,
    extract_output_text,
    openai_request,
    parse_json_output,
    parse_discovery_response,
    parse_retry_after_seconds,
    preferred_official_website,
)


class OpenAIDiscoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.seed = LocationSeed(
            location_id="toronto-on",
            location_name="Toronto, Ontario, Canada",
            city="Toronto",
            region="Ontario",
            country="Canada",
            postal_code="",
            latitude="43.6532",
            longitude="-79.3832",
            query_hint="road gravel mtb",
        )

    def test_discovery_payload_uses_web_search_and_schema(self) -> None:
        class Args:
            discovery_model = "gpt-5.4-mini"
            verification_model = "gpt-5.4-mini"
            reasoning_effort = "low"
            max_candidates_per_location = 6
            discovery_max_output_tokens = 1800

        payload = build_discovery_payload(self.seed, Args())
        self.assertEqual(payload["model"], "gpt-5.4-mini")
        self.assertEqual(payload["tools"][0]["type"], "web_search")
        self.assertEqual(payload["tools"][0]["user_location"], discovery_user_location(self.seed))
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertEqual(payload["max_output_tokens"], 1800)

    def test_prompts_request_editorial_summaries(self) -> None:
        discovery_prompt = build_discovery_prompt(self.seed, 4)
        discovery_text = " ".join(item["content"] for item in discovery_prompt)
        self.assertIn("2 to 4 sentences", discovery_text)
        self.assertIn("fresh original wording", discovery_text)

        candidate = Candidate(
            club_name="Wild Bettys",
            official_website="https://wildbettys.com/",
            official_source_url="https://wildbettys.com/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["MTB"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        verification_prompt = build_verification_prompt(candidate, self.seed)
        verification_text = " ".join(item["content"] for item in verification_prompt)
        self.assertIn("fun to read", verification_text)
        self.assertIn("2 to 4 sentences", verification_text)

    def test_parse_discovery_response_extracts_candidates_and_sources(self) -> None:
        response = {
            "output": [
                {
                    "type": "web_search_call",
                    "action": {
                        "sources": [
                            {"url": "https://wildbettys.com/"},
                            {"url": "https://www.instagram.com/wildbettys/"},
                        ]
                    },
                },
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps(
                                {
                                    "candidates": [
                                        {
                                            "club_name": "Wild Bettys",
                                            "official_website": "https://wildbettys.com/",
                                            "official_source_url": "https://wildbettys.com/",
                                            "supporting_source_urls": [
                                                "https://www.instagram.com/wildbettys/"
                                            ],
                                            "audience": "Women Only",
                                            "disciplines": ["MTB"],
                                            "city": "Toronto",
                                            "region": "Ontario",
                                            "country": "Canada",
                                            "summary": "A Toronto-based club for women mountain bikers.",
                                            "why_it_qualifies": "The club's official site describes women-only mountain bike rides.",
                                            "classification": "club",
                                            "is_real_cycling_club": True,
                                            "is_women_relevant": True,
                                            "is_shop": False,
                                            "is_individual": False,
                                            "is_directory": False,
                                            "confidence": 0.93,
                                        }
                                    ]
                                }
                            ),
                        }
                    ],
                },
            ]
        }
        candidates, sources = parse_discovery_response(response)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].club_name, "Wild Bettys")
        self.assertEqual(sources, ["https://wildbettys.com/", "https://www.instagram.com/wildbettys/"])

    def test_extract_output_text_retries_incomplete_response(self) -> None:
        response = {
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": "{\"candidates\": ["}],
                }
            ],
        }
        with self.assertRaises(RetryableOpenAIResponseError) as context:
            extract_output_text(response)
        self.assertIn("reason=max_output_tokens", str(context.exception))
        self.assertTrue(context.exception.increase_output_tokens)

    def test_parse_json_output_retries_invalid_json(self) -> None:
        response = {
            "status": "completed",
            "output": [],
        }
        with self.assertRaises(RetryableOpenAIResponseError):
            parse_json_output("{\"candidates\": [", response=response)

    def test_extract_output_text_reports_refusal(self) -> None:
        response = {
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "refusal", "refusal": "I can’t help with that."}],
                }
            ],
        }
        with self.assertRaisesRegex(OpenAIResponseError, "Model refusal"):
            extract_output_text(response)

    def test_openai_request_retries_socket_timeout(self) -> None:
        import scripts.discover_clubs_with_openai as module

        original_urlopen = module.urlopen

        def raising_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
            del request, timeout
            raise socket.timeout("timed out")

        module.urlopen = raising_urlopen
        try:
            with self.assertRaises(RetryableOpenAITimeoutError):
                openai_request("test-key", {"model": "gpt-5.4-mini"}, 5.0)
        finally:
            module.urlopen = original_urlopen

    def test_openai_request_retries_urlerror_timeout(self) -> None:
        import scripts.discover_clubs_with_openai as module

        original_urlopen = module.urlopen

        def raising_urlopen(request, timeout=0):  # type: ignore[no-untyped-def]
            del request, timeout
            raise URLError("timed out")

        module.urlopen = raising_urlopen
        try:
            with self.assertRaises(RetryableOpenAITimeoutError):
                openai_request("test-key", {"model": "gpt-5.4-mini"}, 5.0)
        finally:
            module.urlopen = original_urlopen

    def test_candidate_acceptance_and_verification(self) -> None:
        candidate = Candidate(
            club_name="Wild Bettys",
            official_website="https://wildbettys.com/",
            official_source_url="https://wildbettys.com/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["MTB"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="A Toronto-based club for women mountain bikers.",
            why_it_qualifies="Official site supports it.",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        self.assertTrue(candidate_is_acceptable(candidate))
        verified = apply_verification(
            candidate,
            {
                "accept": True,
                "audience": "Women Only",
                "disciplines": ["MTB"],
                "summary": "Verified summary",
                "why_it_qualifies": "Verified reason",
                "confidence": 0.95,
            },
        )
        self.assertIsNotNone(verified)
        self.assertEqual(verified.summary, "Verified summary")
        self.assertEqual(verified.confidence, 0.95)

    def test_candidate_acceptance_rejects_low_confidence_and_bad_official_hosts(self) -> None:
        low_confidence = Candidate(
            club_name="Borderline Club",
            official_website="https://exampleclub.com/",
            official_source_url="https://exampleclub.com/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["Road"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.6,
        )
        self.assertFalse(candidate_is_acceptable(low_confidence))

        meetup_candidate = Candidate(
            club_name="Meetup Riders",
            official_website="https://www.meetup.com/some-riders/",
            official_source_url="https://www.meetup.com/some-riders/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["Road"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="ride-group",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        self.assertFalse(candidate_is_acceptable(meetup_candidate))

    def test_candidate_to_search_result_uses_official_presence(self) -> None:
        candidate = Candidate(
            club_name="Wild Bettys",
            official_website="https://wildbettys.com/",
            official_source_url="https://wildbettys.com/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["MTB"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="A Toronto-based club for women mountain bikers.",
            why_it_qualifies="Official site supports it.",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        result = candidate_to_search_result(candidate, ["https://wildbettys.com/"], self.seed)
        self.assertEqual(result.link, "https://wildbettys.com/")

    def test_preferred_official_website_prioritizes_homepage_over_about_page(self) -> None:
        candidate = Candidate(
            club_name="Wild Bettys",
            official_website="https://www.wildbettys.com/ClubInfo/AboutWildBettys.aspx",
            official_source_url="https://www.wildbettys.com/ClubInfo/AboutWildBettys.aspx",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["MTB"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.95,
        )
        self.assertEqual(
            preferred_official_website(candidate),
            "https://www.wildbettys.com/Home.aspx",
        )

    def test_preferred_official_website_avoids_pdf_sources(self) -> None:
        candidate = Candidate(
            club_name="Saddle Sisters of High Park",
            official_website="https://saddlesisters.ca/files/SSHPCodeOfConduct.pdf",
            official_source_url="https://saddlesisters.ca/files/SSHPCodeOfConduct.pdf",
            supporting_source_urls=[
                "https://saddlesisters.ca/",
                "https://saddlesisters.ca/community",
            ],
            audience="Women Only",
            disciplines=["Road", "Gravel"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        self.assertEqual(preferred_official_website(candidate), "https://saddlesisters.ca/")

    def test_verification_payload_restricts_to_official_domain(self) -> None:
        class Args:
            verification_model = "gpt-5.4-mini"
            reasoning_effort = "low"
            verification_max_output_tokens = 700

        candidate = Candidate(
            club_name="Wild Bettys",
            official_website="https://wildbettys.com/",
            official_source_url="https://wildbettys.com/",
            supporting_source_urls=[],
            audience="Women Only",
            disciplines=["MTB"],
            city="Toronto",
            region="Ontario",
            country="Canada",
            summary="Summary",
            why_it_qualifies="Reason",
            classification="club",
            is_real_cycling_club=True,
            is_women_relevant=True,
            is_shop=False,
            is_individual=False,
            is_directory=False,
            confidence=0.9,
        )
        payload = build_verification_payload(candidate, self.seed, Args())
        self.assertEqual(
            payload["tools"][0]["filters"]["allowed_domains"],
            ["wildbettys.com"],
        )
        self.assertEqual(payload["max_output_tokens"], 700)

    def test_parse_retry_after_seconds(self) -> None:
        self.assertEqual(
            parse_retry_after_seconds("Rate limit reached. Please try again in 2.117s."),
            2.117,
        )
        self.assertIsNone(parse_retry_after_seconds("No retry hint here."))

    def test_clean_summary_text(self) -> None:
        self.assertEqual(clean_summary_text("  A fun summary.  "), "A fun summary.")

    def test_drop_conflicting_rows_removes_duplicate_external_id_and_keys(self) -> None:
        rows = [
            {
                "external_id": "newhopecommunitybikes-com-hamilton-ontario",
                "website": "https://www.newhopecommunitybikes.com",
                "facebook_url": "",
                "instagram_url": "https://www.instagram.com/everyoneridesinitiative",
                "twitter_url": "",
            },
            {
                "external_id": "different-club-hamilton-ontario",
                "website": "https://example.com",
                "facebook_url": "",
                "instagram_url": "",
                "twitter_url": "",
            },
        ]
        remaining = drop_conflicting_rows(
            rows,
            keys={
                "newhopecommunitybikes-com-hamilton-ontario",
                "newhopecommunitybikes.com",
                "https://www.instagram.com/everyoneridesinitiative",
            },
            external_id="newhopecommunitybikes-com-hamilton-ontario",
        )
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["external_id"], "different-club-hamilton-ontario")


if __name__ == "__main__":
    unittest.main()
