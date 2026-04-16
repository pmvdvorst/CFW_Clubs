#!/usr/bin/env python3
"""Discover cycling clubs with OpenAI Responses API web search and structured outputs."""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.discover_clubs import (  # noqa: E402
    LocationSeed,
    SearchResult,
    apply_social_logo_fallback,
    base_domain,
    build_staging_row,
    clean_text,
    country_code,
    empty_metadata,
    existing_keys,
    extract_metadata_with_asset_fallback,
    fetch_text,
    homepage_candidate_urls,
    link_key,
    merge_metadata,
    join_metadata_urls,
    normalize_url,
    split_metadata_urls,
    unique_preserve_order,
    write_staging_rows,
)


OPENAI_API_URL = "https://api.openai.com/v1/responses"
DEFAULT_DISCOVERY_MODEL = "gpt-5.4-mini"
DEFAULT_VERIFICATION_MODEL = "gpt-5.4-mini"
DEFAULT_REASONING_EFFORT = "low"
DEFAULT_MAX_CANDIDATES = 8
DEFAULT_MIN_CONFIDENCE = 0.75
DEFAULT_MAX_RETRIES = 5
DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS = 2600
DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS = 900
DEFAULT_RETRY_BUFFER_SECONDS = 1.0
MAX_DISCOVERY_OUTPUT_TOKENS = 6000
MAX_VERIFICATION_OUTPUT_TOKENS = 2400
NON_HTML_PATH_SUFFIXES = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".svg",
    ".zip",
)

NEGATIVE_OFFICIAL_HOSTS = {
    "eventbrite.com",
    "meetup.com",
    "youtube.com",
    "youtu.be",
    "linkedin.com",
    "wikipedia.org",
}

DISCOVERY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "club_name": {"type": "string"},
                    "official_website": {"type": "string"},
                    "official_source_url": {"type": "string"},
                    "supporting_source_urls": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "audience": {
                        "type": "string",
                        "enum": ["Women Only", "Mixed Gender", "Unknown"],
                    },
                    "disciplines": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "MTB",
                                "Road",
                                "Gravel",
                                "MTB XC",
                                "Track",
                                "Touring",
                                "Cyclo-cross",
                            ],
                        },
                    },
                    "city": {"type": "string"},
                    "region": {"type": "string"},
                    "country": {"type": "string"},
                    "summary": {"type": "string"},
                    "why_it_qualifies": {"type": "string"},
                    "classification": {
                        "type": "string",
                        "enum": ["club", "team", "ride-group", "program", "unknown"],
                    },
                    "is_real_cycling_club": {"type": "boolean"},
                    "is_women_relevant": {"type": "boolean"},
                    "is_shop": {"type": "boolean"},
                    "is_individual": {"type": "boolean"},
                    "is_directory": {"type": "boolean"},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "club_name",
                    "official_website",
                    "official_source_url",
                    "supporting_source_urls",
                    "audience",
                    "disciplines",
                    "city",
                    "region",
                    "country",
                    "summary",
                    "why_it_qualifies",
                    "classification",
                    "is_real_cycling_club",
                    "is_women_relevant",
                    "is_shop",
                    "is_individual",
                    "is_directory",
                    "confidence",
                ],
            },
        }
    },
    "required": ["candidates"],
}

VERIFICATION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "accept": {"type": "boolean"},
        "audience": {
            "type": "string",
            "enum": ["Women Only", "Mixed Gender", "Unknown"],
        },
        "disciplines": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "MTB",
                    "Road",
                    "Gravel",
                    "MTB XC",
                    "Track",
                    "Touring",
                    "Cyclo-cross",
                ],
            },
        },
        "summary": {"type": "string"},
        "why_it_qualifies": {"type": "string"},
        "confidence": {"type": "number"},
    },
    "required": ["accept", "audience", "disciplines", "summary", "why_it_qualifies", "confidence"],
}


class OpenAIResponseError(RuntimeError):
    """Raised when the OpenAI API response cannot be used."""


class OpenAIRateLimitError(OpenAIResponseError):
    """Raised when the OpenAI API asks the caller to slow down."""

    def __init__(self, message: str, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class RetryableOpenAIResponseError(OpenAIResponseError):
    """Raised when the caller should retry the request."""

    def __init__(
        self,
        message: str,
        *,
        increase_output_tokens: bool = False,
    ) -> None:
        super().__init__(message)
        self.increase_output_tokens = increase_output_tokens


class RetryableOpenAITimeoutError(RetryableOpenAIResponseError):
    """Raised when the OpenAI request timed out or the upstream connection stalled."""


@dataclass
class Candidate:
    club_name: str
    official_website: str
    official_source_url: str
    supporting_source_urls: list[str]
    audience: str
    disciplines: list[str]
    city: str
    region: str
    country: str
    summary: str
    why_it_qualifies: str
    classification: str
    is_real_cycling_club: bool
    is_women_relevant: bool
    is_shop: bool
    is_individual: bool
    is_directory: bool
    confidence: float


def clean_summary_text(value: str) -> str:
    return clean_text(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover cycling clubs using OpenAI web search and structured outputs."
    )
    parser.add_argument("locations_csv", type=Path, help="CSV of discovery locations.")
    parser.add_argument("staging_csv", type=Path, help="Staging CSV to create or update.")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("OPENAI_API_KEY", ""),
        help="OpenAI API key. Defaults to OPENAI_API_KEY.",
    )
    parser.add_argument(
        "--discovery-model",
        default=DEFAULT_DISCOVERY_MODEL,
        help=f"Responses model for discovery. Default: {DEFAULT_DISCOVERY_MODEL}",
    )
    parser.add_argument(
        "--verification-model",
        default=DEFAULT_VERIFICATION_MODEL,
        help=f"Responses model for verification. Default: {DEFAULT_VERIFICATION_MODEL}",
    )
    parser.add_argument(
        "--reasoning-effort",
        default=DEFAULT_REASONING_EFFORT,
        help=f"Reasoning effort for discovery and verification. Default: {DEFAULT_REASONING_EFFORT}",
    )
    parser.add_argument(
        "--max-candidates-per-location",
        type=int,
        default=DEFAULT_MAX_CANDIDATES,
        help=f"Maximum accepted candidates to ask for per location. Default: {DEFAULT_MAX_CANDIDATES}",
    )
    parser.add_argument(
        "--max-locations",
        type=int,
        default=0,
        help="Maximum number of locations to process from the input CSV. Default: all",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=DEFAULT_MIN_CONFIDENCE,
        help=f"Minimum AI confidence required for a candidate. Default: {DEFAULT_MIN_CONFIDENCE}",
    )
    parser.add_argument(
        "--discovery-max-output-tokens",
        type=int,
        default=DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS,
        help=(
            "Upper bound for OpenAI discovery output tokens, including reasoning tokens. "
            f"Default: {DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS}"
        ),
    )
    parser.add_argument(
        "--verification-max-output-tokens",
        type=int,
        default=DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS,
        help=(
            "Upper bound for OpenAI verification output tokens, including reasoning tokens. "
            f"Default: {DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS}"
        ),
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum retries for OpenAI rate-limit errors. Default: {DEFAULT_MAX_RETRIES}",
    )
    parser.add_argument(
        "--retry-buffer-seconds",
        type=float,
        default=DEFAULT_RETRY_BUFFER_SECONDS,
        help=f"Extra wait time added to OpenAI retry delays. Default: {DEFAULT_RETRY_BUFFER_SECONDS}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds. Default: 60",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.5,
        help="Delay between candidate enrichments. Default: 0.5",
    )
    parser.add_argument(
        "--skip-verification",
        action="store_true",
        help="Skip the second pass that verifies each candidate against its official domain.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace matching rows in the staging CSV instead of skipping them.",
    )
    return parser.parse_args()


def load_location_seeds(path: Path) -> list[LocationSeed]:
    from scripts.discover_clubs import load_location_seeds as load_seed_rows

    return load_seed_rows(path)


def discovery_user_location(seed: LocationSeed) -> dict[str, str]:
    payload = {
        "type": "approximate",
        "country": country_code(seed.country) or clean_text(seed.country),
        "city": clean_text(seed.city),
        "region": clean_text(seed.region),
    }
    return {key: value for key, value in payload.items() if value}


def json_schema_format(name: str, schema: dict) -> dict:
    return {
        "type": "json_schema",
        "name": name,
        "strict": True,
        "schema": schema,
    }


def build_discovery_prompt(seed: LocationSeed, max_candidates: int) -> list[dict]:
    location_label = ", ".join(part for part in [seed.city, seed.region, seed.country] if clean_text(part))
    query_hint = clean_text(seed.query_hint)
    hint_line = f"Disciplines to pay extra attention to: {query_hint}." if query_hint else ""
    system_prompt = (
        "You are building a high-quality directory of cycling clubs for women. "
        "Use web search to find organizations in the requested location. "
        "Return only real cycling clubs, cycling teams, recurring ride groups, or formal club programs. "
        "Exclude bike shops, retail stores, mechanics, individual coaches, personal athlete pages, race-result pages, "
        "event listings, general club directories, tourism pages, and one-off events. "
        "Mixed clubs are allowed only if they clearly have women's rides, women's teams, or women's development programs. "
        "Prefer official club websites; if there is no website, an official social profile is acceptable as the official source. "
        "The official_source_url must be a direct official page for the club or program, not a directory page, article, or search result. "
        "Write the summary in fresh original wording, not copied website text. "
        "Make it lively and inviting, but strictly factual and source-backed. "
        "The summary should be 2 to 4 sentences and should mention the club's women relevance plus ride style, mission, history, or community focus when supported by sources. "
        "If you are not confident, omit the candidate."
    )
    user_prompt = (
        f"Find up to {max_candidates} cycling clubs relevant to women in {location_label}. "
        f"{hint_line} "
        "For each candidate, search the live web and return structured data only. "
        "Each returned candidate must satisfy all of these rules: "
        "1) it is a real organization or recurring group, "
        "2) it is about cycling, "
        "3) it is in this locality or clearly serves it, "
        "4) it is women-only or clearly has a women's program/ride stream. "
        "Include source URLs that support the classification. "
        "For the summary, avoid generic titles like 'Home' or pasted FAQ text."
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def build_verification_prompt(candidate: Candidate, seed: LocationSeed) -> list[dict]:
    location_label = ", ".join(part for part in [seed.city, seed.region, seed.country] if clean_text(part))
    official_presence = candidate.official_website or candidate.official_source_url
    system_prompt = (
        "You are verifying whether a discovery candidate belongs in a directory of cycling clubs for women. "
        "Use the provided official domain or official page only. "
        "Reject the candidate if the official presence is actually a shop, an individual, a general directory, "
        "or an event page instead of a real club or recurring program. "
        "Rewrite the summary in fresh editorial wording that is fun to read, but do not invent anything."
    )
    user_prompt = (
        f"Verify this candidate for {location_label}: "
        f"club_name={candidate.club_name}; official_presence={official_presence}; "
        f"current_audience={candidate.audience}; current_disciplines={', '.join(candidate.disciplines)}. "
        "Use web search restricted to the official domain/page and return structured JSON. "
        "Accept only if the official presence supports that this is a real cycling club, team, recurring ride group, or program relevant to women. "
        "The summary should be 2 to 4 sentences, mention the women focus or women-relevant program, and highlight ride style, mission, history, or community feel when supported."
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def build_discovery_payload(seed: LocationSeed, args: argparse.Namespace) -> dict:
    return {
        "model": args.discovery_model,
        "reasoning": {"effort": args.reasoning_effort},
        "max_output_tokens": args.discovery_max_output_tokens,
        "tools": [
            {
                "type": "web_search",
                "user_location": discovery_user_location(seed),
            }
        ],
        "tool_choice": "auto",
        "include": ["web_search_call.action.sources"],
        "text": {"format": json_schema_format("cycling_club_candidates", DISCOVERY_SCHEMA)},
        "input": build_discovery_prompt(seed, args.max_candidates_per_location),
    }


def build_verification_payload(candidate: Candidate, seed: LocationSeed, args: argparse.Namespace) -> dict:
    allowed_domains = []
    official_presence = candidate.official_website or candidate.official_source_url
    parsed = normalize_url(official_presence)
    if parsed:
        host = parsed.split("://", 1)[1].split("/", 1)[0]
        if host:
            allowed_domains.append(host)

    tool = {
        "type": "web_search",
        "user_location": discovery_user_location(seed),
    }
    if allowed_domains:
        tool["filters"] = {"allowed_domains": allowed_domains}

    return {
        "model": args.verification_model,
        "reasoning": {"effort": args.reasoning_effort},
        "max_output_tokens": args.verification_max_output_tokens,
        "tools": [tool],
        "tool_choice": "auto",
        "include": ["web_search_call.action.sources"],
        "text": {"format": json_schema_format("cycling_club_verification", VERIFICATION_SCHEMA)},
        "input": build_verification_prompt(candidate, seed),
    }


def parse_retry_after_seconds(error_text: str) -> float | None:
    match = re.search(r"Please try again in ([0-9.]+)s", error_text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def response_status(response: dict) -> str:
    return clean_text(str(response.get("status", ""))).lower()


def incomplete_reason(response: dict) -> str:
    details = response.get("incomplete_details") or {}
    if not isinstance(details, dict):
        return ""
    return clean_text(str(details.get("reason", ""))).lower()


def refusal_text(response: dict) -> str:
    refusals: list[str] = []
    for item in response.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if content.get("type") == "refusal":
                refusal = clean_text(content.get("refusal", ""))
                if refusal:
                    refusals.append(refusal)
    return " ".join(refusals)


def output_content_types(response: dict) -> list[str]:
    types: list[str] = []
    for item in response.get("output", []) or []:
        item_type = clean_text(item.get("type", ""))
        if item_type:
            types.append(item_type)
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            content_type = clean_text(content.get("type", ""))
            if content_type:
                types.append(f"message:{content_type}")
    return types


def openai_request(api_key: str, payload: dict, timeout: float) -> dict:
    request = Request(
        OPENAI_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read().decode(charset, errors="replace")
            return json.loads(body)
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        error_text = clean_text(error_body[:800])
        retry_after_header = clean_text(exc.headers.get("retry-after", "")) if exc.headers else ""
        retry_after_seconds = None
        if retry_after_header:
            try:
                retry_after_seconds = float(retry_after_header)
            except ValueError:
                retry_after_seconds = None
        retry_after_seconds = retry_after_seconds or parse_retry_after_seconds(error_text)
        if exc.code == 429:
            raise OpenAIRateLimitError(
                f"OpenAI API error {exc.code}: {error_text}",
                retry_after_seconds=retry_after_seconds,
            ) from exc
        raise OpenAIResponseError(f"OpenAI API error {exc.code}: {error_text}") from exc
    except socket.timeout as exc:
        raise RetryableOpenAITimeoutError("The OpenAI request timed out while waiting for a response.") from exc
    except TimeoutError as exc:
        raise RetryableOpenAITimeoutError("The OpenAI request timed out while waiting for a response.") from exc
    except URLError as exc:
        reason = clean_text(str(getattr(exc, "reason", exc)))
        if "timed out" in reason.lower():
            raise RetryableOpenAITimeoutError(
                f"The OpenAI request timed out while waiting for a response: {reason}"
            ) from exc
        raise OpenAIResponseError(f"OpenAI request failed: {reason}") from exc
    except json.JSONDecodeError as exc:
        raise OpenAIResponseError(f"OpenAI API returned non-JSON content: {exc}") from exc


def expanded_output_token_cap(payload: dict) -> int:
    current = int(payload.get("max_output_tokens", 0) or 0)
    model = clean_text(str(payload.get("model", ""))).lower()
    hard_cap = MAX_DISCOVERY_OUTPUT_TOKENS
    if current <= DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS or "verification" in model:
        hard_cap = MAX_VERIFICATION_OUTPUT_TOKENS
    if current >= hard_cap:
        return current
    if current <= 0:
        return hard_cap
    return min(hard_cap, max(current + 400, int(current * 1.5)))


def openai_request_with_retry(
    api_key: str,
    payload: dict,
    timeout: float,
    *,
    max_retries: int,
    retry_buffer_seconds: float,
    response_parser: Callable[[dict], object] | None = None,
) -> object:
    import time

    attempt = 0
    attempt_payload = dict(payload)
    while True:
        try:
            response = openai_request(api_key, attempt_payload, timeout)
            if response_parser is None:
                return response
            return response_parser(response)
        except OpenAIRateLimitError as exc:
            if attempt >= max_retries:
                raise
            suggested_wait = exc.retry_after_seconds if exc.retry_after_seconds is not None else min(30.0, 2 ** attempt)
            jitter = random.uniform(0.0, 0.75)
            sleep_seconds = max(0.0, suggested_wait + retry_buffer_seconds + jitter)
            print(
                f"Rate limit hit. Waiting {sleep_seconds:.1f}s before retry {attempt + 1}/{max_retries}...",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
            attempt += 1
        except RetryableOpenAIResponseError as exc:
            if attempt >= max_retries:
                raise
            if exc.increase_output_tokens:
                expanded = expanded_output_token_cap(attempt_payload)
                if expanded > int(attempt_payload.get("max_output_tokens", 0) or 0):
                    attempt_payload["max_output_tokens"] = expanded
            sleep_seconds = max(0.0, retry_buffer_seconds + random.uniform(0.25, 1.0))
            print(
                f"Retrying after unusable OpenAI response ({exc}). Attempt {attempt + 1}/{max_retries}...",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
            attempt += 1


def extract_output_text(response: dict) -> str:
    if response_status(response) == "incomplete":
        reason = incomplete_reason(response) or "unknown"
        raise RetryableOpenAIResponseError(
            f"OpenAI response incomplete (reason={reason})",
            increase_output_tokens=(reason == "max_output_tokens"),
        )

    top_level_output_text = response.get("output_text")
    if isinstance(top_level_output_text, str) and clean_text(top_level_output_text):
        return top_level_output_text

    texts: list[str] = []
    output = response.get("output", [])
    for item in output:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                text = content.get("text", "")
                if clean_text(text):
                    texts.append(text)

    if texts:
        return "".join(texts)

    refusal = refusal_text(response)
    if refusal:
        raise OpenAIResponseError(f"Model refusal: {refusal}")

    raise RetryableOpenAIResponseError(
        "No output_text item found in OpenAI response. "
        f"status={response_status(response) or 'unknown'}; "
        f"content_types={', '.join(output_content_types(response)) or '<none>'}",
    )


def parse_json_output(text: str, *, response: dict) -> dict:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        status = response_status(response)
        reason = incomplete_reason(response)
        snippet = clean_text(text[:240])
        raise RetryableOpenAIResponseError(
            "OpenAI returned invalid structured JSON "
            f"(status={status or 'unknown'}"
            f"{'; reason=' + reason if reason else ''}): "
            f"{snippet or '<empty output>'}",
            increase_output_tokens=(status == "incomplete" or reason == "max_output_tokens"),
        ) from exc
    if not isinstance(parsed, dict):
        raise OpenAIResponseError("OpenAI structured output was not a JSON object.")
    return parsed


def collect_source_urls(response: dict) -> list[str]:
    urls: list[str] = []
    for item in response.get("output", []):
        if item.get("type") == "web_search_call":
            action = item.get("action") or {}
            for source in action.get("sources", []) or []:
                if isinstance(source, dict):
                    url = normalize_url(source.get("url", ""))
                    if url:
                        urls.append(url)
    return list(dict.fromkeys(urls))


def parse_discovery_response(response: dict) -> tuple[list[Candidate], list[str]]:
    text = extract_output_text(response)
    payload = parse_json_output(text, response=response)
    candidates = [
        Candidate(
            club_name=clean_text(item["club_name"]),
            official_website=normalize_url(item["official_website"]),
            official_source_url=normalize_url(item["official_source_url"]),
            supporting_source_urls=[normalize_url(url) for url in item["supporting_source_urls"] if clean_text(url)],
            audience=item["audience"],
            disciplines=[clean_text(value) for value in item["disciplines"] if clean_text(value)],
            city=clean_text(item["city"]),
            region=clean_text(item["region"]),
            country=clean_text(item["country"]),
            summary=clean_text(item["summary"]),
            why_it_qualifies=clean_text(item["why_it_qualifies"]),
            classification=item["classification"],
            is_real_cycling_club=bool(item["is_real_cycling_club"]),
            is_women_relevant=bool(item["is_women_relevant"]),
            is_shop=bool(item["is_shop"]),
            is_individual=bool(item["is_individual"]),
            is_directory=bool(item["is_directory"]),
            confidence=float(item["confidence"]),
        )
        for item in payload.get("candidates", [])
    ]
    return candidates, collect_source_urls(response)


def parse_verification_response(response: dict) -> dict:
    text = extract_output_text(response)
    return parse_json_output(text, response=response)


def official_presence(candidate: Candidate) -> str:
    return candidate.official_website or candidate.official_source_url


def html_like_url(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    path = normalized.split("://", 1)[1].split("?", 1)[0]
    return not path.lower().endswith(NON_HTML_PATH_SUFFIXES)


def preferred_official_website(candidate: Candidate) -> str:
    for url in [candidate.official_website, candidate.official_source_url]:
        normalized = normalize_url(url)
        if normalized and html_like_url(normalized) and base_domain(normalized) not in NEGATIVE_OFFICIAL_HOSTS:
            homepage_urls = homepage_candidate_urls(normalized)
            return homepage_urls[0] if homepage_urls else normalized

    candidate_domain = base_domain(candidate.official_website or candidate.official_source_url)
    same_domain_urls: list[str] = []
    for raw_url in candidate.supporting_source_urls:
        url = normalize_url(raw_url)
        if not url or not html_like_url(url):
            continue
        if candidate_domain and base_domain(url) != candidate_domain:
            continue
        same_domain_urls.append(url)
    same_domain_urls = list(dict.fromkeys(same_domain_urls))
    same_domain_urls.sort(key=lambda value: (value.count("/"), len(value), value))
    if same_domain_urls:
        homepage_urls = homepage_candidate_urls(same_domain_urls[0])
        return homepage_urls[0] if homepage_urls else same_domain_urls[0]
    fallback = normalize_url(candidate.official_website or candidate.official_source_url)
    homepage_urls = homepage_candidate_urls(fallback)
    return homepage_urls[0] if homepage_urls else fallback


def official_host(candidate: Candidate) -> str:
    url = official_presence(candidate)
    if not url:
        return ""
    parsed = normalize_url(url)
    if not parsed:
        return ""
    return parsed.split("://", 1)[1].split("/", 1)[0].lower().removeprefix("www.")


def candidate_is_acceptable(candidate: Candidate, min_confidence: float = DEFAULT_MIN_CONFIDENCE) -> bool:
    if not candidate.is_real_cycling_club:
        return False
    if not candidate.is_women_relevant:
        return False
    if candidate.is_shop or candidate.is_individual or candidate.is_directory:
        return False
    if candidate.classification == "unknown":
        return False
    if candidate.audience == "Unknown":
        return False
    if candidate.confidence < min_confidence:
        return False
    if not official_presence(candidate):
        return False
    if official_host(candidate) in NEGATIVE_OFFICIAL_HOSTS:
        return False
    return True


def seed_metadata_from_candidate(candidate: Candidate, source_urls: Iterable[str]) -> dict[str, str]:
    metadata = empty_metadata()
    official_presence = candidate.official_website or candidate.official_source_url
    preferred_website = preferred_official_website(candidate)
    metadata.update(
        {
            "title": candidate.club_name,
            "description": clean_summary_text(candidate.summary),
            "canonical_url": official_presence,
            "site_url": preferred_website,
            "site_name": candidate.club_name,
            "street": "",
            "city": candidate.city,
            "region": candidate.region,
            "country": candidate.country,
            "postal_code": "",
            "latitude": "",
            "longitude": "",
            "image_rights_status": "",
            "logo_rights_status": "",
        }
    )
    if not metadata["canonical_url"]:
        metadata["canonical_url"] = next((url for url in source_urls if url), "")
    return metadata


def apply_verification(candidate: Candidate, verification: dict) -> Candidate | None:
    if not verification.get("accept"):
        return None
    audience = verification.get("audience", candidate.audience)
    disciplines = [clean_text(value) for value in verification.get("disciplines", candidate.disciplines) if clean_text(value)]
    summary = clean_summary_text(verification.get("summary", candidate.summary))
    why_it_qualifies = clean_text(verification.get("why_it_qualifies", candidate.why_it_qualifies))
    confidence = float(verification.get("confidence", candidate.confidence))
    return Candidate(
        club_name=candidate.club_name,
        official_website=candidate.official_website,
        official_source_url=candidate.official_source_url,
        supporting_source_urls=candidate.supporting_source_urls,
        audience=audience,
        disciplines=disciplines,
        city=candidate.city,
        region=candidate.region,
        country=candidate.country,
        summary=summary,
        why_it_qualifies=why_it_qualifies,
        classification=candidate.classification,
        is_real_cycling_club=True,
        is_women_relevant=True,
        is_shop=False,
        is_individual=False,
        is_directory=False,
        confidence=confidence,
    )


def candidate_to_search_result(candidate: Candidate, source_urls: Iterable[str], seed: LocationSeed) -> SearchResult:
    metadata = seed_metadata_from_candidate(candidate, source_urls)
    metadata["description"] = clean_summary_text(candidate.summary)
    official_presence = preferred_official_website(candidate) or candidate.official_source_url or candidate.official_website
    return SearchResult(
        query=f"openai web search discovery for {seed.city}, {seed.region or seed.country}",
        title=candidate.club_name,
        link=official_presence,
        snippet=candidate.summary,
        source_type="openai-web-search",
        source_url=official_presence,
        seed_metadata=metadata,
    )


def candidate_enrichment_urls(candidate: Candidate) -> list[str]:
    urls: list[str] = []
    homepage_seed_urls: list[str] = []
    for value in (preferred_official_website(candidate), candidate.official_source_url, candidate.official_website):
        url = normalize_url(value)
        if not url:
            continue
        homepage_seed_urls.extend(homepage_candidate_urls(url))

    for url in unique_preserve_order(homepage_seed_urls):
        if url and url not in urls:
            urls.append(url)

    official_domain = base_domain(candidate.official_website or candidate.official_source_url)
    if official_domain:
        for value in candidate.supporting_source_urls:
            url = normalize_url(value)
            if not url or url in urls:
                continue
            if base_domain(url) == official_domain:
                for homepage_url in homepage_candidate_urls(url):
                    if homepage_url not in urls:
                        urls.append(homepage_url)
                if url not in urls:
                    urls.append(url)
            if len(urls) >= 6:
                break
    return urls


def supporting_urls_for_logo(candidate: Candidate) -> list[str]:
    urls: list[str] = []
    for value in candidate.supporting_source_urls:
        url = normalize_url(value)
        if url and url not in urls:
            urls.append(url)
    return urls


def enrich_candidate_metadata(candidate: Candidate, result: SearchResult, timeout: float, pause_seconds: float) -> dict[str, str]:
    import time

    metadata = dict(result.seed_metadata or empty_metadata())
    primary_site = normalize_url(preferred_official_website(candidate))
    for fetch_target in candidate_enrichment_urls(candidate):
        try:
            html = fetch_text(fetch_target, timeout=timeout, user_agent="CFW-Clubs-OpenAI-Discovery/0.1")
            page_metadata = extract_metadata_with_asset_fallback(
                html,
                fetch_target,
                timeout=timeout,
                user_agent="CFW-Clubs-OpenAI-Discovery/0.1",
            )
        except Exception as exc:
            print(f"Warning: failed to enrich {fetch_target}: {exc}", file=sys.stderr)
            continue

        existing_logo = metadata.get("logo_url", "")
        existing_cover = metadata.get("cover_url", "")
        existing_gallery = metadata.get("gallery_urls", "")
        existing_site_url = metadata.get("site_url", "")
        metadata = merge_metadata(metadata, page_metadata)
        if existing_logo and fetch_target != primary_site:
            metadata["logo_url"] = existing_logo
        if existing_site_url and fetch_target != primary_site:
            metadata["site_url"] = existing_site_url
        if existing_cover and fetch_target != primary_site:
            new_cover = normalize_url(metadata.get("cover_url", ""))
            merged_gallery = split_metadata_urls(existing_gallery)
            if new_cover and new_cover != normalize_url(existing_cover):
                merged_gallery.append(new_cover)
            merged_gallery.extend(split_metadata_urls(metadata.get("gallery_urls", "")))
            metadata["cover_url"] = existing_cover
            metadata["gallery_urls"] = join_metadata_urls(unique_preserve_order(merged_gallery))
        time.sleep(max(0.0, pause_seconds))

    metadata = apply_social_logo_fallback(
        metadata,
        timeout=timeout,
        user_agent="CFW-Clubs-OpenAI-Discovery/0.1",
        extra_urls=supporting_urls_for_logo(candidate),
    )
    return metadata


def load_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def row_identity_keys(row: dict[str, str]) -> set[str]:
    keys = {
        clean_text(row.get("external_id", "")),
        link_key(row.get("website", "")),
        link_key(row.get("facebook_url", "")),
        link_key(row.get("instagram_url", "")),
        link_key(row.get("twitter_url", "")),
    }
    return {key for key in keys if key}


def drop_conflicting_rows(
    rows: list[dict[str, str]],
    *,
    keys: set[str],
    external_id: str,
) -> list[dict[str, str]]:
    cleaned_external_id = clean_text(external_id)
    kept_rows: list[dict[str, str]] = []
    for row in rows:
        row_keys = row_identity_keys(row)
        row_external_id = clean_text(row.get("external_id", ""))
        if cleaned_external_id and row_external_id == cleaned_external_id:
            continue
        if keys and row_keys & keys:
            continue
        kept_rows.append(row)
    return kept_rows


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print(
            "Missing OpenAI credentials. Set --api-key or the OPENAI_API_KEY environment variable.",
            file=sys.stderr,
        )
        return 1

    import time

    seeds = load_location_seeds(args.locations_csv)
    if args.max_locations > 0:
        seeds = seeds[: args.max_locations]
    current_rows = load_existing_rows(args.staging_csv)
    current_key_set = existing_keys(current_rows)
    discovered_rows: list[dict[str, str]] = []
    now = __import__("datetime").datetime.now()

    for seed in seeds:
        try:
            candidates, response_sources = openai_request_with_retry(
                args.api_key,
                build_discovery_payload(seed, args),
                args.timeout,
                max_retries=args.max_retries,
                retry_buffer_seconds=args.retry_buffer_seconds,
                response_parser=parse_discovery_response,
            )
        except Exception as exc:
            print(f"Warning: OpenAI discovery failed for {seed.city}, {seed.region}: {exc}", file=sys.stderr)
            continue

        accepted_candidates: list[tuple[Candidate, list[str]]] = []
        for candidate in candidates:
            if not candidate_is_acceptable(candidate, args.min_confidence):
                continue

            supporting_urls = list(dict.fromkeys(
                [candidate.official_source_url, candidate.official_website, *candidate.supporting_source_urls, *response_sources]
            ))

            if not args.skip_verification:
                try:
                    verification = openai_request_with_retry(
                        args.api_key,
                        build_verification_payload(candidate, seed, args),
                        args.timeout,
                        max_retries=args.max_retries,
                        retry_buffer_seconds=args.retry_buffer_seconds,
                        response_parser=parse_verification_response,
                    )
                    verified = apply_verification(candidate, verification)
                    if verified is None:
                        continue
                    candidate = verified
                    if not candidate_is_acceptable(candidate, args.min_confidence):
                        continue
                except Exception as exc:
                    print(
                        f"Warning: OpenAI verification failed for {candidate.club_name} in {seed.city}: {exc}",
                        file=sys.stderr,
                    )
                    continue

            accepted_candidates.append((candidate, supporting_urls))

        for candidate, supporting_urls in accepted_candidates:
            result = candidate_to_search_result(candidate, supporting_urls, seed)
            metadata = enrich_candidate_metadata(candidate, result, args.timeout, args.pause_seconds)

            row = build_staging_row(
                seed=seed,
                result=result,
                metadata=metadata,
                score=max(0.0, min(candidate.confidence, 1.0)),
                discovered_at=now,
            )
            row["summary_final"] = clean_summary_text(candidate.summary)
            row["source_name"] = f"openai web search discovery for {seed.city}, {seed.region or seed.country}"
            row["source_url"] = candidate.official_source_url or candidate.official_website or next((url for url in supporting_urls if url), "")
            row["notes_internal"] = clean_text(
                " | ".join(
                    part
                    for part in [
                        row.get("notes_internal", ""),
                        f"classification={candidate.classification}",
                        f"ai_confidence={candidate.confidence:.2f}",
                        f"official_source={candidate.official_source_url}",
                        f"supporting_sources={'; '.join(url for url in supporting_urls[:5] if url)}",
                    ]
                    if clean_text(part)
                )
            )

            keys = row_identity_keys(row)
            if keys & current_key_set and not args.replace_existing:
                continue

            if args.replace_existing:
                current_rows = drop_conflicting_rows(
                    current_rows,
                    keys=keys,
                    external_id=row.get("external_id", ""),
                )
                discovered_rows = drop_conflicting_rows(
                    discovered_rows,
                    keys=keys,
                    external_id=row.get("external_id", ""),
                )
                current_key_set = existing_keys(current_rows + discovered_rows)

            discovered_rows.append(row)
            current_key_set.update(keys)

    all_rows = current_rows + discovered_rows
    write_staging_rows(args.staging_csv, all_rows)
    print(
        f"Added {len(discovered_rows)} OpenAI-discovered candidates across {len(seeds)} locations "
        f"to {args.staging_csv}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
