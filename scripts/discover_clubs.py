#!/usr/bin/env python3
"""Discover cycling clubs by location and write candidates into the staging CSV."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


STAGING_FIELDNAMES = [
    "external_id",
    "existing_gd_id",
    "source_type",
    "source_name",
    "source_url",
    "discovered_at",
    "last_checked",
    "review_status",
    "confidence_score",
    "club_name",
    "summary_raw",
    "summary_final",
    "audience",
    "discipline_primary",
    "disciplines_csv",
    "website",
    "facebook_url",
    "instagram_url",
    "twitter_url",
    "twitter_handle",
    "youtube_url",
    "strava_url",
    "email",
    "street",
    "street2",
    "city",
    "region",
    "country",
    "postal_code",
    "plus_code",
    "latitude",
    "longitude",
    "logo_source_url",
    "logo_rights_status",
    "cover_source_url",
    "gallery_source_urls",
    "image_rights_status",
    "image_notes",
    "focus",
    "notes_internal",
]

DEFAULT_QUERY_TEMPLATES = [
    'women cycling club "{city}" "{region}"',
    '"women\'s cycling club" "{city}" "{region}"',
    '"cycling club" "{city}" "{region}" women',
    '"women mountain bike" club "{city}" "{region}"',
]

DEFAULT_PLACE_QUERIES = [
    "cycling club",
    "bicycle club",
    "cycling team",
    "women cycling",
]

POSITIVE_KEYWORDS = {
    "cycling": 0.18,
    "bicycle": 0.16,
    "bike": 0.14,
    "velo": 0.12,
    "club": 0.16,
    "team": 0.08,
    "ride": 0.07,
    "racing": 0.07,
    "women": 0.22,
    "women's": 0.22,
    "womens": 0.22,
    "girls": 0.18,
    "ladies": 0.18,
    "mtb": 0.12,
    "road": 0.10,
    "gravel": 0.10,
    "track": 0.10,
    "touring": 0.10,
    "cyclocross": 0.10,
    "cyclo-cross": 0.10,
}

NEGATIVE_KEYWORDS = {
    "shop": -0.35,
    "store": -0.35,
    "repair": -0.30,
    "rental": -0.30,
    "eventbrite": -0.30,
    "hotel": -0.30,
    "tourism": -0.25,
    "news": -0.20,
    "results": -0.20,
    "magazine": -0.20,
    "podcast": -0.15,
}

DISCIPLINE_KEYWORDS = {
    "MTB": ["mtb", "mountain bike", "mountain biking", "trail ride"],
    "Road": ["road", "road cycling", "road bike", "road ride", "criterium", "road race"],
    "Gravel": ["gravel"],
    "MTB XC": ["xc", "cross-country"],
    "Track": ["track", "velodrome"],
    "Touring": ["touring", "bikepacking", "bicycle touring"],
    "Cyclo-cross": ["cyclocross", "cyclo-cross", "cyclo cross", "cx"],
}

WOMEN_ONLY_PATTERNS = [
    "women only",
    "for women",
    "women's cycling club",
    "womens cycling club",
    "ladies ride",
    "girls ride",
]

MIXED_PATTERNS = [
    "inclusive",
    "all genders",
    "mixed",
    "co-ed",
    "coed",
    "women's ride",
    "womens ride",
    "women's team",
    "womens team",
]

SOCIAL_HOSTS = {
    "facebook.com": "facebook_url",
    "instagram.com": "instagram_url",
    "twitter.com": "twitter_url",
    "x.com": "twitter_url",
    "tiktok.com": "tiktok_url",
    "youtube.com": "youtube_url",
    "youtu.be": "youtube_url",
    "strava.com": "strava_url",
}

SOCIAL_LOGO_PRIORITY = [
    "instagram_url",
    "strava_url",
    "facebook_url",
    "twitter_url",
    "tiktok_url",
    "youtube_url",
]

GENERIC_NAME_CANDIDATES = {
    "home",
    "homepage",
    "welcome",
    "index",
    "main",
    "about",
    "about us",
    "default",
}

IMAGE_LOGO_HINTS = {
    "logo",
    "wordmark",
    "brand",
    "masthead",
    "emblem",
    "corporate logo",
}

IMAGE_NON_PHOTO_HINTS = {
    "icon",
    "favicon",
    "avatar",
    "gravatar",
    "badge",
    "poster",
    "flyer",
    "sponsor",
    "sponsors",
    "partner",
    "partners",
    "app store",
    "google play",
    "collection",
    "collections",
    "map",
    "radar",
    "weather",
    "preview",
    "profile pic",
    "profilepic",
    "youtube profile",
    "initials",
}

IMAGE_PHOTO_HINTS = {
    "ride",
    "rides",
    "riding",
    "team",
    "gravel",
    "road",
    "mountain",
    "mtb",
    "women",
    "womens",
    "club",
    "clinic",
    "event",
    "race",
    "photo",
    "cover",
    "homepage",
    "banner",
    "hero",
    "action",
    "chapter",
    "program",
    "tour",
}

MIN_PHOTO_CANDIDATE_SCORE = 1.0
MAX_SHELL_ASSET_FETCHES = 6
MAX_MANIFEST_ASSET_FETCHES = 8

IMAGE_ASSET_EXTENSION_PATTERN = re.compile(
    r"\.(?:png|jpe?g|webp|svg)(?:[?#][^\"')\s]+)?$",
    re.IGNORECASE,
)

KNOWN_ASSET_CDN_HOSTS = {
    "static.wixstatic.com",
    "static.parastorage.com",
    "images.squarespace-cdn.com",
    "images.squarespace.com",
    "images.ctfassets.net",
    "cdn.shopify.com",
}

SITE_ONLY_NEGATIVE_HOSTS = {
    "eventbrite.com",
    "youtube.com",
    "youtu.be",
    "linkedin.com",
}

HOME_LIKE_PATHS = {
    "/",
    "/home",
    "/home/",
    "/home.aspx",
    "/default.aspx",
    "/index.html",
    "/index.htm",
}

COUNTRY_NAME_TO_CODE = {
    "canada": "CA",
    "united states": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "great britain": "GB",
    "australia": "AU",
    "new zealand": "NZ",
    "ireland": "IE",
}

COUNTRY_CODE_TO_NAME = {
    "CA": "Canada",
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "NZ": "New Zealand",
    "IE": "Ireland",
}


class DiscoveryError(Exception):
    """Raised when a discovery step cannot continue safely."""


class ApiResponseError(DiscoveryError):
    """Raised when an upstream API response cannot be parsed."""


@dataclass
class LocationSeed:
    location_id: str
    location_name: str
    city: str
    region: str
    country: str
    postal_code: str
    latitude: str
    longitude: str
    query_hint: str


@dataclass
class SearchResult:
    query: str
    title: str
    link: str
    snippet: str
    source_type: str = "brave-web-search"
    source_url: str = ""
    seed_metadata: dict[str, str] | None = None


class MetadataHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.meta: dict[str, str] = {}
        self.links: dict[str, str] = {}
        self.stylesheet_hrefs: list[str] = []
        self.script_srcs: list[str] = []
        self.anchors: list[str] = []
        self.images: list[dict[str, str]] = []
        self.element_ids: list[str] = []
        self.title_parts: list[str] = []
        self._in_title = False
        self._in_jsonld = False
        self._jsonld_chunks: list[str] = []
        self.jsonld_blocks: list[str] = []
        self._in_noscript = False
        self.noscript_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map: dict[str, str] = {}
        for key, value in attrs:
            normalized_key = key.lower()
            normalized_value = value or ""
            existing_value = attr_map.get(normalized_key, "")
            if existing_value and not normalized_value:
                continue
            if normalized_value or not existing_value:
                attr_map[normalized_key] = normalized_value
        element_id = clean_text(attr_map.get("id", ""))
        if element_id:
            self.element_ids.append(element_id)
        if tag == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").lower()
            content = attr_map.get("content", "")
            if key and content:
                self.meta[key] = content
        elif tag == "link":
            rel = (attr_map.get("rel") or "").lower()
            href = attr_map.get("href", "")
            if rel and href:
                self.links[rel] = href
                if "stylesheet" in rel:
                    self.stylesheet_hrefs.append(href)
        elif tag == "a":
            href = attr_map.get("href", "")
            if href:
                self.anchors.append(href)
        elif tag == "img":
            self.images.append(attr_map)
        elif tag == "title":
            self._in_title = True
        elif tag == "noscript":
            self._in_noscript = True
        elif tag == "script":
            script_src = clean_text(attr_map.get("src", ""))
            if script_src:
                self.script_srcs.append(script_src)
            script_type = (attr_map.get("type") or "").lower()
            if "ld+json" in script_type:
                self._in_jsonld = True
                self._jsonld_chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        elif tag == "noscript":
            self._in_noscript = False
        elif tag == "script" and self._in_jsonld:
            self._in_jsonld = False
            block = "".join(self._jsonld_chunks).strip()
            if block:
                self.jsonld_blocks.append(block)
            self._jsonld_chunks = []

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._in_jsonld:
            self._jsonld_chunks.append(data)
        if self._in_noscript:
            self.noscript_parts.append(data)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def slugify(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def normalize_url(value: str) -> str:
    value = clean_text(unescape(value or ""))
    if not value:
        return ""
    value = value.split("|", 1)[0].strip()
    if value.startswith(("http://", "https://", "mailto:")):
        return value
    if value.startswith("//"):
        return f"https:{value}"
    return f"https://{value}"


def domain_host(url: str) -> str:
    try:
        host = urlparse(normalize_url(url)).netloc.lower()
    except ValueError:
        return ""
    return host.replace("www.", "")


def base_domain(url: str) -> str:
    host = domain_host(url)
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def unique_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


def normalize_social_url(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def empty_metadata() -> dict[str, str]:
    return {
        "title": "",
        "description": "",
        "canonical_url": "",
        "site_url": "",
        "site_name": "",
        "logo_url": "",
    "cover_url": "",
    "gallery_urls": "",
    "facebook_url": "",
    "instagram_url": "",
    "twitter_url": "",
    "tiktok_url": "",
    "youtube_url": "",
    "strava_url": "",
        "email": "",
        "street": "",
        "city": "",
        "region": "",
        "country": "",
        "postal_code": "",
        "latitude": "",
        "longitude": "",
        "logo_rights_status": "",
        "image_rights_status": "",
    }


def merge_metadata(base: dict[str, str], overlay: dict[str, str]) -> dict[str, str]:
    merged = dict(base)
    for key, value in overlay.items():
        if key == "gallery_urls":
            base_urls = split_metadata_urls(merged.get(key, ""))
            overlay_urls = split_metadata_urls(value)
            merged[key] = join_metadata_urls(unique_preserve_order(base_urls + overlay_urls))
            continue
        if clean_text(value):
            merged[key] = value
    return merged


def social_field_for_url(url: str) -> str:
    host = domain_host(url)
    return SOCIAL_HOSTS.get(host, "")


def country_name_from_code(code: str) -> str:
    return COUNTRY_CODE_TO_NAME.get(clean_text(code).upper(), "")


def link_key(url: str) -> str:
    host = domain_host(url)
    if host in SOCIAL_HOSTS:
        return normalize_social_url(url)
    return base_domain(url)


def candidate_identity_key(result: SearchResult, seed: LocationSeed) -> str:
    for url in (result.link, result.source_url):
        key = link_key(url)
        if key:
            return key
    return slugify(f"{result.title}-{seed.city}-{seed.region or seed.country}")


def build_queries(seed: LocationSeed, query_templates: list[str]) -> list[str]:
    region = seed.region or seed.country
    tokens = {
        "location_name": seed.location_name or seed.city,
        "city": seed.city,
        "region": region,
        "country": seed.country,
        "query_hint": seed.query_hint,
    }
    queries = []
    for template in query_templates:
        query = clean_text(template.format(**tokens))
        if seed.query_hint:
            query = clean_text(f"{query} {seed.query_hint}")
        queries.append(query)
    return unique_preserve_order(queries)


def place_location_string(seed: LocationSeed) -> str:
    parts = [seed.city]
    country_code_value = country_code(seed.country)
    if country_code_value == "US" and seed.region:
        parts.append(seed.region)
    if seed.country:
        parts.append(seed.country)
    return clean_text(" ".join(parts))


def build_place_queries(seed: LocationSeed, place_queries: list[str]) -> list[str]:
    queries = []
    for query in place_queries:
        combined = clean_text(query)
        if seed.query_hint:
            combined = clean_text(f"{combined} {seed.query_hint}")
        queries.append(combined)
    return unique_preserve_order(queries)


def score_result(seed: LocationSeed, result: SearchResult) -> float:
    text = " ".join([result.title, result.snippet, result.link]).lower()
    score = 0.0
    for keyword, weight in POSITIVE_KEYWORDS.items():
        if keyword in text:
            score += weight
    for keyword, weight in NEGATIVE_KEYWORDS.items():
        if keyword in text:
            score += weight
    if seed.city.lower() in text:
        score += 0.20
    if seed.region and seed.region.lower() in text:
        score += 0.12
    host = domain_host(result.link)
    if host in SITE_ONLY_NEGATIVE_HOSTS:
        score -= 0.15
    if social_field_for_url(result.link):
        score -= 0.05
    return max(0.0, min(score, 1.0))


def extract_json_objects(block: str) -> list[object]:
    block = block.strip()
    if not block:
        return []
    try:
        parsed = json.loads(block)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def iter_json_nodes(node: object) -> Iterable[dict]:
    if isinstance(node, dict):
        yield node
        if "@graph" in node and isinstance(node["@graph"], list):
            for child in node["@graph"]:
                yield from iter_json_nodes(child)
        for value in node.values():
            if isinstance(value, (dict, list)):
                yield from iter_json_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_json_nodes(item)


def pick_first(*values: str) -> str:
    for value in values:
        cleaned = clean_text(value)
        if cleaned:
            return cleaned
    return ""


def absolute_url(url: str, base_url: str) -> str:
    if not url:
        return ""
    return normalize_url(urljoin(base_url, url))


def homepage_candidate_urls(url: str) -> list[str]:
    normalized = normalize_url(url)
    if not normalized or normalized.startswith("mailto:"):
        return []

    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        return []

    root_url = f"{parsed.scheme}://{parsed.netloc}/"
    path_lower = (parsed.path or "/").lower()
    candidates: list[str] = []

    if path_lower.endswith(".aspx"):
        candidates.extend(
            [
                absolute_url("/Home.aspx", root_url),
                absolute_url("/Default.aspx", root_url),
                root_url,
            ]
        )
    else:
        candidates.append(root_url)

    if path_lower not in HOME_LIKE_PATHS:
        candidates.append(normalized)
    else:
        candidates.append(normalized)

    return unique_preserve_order(candidate for candidate in candidates if candidate)


def extract_social_links(links: Iterable[str]) -> dict[str, str]:
    output = {field: "" for field in SOCIAL_HOSTS.values()}
    for link in links:
        field = social_field_for_url(link)
        if field and not output[field]:
            output[field] = normalize_social_url(link)
    return output


def split_metadata_urls(value: str) -> list[str]:
    value = clean_text(value)
    if not value:
        return []
    return [normalize_url(part) for part in value.split("::") if clean_text(part)]


def join_metadata_urls(urls: Iterable[str]) -> str:
    return "::".join(unique_preserve_order(normalize_url(url) for url in urls if clean_text(url)))


def same_site_url(url: str, page_url: str) -> bool:
    url_host = domain_host(url)
    page_host = domain_host(page_url)
    if not url_host or not page_host:
        return False
    return url_host == page_host or base_domain(url) == base_domain(page_url)


def image_identity_key(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return f"{parsed.netloc.lower().removeprefix('www.')}{parsed.path}".lower()


def parse_image_dimensions(
    attrs: dict[str, str] | None = None,
    url: str = "",
) -> tuple[int, int]:
    attrs = attrs or {}
    width_text = clean_text(attrs.get("width", ""))
    height_text = clean_text(attrs.get("height", ""))
    dimensions_text = clean_text(attrs.get("data-image-dimensions", ""))
    if dimensions_text and "x" in dimensions_text.lower():
        width_text, height_text = dimensions_text.lower().split("x", 1)

    if not width_text and not height_text and url:
        query = urlparse(normalize_url(url)).query.lower()
        width_match = re.search(r"(?:^|[&?])(w|width)=([0-9]{2,5})", query)
        height_match = re.search(r"(?:^|[&?])(h|height)=([0-9]{2,5})", query)
        if width_match:
            width_text = width_match.group(2)
        if height_match:
            height_text = height_match.group(2)

    try:
        width_value = int(width_text or "0")
    except ValueError:
        width_value = 0
    try:
        height_value = int(height_text or "0")
    except ValueError:
        height_value = 0
    return width_value, height_value


def remember_image_dimensions(
    dimension_hints: dict[str, tuple[int, int]],
    url: str,
    attrs: dict[str, str] | None = None,
) -> tuple[int, int]:
    width_value, height_value = parse_image_dimensions(attrs, url)
    if width_value <= 0 or height_value <= 0:
        return width_value, height_value
    normalized = normalize_url(url)
    if normalized:
        dimension_hints[normalized] = (width_value, height_value)
    identity_key = image_identity_key(url)
    if identity_key:
        dimension_hints[identity_key] = (width_value, height_value)
    return width_value, height_value


def image_dimensions_for_candidate(
    url: str,
    attrs: dict[str, str] | None = None,
    dimension_hints: dict[str, tuple[int, int]] | None = None,
) -> tuple[int, int]:
    width_value, height_value = parse_image_dimensions(attrs, url)
    if width_value > 0 and height_value > 0:
        return width_value, height_value
    normalized = normalize_url(url)
    if dimension_hints:
        for key in (normalized, image_identity_key(url)):
            if key and key in dimension_hints:
                return dimension_hints[key]
    return width_value, height_value


def image_orientation_bucket(
    url: str,
    attrs: dict[str, str] | None = None,
    dimension_hints: dict[str, tuple[int, int]] | None = None,
) -> int:
    width_value, height_value = image_dimensions_for_candidate(url, attrs, dimension_hints)
    if width_value <= 0 or height_value <= 0:
        return 0
    aspect_ratio = width_value / height_value
    if aspect_ratio >= 1.35:
        return 2
    if aspect_ratio >= 1.15:
        return 1
    return 0


def image_signal_text(url: str, attrs: dict[str, str] | None = None) -> str:
    parsed = urlparse(normalize_url(url))
    parts = [parsed.path, parsed.query]
    if attrs:
        for field in (
            "alt",
            "class",
            "id",
            "src",
            "data-src",
            "data-image",
            "data-image-dimensions",
            "elementtiming",
            "style",
        ):
            parts.append(clean_text(attrs.get(field, "")))
    text = " ".join(parts)
    text = re.sub(r"grouprides", "group rides", text, flags=re.IGNORECASE)
    text = re.sub(r"groupride", "group ride", text, flags=re.IGNORECASE)
    text = re.sub(r"teamride", "team ride", text, flags=re.IGNORECASE)
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", text)
    text = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def image_signal_tokens(url: str, attrs: dict[str, str] | None = None) -> set[str]:
    return {token for token in image_signal_text(url, attrs).split(" ") if token}


def looks_like_logo_image(url: str, attrs: dict[str, str] | None = None) -> bool:
    signal_text = image_signal_text(url, attrs)
    signal_tokens = image_signal_tokens(url, attrs)
    if signal_tokens & IMAGE_LOGO_HINTS:
        return True
    if any(token in signal_text for token in IMAGE_LOGO_HINTS):
        return True
    parsed = urlparse(normalize_url(url))
    path = parsed.path.lower()
    return path.endswith(".svg")


def looks_like_non_photo_image(url: str, attrs: dict[str, str] | None = None) -> bool:
    signal_tokens = image_signal_tokens(url, attrs)
    if looks_like_logo_image(url, attrs):
        return True
    if signal_tokens & IMAGE_NON_PHOTO_HINTS:
        return True
    host = domain_host(url)
    if host in {"secure.gravatar.com", "gravatar.com"}:
        return True
    parsed = urlparse(normalize_url(url))
    return parsed.path.lower().endswith((".svg", ".ico"))


def photo_candidate_score(
    url: str,
    attrs: dict[str, str] | None = None,
    dimension_hints: dict[str, tuple[int, int]] | None = None,
) -> float:
    if looks_like_non_photo_image(url, attrs):
        return -5.0

    signal_text = image_signal_text(url, attrs)
    signal_tokens = {token for token in signal_text.split(" ") if token}
    parsed = urlparse(normalize_url(url))
    path = parsed.path.lower()
    score = 0.0

    if path.endswith((".jpg", ".jpeg", ".png", ".webp")):
        score += 0.6

    if signal_tokens & IMAGE_PHOTO_HINTS:
        score += 1.8

    if re.search(r"\bride(s|ing)?\b", signal_text):
        score += 0.8
    if re.search(r"\b(team|women|cyclists?)\b", signal_text):
        score += 0.4
    if re.search(r"\b(homepage|hero|banner)\b", signal_text):
        score += 0.6

    width_value, height_value = image_dimensions_for_candidate(url, attrs, dimension_hints)
    if width_value >= 400 or height_value >= 250:
        score += 0.8
    if width_value > 0 and height_value > 0:
        aspect_ratio = width_value / height_value
        if aspect_ratio >= 1.35:
            score += 0.8
        elif aspect_ratio >= 1.15:
            score += 0.4
        elif aspect_ratio < 0.95:
            score -= 0.2
        elif aspect_ratio < 1.05:
            score -= 0.05

    if attrs:
        if clean_text(attrs.get("data-stretch", "")).lower() == "true":
            score += 0.5

    query = parsed.query.lower()
    size_match = re.search(r"(?:^|[&?])(w|width|h|height)=([0-9]{2,4})", query)
    if size_match:
        try:
            size_value = int(size_match.group(2))
        except ValueError:
            size_value = 0
        if size_value >= 500:
            score += 0.4
        elif 0 < size_value <= 240:
            score -= 0.3

    return score


def best_logo_url(urls: Iterable[str]) -> str:
    candidates: list[tuple[float, str]] = []
    for url in urls:
        normalized = normalize_url(url)
        if not normalized:
            continue
        score = 0.0
        if looks_like_logo_image(normalized):
            score += 5.0
        if normalized.lower().endswith(".svg"):
            score += 0.8
        if score > 0:
            candidates.append((score, normalized))

    if not candidates:
        return ""
    candidates.sort(key=lambda item: (-item[0], item[1]))
    return candidates[0][1]


def sanitize_image_selection(
    logo_url: str,
    cover_url: str,
    gallery_urls: Iterable[str],
    *,
    dimension_hints: dict[str, tuple[int, int]] | None = None,
) -> tuple[str, str, list[str]]:
    logo_candidates = [normalize_url(logo_url)]
    photo_inputs = [normalize_url(cover_url), *(normalize_url(url) for url in gallery_urls)]
    logo_candidates.extend(url for url in photo_inputs if looks_like_logo_image(url))
    final_logo = best_logo_url(logo_candidates) or normalize_url(logo_url)

    seen_keys: set[str] = set()
    photo_candidates: list[tuple[int, float, int, str]] = []
    logo_key = image_identity_key(final_logo)
    for index, original_url in enumerate(photo_inputs):
        normalized = normalize_url(original_url)
        if not normalized:
            continue
        identity_key = image_identity_key(normalized)
        if not identity_key or identity_key in seen_keys:
            continue
        seen_keys.add(identity_key)
        if logo_key and identity_key == logo_key:
            continue
        score = photo_candidate_score(normalized, dimension_hints=dimension_hints)
        if index == 0 and not looks_like_non_photo_image(normalized):
            score += 1.0
        # Preserve the caller's chosen cover candidate unless it is clearly non-photo.
        if score < MIN_PHOTO_CANDIDATE_SCORE and index == 0 and not looks_like_non_photo_image(normalized):
            score = MIN_PHOTO_CANDIDATE_SCORE
        if score < MIN_PHOTO_CANDIDATE_SCORE:
            continue
        orientation_bucket = image_orientation_bucket(normalized, dimension_hints=dimension_hints)
        photo_candidates.append((orientation_bucket, score, index, normalized))

    photo_candidates.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
    ordered_photo_urls = [url for _, _, _, url in photo_candidates]
    final_cover = ordered_photo_urls[0] if ordered_photo_urls else ""
    final_gallery = ordered_photo_urls[1:6] if final_cover else ordered_photo_urls[:5]
    return final_logo, final_cover, final_gallery


def choose_asset_images(urls: Iterable[str]) -> tuple[str, str, list[str]]:
    normalized_urls = unique_preserve_order(normalize_url(url) for url in urls if clean_text(url))
    final_logo = best_logo_url(normalized_urls)

    dimension_hints: dict[str, tuple[int, int]] = {}
    photo_candidates: list[tuple[int, float, int, str]] = []
    logo_key = image_identity_key(final_logo)
    for index, url in enumerate(normalized_urls):
        identity_key = image_identity_key(url)
        if not identity_key:
            continue
        if logo_key and identity_key == logo_key:
            continue
        remember_image_dimensions(dimension_hints, url)
        score = photo_candidate_score(url, dimension_hints=dimension_hints)
        if score < MIN_PHOTO_CANDIDATE_SCORE:
            continue
        orientation_bucket = image_orientation_bucket(url, dimension_hints=dimension_hints)
        photo_candidates.append((orientation_bucket, score, index, url))

    photo_candidates.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
    cover_url = photo_candidates[0][3] if photo_candidates else ""
    gallery_urls = [url for _, _, _, url in photo_candidates[1:4]]
    return sanitize_image_selection(final_logo, cover_url, gallery_urls, dimension_hints=dimension_hints)


def is_probable_asset_host(url: str) -> bool:
    host = domain_host(url)
    if not host:
        return False
    if host in KNOWN_ASSET_CDN_HOSTS:
        return True
    return any(hint in host for hint in ("wixstatic.com", "squarespace", "shopifycdn", "ctfassets.net"))


def image_src_from_attrs(attrs: dict[str, str], base_url: str) -> str:
    candidates = [
        attrs.get("src", ""),
        attrs.get("data-src", ""),
        attrs.get("data-lazy-src", ""),
        attrs.get("data-image", ""),
    ]
    srcset = attrs.get("srcset") or attrs.get("data-srcset") or ""
    if srcset:
        srcset_candidates: list[tuple[int, str]] = []
        for raw_item in srcset.split(","):
            item = raw_item.strip()
            if not item:
                continue
            parts = item.split()
            if not parts:
                continue
            candidate_url = parts[0]
            descriptor = parts[1] if len(parts) > 1 else ""
            width_score = 0
            if descriptor.endswith("w"):
                try:
                    width_score = int(descriptor[:-1])
                except ValueError:
                    width_score = 0
            elif descriptor.endswith("x"):
                try:
                    width_score = int(float(descriptor[:-1]) * 1000)
                except ValueError:
                    width_score = 0
            srcset_candidates.append((width_score, candidate_url))
        srcset_candidates.sort(key=lambda item: (-item[0], item[1]))
        candidates.extend(url for _, url in srcset_candidates)
    for candidate in candidates:
        if clean_text(candidate):
            return absolute_url(candidate, base_url)
    return ""


def image_kind_score(attrs: dict[str, str], url: str, kind: str) -> float:
    haystack = " ".join(
        clean_text(attrs.get(field, "")).lower()
        for field in ("alt", "class", "id", "src", "data-src", "data-image")
    )
    url_lower = clean_text(url).lower()
    score = 0.0
    if kind == "logo":
        if any(token in haystack or token in url_lower for token in ("logo", "brand", "wordmark", "masthead")):
            score += 4.0
        if "svg" in url_lower:
            score += 0.6
        if any(token in haystack or token in url_lower for token in ("icon", "favicon", "avatar")):
            score -= 2.5
        if any(token in haystack or token in url_lower for token in ("hero", "banner", "cover", "team", "ride")):
            score -= 1.5
    else:
        if any(token in haystack or token in url_lower for token in ("hero", "banner", "cover", "team", "club", "ride", "gallery", "group")):
            score += 3.0
        if any(token in haystack or token in url_lower for token in ("logo", "brand", "wordmark", "icon", "favicon")):
            score -= 3.0
        width = clean_text(attrs.get("width", ""))
        height = clean_text(attrs.get("height", ""))
        try:
            if int(width or "0") >= 400 or int(height or "0") >= 250:
                score += 0.8
        except ValueError:
            pass
        if any(url_lower.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
            score += 0.3
        if url_lower.endswith(".svg"):
            score -= 1.5
    return score


def choose_image_candidates(images: list[dict[str, str]], base_url: str) -> tuple[str, str, list[str]]:
    logo_candidates: list[tuple[float, str]] = []
    photo_candidates: list[tuple[int, float, int, str]] = []
    dimension_hints: dict[str, tuple[int, int]] = {}
    for index, attrs in enumerate(images):
        image_url = image_src_from_attrs(attrs, base_url)
        if not image_url:
            continue
        host = domain_host(image_url)
        if host and not same_site_url(image_url, base_url) and not is_probable_asset_host(image_url):
            continue
        remember_image_dimensions(dimension_hints, image_url, attrs)
        logo_score = image_kind_score(attrs, image_url, "logo")
        if logo_score > 0:
            logo_candidates.append((logo_score, image_url))
        photo_score = photo_candidate_score(image_url, attrs, dimension_hints=dimension_hints)
        if photo_score >= MIN_PHOTO_CANDIDATE_SCORE:
            orientation_bucket = image_orientation_bucket(image_url, attrs, dimension_hints=dimension_hints)
            photo_candidates.append((orientation_bucket, photo_score, index, image_url))

    logo_candidates.sort(key=lambda item: (-item[0], item[1]))
    photo_candidates.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))

    raw_logo = logo_candidates[0][1] if logo_candidates else ""
    raw_cover = photo_candidates[0][3] if photo_candidates else ""
    raw_gallery = [url for _, _, _, url in photo_candidates[1:4]]
    return sanitize_image_selection(raw_logo, raw_cover, raw_gallery, dimension_hints=dimension_hints)


def resolve_asset_candidate(
    raw_value: str,
    *,
    asset_url: str,
    page_url: str,
    context_kind: str,
) -> str:
    cleaned = clean_text(unescape(raw_value))
    if not cleaned or cleaned.startswith("data:"):
        return ""
    if cleaned.startswith(("http://", "https://", "//", "/")):
        return absolute_url(cleaned, page_url or asset_url)
    if cleaned.startswith(("./", "../")):
        return absolute_url(cleaned, asset_url)
    if context_kind == "css":
        return absolute_url(cleaned, asset_url)
    return absolute_url(cleaned, page_url or asset_url)


def page_looks_like_javascript_shell(parser: MetadataHTMLParser, metadata: dict[str, str]) -> bool:
    if metadata.get("logo_url") or metadata.get("cover_url") or metadata.get("gallery_urls"):
        return False

    root_ids = {clean_text(value).lower() for value in parser.element_ids if clean_text(value)}
    if root_ids & {"root", "__next", "app"}:
        return True

    noscript_text = clean_text(" ".join(parser.noscript_parts)).lower()
    if "enable javascript" in noscript_text or "run this app" in noscript_text:
        return True

    return bool(parser.script_srcs and parser.stylesheet_hrefs and not parser.images)


def asset_urls_from_text(
    text: str,
    asset_url: str,
    *,
    page_url: str,
    context_kind: str,
) -> list[str]:
    candidates: list[str] = []

    for raw_value in re.findall(r"url\(([^)]+)\)", text, re.IGNORECASE):
        resolved = resolve_asset_candidate(
            raw_value.strip().strip("\"'"),
            asset_url=asset_url,
            page_url=page_url,
            context_kind="css" if context_kind == "css" else context_kind,
        )
        if resolved:
            candidates.append(resolved)

    for raw_value in re.findall(r"['\"]([^'\"]+\.(?:png|jpe?g|webp|svg)(?:\?[^'\"]*)?)['\"]", text, re.IGNORECASE):
        resolved = resolve_asset_candidate(
            raw_value,
            asset_url=asset_url,
            page_url=page_url,
            context_kind=context_kind,
        )
        if resolved:
            candidates.append(resolved)

    output: list[str] = []
    for candidate in candidates:
        normalized = normalize_url(candidate)
        if not normalized:
            continue
        parsed = urlparse(normalized)
        if not IMAGE_ASSET_EXTENSION_PATTERN.search(f"{parsed.path}{('?' + parsed.query) if parsed.query else ''}"):
            continue
        output.append(normalized)
    return unique_preserve_order(output)


def discover_shell_asset_images(
    parser: MetadataHTMLParser,
    page_url: str,
    *,
    timeout: float,
    user_agent: str,
    fetcher: Callable[[str, float, str], str],
) -> tuple[str, str, list[str]]:
    resource_urls = []
    for raw_url in [*parser.stylesheet_hrefs, *parser.script_srcs]:
        absolute_resource_url = absolute_url(raw_url, page_url)
        if not absolute_resource_url:
            continue
        if not same_site_url(absolute_resource_url, page_url):
            continue
        resource_urls.append(absolute_resource_url)

    manifest_url = absolute_url("/asset-manifest.json", page_url)
    if manifest_url:
        try:
            manifest_text = fetcher(manifest_url, timeout, user_agent)
            manifest = json.loads(manifest_text)
        except Exception:
            manifest = {}
        files = manifest.get("files", {}) if isinstance(manifest, dict) else {}
        if isinstance(files, dict):
            for raw_value in files.values():
                if not isinstance(raw_value, str):
                    continue
                absolute_resource_url = absolute_url(raw_value, page_url)
                if not absolute_resource_url:
                    continue
                if not same_site_url(absolute_resource_url, page_url):
                    continue
                if not absolute_resource_url.lower().endswith((".js", ".css")):
                    continue
                resource_urls.append(absolute_resource_url)

    discovered_asset_urls: list[str] = []
    max_fetches = MAX_SHELL_ASSET_FETCHES + MAX_MANIFEST_ASSET_FETCHES
    for resource_url in unique_preserve_order(resource_urls)[:max_fetches]:
        try:
            asset_text = fetcher(resource_url, timeout, user_agent)
        except Exception:
            continue
        context_kind = "css" if urlparse(resource_url).path.lower().endswith(".css") else "js"
        discovered_asset_urls.extend(
            asset_urls_from_text(
                asset_text,
                resource_url,
                page_url=page_url,
                context_kind=context_kind,
            )
        )

    page_asset_urls = [absolute_url(url, page_url) for url in parser.stylesheet_hrefs + parser.script_srcs]
    return choose_asset_images([*page_asset_urls, *discovered_asset_urls])


def sanitize_image_metadata(
    metadata: dict[str, str],
    *,
    dimension_hints: dict[str, tuple[int, int]] | None = None,
) -> dict[str, str]:
    cleaned = dict(metadata)
    gallery_urls = split_metadata_urls(cleaned.get("gallery_urls", ""))
    final_logo, final_cover, final_gallery = sanitize_image_selection(
        cleaned.get("logo_url", ""),
        cleaned.get("cover_url", ""),
        gallery_urls,
        dimension_hints=dimension_hints,
    )
    cleaned["logo_url"] = final_logo
    cleaned["cover_url"] = final_cover
    cleaned["gallery_urls"] = join_metadata_urls(final_gallery)
    return cleaned


def extract_place_social_links(profiles: Iterable[dict]) -> dict[str, str]:
    urls = []
    for profile in profiles:
        if isinstance(profile, dict):
            url = profile.get("url", "")
            if url:
                urls.append(url)
    return extract_social_links(urls)


def place_thumbnail_url(result: dict) -> str:
    thumbnail = result.get("thumbnail")
    if isinstance(thumbnail, dict):
        return normalize_url(
            thumbnail.get("original", "")
            or thumbnail.get("src", "")
            or thumbnail.get("url", "")
        )
    return ""


def metadata_from_place_result(result: dict) -> dict[str, str]:
    metadata = empty_metadata()
    profiles = result.get("profiles", []) if isinstance(result.get("profiles"), list) else []
    socials = extract_place_social_links(profiles)
    coordinates = result.get("coordinates") or {}
    address = result.get("postal_address") or {}
    contact = result.get("contact") or {}
    official_url = normalize_url(result.get("url", ""))
    provider_url = normalize_url(result.get("provider_url", ""))
    description = clean_text(result.get("description", ""))
    latitude = ""
    longitude = ""
    if isinstance(coordinates, dict):
        latitude = clean_text(str(coordinates.get("latitude", "")))
        longitude = clean_text(str(coordinates.get("longitude", "")))
    elif isinstance(coordinates, list) and len(coordinates) >= 2:
        latitude = clean_text(str(coordinates[0]))
        longitude = clean_text(str(coordinates[1]))

    category_names = []
    for category in (result.get("categories") or []):
        if isinstance(category, dict):
            name = clean_text(category.get("name", ""))
        else:
            name = clean_text(str(category))
        if name:
            category_names.append(name)
    if category_names:
        description = pick_first(description, ", ".join(category_names))

    country_value = clean_text(address.get("country", ""))
    if len(country_value) == 2:
        country_value = country_name_from_code(country_value) or country_value

    metadata.update(
        {
            "title": clean_text(result.get("title", "")),
            "description": description,
            "canonical_url": official_url,
            "site_url": official_url,
            "site_name": clean_text(result.get("title", "")),
            "logo_url": "",
            "cover_url": place_thumbnail_url(result),
            "facebook_url": socials.get("facebook_url", ""),
            "instagram_url": socials.get("instagram_url", ""),
            "twitter_url": socials.get("twitter_url", ""),
            "youtube_url": socials.get("youtube_url", ""),
            "strava_url": socials.get("strava_url", ""),
            "email": clean_text(contact.get("email", "")),
            "street": clean_text(address.get("streetAddress", "")),
            "city": clean_text(address.get("addressLocality", "")),
            "region": clean_text(address.get("addressRegion", "")),
            "country": country_value,
            "postal_code": clean_text(address.get("postalCode", "")),
            "latitude": latitude,
            "longitude": longitude,
            "logo_rights_status": "",
            "image_rights_status": "unknown" if place_thumbnail_url(result) else "",
        }
    )
    if provider_url and not official_url:
        metadata["canonical_url"] = provider_url
    return metadata


def extract_email(text: str) -> str:
    match = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.IGNORECASE)
    return match.group(0) if match else ""


def extract_metadata(html: str, source_url: str) -> dict[str, str]:
    parser = MetadataHTMLParser()
    parser.feed(html)
    image_dimension_hints: dict[str, tuple[int, int]] = {}
    for attrs in parser.images:
        image_url = image_src_from_attrs(attrs, source_url)
        if image_url:
            remember_image_dimensions(image_dimension_hints, image_url, attrs)
    inferred_logo, inferred_cover, inferred_gallery = choose_image_candidates(parser.images, source_url)
    html_asset_urls = asset_urls_from_text(
        html,
        source_url,
        page_url=source_url,
        context_kind="html",
    )

    title = clean_text(unescape("".join(parser.title_parts)))
    meta = {key: clean_text(unescape(value)) for key, value in parser.meta.items()}
    same_as_links: list[str] = []
    json_name = ""
    json_description = ""
    json_url = ""
    json_logo = ""
    json_image = ""
    street = ""
    city = ""
    region = ""
    postal_code = ""
    country = ""
    latitude = ""
    longitude = ""

    for block in parser.jsonld_blocks:
        for obj in extract_json_objects(block):
            for node in iter_json_nodes(obj):
                node_type = node.get("@type")
                node_types = [node_type] if isinstance(node_type, str) else node_type or []
                node_types = [value.lower() for value in node_types if isinstance(value, str)]

                if not json_name:
                    json_name = clean_text(node.get("name", ""))
                if not json_description:
                    json_description = clean_text(node.get("description", ""))
                if not json_url:
                    json_url = normalize_url(node.get("url", ""))
                if not json_logo:
                    logo = node.get("logo")
                    if isinstance(logo, dict):
                        json_logo = absolute_url(logo.get("url", ""), source_url)
                    elif isinstance(logo, str):
                        json_logo = absolute_url(logo, source_url)
                if not json_image:
                    image = node.get("image")
                    if isinstance(image, dict):
                        json_image = absolute_url(image.get("url", ""), source_url)
                    elif isinstance(image, list):
                        first_image = next((absolute_url(item, source_url) for item in image if isinstance(item, str)), "")
                        json_image = first_image
                    elif isinstance(image, str):
                        json_image = absolute_url(image, source_url)

                same_as = node.get("sameAs")
                if isinstance(same_as, str):
                    same_as_links.append(same_as)
                elif isinstance(same_as, list):
                    same_as_links.extend(item for item in same_as if isinstance(item, str))

                address = node.get("address")
                if isinstance(address, dict):
                    street = pick_first(street, address.get("streetAddress", ""))
                    city = pick_first(city, address.get("addressLocality", ""))
                    region = pick_first(region, address.get("addressRegion", ""))
                    postal_code = pick_first(postal_code, address.get("postalCode", ""))
                    country_value = address.get("addressCountry", "")
                    if isinstance(country_value, dict):
                        country_value = country_value.get("name", "")
                    country = pick_first(country, country_value)

                geo = node.get("geo")
                if isinstance(geo, dict):
                    latitude = pick_first(latitude, str(geo.get("latitude", "")))
                    longitude = pick_first(longitude, str(geo.get("longitude", "")))

                if not json_logo and any(t in {"organization", "sportsorganization", "sportsactivitylocation", "localbusiness"} for t in node_types):
                    image_url = node.get("image")
                    if isinstance(image_url, str):
                        json_logo = absolute_url(image_url, source_url)

    link_socials = extract_social_links(parser.anchors + same_as_links)
    canonical = normalize_url(parser.links.get("canonical", ""))

    metadata = empty_metadata()
    metadata.update(
        {
            "title": pick_first(meta.get("og:title", ""), title),
            "description": pick_first(meta.get("description", ""), meta.get("og:description", ""), json_description),
            "canonical_url": canonical,
            "site_url": pick_first(json_url, canonical, source_url),
            "site_name": pick_first(json_name, meta.get("og:site_name", "")),
            "logo_url": pick_first(
                json_logo,
                absolute_url(meta.get("og:logo", ""), source_url),
                inferred_logo,
            ),
            "cover_url": pick_first(
                json_image,
                absolute_url(meta.get("og:image", ""), source_url),
                absolute_url(meta.get("twitter:image", ""), source_url),
                inferred_cover,
            ),
            "gallery_urls": join_metadata_urls(
                url
                for url in [
                    json_image,
                    absolute_url(meta.get("og:image", ""), source_url),
                    absolute_url(meta.get("twitter:image", ""), source_url),
                    inferred_cover,
                    *inferred_gallery,
                    *html_asset_urls,
                ]
                if clean_text(url)
            ),
            "facebook_url": link_socials.get("facebook_url", ""),
            "instagram_url": link_socials.get("instagram_url", ""),
            "twitter_url": link_socials.get("twitter_url", ""),
            "tiktok_url": link_socials.get("tiktok_url", ""),
            "youtube_url": link_socials.get("youtube_url", ""),
            "strava_url": link_socials.get("strava_url", ""),
            "email": extract_email(html),
            "street": street,
            "city": city,
            "region": region,
            "country": country,
            "postal_code": postal_code,
            "latitude": latitude,
            "longitude": longitude,
        }
    )
    return sanitize_image_metadata(metadata, dimension_hints=image_dimension_hints)


def extract_metadata_with_asset_fallback(
    html: str,
    source_url: str,
    *,
    timeout: float,
    user_agent: str,
    fetcher: Callable[[str, float, str], str] | None = None,
) -> dict[str, str]:
    fetcher = fetcher or fetch_text
    metadata = extract_metadata(html, source_url)
    parser = MetadataHTMLParser()
    parser.feed(html)

    needs_linked_asset_fallback = not clean_text(metadata.get("cover_url", ""))
    if not page_looks_like_javascript_shell(parser, metadata) and not needs_linked_asset_fallback:
        return metadata

    fallback_logo, fallback_cover, fallback_gallery = discover_shell_asset_images(
        parser,
        source_url,
        timeout=timeout,
        user_agent=user_agent,
        fetcher=fetcher,
    )
    if not fallback_logo and not fallback_cover and not fallback_gallery:
        return metadata

    fallback_metadata = {
        "logo_url": fallback_logo,
        "cover_url": fallback_cover,
        "gallery_urls": join_metadata_urls(fallback_gallery),
    }
    return sanitize_image_metadata(merge_metadata(metadata, fallback_metadata))


def social_profile_image_from_html(html: str, page_url: str) -> str:
    parser = MetadataHTMLParser()
    parser.feed(html)
    meta = {key: clean_text(unescape(value)) for key, value in parser.meta.items()}
    for candidate in (
        absolute_url(meta.get("og:image", ""), page_url),
        absolute_url(meta.get("twitter:image", ""), page_url),
    ):
        if candidate:
            return candidate

    for attrs in parser.images:
        image_url = image_src_from_attrs(attrs, page_url)
        if image_url:
            return image_url
    return ""


def social_logo_fallback_urls(metadata: dict[str, str], extra_urls: Iterable[str] = ()) -> list[str]:
    ordered_urls: list[str] = []
    for field in SOCIAL_LOGO_PRIORITY:
        url = normalize_url(metadata.get(field, ""))
        if url:
            ordered_urls.append(url)

    for raw_url in extra_urls:
        url = normalize_url(raw_url)
        if not url:
            continue
        field = social_field_for_url(url)
        if field in SOCIAL_LOGO_PRIORITY:
            ordered_urls.append(url)

    priority_rank = {field: index for index, field in enumerate(SOCIAL_LOGO_PRIORITY)}
    urls_with_priority: list[tuple[int, str]] = []
    for url in unique_preserve_order(ordered_urls):
        field = social_field_for_url(url)
        if field not in priority_rank:
            continue
        urls_with_priority.append((priority_rank[field], url))
    urls_with_priority.sort(key=lambda item: (item[0], item[1]))
    return [url for _, url in urls_with_priority]


def apply_social_logo_fallback(
    metadata: dict[str, str],
    *,
    timeout: float,
    user_agent: str,
    fetcher: Callable[[str, float, str], str] | None = None,
    extra_urls: Iterable[str] = (),
) -> dict[str, str]:
    fetcher = fetcher or fetch_text
    if clean_text(metadata.get("logo_url", "")):
        return metadata

    updated = dict(metadata)
    for profile_url in social_logo_fallback_urls(metadata, extra_urls):
        try:
            html = fetcher(profile_url, timeout, user_agent)
            profile_metadata = extract_metadata_with_asset_fallback(
                html,
                profile_url,
                timeout=timeout,
                user_agent=user_agent,
                fetcher=fetcher,
            )
        except Exception:
            continue

        social_logo = (
            clean_text(profile_metadata.get("logo_url", ""))
            or clean_text(profile_metadata.get("cover_url", ""))
            or social_profile_image_from_html(html, profile_url)
        )
        if not social_logo:
            continue

        updated["logo_url"] = social_logo
        updated["logo_rights_status"] = "official-social"
        return sanitize_image_metadata(updated)

    return updated


def infer_disciplines(*texts: str) -> tuple[str, str]:
    haystack = " ".join(clean_text(text).lower() for text in texts if text)
    matches = []
    for label, keywords in DISCIPLINE_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            matches.append(label)
    matches = unique_preserve_order(matches)
    if not matches:
        return "", ""
    return matches[0], ",".join(matches)


def infer_audience(*texts: str) -> str:
    haystack = " ".join(clean_text(text).lower() for text in texts if text)
    if any(pattern in haystack for pattern in WOMEN_ONLY_PATTERNS):
        return "Women Only"
    if any(pattern in haystack for pattern in MIXED_PATTERNS):
        return "Mixed Gender"
    if "women" in haystack or "women's" in haystack or "womens" in haystack:
        return "Women Only"
    return ""


def choose_club_name(result: SearchResult, metadata: dict[str, str]) -> str:
    candidates = [
        metadata.get("site_name", ""),
        metadata.get("title", ""),
        result.title,
    ]
    for candidate in candidates:
        cleaned = clean_text(candidate)
        if cleaned:
            cleaned = re.sub(r"\s*[|\-–]\s*.*$", "", cleaned)
            normalized = re.sub(r"[^\w\s]", " ", cleaned.lower())
            normalized = re.sub(r"\s+", " ", normalized).strip()
            if normalized in GENERIC_NAME_CANDIDATES:
                continue
            return cleaned
    return ""


def extract_twitter_handle(twitter_url: str) -> str:
    twitter_url = normalize_social_url(twitter_url)
    if not twitter_url:
        return ""
    path = urlparse(twitter_url).path.strip("/")
    return path.split("/", 1)[0] if path else ""


def preferred_site_url(result: SearchResult, metadata: dict[str, str]) -> str:
    site_url = normalize_url(metadata.get("site_url", ""))
    if site_url:
        return site_url
    if result.source_type == "brave-place-search":
        return ""
    return normalize_url(result.link)


def derive_external_id(seed: LocationSeed, website: str, club_name: str) -> str:
    website_key = link_key(website)
    if website_key:
        return slugify(f"{website_key}-{seed.city}-{seed.region or seed.country}")
    return slugify(f"{club_name}-{seed.city}-{seed.region or seed.country}")


def note_parts(parts: Iterable[str]) -> str:
    return " | ".join(part for part in parts if clean_text(part))


def image_rights_for_website(website: str) -> str:
    host = domain_host(website)
    if host in SOCIAL_HOSTS:
        return "official-social"
    if host:
        return "official-site"
    return ""


def fetch_text(url: str, timeout: float, user_agent: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def fetch_json(url: str, headers: dict[str, str], timeout: float) -> dict:
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        raw = response.read()
        content_encoding = clean_text(response.headers.get("Content-Encoding", "")).lower()
        if content_encoding == "gzip":
            raw = gzip.decompress(raw)
        text = raw.decode(charset, errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            snippet = clean_text(text[:200])
            raise ApiResponseError(
                f"Non-JSON response from {url} "
                f"(status={response.status}, encoding={content_encoding or 'identity'}): "
                f"{snippet or '<empty body>'}"
            ) from exc


def country_code(country: str) -> str:
    return COUNTRY_NAME_TO_CODE.get(clean_text(country).lower(), "")


def brave_web_search(
    *,
    query: str,
    api_key: str,
    count: int,
    timeout: float,
    user_agent: str,
    search_country: str,
    latitude: str,
    longitude: str,
) -> list[SearchResult]:
    params = {
        "q": query,
        "count": max(1, min(count, 20)),
        "search_lang": "en",
    }
    if search_country:
        params["country"] = search_country
    url = f"https://api.search.brave.com/res/v1/web/search?{urlencode(params)}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": user_agent,
        "X-Subscription-Token": api_key,
    }
    if clean_text(latitude):
        headers["x-loc-lat"] = clean_text(latitude)
    if clean_text(longitude):
        headers["x-loc-long"] = clean_text(longitude)

    payload = fetch_json(url, headers=headers, timeout=timeout)
    items = (payload.get("web") or {}).get("results", []) or []
    results = []
    for item in items:
        link = normalize_url(item.get("url", ""))
        if not link:
            continue
        results.append(
            SearchResult(
                query=query,
                title=clean_text(item.get("title", "")),
                link=link,
                snippet=clean_text(item.get("description", "")),
            )
        )
    return results


def brave_place_search(
    *,
    query: str,
    api_key: str,
    count: int,
    timeout: float,
    user_agent: str,
    seed: LocationSeed,
    radius_meters: int,
) -> list[SearchResult]:
    params = {
        "q": query,
        "count": max(1, min(count, 50)),
    }
    country_value = country_code(seed.country)
    if country_value:
        params["country"] = country_value
    params["search_lang"] = "en"

    if clean_text(seed.latitude) and clean_text(seed.longitude):
        params["latitude"] = clean_text(seed.latitude)
        params["longitude"] = clean_text(seed.longitude)
    else:
        params["location"] = place_location_string(seed)
    if radius_meters > 0:
        params["radius"] = radius_meters

    url = f"https://api.search.brave.com/res/v1/local/place_search?{urlencode(params)}"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": user_agent,
        "X-Subscription-Token": api_key,
    }
    payload = fetch_json(url, headers=headers, timeout=timeout)
    items = payload.get("results", []) or []
    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        official_url = normalize_url(item.get("url", ""))
        provider_url = normalize_url(item.get("provider_url", ""))
        metadata = metadata_from_place_result(item)
        results.append(
            SearchResult(
                query=query,
                title=clean_text(item.get("title", "")),
                link=official_url,
                snippet=pick_first(metadata.get("description", ""), clean_text(item.get("title", ""))),
                source_type="brave-place-search",
                source_url=provider_url or official_url,
                seed_metadata=metadata,
            )
        )
    return results


def load_location_seeds(path: Path) -> list[LocationSeed]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise DiscoveryError("Location seed CSV is missing headers.")
        seeds = []
        for line_number, row in enumerate(reader, start=2):
            city = clean_text(row.get("city", ""))
            region = clean_text(row.get("region", ""))
            country = clean_text(row.get("country", ""))
            if not city or not country:
                raise DiscoveryError(f"Location seed line {line_number} is missing city or country.")
            location_name = clean_text(row.get("location_name", "")) or ", ".join(
                part for part in [city, region, country] if part
            )
            location_id = clean_text(row.get("location_id", "")) or slugify(location_name)
            seeds.append(
                LocationSeed(
                    location_id=location_id,
                    location_name=location_name,
                    city=city,
                    region=region,
                    country=country,
                    postal_code=clean_text(row.get("postal_code", "")),
                    latitude=clean_text(row.get("latitude", "")),
                    longitude=clean_text(row.get("longitude", "")),
                    query_hint=clean_text(row.get("query_hint", "")),
                )
            )
    return seeds


def load_existing_staging_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def existing_keys(rows: list[dict[str, str]]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        if clean_text(row.get("external_id", "")):
            keys.add(clean_text(row["external_id"]))
        website = normalize_url(row.get("website", ""))
        if website:
            keys.add(link_key(website))
        for field in ("facebook_url", "instagram_url", "twitter_url"):
            url = normalize_url(row.get(field, ""))
            if url:
                keys.add(link_key(url))
    return keys


def build_staging_row(
    *,
    seed: LocationSeed,
    result: SearchResult,
    metadata: dict[str, str],
    score: float,
    discovered_at: datetime,
) -> dict[str, str]:
    metadata = sanitize_image_metadata(metadata)
    website = preferred_site_url(result, metadata)
    club_name = choose_club_name(result, metadata)
    summary_raw = pick_first(metadata.get("description", ""), result.snippet)
    discipline_primary, disciplines_csv = infer_disciplines(
        club_name,
        summary_raw,
        metadata.get("description", ""),
        result.title,
        result.snippet,
        website,
    )
    audience = infer_audience(club_name, summary_raw, result.title, result.snippet)
    location_city = pick_first(metadata.get("city", ""), seed.city)
    location_region = pick_first(metadata.get("region", ""), seed.region)
    location_country = pick_first(metadata.get("country", ""), seed.country)
    external_id = derive_external_id(seed, website or result.source_url or result.link, club_name or result.title)
    image_rights = image_rights_for_website(website or result.link)
    logo_rights_status = pick_first(metadata.get("logo_rights_status", ""), image_rights if metadata.get("logo_url") else "")
    image_rights_status = pick_first(metadata.get("image_rights_status", ""), image_rights if metadata.get("cover_url") else "")

    notes = []
    if location_city == seed.city and not metadata.get("city"):
        notes.append("location uses seed city")
    if not audience:
        notes.append("audience needs review")
    if not discipline_primary:
        notes.append("discipline needs review")
    if social_field_for_url(result.link) or social_field_for_url(result.source_url):
        notes.append("official site may be social profile")

    return {
        "external_id": external_id,
        "existing_gd_id": "",
        "source_type": result.source_type,
        "source_name": result.query,
        "source_url": result.source_url or result.link,
        "discovered_at": discovered_at.strftime("%Y-%m-%d %H:%M:%S"),
        "last_checked": discovered_at.strftime("%Y-%m-%d %H:%M:%S"),
        "review_status": "pending",
        "confidence_score": f"{score:.2f}",
        "club_name": club_name,
        "summary_raw": summary_raw,
        "summary_final": "",
        "audience": audience,
        "discipline_primary": discipline_primary,
        "disciplines_csv": disciplines_csv,
        "website": website,
        "facebook_url": metadata.get("facebook_url", ""),
        "instagram_url": metadata.get("instagram_url", ""),
        "twitter_url": metadata.get("twitter_url", ""),
        "twitter_handle": extract_twitter_handle(metadata.get("twitter_url", "")),
        "youtube_url": metadata.get("youtube_url", ""),
        "strava_url": metadata.get("strava_url", ""),
        "email": metadata.get("email", ""),
        "street": metadata.get("street", ""),
        "street2": "",
        "city": location_city,
        "region": location_region,
        "country": location_country,
        "postal_code": pick_first(metadata.get("postal_code", ""), seed.postal_code),
        "plus_code": "",
        "latitude": pick_first(metadata.get("latitude", ""), seed.latitude),
        "longitude": pick_first(metadata.get("longitude", ""), seed.longitude),
        "logo_source_url": metadata.get("logo_url", ""),
        "logo_rights_status": logo_rights_status,
        "cover_source_url": metadata.get("cover_url", ""),
        "gallery_source_urls": metadata.get("gallery_urls", ""),
        "image_rights_status": image_rights_status,
        "image_notes": "autodiscovered from search/page metadata" if metadata.get("cover_url") or metadata.get("logo_url") else "",
        "focus": "",
        "notes_internal": note_parts(
            [
                f"score={score:.2f}",
                f"location={seed.location_name}",
                *notes,
            ]
        ),
    }


def write_staging_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=STAGING_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover cycling clubs by location and write staged candidates."
    )
    parser.add_argument("locations_csv", type=Path, help="CSV of search locations.")
    parser.add_argument("staging_csv", type=Path, help="Staging CSV to create or update.")
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BRAVE_SEARCH_API_KEY", ""),
        help="Brave Search API key. Defaults to BRAVE_SEARCH_API_KEY.",
    )
    parser.add_argument(
        "--query-template",
        action="append",
        default=[],
        help="Additional query template. Tokens: {city} {region} {country} {location_name} {query_hint}.",
    )
    parser.add_argument(
        "--place-query",
        action="append",
        default=[],
        help="Additional Brave Place Search query string, for example 'cycling club'.",
    )
    parser.add_argument(
        "--providers",
        default="web,place",
        help="Comma-separated providers to use: web, place, or both. Default: web,place",
    )
    parser.add_argument(
        "--results-per-query",
        type=int,
        default=10,
        help="Results to request per query, max 20 for Brave Web Search. Default: 10",
    )
    parser.add_argument(
        "--max-candidates-per-location",
        type=int,
        default=10,
        help="Max candidates to fetch and stage per location. Default: 10",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.20,
        help="Minimum search-result relevance score to consider. Default: 0.20",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds. Default: 20",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.5,
        help="Delay between page fetches. Default: 0.5",
    )
    parser.add_argument(
        "--place-radius-meters",
        type=int,
        default=50000,
        help="Radius hint for Brave Place Search in meters. Default: 50000",
    )
    parser.add_argument(
        "--user-agent",
        default="CFW-Clubs-Discovery/0.1 (+https://cycling-for-women.com)",
        help="User-Agent for HTTP requests.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace matching rows in the staging CSV instead of skipping them.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print(
            "Missing Brave Search credentials. Set --api-key "
            "or the BRAVE_SEARCH_API_KEY environment variable.",
            file=sys.stderr,
        )
        return 1

    query_templates = unique_preserve_order(DEFAULT_QUERY_TEMPLATES + args.query_template)
    place_queries = unique_preserve_order(DEFAULT_PLACE_QUERIES + args.place_query)
    providers = {
        clean_text(provider).lower()
        for provider in args.providers.split(",")
        if clean_text(provider)
    }
    invalid_providers = providers - {"web", "place"}
    if invalid_providers:
        print(
            f"Unsupported providers: {', '.join(sorted(invalid_providers))}. "
            "Use 'web', 'place', or 'web,place'.",
            file=sys.stderr,
        )
        return 1
    if not providers:
        providers = {"web", "place"}

    seeds = load_location_seeds(args.locations_csv)
    current_rows = load_existing_staging_rows(args.staging_csv)
    current_key_set = existing_keys(current_rows)
    discovered_rows: list[dict[str, str]] = []
    now = datetime.now()

    for seed in seeds:
        candidate_results: dict[str, tuple[float, SearchResult]] = {}
        if "web" in providers:
            for query in build_queries(seed, query_templates):
                try:
                    results = brave_web_search(
                        query=query,
                        api_key=args.api_key,
                        count=max(1, min(args.results_per_query, 20)),
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        search_country=country_code(seed.country),
                        latitude=seed.latitude,
                        longitude=seed.longitude,
                    )
                except Exception as exc:  # pragma: no cover - network failures are runtime concerns
                    print(f"Warning: web search failed for '{query}': {exc}", file=sys.stderr)
                    continue

                for result in results:
                    score = score_result(seed, result)
                    if score < args.min_score:
                        continue
                    key = candidate_identity_key(result, seed)
                    previous = candidate_results.get(key)
                    if previous is None or score > previous[0]:
                        candidate_results[key] = (score, result)

        if "place" in providers:
            for query in build_place_queries(seed, place_queries):
                try:
                    results = brave_place_search(
                        query=query,
                        api_key=args.api_key,
                        count=max(1, min(args.results_per_query, 50)),
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        seed=seed,
                        radius_meters=max(0, args.place_radius_meters),
                    )
                except Exception as exc:  # pragma: no cover - network failures are runtime concerns
                    print(f"Warning: place search failed for '{query}': {exc}", file=sys.stderr)
                    continue

                for result in results:
                    score = score_result(seed, result)
                    if score < args.min_score:
                        continue
                    key = candidate_identity_key(result, seed)
                    previous = candidate_results.get(key)
                    if previous is None or score > previous[0]:
                        candidate_results[key] = (score, result)

        sorted_candidates = sorted(candidate_results.values(), key=lambda item: item[0], reverse=True)
        for score, result in sorted_candidates[: args.max_candidates_per_location]:
            metadata = dict(result.seed_metadata or empty_metadata())
            if result.link:
                try:
                    html = fetch_text(result.link, timeout=args.timeout, user_agent=args.user_agent)
                    metadata = merge_metadata(
                        metadata,
                        extract_metadata_with_asset_fallback(
                            html,
                            result.link,
                            timeout=args.timeout,
                            user_agent=args.user_agent,
                        ),
                    )
                    metadata = apply_social_logo_fallback(
                        metadata,
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                    )
                except Exception as exc:  # pragma: no cover - network failures are runtime concerns
                    print(f"Warning: failed to enrich {result.link}: {exc}", file=sys.stderr)

            row = build_staging_row(
                seed=seed,
                result=result,
                metadata=metadata,
                score=score,
                discovered_at=now,
            )

            row_keys = {
                row["external_id"],
                link_key(row.get("website", "")),
                link_key(row.get("facebook_url", "")),
                link_key(row.get("instagram_url", "")),
                link_key(row.get("twitter_url", "")),
            }
            row_keys = {key for key in row_keys if key}
            if row_keys & current_key_set and not args.replace_existing:
                continue

            if args.replace_existing and row["external_id"]:
                current_rows = [
                    existing
                    for existing in current_rows
                    if clean_text(existing.get("external_id", "")) != row["external_id"]
                ]
                current_key_set = existing_keys(current_rows + discovered_rows)

            discovered_rows.append(row)
            current_key_set.update(row_keys)
            time.sleep(max(0.0, args.pause_seconds))

    all_rows = current_rows + discovered_rows
    write_staging_rows(args.staging_csv, all_rows)
    print(
        f"Added {len(discovered_rows)} discovered candidates across {len(seeds)} locations "
        f"to {args.staging_csv}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
