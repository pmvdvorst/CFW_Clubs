#!/usr/bin/env python3
"""Match staged clubs to an existing GeoDirectory export and fill existing_gd_id."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


GD_SOCIAL_FIELDS = {
    "facebook": "facebook_url",
    "instagram": "instagram_url",
    "twitter": "twitter_url",
}


class MatchingError(Exception):
    """Raised when matching cannot continue safely."""


@dataclass(frozen=True)
class MatchCandidate:
    gd_id: str
    method: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill existing_gd_id in a staging CSV by matching against a GeoDirectory export."
    )
    parser.add_argument("staging_csv", type=Path, help="Path to the club staging CSV.")
    parser.add_argument("geodirectory_csv", type=Path, help="Path to the existing GeoDirectory gd_place export CSV.")
    parser.add_argument("output_csv", type=Path, help="Path to write the matched staging CSV.")
    parser.add_argument(
        "--replace-existing-ids",
        action="store_true",
        help="Replace existing_gd_id values that are already present in the staging CSV.",
    )
    return parser.parse_args()


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_url(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    value = value.split("|", 1)[0].strip()
    if value.startswith(("http://", "https://", "mailto:")):
        return value
    return f"https://{value}"


def normalize_social_url(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def domain_host(url: str) -> str:
    parsed = urlparse(normalize_url(url))
    return parsed.netloc.lower().removeprefix("www.")


def base_domain(url: str) -> str:
    host = domain_host(url)
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def normalize_name(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_location_part(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^\w\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def note_parts(parts: list[str]) -> str:
    return " | ".join(part for part in parts if clean_text(part))


def append_note(row: dict[str, str], note: str) -> None:
    existing = clean_text(row.get("notes_internal", ""))
    parts = [part for part in existing.split(" | ") if clean_text(part)]
    if note not in parts:
        parts.append(note)
    row["notes_internal"] = note_parts(parts)


def load_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise MatchingError(f"{path} is missing headers.")
        return list(reader), list(reader.fieldnames)


def unique_ids(rows: list[dict[str, str]]) -> set[str]:
    return {clean_text(row.get("ID", "")) for row in rows if clean_text(row.get("ID", ""))}


def add_index(index: dict[str, list[MatchCandidate]], key: str, gd_id: str, method: str) -> None:
    key = clean_text(key)
    gd_id = clean_text(gd_id)
    if not key or not gd_id:
        return
    index[key].append(MatchCandidate(gd_id=gd_id, method=method))


def build_indexes(rows: list[dict[str, str]]) -> dict[str, dict[str, list[MatchCandidate]]]:
    indexes = {
        "website_domain": defaultdict(list),
        "source_domain": defaultdict(list),
        "social_url": defaultdict(list),
        "name_location": defaultdict(list),
    }
    for row in rows:
        gd_id = clean_text(row.get("ID", ""))
        if not gd_id:
            continue
        website = normalize_url(row.get("website", ""))
        add_index(indexes["website_domain"], base_domain(website), gd_id, "website_domain")
        add_index(indexes["source_domain"], base_domain(website), gd_id, "source_domain")
        for gd_field in GD_SOCIAL_FIELDS:
            social_url = normalize_social_url(row.get(gd_field, ""))
            add_index(indexes["social_url"], social_url, gd_id, f"{gd_field}_url")

        title = normalize_name(row.get("post_title", ""))
        city = normalize_location_part(row.get("city", ""))
        region = normalize_location_part(row.get("region", ""))
        country = normalize_location_part(row.get("country", ""))
        if title and city:
            key = "|".join([title, city, region, country])
            add_index(indexes["name_location"], key, gd_id, "name_location")
    return indexes


def dedupe_candidates(candidates: list[MatchCandidate]) -> list[MatchCandidate]:
    unique: dict[tuple[str, str], MatchCandidate] = {}
    for candidate in candidates:
        unique[(candidate.gd_id, candidate.method)] = candidate
    return list(unique.values())


def candidates_for_row(row: dict[str, str], indexes: dict[str, dict[str, list[MatchCandidate]]]) -> list[MatchCandidate]:
    candidates: list[MatchCandidate] = []
    website = normalize_url(row.get("website", ""))
    source_url = normalize_url(row.get("source_url", ""))
    for key in [base_domain(website), base_domain(source_url)]:
        if key:
            candidates.extend(indexes["website_domain"].get(key, []))
            candidates.extend(indexes["source_domain"].get(key, []))

    for staging_field in GD_SOCIAL_FIELDS.values():
        social_url = normalize_social_url(row.get(staging_field, ""))
        if social_url:
            candidates.extend(indexes["social_url"].get(social_url, []))

    name = normalize_name(row.get("club_name", ""))
    city = normalize_location_part(row.get("city", ""))
    region = normalize_location_part(row.get("region", ""))
    country = normalize_location_part(row.get("country", ""))
    if name and city:
        key = "|".join([name, city, region, country])
        candidates.extend(indexes["name_location"].get(key, []))

    return dedupe_candidates(candidates)


def choose_match(candidates: list[MatchCandidate]) -> tuple[str, str] | None:
    if not candidates:
        return None
    id_counts = Counter(candidate.gd_id for candidate in candidates)
    if len(id_counts) != 1:
        return None
    gd_id = next(iter(id_counts))
    methods = sorted({candidate.method for candidate in candidates})
    return gd_id, ",".join(methods)


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    staging_rows, staging_fieldnames = load_csv_rows(args.staging_csv)
    gd_rows, _ = load_csv_rows(args.geodirectory_csv)
    indexes = build_indexes(gd_rows)
    known_ids = unique_ids(gd_rows)

    matched = 0
    preserved = 0
    ambiguous = 0
    unmatched = 0
    method_counts: Counter[str] = Counter()

    for row in staging_rows:
        existing_gd_id = clean_text(row.get("existing_gd_id", ""))
        if existing_gd_id and existing_gd_id in known_ids and not args.replace_existing_ids:
            preserved += 1
            continue

        candidates = candidates_for_row(row, indexes)
        selection = choose_match(candidates)
        if selection is None:
            if candidates:
                ambiguous += 1
                append_note(row, "existing_gd_id ambiguous")
            else:
                unmatched += 1
            if args.replace_existing_ids and not candidates:
                row["existing_gd_id"] = ""
            continue

        gd_id, method_string = selection
        row["existing_gd_id"] = gd_id
        append_note(row, f"matched_existing_gd_id={gd_id}")
        append_note(row, f"gd_match_method={method_string}")
        matched += 1
        for method in method_string.split(","):
            method_counts[method] += 1

    write_rows(args.output_csv, staging_fieldnames, staging_rows)

    print(
        f"Wrote matched staging CSV to {args.output_csv}. "
        f"Matched {matched}, preserved {preserved}, ambiguous {ambiguous}, unmatched {unmatched}."
    )
    if method_counts:
        detail = ", ".join(f"{method}={count}" for method, count in sorted(method_counts.items()))
        print(f"Match methods: {detail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
