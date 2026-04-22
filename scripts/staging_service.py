#!/usr/bin/env python3
"""Services for reviewing and editing staging CSV files."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from scripts.discover_clubs import STAGING_FIELDNAMES

EDITABLE_FIELDS = {
    "review_status",
    "club_name",
    "summary_final",
    "audience",
    "discipline_primary",
    "disciplines_csv",
    "city",
    "region",
    "country",
    "website",
    "existing_gd_id",
    "logo_source_url",
    "logo_rights_status",
    "cover_source_url",
    "gallery_source_urls",
    "image_rights_status",
}

FLAG_LABELS = {
    "audience_needs_review": "audience needs review",
    "discipline_needs_review": "discipline needs review",
    "location_uses_seed_city": "location uses seed city",
    "official_site_may_be_social_profile": "official site may be social profile",
    "blank_summary_final": "blank summary_final",
    "missing_required_location_fields": "missing required location fields",
    "suspicious_country_mismatch": "suspicious country mismatch",
    "existing_gd_id_present": "existing GD ID already present",
}

NOTE_TO_FLAG = {
    "audience needs review": "audience_needs_review",
    "discipline needs review": "discipline_needs_review",
    "location uses seed city": "location_uses_seed_city",
    "official site may be social profile": "official_site_may_be_social_profile",
}

CANADIAN_REGIONS = {
    "alberta",
    "british columbia",
    "manitoba",
    "new brunswick",
    "newfoundland and labrador",
    "nova scotia",
    "ontario",
    "prince edward island",
    "quebec",
    "saskatchewan",
    "northwest territories",
    "nunavut",
    "yukon",
}

US_REGIONS = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district of columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
}

REQUIRED_LOCATION_FIELDS = ("city", "region", "country", "latitude", "longitude")


class StagingFileLockedError(RuntimeError):
    """Raised when a staging file is temporarily locked for edits/export."""


class StagingRowNotFoundError(KeyError):
    """Raised when a requested staging row no longer exists."""


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def parse_gallery_source_urls(value: str) -> list[str]:
    raw = value or ""
    return [part.strip() for part in raw.split("::") if part.strip()]


def build_row_media_preview(row: dict[str, str]) -> dict[str, object]:
    logo_url = clean_text(row.get("logo_source_url", ""))
    photo_urls: list[str] = []
    photo_items: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    cover_url = clean_text(row.get("cover_source_url", ""))
    if cover_url:
        seen_urls.add(cover_url)
        photo_urls.append(cover_url)
        photo_items.append({"url": cover_url, "kind": "cover", "label": "Cover"})
    gallery_index = 0
    for candidate in parse_gallery_source_urls(row.get("gallery_source_urls", "")):
        if not candidate or candidate in seen_urls:
            continue
        seen_urls.add(candidate)
        photo_urls.append(candidate)
        photo_items.append(
            {
                "url": candidate,
                "kind": "gallery",
                "label": "Picture",
                "index": gallery_index,
            }
        )
        gallery_index += 1
    return {
        "logo_url": logo_url,
        "photo_urls": photo_urls,
        "photo_items": photo_items,
    }


def normalize_fieldnames(fieldnames: list[str] | None) -> list[str]:
    ordered = list(STAGING_FIELDNAMES)
    for field in fieldnames or []:
        if field not in ordered:
            ordered.append(field)
    return ordered


def row_identifier(row: dict[str, str], index: int) -> str:
    external_id = clean_text(row.get("external_id", ""))
    if external_id:
        return external_id
    fingerprint = "|".join(
        [
            clean_text(row.get("club_name", "")),
            clean_text(row.get("source_url", "")),
            clean_text(row.get("discovered_at", "")),
            clean_text(row.get("city", "")),
            clean_text(row.get("region", "")),
            clean_text(row.get("country", "")),
            str(index),
        ]
    )
    digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:12]
    return f"row-{digest}"


def normalize_country(value: str) -> str:
    country = clean_text(value).lower()
    aliases = {
        "ca": "canada",
        "can": "canada",
        "usa": "united states",
        "us": "united states",
        "u.s.": "united states",
        "u.s.a.": "united states",
        "united states of america": "united states",
    }
    return aliases.get(country, country)


def suspicious_country_mismatch(row: dict[str, str]) -> bool:
    region = clean_text(row.get("region", "")).lower()
    country = normalize_country(row.get("country", ""))
    if not region or not country:
        return False
    if region in CANADIAN_REGIONS and country != "canada":
        return True
    if region in US_REGIONS and country != "united states":
        return True
    return False


def compute_review_flag_ids(row: dict[str, str]) -> list[str]:
    flags: list[str] = []
    notes_text = clean_text(row.get("notes_internal", "")).lower()
    for note, flag_id in NOTE_TO_FLAG.items():
        if note in notes_text and flag_id not in flags:
            flags.append(flag_id)
    if not clean_text(row.get("summary_final", "")):
        flags.append("blank_summary_final")
    if any(not clean_text(row.get(field, "")) for field in REQUIRED_LOCATION_FIELDS):
        flags.append("missing_required_location_fields")
    if suspicious_country_mismatch(row):
        flags.append("suspicious_country_mismatch")
    if clean_text(row.get("existing_gd_id", "")):
        flags.append("existing_gd_id_present")
    seen: set[str] = set()
    ordered: list[str] = []
    for flag in flags:
        if flag in FLAG_LABELS and flag not in seen:
            seen.add(flag)
            ordered.append(flag)
    return ordered


def annotate_row(row: dict[str, str], index: int) -> dict[str, object]:
    view_row: dict[str, object] = dict(row)
    flag_ids = compute_review_flag_ids(row)
    media_preview = build_row_media_preview(row)
    try:
        confidence_value = float(clean_text(row.get("confidence_score", "")) or 0.0)
    except ValueError:
        confidence_value = 0.0
    disciplines = [
        clean_text(item)
        for item in re.split(r"[;,|]", row.get("disciplines_csv", ""))
        if clean_text(item)
    ]
    image_statuses = [
        clean_text(row.get("logo_rights_status", "")).lower(),
        clean_text(row.get("image_rights_status", "")).lower(),
    ]
    image_statuses = [status for status in image_statuses if status]
    view_row["_row_id"] = row_identifier(row, index)
    view_row["_flag_ids"] = flag_ids
    view_row["_flag_labels"] = [FLAG_LABELS[flag_id] for flag_id in flag_ids]
    view_row["_confidence_value"] = confidence_value
    view_row["_disciplines"] = disciplines
    view_row["_image_statuses"] = image_statuses
    view_row["_has_existing_gd_id"] = bool(clean_text(row.get("existing_gd_id", "")))
    view_row["_logo_preview_url"] = media_preview["logo_url"]
    view_row["_photo_preview_urls"] = media_preview["photo_urls"]
    view_row["_photo_preview_items"] = media_preview["photo_items"]
    return view_row


def load_staging_file(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return normalize_fieldnames(None), []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = normalize_fieldnames(reader.fieldnames)
        rows = list(reader)
    return fieldnames, rows


def save_staging_file(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_fieldnames = normalize_fieldnames(fieldnames)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=normalized_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in normalized_fieldnames})


def normalize_input_value(field_name: str, value: str) -> str:
    raw = value or ""
    if field_name == "gallery_source_urls":
        if "\n" in raw and "::" not in raw:
            return "::".join(part.strip() for part in raw.splitlines() if part.strip())
        return "::".join(parse_gallery_source_urls(raw))
    return clean_text(raw)


def find_row_index(rows: list[dict[str, str]], row_id: str) -> int:
    for index, row in enumerate(rows):
        if row_identifier(row, index) == row_id:
            return index
    raise StagingRowNotFoundError(row_id)


def update_row_fields(
    path: Path,
    row_id: str,
    updates: dict[str, str],
    *,
    lock_registry: "StagingLockRegistry | None" = None,
) -> dict[str, object]:
    if lock_registry is not None and lock_registry.is_locked(path):
        raise StagingFileLockedError(str(path))
    fieldnames, rows = load_staging_file(path)
    row_index = find_row_index(rows, row_id)
    row = dict(rows[row_index])
    for field_name, value in updates.items():
        if field_name not in EDITABLE_FIELDS:
            continue
        row[field_name] = normalize_input_value(field_name, value)
    row["last_checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows[row_index] = row
    save_staging_file(path, fieldnames, rows)
    return annotate_row(row, row_index)


def update_review_status_for_rows(
    path: Path,
    row_ids: list[str],
    review_status: str,
    *,
    lock_registry: "StagingLockRegistry | None" = None,
) -> int:
    if lock_registry is not None and lock_registry.is_locked(path):
        raise StagingFileLockedError(str(path))
    fieldnames, rows = load_staging_file(path)
    normalized_status = clean_text(review_status).lower()
    updated = 0
    for index, row in enumerate(rows):
        if row_identifier(row, index) not in row_ids:
            continue
        row["review_status"] = normalized_status
        row["last_checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated += 1
    save_staging_file(path, fieldnames, rows)
    return updated


def filter_rows(rows: list[dict[str, str]], filters: dict[str, str]) -> list[dict[str, object]]:
    annotated_rows = [annotate_row(row, index) for index, row in enumerate(rows)]
    filtered_rows: list[dict[str, object]] = []
    review_status = clean_text(filters.get("review_status", "")).lower()
    city = clean_text(filters.get("city", "")).lower()
    region = clean_text(filters.get("region", "")).lower()
    audience = clean_text(filters.get("audience", "")).lower()
    discipline = clean_text(filters.get("discipline", "")).lower()
    existing_state = clean_text(filters.get("existing_state", "")).lower()
    image_status = clean_text(filters.get("image_rights_status", "")).lower()
    flag_filter = clean_text(filters.get("flag", "")).lower()

    min_confidence_text = clean_text(filters.get("min_confidence", ""))
    max_confidence_text = clean_text(filters.get("max_confidence", ""))
    try:
        min_confidence = float(min_confidence_text) if min_confidence_text else None
    except ValueError:
        min_confidence = None
    try:
        max_confidence = float(max_confidence_text) if max_confidence_text else None
    except ValueError:
        max_confidence = None

    for row in annotated_rows:
        row_status = clean_text(str(row.get("review_status", ""))).lower()
        row_city = clean_text(str(row.get("city", ""))).lower()
        row_region = clean_text(str(row.get("region", ""))).lower()
        row_audience = clean_text(str(row.get("audience", ""))).lower()
        row_disciplines = {clean_text(str(item)).lower() for item in row.get("_disciplines", [])}
        row_image_statuses = {clean_text(str(item)).lower() for item in row.get("_image_statuses", [])}
        row_flag_ids = {clean_text(str(item)).lower() for item in row.get("_flag_ids", [])}
        row_confidence = float(row.get("_confidence_value", 0.0))

        if review_status and row_status != review_status:
            continue
        if city and row_city != city:
            continue
        if region and row_region != region:
            continue
        if audience and row_audience != audience:
            continue
        if discipline and discipline not in row_disciplines and discipline != clean_text(str(row.get("discipline_primary", ""))).lower():
            continue
        if existing_state == "new" and row.get("_has_existing_gd_id"):
            continue
        if existing_state == "existing" and not row.get("_has_existing_gd_id"):
            continue
        if image_status and image_status not in row_image_statuses:
            continue
        if flag_filter == "flagged" and not row_flag_ids:
            continue
        if flag_filter and flag_filter not in {"flagged"} and flag_filter not in row_flag_ids:
            continue
        if min_confidence is not None and row_confidence < min_confidence:
            continue
        if max_confidence is not None and row_confidence > max_confidence:
            continue
        filtered_rows.append(row)

    return filtered_rows


def distinct_values(rows: list[dict[str, str]], field_name: str) -> list[str]:
    values = sorted(
        {
            clean_text(row.get(field_name, ""))
            for row in rows
            if clean_text(row.get(field_name, ""))
        }
    )
    return values


def filter_options(rows: list[dict[str, str]]) -> dict[str, list[str]]:
    disciplines: set[str] = set()
    image_statuses: set[str] = set()
    for index, row in enumerate(rows):
        annotated = annotate_row(row, index)
        disciplines.update(clean_text(str(item)) for item in annotated.get("_disciplines", []))
        image_statuses.update(clean_text(str(item)) for item in annotated.get("_image_statuses", []))
    return {
        "review_statuses": distinct_values(rows, "review_status"),
        "cities": distinct_values(rows, "city"),
        "regions": distinct_values(rows, "region"),
        "audiences": distinct_values(rows, "audience"),
        "disciplines": sorted(value for value in disciplines if value),
        "image_statuses": sorted(value for value in image_statuses if value),
        "flags": [flag_id for flag_id in FLAG_LABELS],
    }


@dataclass
class LockDetails:
    owner: str
    created_at: str


class StagingLockRegistry:
    """Simple file-backed lock registry for staging files."""

    def __init__(self) -> None:
        self._guard = threading.Lock()

    def lock_path(self, staging_path: Path) -> Path:
        suffix = f"{staging_path.suffix}.review.lock" if staging_path.suffix else ".review.lock"
        return staging_path.with_suffix(suffix)

    def acquire(self, staging_path: Path, *, owner: str) -> None:
        lock_path = self.lock_path(staging_path)
        payload = {
            "owner": owner,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with self._guard:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError as exc:
                raise StagingFileLockedError(str(staging_path)) from exc
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)

    def release(self, staging_path: Path) -> None:
        lock_path = self.lock_path(staging_path)
        with self._guard:
            if lock_path.exists():
                lock_path.unlink()

    def is_locked(self, staging_path: Path) -> bool:
        return self.lock_path(staging_path).exists()

    def details(self, staging_path: Path) -> LockDetails | None:
        lock_path = self.lock_path(staging_path)
        if not lock_path.exists():
            return None
        try:
            payload = json.loads(lock_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return LockDetails(owner="unknown", created_at="")
        return LockDetails(
            owner=clean_text(str(payload.get("owner", ""))) or "unknown",
            created_at=clean_text(str(payload.get("created_at", ""))),
        )
