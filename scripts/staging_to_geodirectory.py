#!/usr/bin/env python3
"""Convert staged cycling club rows into a GeoDirectory gd_place import CSV."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

GD_FIELDNAMES = [
    "ID",
    "post_title",
    "post_content",
    "post_status",
    "post_author",
    "post_type",
    "post_date",
    "post_modified",
    "post_tags",
    "post_category",
    "default_category",
    "featured",
    "street",
    "street2",
    "city",
    "region",
    "country",
    "zip",
    "latitude",
    "longitude",
    "logo",
    "website",
    "twitter",
    "twitterusername",
    "facebook",
    "instagram",
    "video",
    "strava",
    "focus",
    "email",
    "post_images",
]

CATEGORY_NAME_TO_ID = {
    "mtb": "41",
    "road": "42",
    "gravel": "43",
    "mtb xc": "48",
    "track": "49",
    "touring": "50",
    "cyclo-cross": "51",
    "social": "57",
}

DISCIPLINE_ALIASES = {
    "mountain bike": "mtb",
    "mountain biking": "mtb",
    "mountain biking trail": "mtb",
    "mountain": "mtb",
    "cx": "cyclo-cross",
    "cyclocross": "cyclo-cross",
    "cyclo cross": "cyclo-cross",
    "road cycling": "road",
    "gravel riding": "gravel",
    "xc": "mtb xc",
    "community": "social",
    "social rides": "social",
    "social ride": "social",
}

AUDIENCE_ALIASES = {
    "women only": "Women Only",
    "women-only": "Women Only",
    "women": "Women Only",
    "womens": "Women Only",
    "women's": "Women Only",
    "mixed gender": "Mixed Gender",
    "mixed-gender": "Mixed Gender",
    "mixed": "Mixed Gender",
    "inclusive": "Mixed Gender",
    "co-ed": "Mixed Gender",
    "coed": "Mixed Gender",
}

APPROVED_IMAGE_RIGHTS = {
    "official-site",
    "official-social",
    "club-provided",
    "licensed",
}

DEFAULT_INCLUDE_STATUSES = {"approved", "published"}
DISCIPLINE_SPLIT_RE = re.compile(r"[;,|]")
IMAGE_LOGO_HINTS = {
    "logo",
    "wordmark",
    "brand",
    "masthead",
    "emblem",
    "corporate logo",
    "favicon",
    "icon",
    "avatar",
    "gravatar",
    "badge",
    "profile pic",
    "profilepic",
    "youtube profile",
    "app store",
    "google play",
    "collection",
    "collections",
    "map",
    "radar",
    "weather",
    "preview",
    "poster",
    "flyer",
    "sponsor",
    "sponsors",
    "partner",
    "partners",
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
    "slider",
    "carousel",
    "action",
    "chapter",
    "program",
    "tour",
    "cycling",
}
MIN_PHOTO_SCORE = 1.0


class ValidationError(Exception):
    """Raised when a row cannot be safely converted."""


@dataclass
class ConversionPreview:
    """Preview of a staged-to-GeoDirectory export run."""

    rows_to_write: list[dict[str, str]]
    skipped: int
    skipped_for_status: int
    skipped_errors: list[str]
    include_statuses: set[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a staged club CSV into a GeoDirectory gd_place import CSV."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the staging CSV.")
    parser.add_argument("output_csv", type=Path, help="Path to the GeoDirectory CSV to write.")
    parser.add_argument(
        "--post-status",
        default="draft",
        help="GeoDirectory post_status for exported rows. Default: draft",
    )
    parser.add_argument(
        "--post-author",
        default="1",
        help="GeoDirectory post_author for exported rows. Default: 1",
    )
    parser.add_argument(
        "--post-type",
        default="gd_place",
        help="GeoDirectory post_type for exported rows. Default: gd_place",
    )
    parser.add_argument(
        "--include-status",
        default="approved,published",
        help="Comma-separated review statuses to export. Default: approved,published",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with an error instead of skipping invalid rows.",
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


def split_disciplines(value: str) -> list[str]:
    value = clean_text(value)
    if not value:
        return []
    parts = [part.strip() for part in DISCIPLINE_SPLIT_RE.split(value) if part.strip()]
    if not parts:
        parts = [value]
    return parts


def split_urls(value: str) -> list[str]:
    value = (value or "").strip()
    if not value:
        return []
    if "::" in value:
        parts = value.split("::")
    elif "\n" in value:
        parts = value.splitlines()
    else:
        parts = [value]
    return [normalize_url(part) for part in parts if clean_text(part)]


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


def image_identity_key(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return f"{parsed.netloc.lower().removeprefix('www.')}{parsed.path}".lower()


def parse_image_dimensions(url: str) -> tuple[int, int]:
    parsed = urlparse(normalize_url(url))
    query = parsed.query.lower()
    width_match = re.search(r"(?:^|[&?])(w|width)=([0-9]{2,5})", query)
    height_match = re.search(r"(?:^|[&?])(h|height)=([0-9]{2,5})", query)
    try:
        width_value = int(width_match.group(2)) if width_match else 0
    except ValueError:
        width_value = 0
    try:
        height_value = int(height_match.group(2)) if height_match else 0
    except ValueError:
        height_value = 0
    return width_value, height_value


def image_orientation_bucket(url: str) -> int:
    width_value, height_value = parse_image_dimensions(url)
    if width_value <= 0 or height_value <= 0:
        return 0
    aspect_ratio = width_value / height_value
    if aspect_ratio >= 1.35:
        return 2
    if aspect_ratio >= 1.15:
        return 1
    return 0


def image_signal_tokens(url: str) -> set[str]:
    normalized = normalize_url(url).lower()
    parsed = urlparse(normalized)
    haystack = f"{parsed.netloc}{parsed.path} {parsed.query}".replace("-", " ").replace("_", " ")
    haystack = re.sub(r"grouprides", "group rides", haystack)
    haystack = re.sub(r"groupride", "group ride", haystack)
    haystack = re.sub(r"teamride", "team ride", haystack)
    haystack = re.sub(r"[^a-z0-9]+", " ", haystack)
    return {token for token in haystack.split() if token}


def looks_like_logo_or_nonphoto(url: str) -> bool:
    normalized = normalize_url(url).lower()
    if not normalized:
        return False
    parsed = urlparse(normalized)
    haystack = f"{parsed.netloc}{parsed.path}".replace("-", " ").replace("_", " ")
    signal_tokens = image_signal_tokens(url)
    if parsed.path.endswith((".svg", ".ico")):
        return True
    if signal_tokens & IMAGE_LOGO_HINTS:
        return True
    return any(token in haystack for token in IMAGE_LOGO_HINTS)


def photo_like_score(url: str) -> float:
    normalized = normalize_url(url).lower()
    if not normalized or looks_like_logo_or_nonphoto(normalized):
        return -5.0
    parsed = urlparse(normalized)
    signal_tokens = image_signal_tokens(url)
    signal_text = " ".join(sorted(signal_tokens))
    score = 0.0
    if parsed.path.endswith((".jpg", ".jpeg", ".png", ".webp")):
        score += 0.6
    if signal_tokens & IMAGE_PHOTO_HINTS:
        score += 1.8
    if re.search(r"\bride(s|ing)?\b", signal_text):
        score += 0.8
    if re.search(r"\b(team|women|cyclists?)\b", signal_text):
        score += 0.4
    if re.search(r"\b(homepage|hero|banner)\b", signal_text):
        score += 0.6

    width_value, height_value = parse_image_dimensions(url)
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
    return score


def normalize_discipline_name(value: str) -> str:
    key = clean_text(value).lower()
    key = DISCIPLINE_ALIASES.get(key, key)
    if key not in CATEGORY_NAME_TO_ID:
        raise ValidationError(f"Unknown discipline '{value}'")
    return key


def map_disciplines(primary: str, disciplines_csv: str) -> tuple[str, str]:
    names = split_disciplines(disciplines_csv)
    if not names and clean_text(primary):
        names = [primary]
    if not names:
        names = ["Social"]

    normalized = [normalize_discipline_name(name) for name in names]
    normalized = unique_preserve_order(normalized)

    primary_name = clean_text(primary)
    if primary_name:
        primary_normalized = normalize_discipline_name(primary_name)
    else:
        primary_normalized = normalized[0]

    category_ids = sorted({CATEGORY_NAME_TO_ID[name] for name in normalized}, key=int)
    return f",{','.join(category_ids)},", CATEGORY_NAME_TO_ID[primary_normalized]


def normalize_audience(value: str) -> str:
    key = clean_text(value).lower()
    if not key:
        return "Mixed Gender"
    audience = AUDIENCE_ALIASES.get(key)
    if audience:
        return audience
    if value in {"Women Only", "Mixed Gender"}:
        return value
    raise ValidationError(f"Unknown audience '{value}'")


def format_datetime(value: str, fallback: datetime) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return fallback.strftime("%Y-%m-%d %H:%M:%S")
    try:
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return fallback.strftime("%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def extract_twitter_handle(handle: str, twitter_url: str) -> str:
    handle = clean_text(handle).lstrip("@")
    if handle:
        return handle
    twitter_url = normalize_url(twitter_url)
    if not twitter_url:
        return ""
    path = urlparse(twitter_url).path.strip("/")
    if not path:
        return ""
    return path.split("/", 1)[0].lstrip("@")


def approved_image_status(value: str) -> bool:
    return clean_text(value).lower() in APPROVED_IMAGE_RIGHTS


def format_image_token(url: str) -> str:
    url = normalize_url(url)
    return f"{url}|||" if url else ""


def build_logo_value(row: dict[str, str]) -> str:
    rights = row.get("logo_rights_status") or row.get("image_rights_status") or ""
    if not approved_image_status(rights):
        return ""
    return format_image_token(row.get("logo_source_url", ""))


def build_gallery_value(row: dict[str, str]) -> str:
    rights = row.get("image_rights_status") or row.get("cover_rights_status") or ""
    if not approved_image_status(rights):
        return ""
    logo_key = image_identity_key(row.get("logo_source_url", ""))
    urls = split_urls(row.get("cover_source_url", ""))
    urls.extend(split_urls(row.get("gallery_source_urls", "")))
    filtered_urls: list[tuple[int, int, float, str]] = []
    for index, url in enumerate(unique_preserve_order(urls)):
        identity_key = image_identity_key(url)
        if not identity_key:
            continue
        if logo_key and identity_key == logo_key:
            continue
        if looks_like_logo_or_nonphoto(url):
            continue
        score = photo_like_score(url)
        if score < MIN_PHOTO_SCORE:
            continue
        orientation_bucket = image_orientation_bucket(url)
        filtered_urls.append((orientation_bucket, index, score, url))
    filtered_urls.sort(key=lambda item: (-item[0], item[1], item[3]))
    return "::".join(format_image_token(url) for _, _, _, url in filtered_urls[:6] if url)


def require_value(row: dict[str, str], field_name: str) -> str:
    value = clean_text(row.get(field_name, ""))
    if not value:
        raise ValidationError(f"Missing {field_name}")
    return value


def convert_row(
    row: dict[str, str],
    *,
    post_status: str,
    post_author: str,
    post_type: str,
    now: datetime,
) -> dict[str, str]:
    post_category, default_category = map_disciplines(
        row.get("discipline_primary", ""),
        row.get("disciplines_csv", ""),
    )
    summary = clean_text(row.get("summary_final", "")) or clean_text(row.get("summary_raw", ""))
    if not summary:
        raise ValidationError("Missing summary_final/summary_raw")

    latitude = require_value(row, "latitude")
    longitude = require_value(row, "longitude")

    return {
        "ID": clean_text(row.get("existing_gd_id", "")),
        "post_title": require_value(row, "club_name"),
        "post_content": summary,
        "post_status": post_status,
        "post_author": clean_text(post_author),
        "post_type": clean_text(post_type),
        "post_date": format_datetime(row.get("discovered_at", ""), now),
        "post_modified": format_datetime(row.get("last_checked", ""), now),
        "post_tags": normalize_audience(row.get("audience", "")),
        "post_category": post_category,
        "default_category": default_category,
        "featured": "0",
        "street": clean_text(row.get("street", "")) or clean_text(row.get("plus_code", "")),
        "street2": clean_text(row.get("street2", "")),
        "city": require_value(row, "city"),
        "region": require_value(row, "region"),
        "country": require_value(row, "country"),
        "zip": clean_text(row.get("postal_code", "")),
        "latitude": latitude,
        "longitude": longitude,
        "logo": build_logo_value(row),
        "website": normalize_url(row.get("website", "")),
        "twitter": normalize_url(row.get("twitter_url", "")),
        "twitterusername": extract_twitter_handle(
            row.get("twitter_handle", ""),
            row.get("twitter_url", ""),
        ),
        "facebook": normalize_url(row.get("facebook_url", "")),
        "instagram": normalize_url(row.get("instagram_url", "")),
        "video": normalize_url(row.get("youtube_url", "")),
        "strava": normalize_url(row.get("strava_url", "")),
        "focus": clean_text(row.get("focus", "")),
        "email": clean_text(row.get("email", "")),
        "post_images": build_gallery_value(row),
    }


def should_include(row: dict[str, str], include_statuses: set[str]) -> bool:
    return clean_text(row.get("review_status", "")).lower() in include_statuses


def parse_include_statuses(value: str | set[str] | list[str] | tuple[str, ...] | None) -> set[str]:
    if value is None:
        return set(DEFAULT_INCLUDE_STATUSES)
    if isinstance(value, str):
        statuses = value.split(",")
    else:
        statuses = list(value)
    parsed = {
        clean_text(status).lower()
        for status in statuses
        if clean_text(status)
    }
    return parsed or set(DEFAULT_INCLUDE_STATUSES)


def prepare_rows_for_export(
    rows: list[dict[str, str]],
    *,
    post_status: str,
    post_author: str,
    post_type: str,
    strict: bool = False,
    include_statuses: set[str] | str | list[str] | tuple[str, ...] | None = None,
    start_line: int = 2,
    now: datetime | None = None,
) -> ConversionPreview:
    allowed_statuses = parse_include_statuses(include_statuses)
    preview_now = now or datetime.now()
    rows_to_write: list[dict[str, str]] = []
    skipped = 0
    skipped_for_status = 0
    skipped_errors: list[str] = []

    for offset, row in enumerate(rows):
        line_number = start_line + offset
        title = clean_text(row.get("club_name", "")) or clean_text(row.get("external_id", ""))
        if not should_include(row, allowed_statuses):
            skipped += 1
            skipped_for_status += 1
            continue
        try:
            rows_to_write.append(
                convert_row(
                    row,
                    post_status=post_status,
                    post_author=post_author,
                    post_type=post_type,
                    now=preview_now,
                )
            )
        except ValidationError as exc:
            message = f"Line {line_number} ({title or 'unnamed row'}): {exc}"
            if strict:
                raise ValidationError(message) from exc
            skipped += 1
            skipped_errors.append(message)

    return ConversionPreview(
        rows_to_write=rows_to_write,
        skipped=skipped,
        skipped_for_status=skipped_for_status,
        skipped_errors=skipped_errors,
        include_statuses=allowed_statuses,
    )


def load_staging_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None:
            raise ValidationError("Input CSV is missing headers.")
        return list(reader), list(reader.fieldnames)


def write_geodirectory_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=GD_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    try:
        input_rows, _ = load_staging_rows(args.input_csv)
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        preview = prepare_rows_for_export(
            input_rows,
            post_status=args.post_status,
            post_author=args.post_author,
            post_type=args.post_type,
            strict=args.strict,
            include_statuses=args.include_status,
            start_line=2,
        )
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    write_geodirectory_csv(args.output_csv, preview.rows_to_write)

    print(
        f"Wrote {len(preview.rows_to_write)} GeoDirectory rows to {args.output_csv} "
        f"(skipped {preview.skipped})."
    )
    if not preview.rows_to_write and preview.skipped_for_status:
        printable_statuses = ", ".join(sorted(preview.include_statuses))
        print(
            "Note: all skipped rows were filtered out by review_status. "
            f"Current export only includes: {printable_statuses}.",
            file=sys.stderr,
        )
        print(
            "Tip: mark reviewed rows as approved/published, or rerun with "
            "--include-status pending,approved,published for a draft import.",
            file=sys.stderr,
        )
    for message in preview.skipped_errors:
        print(f"Warning: {message}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
