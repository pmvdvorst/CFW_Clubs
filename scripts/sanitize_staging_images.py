#!/usr/bin/env python3
"""Clean staging image fields so logos and club photos stay separated."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.discover_clubs import (
    clean_text,
    join_metadata_urls,
    normalize_url,
    sanitize_image_selection,
    split_metadata_urls,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sanitize logo and gallery image fields in a staging CSV."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the staging CSV.")
    parser.add_argument("output_csv", type=Path, help="Path to write the sanitized staging CSV.")
    return parser.parse_args()


def note_parts(parts: list[str]) -> str:
    return " | ".join(part for part in parts if clean_text(part))


def append_note(row: dict[str, str], note: str) -> None:
    existing = clean_text(row.get("notes_internal", ""))
    parts = [part for part in existing.split(" | ") if clean_text(part)]
    if note not in parts:
        parts.append(note)
    row["notes_internal"] = note_parts(parts)


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    with args.input_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SystemExit("Input CSV is missing headers.")
        rows = list(reader)
        fieldnames = list(reader.fieldnames)

    updated = 0
    for row in rows:
        original_logo = normalize_url(row.get("logo_source_url", ""))
        original_cover = normalize_url(row.get("cover_source_url", ""))
        original_gallery = split_metadata_urls(row.get("gallery_source_urls", ""))
        final_logo, final_cover, final_gallery = sanitize_image_selection(
            original_logo,
            original_cover,
            original_gallery,
        )
        if (
            final_logo != original_logo
            or final_cover != original_cover
            or join_metadata_urls(final_gallery) != join_metadata_urls(original_gallery)
        ):
            updated += 1
            row["logo_source_url"] = final_logo
            row["cover_source_url"] = final_cover
            row["gallery_source_urls"] = join_metadata_urls(final_gallery)
            if not final_logo:
                row["logo_rights_status"] = ""
            if not final_cover and not final_gallery:
                row["image_rights_status"] = ""
            append_note(row, "sanitized image selection")

    write_rows(args.output_csv, fieldnames, rows)
    print(f"Wrote sanitized staging CSV to {args.output_csv} with {updated} updated rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
