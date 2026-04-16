#!/usr/bin/env python3
"""Backfill friendly summary_final text for staging rows."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill summary_final in a staging CSV using row data and source-backed heuristics."
    )
    parser.add_argument("input_csv", type=Path, help="Path to the staging CSV.")
    parser.add_argument("output_csv", type=Path, help="Path to write the updated staging CSV.")
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace existing summary_final values instead of filling blanks only.",
    )
    return parser.parse_args()


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def split_disciplines(value: str) -> list[str]:
    cleaned = clean_text(value)
    if not cleaned:
        return []
    return [part.strip() for part in re.split(r"[;,|]", cleaned) if part.strip()]


def human_join(values: list[str]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def title_case_disciplines(values: list[str]) -> list[str]:
    output = []
    for value in values:
        key = clean_text(value)
        if not key:
            continue
        lower = key.lower()
        if lower == "mtb":
            output.append("mountain bike")
        elif lower == "mtb xc":
            output.append("cross-country mountain bike")
        elif lower == "cyclo-cross":
            output.append("cyclocross")
        elif lower == "social":
            output.append("social rides")
        else:
            output.append(lower)
    return output


def classification_from_notes(notes: str) -> str:
    match = re.search(r"classification=([a-z\-]+)", notes)
    return clean_text(match.group(1)) if match else ""


def audience_phrase(audience: str, classification: str) -> str:
    key = clean_text(audience).lower()
    if key == "women only":
        if classification == "program":
            return "a women-first cycling program"
        if classification == "ride-group":
            return "a women-first ride group"
        return "a women-focused cycling club"
    if classification == "program":
        return "a cycling program with a clear women-relevant angle"
    if classification == "ride-group":
        return "a mixed-gender ride group with space for women riders"
    return "a mixed-gender cycling club with a clear women-relevant side"


def ride_phrase(disciplines: list[str], classification: str) -> str:
    readable = title_case_disciplines(disciplines)
    if not readable:
        if classification == "program":
            return "regular cycling programs"
        if classification == "ride-group":
            return "group rides"
        return "club rides"
    if len(readable) == 1:
        return f"{readable[0]} rides"
    return f"{human_join(readable)} rides"


def extract_signals(summary_raw: str) -> dict[str, str]:
    raw = clean_text(summary_raw)
    lower = raw.lower()
    signals: dict[str, str] = {}

    founded_match = re.search(r"\bfounded in (\d{4})\b", lower)
    if founded_match:
        signals["founded_year"] = founded_match.group(1)

    created_match = re.search(r"\b(created|started) in (\d{4})\b", lower)
    if created_match:
        signals["created_year"] = created_match.group(2)

    if "not-for-profit" in lower or "non-profit" in lower:
        signals["nonprofit"] = "1"
    if "empowerment" in lower:
        signals["empowerment"] = "1"
    if "weekly" in lower:
        signals["weekly"] = "1"
    if "bi-weekly" in lower or "biweekly" in lower:
        signals["biweekly"] = "1"
    if "every other saturday" in lower:
        signals["every_other_saturday"] = "1"
    if "monthly" in lower:
        signals["monthly"] = "1"
    if "beginner" in lower or "novice" in lower:
        signals["beginner"] = "1"
    if "development" in lower:
        signals["development"] = "1"
    if "community" in lower:
        signals["community"] = "1"
    if "chapters" in lower:
        signals["chapters"] = "1"
    if "time trial" in lower:
        signals["time_trial"] = "1"
    if "mission" in lower:
        signals["mission"] = "1"
    return signals


def summary_from_row(row: dict[str, str]) -> str:
    club_name = clean_text(row.get("club_name", ""))
    city = clean_text(row.get("city", ""))
    audience = clean_text(row.get("audience", "")) or "Mixed Gender"
    disciplines = split_disciplines(row.get("disciplines_csv", "")) or split_disciplines(row.get("discipline_primary", ""))
    classification = classification_from_notes(row.get("notes_internal", ""))
    raw = clean_text(row.get("summary_raw", ""))
    signals = extract_signals(raw)

    sentence_one = f"{club_name} is {audience_phrase(audience, classification)} in {city} built around {ride_phrase(disciplines, classification)}."

    sentence_two = ""
    if "founded_year" in signals:
        sentence_two = f"Founded in {signals['founded_year']}, it brings a long local history to the ride."
    elif "time_trial" in signals and "created_year" in signals:
        sentence_two = f"Its women’s time trial series dates back to {signals['created_year']}, which gives the club a long-running role in developing riders."
    elif "time_trial" in signals:
        sentence_two = "One standout feature is its long-running women’s time trial program, which welcomes everyone from curious first-timers to seasoned racers."
    elif "chapters" in signals and "weekly" in signals:
        sentence_two = "Expect weekly rides and a chapter-based community that gives riders more than one place to plug in."
    elif "beginner" in signals and "every_other_saturday" in signals:
        sentence_two = "The tone is especially welcoming for newer riders, with recurring rides that make it easy to build confidence one outing at a time."
    elif "beginner" in signals:
        sentence_two = "It has a beginner-friendly feel that makes it a solid fit for riders who want support as much as speed."
    elif "empowerment" in signals:
        sentence_two = "Its mission leans hard into empowerment and access, using bikes as a way to build confidence and community."
    elif "nonprofit" in signals:
        sentence_two = "It’s run as a non-profit, so the emphasis is on community, consistency, and getting people out riding together."
    elif "development" in signals:
        sentence_two = "There’s a clear development thread here too, with space for women riders who want to grow their skills and confidence."
    elif "community" in signals:
        sentence_two = "The overall feel is community-minded, with the club doing more than just sending people out for solo miles."
    else:
        sentence_two = "Expect a community-minded atmosphere and a clear sense that riding together is the point."

    sentence_three = ""
    if audience.lower() == "women only":
        sentence_three = "It keeps women riders at the center of the experience, which gives the whole thing a welcoming feel."
    elif classification == "program":
        sentence_three = "It reads less like a one-off event and more like a recurring program riders can come back to."
    elif classification == "ride-group":
        sentence_three = "It feels especially suited to riders who want a welcoming crew as much as a workout."
    elif "weekly" in signals or "biweekly" in signals or "monthly" in signals:
        sentence_three = "That regular cadence gives it the feel of a real club, not just a page on the internet."

    sentences = [clean_text(sentence_one), clean_text(sentence_two), clean_text(sentence_three)]
    return " ".join(sentence for sentence in sentences if sentence)


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
        if clean_text(row.get("summary_final", "")) and not args.replace_existing:
            continue
        row["summary_final"] = summary_from_row(row)
        updated += 1

    write_rows(args.output_csv, fieldnames, rows)
    print(f"Wrote {updated} summary updates to {args.output_csv}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
