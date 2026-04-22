#!/usr/bin/env python3
"""Generate discovery location seeds from GeoNames city dumps."""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
import zipfile
from dataclasses import dataclass
from pathlib import Path


LOCATION_FIELDNAMES = [
    "location_id",
    "location_name",
    "city",
    "region",
    "country",
    "postal_code",
    "latitude",
    "longitude",
    "query_hint",
]

DEFAULT_FEATURE_CODES = {"PPLC", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPL"}
FALLBACK_COUNTRY_NAMES = {
    "CA": "Canada",
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "NZ": "New Zealand",
    "IE": "Ireland",
}


@dataclass(frozen=True)
class SeedGenerationOptions:
    geonames_file: Path
    output_csv: Path
    country_code: str
    admin1: str = ""
    admin1_code: str = ""
    country_info_file: Path | None = None
    admin1_codes_file: Path | None = None
    feature_codes: str = ",".join(sorted(DEFAULT_FEATURE_CODES))
    min_population: int = 15000
    max_locations: int = 250
    query_hint: str = "road gravel mtb"


def clean_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def slugify(value: str) -> str:
    chars = []
    previous_dash = False
    for char in clean_text(value).lower():
        if char.isalnum():
            chars.append(char)
            previous_dash = False
        elif not previous_dash:
            chars.append("-")
            previous_dash = True
    return "".join(chars).strip("-")


@contextlib.contextmanager
def open_text_stream(path: Path):
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            member = next(
                (
                    name
                    for name in archive.namelist()
                    if not name.endswith("/") and not Path(name).name.startswith(".")
                ),
                "",
            )
            if not member:
                raise ValueError(f"No readable file found inside {path}")
            with archive.open(member) as raw_handle:
                with io.TextIOWrapper(raw_handle, encoding="utf-8") as text_handle:
                    yield text_handle
        return

    with path.open(encoding="utf-8") as handle:
        yield handle


def load_country_names(path: Path | None) -> dict[str, str]:
    country_names = dict(FALLBACK_COUNTRY_NAMES)
    if path is None:
        return country_names
    with open_text_stream(path) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                continue
            country_names[parts[0]] = clean_text(parts[4])
    return country_names


def load_admin1_names(path: Path | None) -> dict[tuple[str, str], str]:
    admin1_names: dict[tuple[str, str], str] = {}
    if path is None:
        return admin1_names
    with open_text_stream(path) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2 or "." not in parts[0]:
                continue
            country_code, admin1_code = parts[0].split(".", 1)
            admin1_names[(country_code, admin1_code)] = clean_text(parts[1])
    return admin1_names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate location seed CSV rows from a GeoNames city dump."
    )
    parser.add_argument("geonames_file", type=Path, help="GeoNames city dump file or zip.")
    parser.add_argument("output_csv", type=Path, help="Output location-seeds CSV path.")
    parser.add_argument("--country-code", required=True, help="Country code filter, for example CA or US.")
    parser.add_argument("--admin1", help="Province/state name filter, for example Ontario.")
    parser.add_argument("--admin1-code", help="GeoNames admin1 code filter.")
    parser.add_argument(
        "--country-info-file",
        type=Path,
        help="Optional GeoNames countryInfo.txt file for full country names.",
    )
    parser.add_argument(
        "--admin1-codes-file",
        type=Path,
        help="Optional GeoNames admin1CodesASCII.txt file for province/state names.",
    )
    parser.add_argument(
        "--feature-codes",
        default=",".join(sorted(DEFAULT_FEATURE_CODES)),
        help="Comma-separated GeoNames feature codes to include.",
    )
    parser.add_argument(
        "--min-population",
        type=int,
        default=15000,
        help="Minimum city population to include. Default: 15000",
    )
    parser.add_argument(
        "--max-locations",
        type=int,
        default=250,
        help="Maximum rows to write after sorting by population. Default: 250",
    )
    parser.add_argument(
        "--query-hint",
        default="road gravel mtb",
        help="Optional discovery hint to append to each seed. Default: road gravel mtb",
    )
    return parser.parse_args()


def parse_feature_codes(value: str) -> set[str]:
    feature_codes = {
        clean_text(code).upper()
        for code in value.split(",")
        if clean_text(code)
    } or DEFAULT_FEATURE_CODES
    return feature_codes


def generate_location_seed_rows(options: SeedGenerationOptions) -> tuple[list[dict[str, str]], str]:
    country_code = clean_text(options.country_code).upper()
    feature_codes = parse_feature_codes(options.feature_codes)
    country_names = load_country_names(options.country_info_file)
    admin1_names = load_admin1_names(options.admin1_codes_file)
    selected_admin1_code = clean_text(options.admin1_code)
    selected_admin1_name = clean_text(options.admin1).lower()

    if selected_admin1_name and not admin1_names and not selected_admin1_code:
        raise ValueError(
            "--admin1 requires --admin1-codes-file so the province/state name can be resolved."
        )

    rows: list[tuple[int, dict[str, str]]] = []
    seen_ids: set[str] = set()
    country_name = country_names.get(country_code, country_code)

    with open_text_stream(options.geonames_file) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 19:
                continue

            (
                geonameid,
                name,
                asciiname,
                _alternatenames,
                latitude,
                longitude,
                feature_class,
                feature_code,
                row_country_code,
                _cc2,
                admin1_code,
                _admin2,
                _admin3,
                _admin4,
                population,
                _elevation,
                _dem,
                _timezone,
                _modification_date,
            ) = parts[:19]

            if clean_text(row_country_code).upper() != country_code:
                continue
            if clean_text(feature_class).upper() != "P":
                continue
            if clean_text(feature_code).upper() not in feature_codes:
                continue

            region_name = admin1_names.get((country_code, admin1_code), clean_text(admin1_code))
            if selected_admin1_code and clean_text(admin1_code) != selected_admin1_code:
                continue
            if selected_admin1_name and clean_text(region_name).lower() != selected_admin1_name:
                continue

            population_value = int(population or 0)
            if population_value < options.min_population:
                continue

            city = clean_text(asciiname or name)
            if not city:
                continue

            location_id = slugify(f"{city}-{region_name}-{country_code}-{geonameid}")
            if location_id in seen_ids:
                continue
            seen_ids.add(location_id)

            rows.append(
                (
                    population_value,
                    {
                        "location_id": location_id,
                        "location_name": f"{city}, {region_name}, {country_name}",
                        "city": city,
                        "region": region_name,
                        "country": country_name,
                        "postal_code": "",
                        "latitude": clean_text(latitude),
                        "longitude": clean_text(longitude),
                        "query_hint": clean_text(options.query_hint),
                    },
                )
            )

    rows.sort(key=lambda item: item[0], reverse=True)
    selected_rows = [row for _, row in rows[: max(0, options.max_locations)]]
    return selected_rows, country_name


def write_location_seed_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOCATION_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def build_seed_generation_message(
    *,
    row_count: int,
    country_name: str,
    output_csv: Path,
    admin1: str = "",
) -> str:
    return (
        f"Wrote {row_count} location seeds for {country_name}"
        + (f" / {admin1}" if clean_text(admin1) else "")
        + f" to {output_csv}."
    )


def main() -> int:
    args = parse_args()
    options = SeedGenerationOptions(
        geonames_file=args.geonames_file,
        output_csv=args.output_csv,
        country_code=args.country_code,
        admin1=args.admin1 or "",
        admin1_code=args.admin1_code or "",
        country_info_file=args.country_info_file,
        admin1_codes_file=args.admin1_codes_file,
        feature_codes=args.feature_codes,
        min_population=args.min_population,
        max_locations=args.max_locations,
        query_hint=args.query_hint,
    )
    try:
        selected_rows, country_name = generate_location_seed_rows(options)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    write_location_seed_csv(options.output_csv, selected_rows)

    print(
        build_seed_generation_message(
            row_count=len(selected_rows),
            country_name=country_name,
            admin1=options.admin1,
            output_csv=options.output_csv,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
