#!/usr/bin/env python3
"""Local review app for staging, approval, and GeoDirectory export."""

from __future__ import annotations

import argparse
import math
import os
import secrets
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VENDOR_DIR = ROOT / ".vendor"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if VENDOR_DIR.exists() and str(VENDOR_DIR) not in sys.path:
    sys.path.insert(0, str(VENDOR_DIR))

from flask import (  # type: ignore[import-not-found]
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from scripts.discover_clubs_with_openai import (
    DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS,
    DEFAULT_DISCOVERY_MODEL,
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_REASONING_EFFORT,
    DEFAULT_RETRY_BUFFER_SECONDS,
    DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS,
    DEFAULT_VERIFICATION_MODEL,
)
from scripts.generate_location_seeds_from_geonames import (
    DEFAULT_FEATURE_CODES,
    LOCATION_FIELDNAMES,
    SeedGenerationOptions,
    build_seed_generation_message,
    generate_location_seed_rows,
    write_location_seed_csv,
)
from scripts.staging_service import (
    EDITABLE_FIELDS,
    FLAG_LABELS,
    StagingFileLockedError,
    StagingLockRegistry,
    StagingRowNotFoundError,
    annotate_row,
    clean_text,
    filter_options,
    filter_rows,
    load_staging_file,
    row_identifier,
    update_review_status_for_rows,
    update_row_fields,
)
from scripts.staging_to_geodirectory import (
    GD_FIELDNAMES,
    ValidationError,
    prepare_rows_for_export,
    write_geodirectory_csv,
)


BULK_STATUS_ACTIONS = {
    "approve": "approved",
    "reject": "rejected",
    "needs_research": "needs_research",
}

SEED_NAME_HINTS = ("location-seeds",)
STAGING_NAME_HINTS = ("club-staging",)
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_PAUSE_SECONDS = 0.5
DEFAULT_SEED_COUNTRY_CODE = "CA"
DEFAULT_SEED_MIN_POPULATION = 15000
DEFAULT_SEED_MAX_LOCATIONS = 250
DEFAULT_SEED_QUERY_HINT = "road gravel mtb"
SEED_INPUT_FOLDERS = ("data", "examples", "templates", "geonames")

FIELD_HELP = {
    "seed": {
        "geonames_file": "GeoNames city dump or zip file used to build location seeds. The bundled cities15000 zip is a good starting point.",
        "output_csv": "Where the generated seed CSV should be written after you verify the preview rows.",
        "country_code": "Two-letter country code to filter GeoNames rows, such as CA or US.",
        "admin1": "Optional province or state name filter, such as Ontario. Leave blank for a country-wide seed file.",
        "admin1_code": "Optional GeoNames admin1 code if you prefer an exact region code instead of the region name.",
        "country_info_file": "Optional GeoNames countryInfo file used to turn country codes like CA into full country names like Canada.",
        "admin1_codes_file": "Optional GeoNames admin1CodesASCII file used to resolve province or state names from admin1 codes.",
        "feature_codes": "Comma-separated GeoNames place types to include. The defaults cover populated places and administrative cities.",
        "min_population": "Minimum city population required to keep a location in the seed preview.",
        "max_locations": "Maximum number of locations to keep after sorting by population.",
        "query_hint": "Optional discovery hint copied into every seed row so the later discovery step can emphasize certain ride styles.",
    },
    "discover": {
        "seed_csv": "The verified location seed CSV that discovery should process, usually generated from the Seed Setup step.",
        "staging_csv": "The staging CSV that receives discovered clubs as reviewable pending rows.",
        "openai_api_key": "Optional per-session OpenAI API key for discovery. Leave it blank to reuse a key already saved in this app session or the app's inherited OPENAI_API_KEY. The key is passed through the subprocess environment so it never appears in the command preview.",
        "discovery_model": "Responses model used for the first-pass web search discovery step.",
        "verification_model": "Responses model used for the second-pass official-site verification step.",
        "reasoning_effort": "How much reasoning effort the model should use when deciding whether a club belongs in the directory.",
        "max_locations": "Maximum number of seed rows to process from the seed CSV. Use 0 to process them all.",
        "min_confidence": "Minimum model confidence required before a candidate is kept in staging.",
        "max_candidates_per_location": "Maximum number of accepted clubs the model should return for each seed location.",
        "discovery_max_output_tokens": "Upper token cap for the discovery pass, including reasoning tokens.",
        "verification_max_output_tokens": "Upper token cap for the verification pass, including reasoning tokens.",
        "max_retries": "How many times discovery should retry temporary OpenAI rate-limit or incomplete-response failures.",
        "retry_buffer_seconds": "Extra wait time added to any retry delay returned by the API.",
        "timeout": "HTTP timeout used for discovery requests and website enrichment fetches.",
        "pause_seconds": "Pause inserted between candidate enrichments to keep requests gentler.",
        "skip_verification": "Runs only the first discovery pass and skips the official-domain verification step.",
        "replace_existing": "Replaces matching staging rows instead of skipping rows that already exist in the staging CSV.",
    },
    "review_filters": {
        "review_status": "Show only rows with a specific review status such as pending or approved.",
        "city": "Limit the review table to one city.",
        "region": "Limit the review table to one province or state.",
        "min_confidence": "Show only rows at or above this confidence score.",
        "max_confidence": "Show only rows at or below this confidence score.",
        "audience": "Filter by the current audience classification.",
        "discipline": "Filter by the inferred primary or secondary ride discipline.",
        "existing_state": "Separate new discoveries from rows already matched to an existing GeoDirectory ID.",
        "image_rights_status": "Filter by current logo or gallery image-rights status.",
        "flag": "Show only rows with a specific review flag, or any flagged row.",
    },
    "review_detail": {
        "review_status": "Editorial state for the row. Only approved or published rows are eligible for export.",
        "club_name": "Final listing title that will become the GeoDirectory post title.",
        "summary_final": "Polished editorial summary used for GeoDirectory post content. Leave blank only if you still need to write it.",
        "audience": "Directory audience tag, currently mapped to Women Only or Mixed Gender.",
        "discipline_primary": "Primary ride discipline that becomes the default GeoDirectory category.",
        "disciplines_csv": "Comma-separated discipline list used to populate GeoDirectory categories.",
        "city": "City displayed in the final listing and used for filtering in this app.",
        "region": "Province or state displayed in the final listing.",
        "country": "Country displayed in the final listing. This is also checked for suspicious region-country mismatches.",
        "existing_gd_id": "Existing GeoDirectory post ID if this row should update an existing listing instead of creating a new one.",
        "website": "Preferred official website or landing page for the club.",
        "logo_source_url": "Approved official logo image URL to import into the GeoDirectory logo field.",
        "logo_rights_status": "Rights status for the logo image. Only approved statuses are exported.",
        "image_rights_status": "Rights status for cover and gallery images. Only approved statuses are exported.",
        "cover_source_url": "Primary cover image URL for the listing gallery.",
        "gallery_source_urls": "Additional gallery image URLs. Use one per line in the editor; they are stored as double-colon separated values.",
    },
    "export": {
        "output_csv": "Final GeoDirectory import CSV path that will be written when you confirm export.",
        "post_status": "GeoDirectory post_status value written to every exported row, usually draft for the first import.",
        "post_author": "WordPress user ID that will own the imported listings.",
        "post_type": "GeoDirectory post type to export. The current workflow uses gd_place.",
    },
}


def workspace_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def resolve_workspace_path(raw_value: str, *, must_exist: bool = True) -> Path:
    value = clean_text(raw_value)
    if not value:
        raise ValueError("Path is required.")
    path = Path(value)
    if not path.is_absolute():
        path = ROOT / path
    path = path.resolve()
    try:
        path.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError("Path must stay inside this workspace.") from exc
    if must_exist and not path.exists():
        raise ValueError(f"Path does not exist: {workspace_relative(path)}")
    return path


def list_workspace_files(
    *,
    folders: tuple[str, ...] = SEED_INPUT_FOLDERS,
    suffixes: tuple[str, ...] | None = None,
) -> list[Path]:
    matches: list[Path] = []
    allowed_suffixes = {suffix.lower() for suffix in suffixes or ()}
    for folder_name in folders:
        folder = ROOT / folder_name
        if not folder.exists():
            continue
        for path in sorted(folder.rglob("*")):
            if not path.is_file():
                continue
            if allowed_suffixes and path.suffix.lower() not in allowed_suffixes:
                continue
            matches.append(path)
    return matches


def list_csv_files() -> list[Path]:
    return list_workspace_files(folders=("data", "examples", "templates"), suffixes=(".csv",))


def list_seed_input_files() -> list[Path]:
    return list_workspace_files(folders=SEED_INPUT_FOLDERS, suffixes=(".csv", ".txt", ".zip"))


def suggest_seed_csv() -> Path:
    candidates = [path for path in list_csv_files() if any(hint in path.name for hint in SEED_NAME_HINTS)]
    if candidates:
        return candidates[0]
    return ROOT / "examples" / "location-seeds-sample.csv"


def suggest_staging_csv() -> Path:
    candidates = [path for path in list_csv_files() if any(hint in path.name for hint in STAGING_NAME_HINTS)]
    for candidate in candidates:
        if candidate.parent.name == "data":
            return candidate
    if candidates:
        return candidates[0]
    return ROOT / "data" / "club-staging-openai.csv"


def suggest_geonames_file() -> Path:
    candidate = ROOT / "geonames" / "cities15000.zip"
    if candidate.exists():
        return candidate
    candidates = [path for path in list_workspace_files(folders=("geonames",), suffixes=(".zip", ".txt")) if "cities" in path.name.lower()]
    return candidates[0] if candidates else ROOT / "geonames" / "cities15000.zip"


def suggest_country_info_file() -> Path:
    candidate = ROOT / "geonames" / "countryInfo.txt"
    if candidate.exists():
        return candidate
    candidates = [path for path in list_workspace_files(folders=("geonames",), suffixes=(".txt",)) if path.name == "countryInfo.txt"]
    return candidates[0] if candidates else ROOT / "geonames" / "countryInfo.txt"


def suggest_admin1_codes_file() -> Path:
    candidate = ROOT / "geonames" / "admin1CodesASCII.txt"
    if candidate.exists():
        return candidate
    candidates = [path for path in list_workspace_files(folders=("geonames",), suffixes=(".txt",)) if path.name == "admin1CodesASCII.txt"]
    return candidates[0] if candidates else ROOT / "geonames" / "admin1CodesASCII.txt"


def default_output_csv(staging_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stem = staging_path.stem.replace("club-staging", "gd_place-import")
    return ROOT / "data" / f"{stem}-{timestamp}.csv"


def parse_bool_form(name: str) -> bool:
    return request.form.get(name, "") in {"1", "true", "on", "yes"}


def parse_row_ids_csv(raw_value: str) -> list[str]:
    return [clean_text(part) for part in raw_value.split(",") if clean_text(part)]


def parse_optional_int(raw_value: str, *, default: int, minimum: int = 0) -> int:
    value = clean_text(raw_value)
    if not value:
        return default
    parsed = int(value)
    return max(minimum, parsed)


def default_seed_form_values(seed_path: Path) -> dict[str, str]:
    return {
        "geonames_file": workspace_relative(suggest_geonames_file()),
        "output_csv": workspace_relative(seed_path),
        "country_code": DEFAULT_SEED_COUNTRY_CODE,
        "admin1": "",
        "admin1_code": "",
        "country_info_file": workspace_relative(suggest_country_info_file()),
        "admin1_codes_file": workspace_relative(suggest_admin1_codes_file()),
        "feature_codes": ",".join(sorted(DEFAULT_FEATURE_CODES)),
        "min_population": str(DEFAULT_SEED_MIN_POPULATION),
        "max_locations": str(DEFAULT_SEED_MAX_LOCATIONS),
        "query_hint": DEFAULT_SEED_QUERY_HINT,
    }


def build_discovery_command(form_data: dict[str, str], *, seed_path: Path, staging_path: Path) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "discover_clubs_with_openai.py"),
        str(seed_path),
        str(staging_path),
        "--discovery-model",
        clean_text(form_data.get("discovery_model", "")) or DEFAULT_DISCOVERY_MODEL,
        "--verification-model",
        clean_text(form_data.get("verification_model", "")) or DEFAULT_VERIFICATION_MODEL,
        "--reasoning-effort",
        clean_text(form_data.get("reasoning_effort", "")) or DEFAULT_REASONING_EFFORT,
        "--max-locations",
        clean_text(form_data.get("max_locations", "")) or "0",
        "--min-confidence",
        clean_text(form_data.get("min_confidence", "")) or str(DEFAULT_MIN_CONFIDENCE),
        "--max-candidates-per-location",
        clean_text(form_data.get("max_candidates_per_location", "")) or str(DEFAULT_MAX_CANDIDATES),
        "--discovery-max-output-tokens",
        clean_text(form_data.get("discovery_max_output_tokens", "")) or str(DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS),
        "--verification-max-output-tokens",
        clean_text(form_data.get("verification_max_output_tokens", "")) or str(DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS),
        "--max-retries",
        clean_text(form_data.get("max_retries", "")) or str(DEFAULT_MAX_RETRIES),
        "--retry-buffer-seconds",
        clean_text(form_data.get("retry_buffer_seconds", "")) or str(DEFAULT_RETRY_BUFFER_SECONDS),
        "--timeout",
        clean_text(form_data.get("timeout", "")) or str(DEFAULT_TIMEOUT_SECONDS),
        "--pause-seconds",
        clean_text(form_data.get("pause_seconds", "")) or str(DEFAULT_PAUSE_SECONDS),
    ]
    if form_data.get("skip_verification"):
        command.append("--skip-verification")
    if form_data.get("replace_existing"):
        command.append("--replace-existing")
    return command


@dataclass
class DiscoveryJob:
    staging_path: Path
    seed_path: Path
    command: list[str]
    started_at: str
    process: subprocess.Popen[str] | None = None
    logs: list[str] = field(default_factory=list)
    return_code: int | None = None
    finished_at: str = ""
    status: str = "queued"

    def snapshot(self) -> dict[str, Any]:
        return {
            "staging_csv": workspace_relative(self.staging_path),
            "seed_csv": workspace_relative(self.seed_path),
            "command": self.command,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "running": self.status == "running",
            "return_code": self.return_code,
            "log": "".join(self.logs[-400:]),
        }


class DiscoveryJobManager:
    """Manage background discovery runs and their logs."""

    def __init__(self, lock_registry: StagingLockRegistry) -> None:
        self._lock_registry = lock_registry
        self._guard = threading.Lock()
        self._jobs: dict[str, DiscoveryJob] = {}

    def start(
        self,
        *,
        staging_path: Path,
        seed_path: Path,
        command: list[str],
        env: dict[str, str] | None = None,
    ) -> DiscoveryJob:
        key = str(staging_path)
        with self._guard:
            existing = self._jobs.get(key)
            if existing is not None and existing.status == "running":
                raise RuntimeError("A discovery run is already active for this staging file.")
            self._lock_registry.acquire(staging_path, owner="openai-discovery")
            try:
                process = subprocess.Popen(
                    command,
                    cwd=str(ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    env=env,
                )
            except Exception:
                self._lock_registry.release(staging_path)
                raise
            job = DiscoveryJob(
                staging_path=staging_path,
                seed_path=seed_path,
                command=command,
                started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                process=process,
                status="running",
            )
            self._jobs[key] = job
            thread = threading.Thread(target=self._consume_logs, args=(job,), daemon=True)
            thread.start()
            return job

    def _consume_logs(self, job: DiscoveryJob) -> None:
        assert job.process is not None
        try:
            if job.process.stdout is not None:
                for line in job.process.stdout:
                    job.logs.append(line)
            job.return_code = job.process.wait()
            job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            job.status = "completed" if job.return_code == 0 else "failed"
        finally:
            self._lock_registry.release(job.staging_path)

    def snapshot(self, staging_path: Path) -> dict[str, Any]:
        job = self._jobs.get(str(staging_path))
        if job is None:
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
        return job.snapshot()


def parse_coordinate(raw_value: str) -> float | None:
    value = clean_text(raw_value)
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def build_seed_map_groups(rows: list[dict[str, str]]) -> dict[str, Any]:
    points: list[dict[str, Any]] = []
    for row in rows:
        latitude = parse_coordinate(row.get("latitude", ""))
        longitude = parse_coordinate(row.get("longitude", ""))
        if latitude is None or longitude is None:
            continue
        points.append(
            {
                "row": row,
                "latitude": latitude,
                "longitude": longitude,
                "region": clean_text(row.get("region", "")),
                "city": clean_text(row.get("city", "")),
            }
        )

    if not points:
        return {"clusters": [], "has_points": False, "width": 640, "height": 380}

    min_lat = min(point["latitude"] for point in points)
    max_lat = max(point["latitude"] for point in points)
    min_lon = min(point["longitude"] for point in points)
    max_lon = max(point["longitude"] for point in points)
    lat_span = max(max_lat - min_lat, 0.0001)
    lon_span = max(max_lon - min_lon, 0.0001)
    distance_threshold = min(4.0, max(0.85, max(lat_span, lon_span) / 7.5))

    clusters: list[dict[str, Any]] = []
    for point in points:
        region_key = clean_text(point["region"]).lower()
        assigned_cluster = None
        for cluster in clusters:
            if clean_text(cluster["region"]).lower() != region_key:
                continue
            distance = math.hypot(
                point["latitude"] - cluster["centroid_latitude"],
                point["longitude"] - cluster["centroid_longitude"],
            )
            if distance <= distance_threshold:
                assigned_cluster = cluster
                break
        if assigned_cluster is None:
            clusters.append(
                {
                    "region": point["region"],
                    "points": [point],
                    "centroid_latitude": point["latitude"],
                    "centroid_longitude": point["longitude"],
                }
            )
            continue
        assigned_cluster["points"].append(point)
        point_count = len(assigned_cluster["points"])
        assigned_cluster["centroid_latitude"] = sum(item["latitude"] for item in assigned_cluster["points"]) / point_count
        assigned_cluster["centroid_longitude"] = sum(item["longitude"] for item in assigned_cluster["points"]) / point_count

    width = 640
    height = 380
    padding = 28
    drawable_width = width - padding * 2
    drawable_height = height - padding * 2
    cluster_views: list[dict[str, Any]] = []
    ordered_clusters = sorted(
        clusters,
        key=lambda cluster: (
            clean_text(cluster["region"]).lower(),
            -len(cluster["points"]),
            clean_text(cluster["points"][0]["city"]).lower() if cluster["points"] else "",
        ),
    )

    for index, cluster in enumerate(ordered_clusters, start=1):
        cluster_points = cluster["points"]
        sample_cities = [point["city"] for point in cluster_points if point["city"]]
        anchor_city = sample_cities[0] if sample_cities else f"Cluster {index}"
        region_name = clean_text(cluster["region"]) or "Unknown region"
        if len(cluster_points) == 1:
            cluster_name = f"{anchor_city}"
        else:
            cluster_name = f"{anchor_city} area"
        x_ratio = (cluster["centroid_longitude"] - min_lon) / lon_span if lon_span else 0.5
        y_ratio = (max_lat - cluster["centroid_latitude"]) / lat_span if lat_span else 0.5
        bubble_x = padding + x_ratio * drawable_width
        bubble_y = padding + y_ratio * drawable_height
        cluster_views.append(
            {
                "id": f"seed-cluster-{index}",
                "name": cluster_name,
                "region": region_name,
                "count": len(cluster_points),
                "cities": sample_cities,
                "city_list": ", ".join(sample_cities[:8]),
                "centroid_latitude": f"{cluster['centroid_latitude']:.2f}",
                "centroid_longitude": f"{cluster['centroid_longitude']:.2f}",
                "x": round(bubble_x, 2),
                "y": round(bubble_y, 2),
                "radius": min(28, 11 + len(cluster_points) * 3),
                "label_x": round(bubble_x, 2),
                "label_y": round(max(18, bubble_y - 18), 2),
            }
        )

    return {
        "clusters": cluster_views,
        "has_points": bool(cluster_views),
        "width": width,
        "height": height,
        "min_latitude": f"{min_lat:.2f}",
        "max_latitude": f"{max_lat:.2f}",
        "min_longitude": f"{min_lon:.2f}",
        "max_longitude": f"{max_lon:.2f}",
    }


def create_app(test_config: dict[str, Any] | None = None) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(ROOT / "scripts" / "review_app_templates"),
        static_folder=str(ROOT / "scripts" / "review_app_static"),
    )
    app.config.update(
        SECRET_KEY="cfw-review-app-dev",
        LOCK_REGISTRY=StagingLockRegistry(),
        DISCOVERY_MANAGER=None,
        SESSION_STATE={},
        DEFAULT_STAGING_CSV=workspace_relative(suggest_staging_csv()),
        DEFAULT_SEED_CSV=workspace_relative(suggest_seed_csv()),
    )
    if test_config:
        app.config.update(test_config)
    if app.config["DISCOVERY_MANAGER"] is None:
        app.config["DISCOVERY_MANAGER"] = DiscoveryJobManager(app.config["LOCK_REGISTRY"])

    def session_state() -> dict[str, Any]:
        session_id = session.get("review_session_id")
        if not session_id:
            session_id = secrets.token_hex(12)
            session["review_session_id"] = session_id
        store = app.config["SESSION_STATE"].setdefault(
            session_id,
            {
                "selected_ids": {},
                "current_staging_csv": app.config["DEFAULT_STAGING_CSV"],
                "current_seed_csv": app.config["DEFAULT_SEED_CSV"],
                "last_output_csv": "",
            },
        )
        return store

    def selected_ids_for(staging_path: Path) -> set[str]:
        state = session_state()
        selected = state["selected_ids"].setdefault(str(staging_path), set())
        return selected

    def remember_current_paths(*, staging_path: Path | None = None, seed_path: Path | None = None, output_path: Path | None = None) -> None:
        state = session_state()
        if staging_path is not None:
            state["current_staging_csv"] = workspace_relative(staging_path)
        if seed_path is not None:
            state["current_seed_csv"] = workspace_relative(seed_path)
        if output_path is not None:
            state["last_output_csv"] = workspace_relative(output_path)

    def current_staging_path() -> Path:
        raw_value = clean_text(request.values.get("staging_csv", ""))
        if raw_value:
            path = resolve_workspace_path(raw_value, must_exist=False)
            remember_current_paths(staging_path=path)
            return path
        state = session_state()
        return resolve_workspace_path(state.get("current_staging_csv", workspace_relative(suggest_staging_csv())), must_exist=False)

    def current_seed_path() -> Path:
        raw_value = clean_text(request.values.get("seed_csv", ""))
        if raw_value:
            path = resolve_workspace_path(raw_value, must_exist=False)
            remember_current_paths(seed_path=path)
            return path
        state = session_state()
        return resolve_workspace_path(state.get("current_seed_csv", workspace_relative(suggest_seed_csv())), must_exist=False)

    def stored_openai_api_key() -> str:
        return clean_text(session_state().get("openai_api_key", ""))

    def save_openai_api_key(api_key: str) -> None:
        session_state()["openai_api_key"] = api_key

    def inherited_openai_api_key() -> str:
        return clean_text(os.environ.get("OPENAI_API_KEY", ""))

    def resolve_openai_api_key(submitted_key: str = "") -> tuple[str, str]:
        api_key = clean_text(submitted_key)
        if api_key:
            return api_key, "session"
        api_key = stored_openai_api_key()
        if api_key:
            return api_key, "session"
        api_key = inherited_openai_api_key()
        if api_key:
            return api_key, "environment"
        return "", "missing"

    def openai_api_key_status() -> dict[str, str | bool]:
        session_key = stored_openai_api_key()
        if session_key:
            return {
                "configured": True,
                "source": "session",
                "label": "Saved in this app session",
                "message": "Discovery will reuse the masked key you entered here until you replace it or restart the app.",
            }
        if inherited_openai_api_key():
            return {
                "configured": True,
                "source": "environment",
                "label": "Inherited from OPENAI_API_KEY",
                "message": "Discovery can use the key that was already present in the review app environment.",
            }
        return {
            "configured": False,
            "source": "missing",
            "label": "Missing",
            "message": "Add a key below or relaunch the app with OPENAI_API_KEY set before running discovery.",
        }

    def export_preview_for(staging_path: Path) -> tuple[list[dict[str, str]], Any, list[dict[str, object]]]:
        _, rows = load_staging_file(staging_path)
        selected_ids = selected_ids_for(staging_path)
        selected_rows = [
            row
            for index, row in enumerate(rows)
            if row_identifier(row, index) in selected_ids
        ]
        selected_view_rows = [
            annotate_row(row, index)
            for index, row in enumerate(rows)
            if row_identifier(row, index) in selected_ids
        ]
        preview = prepare_rows_for_export(
            selected_rows,
            post_status=clean_text(request.values.get("post_status", "")) or "draft",
            post_author=clean_text(request.values.get("post_author", "")) or "1",
            post_type=clean_text(request.values.get("post_type", "")) or "gd_place",
            include_statuses={"approved", "published"},
            strict=False,
            start_line=1,
        )
        return selected_rows, preview, selected_view_rows

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        def current_url_with(**updates: str) -> str:
            params = request.args.to_dict(flat=True)
            for key, value in updates.items():
                if value is None:
                    params.pop(key, None)
                else:
                    params[key] = value
            endpoint = request.endpoint or "review_page"
            return url_for(endpoint, **params)

        return {
            "flag_labels": FLAG_LABELS,
            "workspace_relative": workspace_relative,
            "all_flag_items": list(FLAG_LABELS.items()),
            "current_url_with": current_url_with,
            "field_help": FIELD_HELP,
        }

    @app.get("/")
    def root() -> Any:
        return redirect(url_for("seed_page"))

    @app.route("/seed", methods=["GET", "POST"])
    def seed_page() -> Any:
        state = session_state()
        current_seed = resolve_workspace_path(
            state.get("current_seed_csv", workspace_relative(suggest_seed_csv())),
            must_exist=False,
        )
        form_values = default_seed_form_values(current_seed)
        preview_rows: list[dict[str, str]] = []
        seed_map = {"clusters": [], "has_points": False, "width": 640, "height": 380}
        preview_message = ""
        preview_error = ""
        saved_output_csv = ""
        output_exists = current_seed.exists()

        if request.method == "POST":
            for key in form_values:
                if key in request.form:
                    form_values[key] = clean_text(request.form.get(key, ""))
            seed_action = clean_text(request.form.get("seed_action", "")) or "preview"
            try:
                geonames_file = resolve_workspace_path(form_values["geonames_file"], must_exist=True)
                output_csv = resolve_workspace_path(form_values["output_csv"], must_exist=False)
                country_info_file = resolve_workspace_path(form_values["country_info_file"], must_exist=True) if clean_text(form_values["country_info_file"]) else None
                admin1_codes_file = resolve_workspace_path(form_values["admin1_codes_file"], must_exist=True) if clean_text(form_values["admin1_codes_file"]) else None
                options = SeedGenerationOptions(
                    geonames_file=geonames_file,
                    output_csv=output_csv,
                    country_code=form_values["country_code"],
                    admin1=form_values["admin1"],
                    admin1_code=form_values["admin1_code"],
                    country_info_file=country_info_file,
                    admin1_codes_file=admin1_codes_file,
                    feature_codes=form_values["feature_codes"],
                    min_population=parse_optional_int(form_values["min_population"], default=DEFAULT_SEED_MIN_POPULATION),
                    max_locations=parse_optional_int(form_values["max_locations"], default=DEFAULT_SEED_MAX_LOCATIONS),
                    query_hint=form_values["query_hint"],
                )
                preview_rows, country_name = generate_location_seed_rows(options)
                seed_map = build_seed_map_groups(preview_rows)
                preview_message = (
                    f"Preview shows {len(preview_rows)} location seeds for {country_name}"
                    + (f" / {options.admin1}" if clean_text(options.admin1) else "")
                    + "."
                )
                output_exists = output_csv.exists()
                if seed_action == "save":
                    write_location_seed_csv(output_csv, preview_rows)
                    remember_current_paths(seed_path=output_csv)
                    state["current_seed_csv"] = workspace_relative(output_csv)
                    saved_output_csv = workspace_relative(output_csv)
                    output_exists = True
                    flash(
                        build_seed_generation_message(
                            row_count=len(preview_rows),
                            country_name=country_name,
                            admin1=options.admin1,
                            output_csv=output_csv,
                        ),
                        "success",
                    )
            except (ValueError, OSError) as exc:
                preview_error = str(exc)
                flash(str(exc), "error")
        else:
            for key in form_values:
                if key in request.args:
                    form_values[key] = clean_text(request.args.get(key, "")) or form_values[key]

        current_output_csv = form_values["output_csv"]
        seed_input_files = [workspace_relative(path) for path in list_seed_input_files()]
        return render_template(
            "seed.html",
            page_title="Seed Setup",
            form_values=form_values,
            preview_rows=preview_rows,
            seed_map=seed_map,
            preview_columns=LOCATION_FIELDNAMES,
            preview_message=preview_message,
            preview_error=preview_error,
            saved_output_csv=saved_output_csv,
            output_exists=output_exists,
            seed_input_files=seed_input_files,
            output_csv=current_output_csv,
            staging_csv=workspace_relative(current_staging_path()),
        )

    @app.get("/discover")
    def discover_page() -> Any:
        staging_path = current_staging_path()
        seed_path = current_seed_path()
        remember_current_paths(staging_path=staging_path, seed_path=seed_path)
        lock_registry: StagingLockRegistry = app.config["LOCK_REGISTRY"]
        csv_files = [workspace_relative(path) for path in list_csv_files()]
        job_snapshot = app.config["DISCOVERY_MANAGER"].snapshot(staging_path)
        form_values = {
            "seed_csv": workspace_relative(seed_path),
            "staging_csv": workspace_relative(staging_path),
            "discovery_model": request.args.get("discovery_model", DEFAULT_DISCOVERY_MODEL),
            "verification_model": request.args.get("verification_model", DEFAULT_VERIFICATION_MODEL),
            "reasoning_effort": request.args.get("reasoning_effort", DEFAULT_REASONING_EFFORT),
            "max_locations": request.args.get("max_locations", "0"),
            "min_confidence": request.args.get("min_confidence", str(DEFAULT_MIN_CONFIDENCE)),
            "max_candidates_per_location": request.args.get("max_candidates_per_location", str(DEFAULT_MAX_CANDIDATES)),
            "discovery_max_output_tokens": request.args.get("discovery_max_output_tokens", str(DEFAULT_DISCOVERY_MAX_OUTPUT_TOKENS)),
            "verification_max_output_tokens": request.args.get("verification_max_output_tokens", str(DEFAULT_VERIFICATION_MAX_OUTPUT_TOKENS)),
            "max_retries": request.args.get("max_retries", str(DEFAULT_MAX_RETRIES)),
            "retry_buffer_seconds": request.args.get("retry_buffer_seconds", str(DEFAULT_RETRY_BUFFER_SECONDS)),
            "timeout": request.args.get("timeout", str(DEFAULT_TIMEOUT_SECONDS)),
            "pause_seconds": request.args.get("pause_seconds", str(DEFAULT_PAUSE_SECONDS)),
            "skip_verification": request.args.get("skip_verification", ""),
            "replace_existing": request.args.get("replace_existing", ""),
        }
        return render_template(
            "discover.html",
            page_title="Discovery",
            csv_files=csv_files,
            form_values=form_values,
            job_snapshot=job_snapshot,
            openai_api_key_status=openai_api_key_status(),
            staging_locked=lock_registry.is_locked(staging_path),
            lock_details=lock_registry.details(staging_path),
            staging_csv=workspace_relative(staging_path),
        )

    @app.post("/discover/run")
    def discover_run() -> Any:
        try:
            staging_path = resolve_workspace_path(request.form.get("staging_csv", ""), must_exist=False)
            seed_path = resolve_workspace_path(request.form.get("seed_csv", ""), must_exist=True)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("discover_page"))

        form_data = {
            key: value
            for key, value in request.form.items()
        }
        if parse_bool_form("skip_verification"):
            form_data["skip_verification"] = "1"
        if parse_bool_form("replace_existing"):
            form_data["replace_existing"] = "1"
        submitted_openai_api_key = clean_text(request.form.get("openai_api_key", ""))
        if submitted_openai_api_key:
            save_openai_api_key(submitted_openai_api_key)
        api_key, api_key_source = resolve_openai_api_key(submitted_openai_api_key)
        if not api_key:
            flash(
                "Discovery needs an OpenAI API key. Add one in the Discovery form or relaunch the app with OPENAI_API_KEY set.",
                "error",
            )
            return redirect(
                url_for(
                    "discover_page",
                    staging_csv=workspace_relative(staging_path),
                    seed_csv=workspace_relative(seed_path),
                )
            )
        command = build_discovery_command(form_data, seed_path=seed_path, staging_path=staging_path)
        remember_current_paths(staging_path=staging_path, seed_path=seed_path)
        discovery_env = dict(os.environ)
        discovery_env["OPENAI_API_KEY"] = api_key
        try:
            app.config["DISCOVERY_MANAGER"].start(
                staging_path=staging_path,
                seed_path=seed_path,
                command=command,
                env=discovery_env,
            )
            if submitted_openai_api_key:
                flash("Discovery started. OpenAI key saved for this app session and logs will stream below.", "success")
            elif api_key_source == "session":
                flash("Discovery started with the saved OpenAI key. Logs will stream below.", "success")
            else:
                flash("Discovery started using the inherited OPENAI key. Logs will stream below.", "success")
        except StagingFileLockedError:
            flash("That staging file is already locked by an active discovery run.", "error")
        except RuntimeError as exc:
            flash(str(exc), "error")
        except Exception as exc:  # pragma: no cover - subprocess failures are runtime concerns
            flash(f"Failed to start discovery: {exc}", "error")
        return redirect(
            url_for(
                "discover_page",
                staging_csv=workspace_relative(staging_path),
                seed_csv=workspace_relative(seed_path),
            )
        )

    @app.get("/api/discovery/status")
    def discovery_status() -> Any:
        try:
            staging_path = current_staging_path()
        except ValueError:
            staging_path = suggest_staging_csv()
        lock_registry: StagingLockRegistry = app.config["LOCK_REGISTRY"]
        snapshot = app.config["DISCOVERY_MANAGER"].snapshot(staging_path)
        snapshot["staging_locked"] = lock_registry.is_locked(staging_path)
        details = lock_registry.details(staging_path)
        snapshot["lock_owner"] = details.owner if details else ""
        snapshot["lock_created_at"] = details.created_at if details else ""
        return jsonify(snapshot)

    @app.get("/review")
    def review_page() -> Any:
        staging_path = current_staging_path()
        remember_current_paths(staging_path=staging_path)
        fieldnames, rows = load_staging_file(staging_path)
        filters = {
            "review_status": request.args.get("review_status", ""),
            "city": request.args.get("city", ""),
            "region": request.args.get("region", ""),
            "min_confidence": request.args.get("min_confidence", ""),
            "max_confidence": request.args.get("max_confidence", ""),
            "audience": request.args.get("audience", ""),
            "discipline": request.args.get("discipline", ""),
            "existing_state": request.args.get("existing_state", ""),
            "image_rights_status": request.args.get("image_rights_status", ""),
            "flag": request.args.get("flag", ""),
        }
        filtered_rows = filter_rows(rows, filters)
        all_rows = [annotate_row(row, index) for index, row in enumerate(rows)]
        selected_ids = selected_ids_for(staging_path)
        for row in filtered_rows:
            row["_selected"] = row.get("_row_id") in selected_ids
        row_id = clean_text(request.args.get("row_id", ""))
        selected_row = None
        if row_id:
            selected_row = next((row for row in all_rows if row.get("_row_id") == row_id), None)
        if selected_row is None and filtered_rows:
            selected_row = filtered_rows[0]
        lock_registry: StagingLockRegistry = app.config["LOCK_REGISTRY"]
        return render_template(
            "review.html",
            page_title="Review",
            staging_csv=workspace_relative(staging_path),
            fieldnames=fieldnames,
            rows=filtered_rows,
            selected_row=selected_row,
            filters=filters,
            options=filter_options(rows),
            total_rows=len(rows),
            filtered_count=len(filtered_rows),
            selected_count=len(selected_ids),
            staging_locked=lock_registry.is_locked(staging_path),
            lock_details=lock_registry.details(staging_path),
        )

    @app.post("/review/bulk")
    def review_bulk() -> Any:
        try:
            staging_path = resolve_workspace_path(request.form.get("staging_csv", ""), must_exist=False)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("review_page"))
        remember_current_paths(staging_path=staging_path)
        action = clean_text(request.form.get("action", "")).lower()
        row_ids = parse_row_ids_csv(request.form.get("row_ids", ""))
        selected_ids = selected_ids_for(staging_path)
        if action == "select_visible":
            selected_ids.update(row_ids)
            flash(f"Selected {len(row_ids)} visible rows for export.", "success")
        elif action == "clear_selection":
            selected_ids.clear()
            flash("Cleared the current export selection.", "success")
        elif action in BULK_STATUS_ACTIONS:
            try:
                updated = update_review_status_for_rows(
                    staging_path,
                    row_ids,
                    BULK_STATUS_ACTIONS[action],
                    lock_registry=app.config["LOCK_REGISTRY"],
                )
                flash(f"Updated {updated} row(s) to {BULK_STATUS_ACTIONS[action]}.", "success")
            except StagingFileLockedError:
                flash("This staging file is locked while discovery is running.", "error")
        else:
            flash("Unknown bulk action.", "error")
        next_url = request.form.get("next") or url_for("review_page", staging_csv=workspace_relative(staging_path))
        return redirect(next_url)

    @app.post("/api/selection")
    def selection_toggle() -> Any:
        try:
            staging_path = resolve_workspace_path(request.form.get("staging_csv", ""), must_exist=False)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        row_id = clean_text(request.form.get("row_id", ""))
        if not row_id:
            return jsonify({"ok": False, "error": "row_id is required"}), 400
        selected = request.form.get("selected", "0") in {"1", "true", "on", "yes"}
        selected_ids = selected_ids_for(staging_path)
        if selected:
            selected_ids.add(row_id)
        else:
            selected_ids.discard(row_id)
        return jsonify({"ok": True, "selected_count": len(selected_ids)})

    @app.post("/api/rows/<row_id>")
    def update_row(row_id: str) -> Any:
        try:
            staging_path = resolve_workspace_path(request.form.get("staging_csv", ""), must_exist=False)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        updates = {
            field_name: request.form.get(field_name, "")
            for field_name in EDITABLE_FIELDS
            if field_name in request.form
        }
        try:
            row = update_row_fields(
                staging_path,
                row_id,
                updates,
                lock_registry=app.config["LOCK_REGISTRY"],
            )
        except StagingFileLockedError:
            return jsonify({"ok": False, "error": "This staging file is locked while discovery is running."}), 423
        except StagingRowNotFoundError:
            return jsonify({"ok": False, "error": "That row could not be found anymore."}), 404
        remember_current_paths(staging_path=staging_path)
        return jsonify(
            {
                "ok": True,
                "row_id": row.get("_row_id", ""),
                "review_status": row.get("review_status", ""),
                "last_checked": row.get("last_checked", ""),
                "flag_labels": row.get("_flag_labels", []),
            }
        )

    @app.get("/export")
    def export_page() -> Any:
        staging_path = current_staging_path()
        remember_current_paths(staging_path=staging_path)
        selected_rows, preview, selected_view_rows = export_preview_for(staging_path)
        state = session_state()
        output_csv = request.args.get("output_csv", state.get("last_output_csv", "")) or workspace_relative(default_output_csv(staging_path))
        lock_registry: StagingLockRegistry = app.config["LOCK_REGISTRY"]
        return render_template(
            "export.html",
            page_title="Export",
            staging_csv=workspace_relative(staging_path),
            output_csv=output_csv,
            selected_rows=selected_view_rows,
            selected_count=len(selected_view_rows),
            preview=preview,
            preview_columns=GD_FIELDNAMES,
            generated_count=len(preview.rows_to_write),
            has_errors=bool(preview.skipped_errors),
            staging_locked=lock_registry.is_locked(staging_path),
            lock_details=lock_registry.details(staging_path),
            post_status=request.args.get("post_status", "draft"),
            post_author=request.args.get("post_author", "1"),
            post_type=request.args.get("post_type", "gd_place"),
        )

    @app.post("/export/generate")
    def export_generate() -> Any:
        try:
            staging_path = resolve_workspace_path(request.form.get("staging_csv", ""), must_exist=False)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("export_page"))
        lock_registry: StagingLockRegistry = app.config["LOCK_REGISTRY"]
        if lock_registry.is_locked(staging_path):
            flash("Export is blocked while discovery is writing to this staging file.", "error")
            return redirect(url_for("export_page", staging_csv=workspace_relative(staging_path)))
        try:
            output_path = resolve_workspace_path(request.form.get("output_csv", ""), must_exist=False)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("export_page", staging_csv=workspace_relative(staging_path)))
        selected_rows, preview, _ = export_preview_for(staging_path)
        if not selected_rows:
            flash("Select at least one approved row before generating the GeoDirectory CSV.", "error")
            return redirect(url_for("export_page", staging_csv=workspace_relative(staging_path)))
        if preview.skipped_errors:
            flash("Fix the validation errors shown in the preview before generating the CSV.", "error")
            return redirect(url_for("export_page", staging_csv=workspace_relative(staging_path)))
        if not preview.rows_to_write:
            flash("No selected rows are currently approved or published.", "error")
            return redirect(url_for("export_page", staging_csv=workspace_relative(staging_path)))
        write_geodirectory_csv(output_path, preview.rows_to_write)
        remember_current_paths(staging_path=staging_path, output_path=output_path)
        flash(f"Wrote {len(preview.rows_to_write)} GeoDirectory rows to {workspace_relative(output_path)}.", "success")
        return redirect(
            url_for(
                "export_page",
                staging_csv=workspace_relative(staging_path),
                output_csv=workspace_relative(output_path),
            )
        )

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local CFW review app.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind. Default: 8765")
    parser.add_argument(
        "--staging-csv",
        default=workspace_relative(suggest_staging_csv()),
        help="Default staging CSV to open.",
    )
    parser.add_argument(
        "--seed-csv",
        default=workspace_relative(suggest_seed_csv()),
        help="Default seed CSV to open on the discovery page.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = create_app(
        {
            "SESSION_STATE": {},
            "DEFAULT_STAGING_CSV": clean_text(args.staging_csv) or workspace_relative(suggest_staging_csv()),
            "DEFAULT_SEED_CSV": clean_text(args.seed_csv) or workspace_relative(suggest_seed_csv()),
        }
    )
    print(f"Serving review app on http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
