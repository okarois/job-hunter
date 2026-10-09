#!/usr/bin/env python3
"""Job Hunter - scan public job boards for roles you care about and track them locally."""

import argparse
import csv
import fcntl
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
import xml.etree.ElementTree as ET
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from html import unescape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONFIG_PATH = Path(__file__).parent / "config.yaml"
EXAMPLE_CONFIG_PATH = Path(__file__).parent / "config.example.yaml"
CACHE_DIR = Path(__file__).parent / ".cache"
REPORTS_DIR = Path(__file__).parent / "reports"
JOBS_STATE_FILE = CACHE_DIR / "jobs_state.json"
DEAD_SLUGS_FILE = CACHE_DIR / "dead_slugs.json"
LOCK_FILE = CACHE_DIR / "job_hunter.lock"

SLUG_URLS = {
    "greenhouse": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/greenhouse_companies.json",
    "lever": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/lever_companies.json",
    "ashby": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/ashby_companies.json",
    "bamboohr": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/bamboohr_companies.json",
    "icims": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/icims_companies.json",
    "workday": "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/workday_companies.json",
}

WORKERS = {
    "greenhouse": 16, "lever": 10, "ashby": 8,
    "bamboohr": 12, "icims": 10, "workday": 8,
}
MAX_RPS = {
    "greenhouse": 16, "lever": 10, "ashby": 8,
    "bamboohr": 12, "icims": 10, "workday": 6,
}
REQUEST_TIMEOUT = {
    "greenhouse": (4, 10),
    "lever": (4, 12),
    "ashby": (4, 10),
    "bamboohr": (4, 20),
    "icims": (4, 15),
    "workday": (4, 30),
}

BROWSER_UA = "Mozilla/5.0 (compatible; job-hunter/1.0; +local job search)"
WORKDAY_MAX_PAGES = 100
WORKDAY_PAGE_SIZE = 20
WORKDAY_DEAD_STATUS_CODES = {400, 404, 410}
ICIMS_TITLE_ACRONYMS = {
    "api": "API",
    "aws": "AWS",
    "hr": "HR",
    "it": "IT",
    "qa": "QA",
    "sql": "SQL",
}
ICIMS_HTML_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "User-Agent": BROWSER_UA,
}

CACHE_TTL_DAYS = 7
DEAD_SLUG_TTL_DAYS = 90
ERROR_PREVIEW_LIMIT = 3
MAX_REQUEST_FAILURE_RATIO = 0.15
STATE_SCHEMA_VERSION = 1
DEFAULT_DASHBOARD_HOST = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 8765
STATUS_ORDER = ["pending", "seen", "applied", "dismissed"]
STATUS_LABELS = {
    "pending": "Pending / Unreviewed",
    "seen": "Seen",
    "applied": "Applied",
    "dismissed": "Dismissed",
}
THREAD_LOCAL = threading.local()


@dataclass(frozen=True)
class Job:
    company: str
    title: str
    location: str
    url: str
    platform: str
    company_name: str = ""
    posted_at: str = ""


class JobList(list):
    """Fetched jobs plus whether the company's full board was read (False when pagination was cut short)."""

    complete = True


@dataclass
class ScanStats:
    platform: str
    total: int
    successful_fetches: int = 0
    stale_slugs: int = 0
    request_errors: int = 0
    skipped_stale: int = 0
    partial_fetches: int = 0
    matches: int = 0
    elapsed_seconds: float = 0.0
    sample_errors: list[str] | None = None
    successful_slugs: set[str] | None = None

    def __post_init__(self):
        if self.sample_errors is None:
            self.sample_errors = []
        if self.successful_slugs is None:
            self.successful_slugs = set()


@dataclass
class PlatformScanResult:
    platform: str
    matches: list[Job] | None
    stats: ScanStats | None
    stale_found: set[str] | None
    cache_state: str | None
    slug_error: str | None
    file_events: list[tuple[str, Path]]


class DeadSlugError(Exception):
    """The upstream company slug no longer resolves on the job board."""


class RateLimiter:
    def __init__(self, max_per_second: float):
        self.min_interval = 1.0 / max_per_second if max_per_second > 0 else 0
        self.lock = threading.Lock()
        self.last_time = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        with self.lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self.last_time)
            if wait > 0:
                time.sleep(wait)
            self.last_time = time.monotonic()


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return default


def save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp_path.replace(path)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path(__file__).parent))
    except ValueError:
        return str(path)


def normalize_text(value: str) -> str:
    translation = str.maketrans(
        {
            ",": " ",
            "/": " ",
            "(": " ",
            ")": " ",
            "-": " ",
            "_": " ",
        }
    )
    return " ".join(str(value).casefold().translate(translation).split())


def normalize_phrases(phrases: list[str]) -> list[str]:
    return [normalize_text(phrase) for phrase in phrases if str(phrase).strip()]


def company_display_name(slug: str) -> str:
    return " ".join(word.capitalize() for word in re.split(r"[-_\s]+", str(slug)) if word)


def iso_date(value) -> str:
    """Reduce an ISO timestamp or epoch milliseconds to YYYY-MM-DD; empty when unknown."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(value / 1000).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return ""
    match = re.match(r"\d{4}-\d{2}-\d{2}", str(value or ""))
    return match.group(0) if match else ""


def job_key(job: Job) -> str:
    if job.url:
        return job.url
    return f"{job.platform}|{job.company}|{job.title}|{job.location}"


def job_fingerprint(platform: str, company: str, title: str, location: str) -> str:
    return "|".join(
        [
            normalize_text(platform),
            normalize_text(company),
            normalize_text(title),
            normalize_text(location),
        ]
    )


def job_fingerprint_from_job(job: Job) -> str:
    return job_fingerprint(job.platform, job.company, job.title, job.location)


def job_fingerprint_from_record(record: dict) -> str:
    return job_fingerprint(
        str(record.get("platform", "")),
        str(record.get("company", "")),
        str(record.get("title", "")),
        str(record.get("location", "")),
    )


def dismissed_job_fingerprints(store: dict) -> set[str]:
    return {
        job_fingerprint_from_record(record)
        for record in store.get("jobs", {}).values()
        if isinstance(record, dict) and record.get("status") == "dismissed"
    }


def split_visible_and_dismissed_matches(store: dict, matches: list[Job]) -> tuple[list[Job], list[Job]]:
    jobs = store.get("jobs", {})
    dismissed_fingerprints = dismissed_job_fingerprints(store)
    visible_matches: list[Job] = []
    dismissed_matches: list[Job] = []

    for job in matches:
        existing = jobs.get(job_key(job))
        if isinstance(existing, dict) and existing.get("status") == "dismissed":
            dismissed_matches.append(job)
            continue
        if job_fingerprint_from_job(job) in dismissed_fingerprints:
            dismissed_matches.append(job)
            continue
        visible_matches.append(job)

    return visible_matches, dismissed_matches


def checked_company_key(platform: str, slug: str) -> tuple[str, str]:
    """Return the persisted company identity for a successfully scanned slug."""
    cleaned_platform = str(platform).strip()
    cleaned_slug = str(slug).strip()
    if cleaned_platform == "workday":
        parts = [part.strip() for part in cleaned_slug.split("|", 2)]
        if len(parts) == 3 and parts[0]:
            return cleaned_platform, parts[0]
    return cleaned_platform, cleaned_slug


def ensure_runtime_dirs(file_events: list[tuple[str, Path]]):
    for path in [CACHE_DIR, REPORTS_DIR]:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            file_events.append(("initialized", path))


def obsolete_state_files() -> list[Path]:
    return [
        CACHE_DIR / "seen.json",
        CACHE_DIR / "pending_jobs.json",
        CACHE_DIR / "history.json",
        CACHE_DIR / "latest_jobs.html",
        CACHE_DIR / "job_hunter.log",
        CACHE_DIR / "cron.log",
        CACHE_DIR / "last_scan.json",
    ]


def cleanup_obsolete_state(file_events: list[tuple[str, Path]]):
    for path in obsolete_state_files():
        if path.exists():
            path.unlink()
            file_events.append(("deleted", path))


def build_empty_store() -> dict:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "last_scan_at": None,
        "jobs": {},
    }


def normalize_job_record(job_id: str, record: dict) -> dict:
    normalized = {
        "id": job_id,
        "platform": str(record.get("platform", "")).strip(),
        "company": str(record.get("company", "")).strip(),
        "company_name": str(record.get("company_name", "")).strip()
        or company_display_name(str(record.get("company", "")).strip()),
        "posted_at": str(record.get("posted_at", "")).strip(),
        "title": str(record.get("title", "")).strip(),
        "location": str(record.get("location", "")).strip(),
        "url": str(record.get("url", "")).strip(),
        "first_seen_at": str(record.get("first_seen_at", "")).strip(),
        "last_seen_at": str(record.get("last_seen_at", "")).strip(),
        "status": str(record.get("status", "pending")).strip() or "pending",
        "status_updated_at": str(record.get("status_updated_at", "")).strip(),
        "is_live": bool(record.get("is_live", False)),
        "last_checked_at": str(record.get("last_checked_at", "")).strip(),
    }
    if normalized["status"] not in STATUS_LABELS:
        normalized["status"] = "pending"
    if not normalized["status_updated_at"]:
        normalized["status_updated_at"] = normalized["first_seen_at"] or normalized["last_seen_at"]
    return normalized


def ensure_job_store(file_events: list[tuple[str, Path]]) -> dict:
    if not JOBS_STATE_FILE.exists():
        store = build_empty_store()
        save_json(JOBS_STATE_FILE, store)
        file_events.append(("initialized", JOBS_STATE_FILE))
        return store
    store = load_json(JOBS_STATE_FILE, build_empty_store())
    jobs = store.get("jobs", {})
    if not isinstance(jobs, dict):
        jobs = {}
    normalized_jobs = {}
    for job_id, record in jobs.items():
        if isinstance(record, dict):
            normalized_jobs[str(job_id)] = normalize_job_record(str(job_id), record)
    store = {
        "schema_version": STATE_SCHEMA_VERSION,
        "last_scan_at": store.get("last_scan_at"),
        "jobs": normalized_jobs,
    }
    return store


def save_job_store(store: dict, file_events: list[tuple[str, Path]] | None = None):
    save_json(JOBS_STATE_FILE, store)
    if file_events is not None:
        file_events.append(("updated", JOBS_STATE_FILE))


@contextmanager
def job_store_lock():
    """Serialize read-modify-write of the job store between scans and the dashboard."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with (CACHE_DIR / "jobs_state.lock").open("w") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def set_job_status(job_id: str, status: str) -> dict:
    if status not in STATUS_LABELS:
        raise ValueError(f"invalid status: {status}")
    with job_store_lock():
        store = ensure_job_store([])
        record = store["jobs"].get(job_id)
        if record is None:
            raise KeyError(job_id)
        record["status"] = status
        record["status_updated_at"] = now_iso()
        save_json(JOBS_STATE_FILE, store)
    return record


def job_record_from_job(job: Job, seen_at: str) -> dict:
    return {
        "id": job_key(job),
        "platform": job.platform,
        "company": job.company,
        "company_name": job.company_name or company_display_name(job.company),
        "posted_at": job.posted_at,
        "title": job.title,
        "location": job.location,
        "url": job.url,
        "first_seen_at": seen_at,
        "last_seen_at": seen_at,
        "status": "pending",
        "status_updated_at": seen_at,
        "is_live": True,
        "last_checked_at": seen_at,
    }


def apply_scan_results(
    store: dict,
    matches: list[Job],
    seen_at: str,
    scanned_platforms: set[str],
    scanned_companies: set[tuple[str, str]] | None = None,
    dismissed_matches: list[Job] | None = None,
) -> tuple[list[Job], int]:
    jobs = store["jobs"]
    current_match_keys = set()
    current_dismissed_fingerprints = set()
    new_jobs: list[Job] = []
    known_matches = 0

    for job in dismissed_matches or []:
        key = job_key(job)
        current_match_keys.add(key)
        current_dismissed_fingerprints.add(job_fingerprint_from_job(job))
        existing = jobs.get(key)
        if existing is None:
            continue
        existing.update(
            {
                "platform": job.platform,
                "company": job.company,
                "company_name": job.company_name or company_display_name(job.company),
                "posted_at": job.posted_at,
                "title": job.title,
                "location": job.location,
                "url": job.url,
                "last_seen_at": seen_at,
                "is_live": True,
                "last_checked_at": seen_at,
            }
        )

    for job in matches:
        key = job_key(job)
        current_match_keys.add(key)
        existing = jobs.get(key)
        if existing is None:
            jobs[key] = job_record_from_job(job, seen_at)
            new_jobs.append(job)
            continue

        known_matches += 1
        existing.update(
            {
                "platform": job.platform,
                "company": job.company,
                "company_name": job.company_name or company_display_name(job.company),
                "posted_at": job.posted_at,
                "title": job.title,
                "location": job.location,
                "url": job.url,
                "last_seen_at": seen_at,
                "is_live": True,
                "last_checked_at": seen_at,
            }
        )

    for job_id, record in jobs.items():
        platform = str(record.get("platform", "")).strip()
        company = str(record.get("company", "")).strip()
        was_checked = (
            (platform, company) in scanned_companies
            if scanned_companies is not None
            else platform in scanned_platforms
        )
        if was_checked and job_id not in current_match_keys:
            if (
                record.get("status") == "dismissed"
                and job_fingerprint_from_record(record) in current_dismissed_fingerprints
            ):
                record["is_live"] = True
                record["last_seen_at"] = seen_at
                record["last_checked_at"] = seen_at
                continue
            record["is_live"] = False
            record["last_checked_at"] = seen_at

    store["last_scan_at"] = seen_at
    return new_jobs, known_matches


def append_daily_report(jobs: list[Job], found_at: str) -> Path | None:
    if not jobs:
        return None

    report_date = found_at[:10] if len(found_at) >= 10 else datetime.now().date().isoformat()
    report_file = REPORTS_DIR / f"{report_date}.csv"
    write_header = not report_file.exists() or report_file.stat().st_size == 0

    with report_file.open("a", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["found_at", "platform", "company", "title", "location", "url"],
        )
        if write_header:
            writer.writeheader()
        for job in jobs:
            writer.writerow(
                {
                    "found_at": found_at,
                    "platform": job.platform,
                    "company": job.company,
                    "title": job.title,
                    "location": job.location,
                    "url": job.url,
                }
            )

    return report_file


def summarize_store(store: dict) -> dict[str, int]:
    counts = {status: 0 for status in STATUS_ORDER}
    counts["total"] = 0
    counts["live"] = 0
    counts["closed"] = 0

    for record in store.get("jobs", {}).values():
        status = record.get("status", "pending")
        if status not in counts:
            counts[status] = 0
        counts[status] += 1
        counts["total"] += 1
        if record.get("is_live"):
            counts["live"] += 1
        else:
            counts["closed"] += 1

    return counts


def timestamp_sort_value(value: str) -> float:
    try:
        return datetime.fromisoformat(value).timestamp()
    except (TypeError, ValueError):
        return 0.0


def sorted_job_records(store: dict) -> list[dict]:
    jobs = store.get("jobs", {}).values()
    status_index = {status: index for index, status in enumerate(STATUS_ORDER)}
    return sorted(
        jobs,
        key=lambda record: (
            status_index.get(record.get("status", "pending"), len(STATUS_ORDER)),
            -timestamp_sort_value(record.get("last_seen_at", "")),
            -timestamp_sort_value(record.get("first_seen_at", "")),
            record.get("company", "").casefold(),
            record.get("title", "").casefold(),
        ),
    )


def dashboard_payload(store: dict) -> dict:
    counts = summarize_store(store)
    last_scan_at = store.get("last_scan_at")
    jobs = []
    for record in sorted_job_records(store):
        if record.get("status") == "dismissed":
            continue
        item = dict(record)
        item["is_new"] = bool(last_scan_at and record.get("first_seen_at") == last_scan_at)
        jobs.append(item)
    return {
        "last_scan_at": last_scan_at,
        "status_counts": counts,
        "statuses": STATUS_LABELS,
        "jobs": jobs,
    }


ANSI_STYLES = {
    "bold": "1",
    "dim": "2",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
}


def output_style_enabled(stream=None) -> bool:
    stream = stream or sys.stdout
    forced = os.environ.get("JOB_HUNTER_COLOR", "").strip().casefold()
    if forced in {"1", "true", "yes", "always"}:
        return True
    if forced in {"0", "false", "no", "never"}:
        return False
    if os.environ.get("NO_COLOR"):
        return False
    return hasattr(stream, "isatty") and stream.isatty() and os.environ.get("TERM") != "dumb"


def paint(text: str, *styles: str, stream=None) -> str:
    if not output_style_enabled(stream):
        return text
    codes = [ANSI_STYLES[style] for style in styles if style in ANSI_STYLES]
    if not codes:
        return text
    return f"\033[{';'.join(codes)}m{text}\033[0m"


def output_width() -> int:
    return max(72, min(shutil.get_terminal_size((78, 20)).columns, 110))


def print_divider(title: str):
    print()
    width = output_width()
    title_text = f" {title} "
    if len(title_text) >= width - 4:
        line = f"== {title}"
    else:
        line = f"=={title_text}{'=' * (width - len(title_text) - 2)}"
    print(paint(line, "bold", "cyan"))


def print_kv(label: str, value):
    label_cell = f"{label:<24}"
    print(f"  {paint(label_cell, 'blue')} {value}")


def format_duration(seconds: float) -> str:
    if seconds < 10:
        return f"{seconds:.1f}s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, remaining = divmod(int(round(seconds)), 60)
    return f"{minutes}m {remaining:02d}s"


def format_outcome_bar(stats: ScanStats, width: int = 18) -> str:
    stale_total = stats.stale_slugs + stats.skipped_stale
    total = max(1, stats.successful_fetches + stale_total + stats.request_errors)
    ok_cells = int(width * stats.successful_fetches / total)
    stale_cells = int(width * stale_total / total)
    error_cells = int(width * stats.request_errors / total)
    empty_cells = max(0, width - ok_cells - stale_cells - error_cells)
    return (
        "["
        f"{paint('#' * ok_cells, 'green')}"
        f"{paint('-' * stale_cells, 'yellow')}"
        f"{paint('!' * error_cells, 'red')}"
        f"{paint('.' * empty_cells, 'dim')}"
        "]"
    )


def format_scan_summary(stats: ScanStats) -> str:
    return (
        f"{format_outcome_bar(stats)} {format_duration(stats.elapsed_seconds)} | "
        f"{stats.successful_fetches} ok | {stats.stale_slugs} stale | {stats.request_errors} errors | "
        f"{stats.matches} matches"
    )


def print_platform_completion(index: int, total: int, result: PlatformScanResult):
    prefix = f"  [{index}/{total}] {result.platform:<10}"
    if result.slug_error:
        print(f"{paint('[FAIL]', 'red', 'bold')} {prefix} slug fetch failed")
        print(f"      {result.slug_error}")
        return
    stats = result.stats
    if stats is None:
        print(f"{paint('[MISS]', 'yellow', 'bold')} {prefix} no scan result")
        return
    stale_total = stats.stale_slugs + stats.skipped_stale
    status = "[WARN]" if stats.request_errors else "[DONE]"
    status_style = "yellow" if stats.request_errors else "green"
    print(
        f"{paint(status, status_style, 'bold')} {prefix} {format_outcome_bar(stats)} "
        f"{format_duration(stats.elapsed_seconds):>7} | "
        f"queued {stats.total:<5} ok {stats.successful_fetches:<5} "
        f"stale {stale_total:<5} err {stats.request_errors:<5} hits {paint(str(stats.matches), 'magenta', 'bold')}"
    )
    if stats.sample_errors:
        print("      sample request errors:")
        for error in stats.sample_errors:
            print(f"        {error}")


def print_file_events(file_events: list[tuple[str, Path]]):
    if not file_events:
        return
    print_divider("Files")
    seen = set()
    for action, path in file_events:
        key = (action, str(path))
        if key in seen:
            continue
        seen.add(key)
        print(f"  {paint(f'{action:<12}', 'green')} {display_path(path)}")


def print_store_snapshot(store: dict):
    counts = summarize_store(store)
    print_divider("Tracked Jobs")
    print_kv("total tracked", counts["total"])
    print_kv("pending / unreviewed", counts["pending"])
    print_kv("seen", counts["seen"])
    print_kv("applied", counts["applied"])
    print_kv("dismissed / hidden", counts["dismissed"])
    print_kv("currently live", counts["live"])
    print_kv("not currently live", counts["closed"])


def print_match_list(title: str, jobs: list[Job]):
    if not jobs:
        return
    print_divider(title)
    for job in jobs:
        company = job.company_name or company_display_name(job.company)
        print(f"  {paint('>>', 'magenta', 'bold')} {paint(company, 'bold')} | {job.title}")
        if job.posted_at:
            print(f"    {paint('posted:', 'blue')}   {job.posted_at}")
        print(f"    {paint('platform:', 'blue')} {job.platform}")
        print(f"    {paint('location:', 'blue')} {job.location or 'N/A'}")
        print(f"    {paint('url:', 'blue')}      {job.url or 'N/A'}")


def show_history():
    store = ensure_job_store([])
    jobs = sorted_job_records(store)
    print_divider(f"Tracked Job History ({len(jobs)} total)")
    if not jobs:
        print("  No tracked jobs yet.")
        return
    for record in jobs:
        print(f"  {paint('[' + record['status'] + ']', 'magenta')} {record['company']} | {record['title']}")
        print(f"    {paint('platform:', 'blue')}   {record['platform']}")
        print(f"    {paint('location:', 'blue')}   {record['location'] or 'N/A'}")
        print(f"    first seen: {record['first_seen_at'] or 'N/A'}")
        print(f"    last seen:  {record['last_seen_at'] or 'N/A'}")
        print(f"    live:       {'yes' if record.get('is_live') else 'no'}")
        print(f"    url:        {record['url'] or 'N/A'}")


def notify(title: str, message: str):
    if not shutil.which("terminal-notifier"):
        # Built-in macOS fallback; json.dumps yields a valid AppleScript string literal.
        script = f"display notification {json.dumps(message, ensure_ascii=False)} with title {json.dumps(title, ensure_ascii=False)} sound name \"Glass\""
        try:
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
        except Exception:
            pass
        return
    cmd = [
        "terminal-notifier",
        "-title",
        title,
        "-message",
        message,
        "-sound",
        "Glass",
        "-group",
        "job-hunter",
        "-sender",
        "com.apple.Terminal",
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=5)
    except Exception:
        pass


def notify_new_jobs(new_jobs: list[Job]):
    if not new_jobs:
        return
    if len(new_jobs) == 1:
        job = new_jobs[0]
        notify("Job Hunter: New job found", f"{job.company} | {job.title}")
        return
    companies = ", ".join(sorted({job.company for job in new_jobs}))[:80]
    notify(f"Job Hunter: {len(new_jobs)} new jobs found", f"Companies: {companies}")


def open_job_in_browser(job: Job):
    if not job.url:
        return
    try:
        webbrowser.open(job.url, new=2)
    except Exception:
        pass


def clean_phrase_list(values) -> list[str]:
    """Normalize a user-supplied list (or single string) of search phrases, dropping blanks and duplicates."""
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return []
    phrases = [str(value).strip() for value in values if str(value).strip()]
    return list(dict.fromkeys(phrases))


def clean_slugs(values) -> list[str]:
    if not isinstance(values, list):
        return []
    slugs = []
    for value in values:
        if not isinstance(value, str):
            continue
        slug = value.strip()
        if slug:
            slugs.append(slug)
    return list(dict.fromkeys(slugs))


def load_slug_cache(cache_file: Path) -> list[str]:
    return clean_slugs(load_json(cache_file, []))


def fetch_slugs(platform: str, force_refresh: bool = False) -> tuple[list[str], str]:
    cache_file = CACHE_DIR / f"{platform}_companies.json"
    cached_slugs = load_slug_cache(cache_file) if cache_file.exists() else []

    if not force_refresh and cached_slugs:
        age_days = (time.time() - cache_file.stat().st_mtime) / 86400
        if age_days < CACHE_TTL_DAYS:
            return cached_slugs, "reused"

    existed = cache_file.exists()
    try:
        response = get_session().get(SLUG_URLS[platform], timeout=(5, 30))
        response.raise_for_status()
        slugs = clean_slugs(response.json())
        if not slugs:
            raise ValueError("upstream slug list was empty or invalid")
    except Exception:
        if cached_slugs:
            return cached_slugs, "reused-stale"
        raise

    save_json(cache_file, slugs)
    return slugs, "updated" if existed else "initialized"


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "job-hunter/1.0 (+local job search)",
        }
    )
    return session


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        THREAD_LOCAL.session = session
    return session


def fetch_greenhouse(session: requests.Session, slug: str, limiter: RateLimiter) -> list[Job]:
    limiter.wait()
    response = session.get(
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
        timeout=REQUEST_TIMEOUT["greenhouse"],
    )
    if response.status_code == 404:
        raise DeadSlugError(slug)
    response.raise_for_status()
    jobs = []
    for payload in response.json().get("jobs", []):
        location = payload.get("location", {})
        jobs.append(
            Job(
                company=slug,
                title=payload.get("title", ""),
                location=location.get("name", "N/A") if isinstance(location, dict) else str(location),
                url=payload.get("absolute_url", ""),
                platform="greenhouse",
                company_name=str(payload.get("company_name") or ""),
                posted_at=iso_date(payload.get("first_published")),
            )
        )
    return jobs


def fetch_lever(session: requests.Session, slug: str, limiter: RateLimiter) -> list[Job]:
    limiter.wait()
    response = session.get(
        f"https://api.lever.co/v0/postings/{slug}?limit=500",
        timeout=REQUEST_TIMEOUT["lever"],
    )
    if response.status_code == 404:
        raise DeadSlugError(slug)
    response.raise_for_status()
    jobs = []
    for payload in response.json():
        categories = payload.get("categories", {})
        jobs.append(
            Job(
                company=slug,
                title=payload.get("text", ""),
                location=categories.get("location", "N/A"),
                url=payload.get("hostedUrl", ""),
                platform="lever",
                posted_at=iso_date(payload.get("createdAt")),
            )
        )
    return jobs


def fetch_ashby(session: requests.Session, slug: str, limiter: RateLimiter) -> list[Job]:
    limiter.wait()
    response = session.get(
        f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
        timeout=REQUEST_TIMEOUT["ashby"],
    )
    if response.status_code == 404:
        raise DeadSlugError(slug)
    response.raise_for_status()
    jobs = []
    for payload in response.json().get("jobs", []):
        jobs.append(
            Job(
                company=slug,
                title=payload.get("title", ""),
                location=payload.get("location", "N/A"),
                url=payload.get("jobUrl", ""),
                platform="ashby",
                posted_at=iso_date(payload.get("publishedAt")),
            )
        )
    return jobs


def _flatten_bamboohr_location(value) -> str:
    if isinstance(value, dict):
        city = str(value.get("city") or "").strip()
        state = str(value.get("state") or "").strip()
        joined = ", ".join(part for part in (city, state) if part)
        return joined
    if value:
        return str(value).strip()
    return ""


def _humanize_icims_title(value: str) -> str:
    words = unquote(value).replace("-", " ").replace("_", " ").split()
    return " ".join(ICIMS_TITLE_ACRONYMS.get(word.casefold(), word.title()) for word in words)


def _response_text(response) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str):
        return text
    content = getattr(response, "content", b"")
    if isinstance(content, str):
        return content
    encoding = getattr(response, "encoding", None) or "utf-8"
    try:
        return content.decode(encoding, errors="replace")
    except (AttributeError, LookupError):
        return ""


def _clean_location_text(value) -> str:
    if value is None:
        return ""
    text = " ".join(unescape(str(value)).split())
    text = re.sub(r"^(?:job\s+)?location(?:\(s\))?\s*:?\s*", "", text, flags=re.IGNORECASE)
    coded = re.fullmatch(r"(?:[A-Z]{2})-([A-Z]{2})-(.+)", text, flags=re.IGNORECASE)
    if coded:
        region = coded.group(1).upper()
        city = coded.group(2).replace("-", " ").strip()
        return f"{city}, {region}" if city else region
    return text


def _html_to_text(value: str) -> str:
    value = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", value)
    return _clean_location_text(re.sub(r"(?s)<[^>]+>", " ", value))


def _json_type_matches(value, expected: str) -> bool:
    if isinstance(value, list):
        return any(_json_type_matches(item, expected) for item in value)
    return str(value).casefold() == expected.casefold()


def _iter_json_objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_json_objects(child)


def _schema_address_location(value) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return _clean_location_text(value)
    if not isinstance(value, dict):
        return ""

    country = value.get("addressCountry")
    if isinstance(country, dict):
        country = country.get("name") or country.get("alternateName")
    parts = [
        value.get("addressLocality"),
        value.get("addressRegion"),
        value.get("postalCode"),
        country,
    ]
    location = ", ".join(_clean_location_text(part) for part in parts if _clean_location_text(part))
    if location:
        return location
    return _clean_location_text(value.get("name") or value.get("streetAddress"))


def _schema_place_location(value) -> str:
    if isinstance(value, list):
        locations = [_schema_place_location(item) for item in value]
        return "; ".join(location for location in locations if location)
    if isinstance(value, str):
        return _clean_location_text(value)
    if not isinstance(value, dict):
        return ""

    location = _schema_address_location(value.get("address"))
    if location:
        return location
    return _clean_location_text(value.get("name") or value.get("description"))


def _jobposting_location(value: dict) -> str:
    location = _schema_place_location(value.get("jobLocation"))
    if location:
        return location
    if str(value.get("jobLocationType") or "").casefold() == "telecommute":
        return "Remote"
    return ""


def _extract_icims_location_from_html(body: str) -> str:
    for match in re.finditer(r"(?is)<script\b([^>]*)>(.*?)</script>", body):
        if "ld+json" not in match.group(1).casefold():
            continue
        raw = unescape(match.group(2)).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for item in _iter_json_objects(payload):
            if _json_type_matches(item.get("@type"), "JobPosting"):
                location = _jobposting_location(item)
                if location:
                    return location

    for match in re.finditer(
        r"(?is)<([a-z0-9]+)\b([^>]*(?:class|id|data-[a-z0-9_-]+)=[\"'][^\"']*location[^\"']*[\"'][^>]*)>(.*?)</\1>",
        body,
    ):
        location = _html_to_text(match.group(3))
        if location and location.casefold() not in {"location", "job location", "job locations"}:
            return location

    page_text = _html_to_text(body)
    for pattern in (
        r"\b(?:job\s+)?location(?:\(s\))?\s*:?\s*((?:[A-Z]{2}-[A-Z]{2}-[A-Za-z][A-Za-z .'-]+)|(?:[A-Za-z][A-Za-z .'-]+,\s*[A-Z]{2}))",
        r"\b((?:[A-Z]{2})-[A-Z]{2}-[A-Za-z][A-Za-z .'-]+)\b",
    ):
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            return _clean_location_text(match.group(1))
    return ""


def _fetch_icims_job_location(session: requests.Session, job_url: str, limiter: RateLimiter) -> str:
    limiter.wait()
    response = session.get(
        job_url,
        timeout=REQUEST_TIMEOUT["icims"],
        headers=ICIMS_HTML_HEADERS,
    )
    if response.status_code == 404:
        return ""
    response.raise_for_status()
    return _extract_icims_location_from_html(_response_text(response))


def _workday_job_url(base_url: str, site_id: str, external_path: str) -> str:
    path = str(external_path or "").strip()
    if not path:
        return f"{base_url}/{site_id}"
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}/{site_id}{path}"


def fetch_bamboohr(session: requests.Session, slug: str, limiter: RateLimiter) -> list[Job]:
    limiter.wait()
    response = session.get(
        f"https://{slug}.bamboohr.com/careers/list",
        timeout=REQUEST_TIMEOUT["bamboohr"],
        headers={"Accept": "application/json", "User-Agent": BROWSER_UA},
        allow_redirects=False,
    )
    # Inactive tenants 302 to /login.php; active tenants 200 with JSON.
    if response.status_code == 404 or 300 <= response.status_code < 400:
        raise DeadSlugError(slug)
    response.raise_for_status()
    # Active tenants with no careers page still sometimes serve HTML here.
    if "application/json" not in response.headers.get("Content-Type", "").lower():
        raise DeadSlugError(slug)
    jobs = []
    for payload in response.json().get("result", []) or []:
        job_id = payload.get("id")
        if job_id is None:
            continue
        jobs.append(
            Job(
                company=slug,
                title=str(payload.get("jobOpeningName") or ""),
                location=_flatten_bamboohr_location(payload.get("location")),
                url=f"https://{slug}.bamboohr.com/careers/{job_id}",
                platform="bamboohr",
            )
        )
    return jobs


def fetch_icims(
    session: requests.Session,
    slug: str,
    limiter: RateLimiter,
    title_terms: list[str] | None = None,
    need_locations: bool = True,
) -> list[Job]:
    limiter.wait()
    response = session.get(
        f"https://careers-{slug}.icims.com/sitemap.xml",
        timeout=REQUEST_TIMEOUT["icims"],
        headers={"Accept": "application/xml", "User-Agent": BROWSER_UA},
    )
    if response.status_code == 404:
        raise DeadSlugError(slug)
    response.raise_for_status()
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as exc:
        raise DeadSlugError(slug) from exc
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    jobs = []
    for loc in root.findall(".//s:url/s:loc", ns):
        job_url = (loc.text or "").strip()
        path = urlparse(job_url).path
        if not job_url or "/jobs/" not in path or path.rstrip("/").endswith("/jobs/intro"):
            continue
        path_tail = path.split("/jobs/", 1)[1]
        parts = path_tail.split("/")
        if len(parts) < 2 or not parts[1]:
            continue
        title = _humanize_icims_title(parts[1])
        if not title:
            continue
        if title_terms is not None and not matches_normalized_phrase(title, title_terms):
            continue
        location = _fetch_icims_job_location(session, job_url, limiter) if need_locations else ""
        jobs.append(
            Job(
                company=slug,
                title=title,
                location=location,
                url=job_url,
                platform="icims",
            )
        )
    return jobs


def fetch_workday(session: requests.Session, slug: str, limiter: RateLimiter) -> list[Job]:
    parts = [part.strip() for part in slug.split("|")]
    if len(parts) != 3 or not all(parts):
        raise DeadSlugError(slug)
    company, wd, site_id = parts
    wd_token = wd.casefold()
    if not wd_token.startswith("wd") or not wd_token[2:].isdigit():
        raise DeadSlugError(slug)
    wd_num = wd_token[2:]
    base_url = f"https://{company}.wd{wd_num}.myworkdayjobs.com"
    api_url = f"{base_url}/wday/cxs/{company}/{site_id}/jobs"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": BROWSER_UA,
        "Origin": base_url,
        "Referer": f"{base_url}/{site_id}",
    }

    jobs = JobList()
    offset = 0
    observed_total: int | None = None
    for page in range(WORKDAY_MAX_PAGES):
        limiter.wait()
        payload = {
            "appliedFacets": {},
            "limit": WORKDAY_PAGE_SIZE,
            "offset": offset,
            "searchText": "",
        }
        response = session.post(
            api_url,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT["workday"],
        )
        if response.status_code in WORKDAY_DEAD_STATUS_CODES:
            if page == 0:
                raise DeadSlugError(slug)
            jobs.complete = False
            break
        if response.status_code != 200:
            if page == 0:
                response.raise_for_status()
            jobs.complete = False
            break
        data = response.json()
        postings = data.get("jobPostings") or []
        total = data.get("total")

        if observed_total is None:
            observed_total = total
        elif isinstance(total, int) and total != observed_total:
            # Workday sometimes lies mid-pagination when throttling.
            jobs.complete = False
            break

        if not postings:
            break

        for job in postings:
            external_path = job.get("externalPath") or ""
            jobs.append(
                Job(
                    company=company,
                    title=str(job.get("title") or ""),
                    location=str(job.get("locationsText") or ""),
                    url=_workday_job_url(base_url, site_id, external_path),
                    platform="workday",
                )
            )

        offset += WORKDAY_PAGE_SIZE
        if isinstance(observed_total, int) and offset >= observed_total:
            break
    else:
        # Hit the page cap before reading every posting.
        jobs.complete = False

    return jobs


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "bamboohr": fetch_bamboohr,
    "icims": fetch_icims,
    "workday": fetch_workday,
}


def contains_whole_phrase(normalized_value: str, phrase: str) -> bool:
    # Whole-word match so "it support" does not hit "audit support" or "credit administrator".
    return re.search(rf"(?<!\w){re.escape(phrase)}(?!\w)", normalized_value) is not None


def matches_normalized_phrase(value: str, phrases: list[str]) -> bool:
    normalized_value = normalize_text(value)
    return any(contains_whole_phrase(normalized_value, phrase) for phrase in phrases)


def matches_phrase(value: str, phrases: list[str]) -> bool:
    return matches_normalized_phrase(value, normalize_phrases(phrases))


def matches_title(title: str, phrases: list[str]) -> bool:
    return matches_phrase(title, phrases)


def matches_location(location: str, phrases: list[str]) -> bool:
    if not phrases:
        return True
    return matches_phrase(location, phrases)


def format_error(slug: str, exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    return f"{slug}: {exc.__class__.__name__}: {message}"


def scan_platform(
    platform: str,
    slugs: list[str],
    title_phrases: list[str],
    location_phrases: list[str],
    show_progress: bool = True,
) -> tuple[list[Job], ScanStats, set[str]]:
    started_at = time.perf_counter()
    fetcher = FETCHERS[platform]
    limiter = RateLimiter(MAX_RPS.get(platform, 0))
    workers = WORKERS.get(platform, 5)
    stats = ScanStats(platform=platform, total=len(slugs))
    title_terms = normalize_phrases(title_phrases)
    location_terms = normalize_phrases(location_phrases)
    icims_needs_locations = bool(location_terms)
    matches: list[Job] = []
    stale_slugs: set[str] = set()
    scanned = 0
    report_every = 100 if len(slugs) > 1000 else 25

    def process(slug: str):
        try:
            session = get_session()
            if fetcher is fetch_icims:
                return (
                    fetcher(session, slug, limiter, title_terms=title_terms, need_locations=icims_needs_locations),
                    slug,
                    None,
                    None,
                )
            return fetcher(session, slug, limiter), slug, None, None
        except DeadSlugError:
            return None, None, slug, None
        except Exception as exc:
            return None, None, None, format_error(slug, exc)

    def handle_result(result, successful_slug, dead_slug, error_message):
        nonlocal scanned
        scanned += 1

        if dead_slug:
            stats.stale_slugs += 1
            stale_slugs.add(dead_slug)
        elif error_message:
            stats.request_errors += 1
            if len(stats.sample_errors) < ERROR_PREVIEW_LIMIT:
                stats.sample_errors.append(error_message)
        else:
            stats.successful_fetches += 1
            # Only fully read boards count as checked; otherwise unseen tracked jobs would be wrongly closed.
            if getattr(result, "complete", True):
                stats.successful_slugs.add(successful_slug)
            else:
                stats.partial_fetches += 1
            for job in result:
                if matches_normalized_phrase(job.title, title_terms) and (
                    not location_terms or matches_normalized_phrase(job.location, location_terms)
                ):
                    matches.append(job)

        if show_progress and (scanned % report_every == 0 or scanned == stats.total):
            progress = (
                f"\r  [{platform}] {scanned}/{stats.total} scanned"
                f" | matches {len(matches)}"
                f" | stale {stats.stale_slugs}"
                f" | errors {stats.request_errors}"
            )
            print(progress, end="", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        slug_iter = iter(slugs)
        max_in_flight = max(1, workers * 2)
        futures = set()
        for _ in range(min(max_in_flight, len(slugs))):
            futures.add(pool.submit(process, next(slug_iter)))

        while futures:
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                handle_result(*future.result())
            for _ in range(len(done)):
                try:
                    slug = next(slug_iter)
                except StopIteration:
                    break
                futures.add(pool.submit(process, slug))

    if show_progress and stats.total:
        print("\r" + " " * 120 + "\r", end="", file=sys.stderr)
    stats.matches = len(matches)
    stats.elapsed_seconds = time.perf_counter() - started_at
    return matches, stats, stale_slugs


def request_failure_ratio(stats: ScanStats) -> float:
    attempts = stats.successful_fetches + stats.request_errors
    if attempts == 0:
        return 0.0
    return stats.request_errors / attempts


def scan_stats_unreliable(stats: ScanStats) -> bool:
    return stats.request_errors > 0 and request_failure_ratio(stats) > MAX_REQUEST_FAILURE_RATIO


def load_dead_slugs() -> dict[str, dict[str, str]]:
    payload = load_json(DEAD_SLUGS_FILE, {})
    if not isinstance(payload, dict):
        return {}
    cleaned = {}
    for platform, slugs in payload.items():
        if isinstance(slugs, dict):
            cleaned[platform] = {slug: seen_at for slug, seen_at in slugs.items() if isinstance(seen_at, str)}
    return cleaned


def save_dead_slugs(dead_slugs: dict[str, dict[str, str]], file_events: list[tuple[str, Path]] | None = None):
    save_json(DEAD_SLUGS_FILE, dead_slugs)
    if file_events is not None:
        file_events.append(("updated", DEAD_SLUGS_FILE))


def prune_dead_slugs(dead_slugs: dict[str, dict[str, str]]) -> bool:
    cutoff = datetime.now() - timedelta(days=DEAD_SLUG_TTL_DAYS)
    changed = False
    for platform in list(dead_slugs):
        fresh = {}
        for slug, seen_at in dead_slugs[platform].items():
            try:
                if datetime.fromisoformat(seen_at) >= cutoff:
                    fresh[slug] = seen_at
            except ValueError:
                continue
        if fresh:
            if fresh != dead_slugs[platform]:
                changed = True
            dead_slugs[platform] = fresh
        else:
            changed = True
            dead_slugs.pop(platform, None)
    return changed


def acquire_run_lock():
    CACHE_DIR.mkdir(exist_ok=True)
    handle = LOCK_FILE.open("w")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.write(now_iso())
    handle.flush()
    return handle


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job Hunter</title>
  <style>
    :root {
      --bg: #f7f8fa;
      --surface: #ffffff;
      --surface-soft: #f1f4f6;
      --ink: #202327;
      --muted: #65707a;
      --line: #dfe4ea;
      --primary: #0f766e;
      --primary-strong: #115e59;
      --pending: #9a5b00;
      --seen: #3b5ba9;
      --applied: #24713d;
      --dismissed: #a23b34;
      --closed: #6f7780;
      --new: #a15c00;
      --focus: rgba(15, 118, 110, 0.28);
    }

    * {
      box-sizing: border-box;
      letter-spacing: 0;
    }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    button,
    input {
      font: inherit;
    }

    .shell {
      width: min(1180px, calc(100% - 32px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }

    .topbar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--line);
    }

    h1,
    h2,
    p {
      margin: 0;
    }

    h1 {
      font-size: 2.1rem;
      line-height: 1.05;
      font-weight: 760;
    }

    .queue-line {
      margin-top: 8px;
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.45;
    }

    .scan-stamp {
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.4;
      text-align: right;
      white-space: nowrap;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 18px;
    }

    .metric {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      padding: 14px;
    }

    .metric-label {
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
    }

    .metric-count {
      margin-top: 8px;
      font-size: 1.8rem;
      line-height: 1;
      font-weight: 760;
    }

    .metric-note {
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.35;
    }

    .controls {
      display: grid;
      grid-template-columns: minmax(260px, 1fr);
      gap: 12px;
      margin-top: 18px;
    }

    .searchbar {
      display: grid;
      gap: 7px;
    }

    label,
    .filter-label {
      color: var(--muted);
      font-size: 0.85rem;
      font-weight: 700;
    }

    .search-line {
      display: flex;
      gap: 8px;
    }

    .search {
      width: 100%;
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      padding: 12px 13px;
      font-size: 1rem;
    }

    .search:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 4px var(--focus);
      outline: none;
    }

    .filter-block {
      display: grid;
      gap: 8px;
    }

    .filter-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .chip,
    .utility,
    .btn {
      appearance: none;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 700;
      text-decoration: none;
      transition: border-color 0.12s ease, background 0.12s ease, color 0.12s ease, opacity 0.12s ease;
    }

    .chip {
      border: 1px solid var(--line);
      background: var(--surface);
      color: var(--ink);
      padding: 9px 11px;
      font-size: 0.9rem;
    }

    .chip:hover,
    .utility:hover,
    .btn-secondary:hover {
      border-color: #b8c2cc;
      background: #eef2f5;
    }

    .chip.active {
      border-color: var(--primary);
      background: var(--primary);
      color: #ffffff;
    }

    .chip-count {
      margin-left: 6px;
      opacity: 0.8;
    }

    .utility {
      border: 1px solid var(--line);
      background: var(--surface);
      color: var(--muted);
      padding: 0 12px;
      white-space: nowrap;
    }

    .utility.hidden {
      display: none;
    }

    .list-panel {
      margin-top: 22px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      overflow: hidden;
    }

    .list-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      padding: 16px;
      border-bottom: 1px solid var(--line);
      background: #fbfcfd;
    }

    .list-title {
      font-size: 1.1rem;
      font-weight: 760;
    }

    .view-stats {
      margin-top: 4px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.4;
    }

    .job-list {
      min-height: 150px;
    }

    .job-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 16px;
      align-items: center;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
    }

    .job-row:last-child {
      border-bottom: 0;
    }

    .job-main {
      min-width: 0;
    }

    .job-kicker {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 7px;
      margin-bottom: 5px;
    }

    .company {
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 760;
    }

    .job-title {
      color: var(--ink);
      font-size: 1rem;
      line-height: 1.35;
      font-weight: 740;
      overflow-wrap: anywhere;
    }

    .location {
      margin-top: 5px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 6px 12px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.8rem;
      line-height: 1.35;
    }

    .meta-item {
      white-space: nowrap;
    }

    .markers {
      display: inline-flex;
      flex-wrap: wrap;
      gap: 6px;
    }

    .marker {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 8px;
      padding: 3px 7px;
      background: var(--surface-soft);
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 760;
    }

    .marker.pending { color: var(--pending); background: #fff4df; }
    .marker.seen { color: var(--seen); background: #eef3ff; }
    .marker.applied { color: var(--applied); background: #eaf7ee; }
    .marker.dismissed { color: var(--dismissed); background: #fff0ef; }
    .marker.live { color: var(--applied); background: #eaf7ee; }
    .marker.closed { color: var(--closed); background: #eef1f4; }
    .marker.new { color: var(--new); background: #fff4df; }

    .actions {
      display: flex;
      justify-content: flex-end;
      flex-wrap: wrap;
      gap: 7px;
      min-width: 320px;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      border: 1px solid transparent;
      padding: 8px 11px;
      font-size: 0.86rem;
      line-height: 1;
      white-space: nowrap;
    }

    .btn-open {
      background: var(--primary);
      color: #ffffff;
    }

    .btn-open:hover {
      background: var(--primary-strong);
    }

    .btn-secondary {
      background: var(--surface-soft);
      border-color: var(--surface-soft);
      color: var(--ink);
    }

    .btn-dismissed {
      color: var(--dismissed);
    }

    .btn:disabled {
      cursor: wait;
      opacity: 0.5;
    }

    .empty {
      padding: 42px 16px;
      color: var(--muted);
      text-align: center;
      line-height: 1.45;
    }

    @media (max-width: 860px) {
      .shell {
        width: min(100% - 24px, 1180px);
        padding-top: 20px;
      }

      .topbar,
      .list-head {
        flex-direction: column;
      }

      .scan-stamp {
        text-align: left;
        white-space: normal;
      }

      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .job-row {
        grid-template-columns: 1fr;
      }

      .actions {
        justify-content: flex-start;
        min-width: 0;
      }
    }

    @media (max-width: 560px) {
      .metrics {
        grid-template-columns: 1fr;
      }

      .search-line {
        flex-direction: column;
      }

      .utility {
        min-height: 40px;
      }

      .actions {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .btn {
        width: 100%;
      }
    }
    /* ---- Hunt theme ---- */
    @media (prefers-color-scheme: dark) {
      :root {
        --bg: #0e1416;
        --surface: #151d20;
        --surface-soft: #1d272b;
        --ink: #e6ecee;
        --muted: #8d9ba1;
        --line: #26333a;
        --primary: #2bb3a3;
        --primary-strong: #3fc9b8;
        --pending: #e0a341;
        --seen: #7d9cf0;
        --applied: #5cc97d;
        --dismissed: #ec7a72;
        --closed: #8a949c;
        --new: #f0a54a;
        --focus: rgba(43, 179, 163, 0.35);
      }
      .list-head { background: var(--surface-soft) !important; }
      .chip:hover, .utility:hover, .btn-secondary:hover { background: #22303a !important; border-color: #33444d !important; }
    }

    body {
      background:
        radial-gradient(circle at 12% -10%, color-mix(in srgb, var(--primary) 14%, transparent), transparent 42%),
        radial-gradient(circle at 100% 0%, color-mix(in srgb, var(--new) 10%, transparent), transparent 35%),
        var(--bg);
      background-attachment: fixed;
    }

    .brand { display: flex; align-items: center; gap: 16px; }

    .radar {
      position: relative;
      flex: none;
      width: 78px;
      height: 78px;
      border-radius: 50%;
      border: 2px solid color-mix(in srgb, var(--primary) 55%, transparent);
      background:
        radial-gradient(circle, transparent 0 30%, color-mix(in srgb, var(--primary) 22%, transparent) 30.5% 31.5%, transparent 32% 62%, color-mix(in srgb, var(--primary) 22%, transparent) 62.5% 63.5%, transparent 64%),
        color-mix(in srgb, var(--primary) 7%, var(--surface));
      overflow: hidden;
    }
    .radar::before {
      content: "";
      position: absolute;
      inset: 0;
      border-radius: 50%;
      background: conic-gradient(from 0deg, color-mix(in srgb, var(--primary) 55%, transparent), transparent 70deg);
      animation: sweep 3.2s linear infinite;
    }
    .radar::after {
      content: "";
      position: absolute;
      left: 50%; top: 50%;
      width: 6px; height: 6px;
      margin: -3px 0 0 -3px;
      border-radius: 50%;
      background: var(--primary);
    }
    .blip {
      position: absolute;
      width: 7px; height: 7px;
      margin: -3.5px 0 0 -3.5px;
      border-radius: 50%;
      background: var(--new);
      box-shadow: 0 0 8px var(--new);
      animation: blip 3.2s ease-in-out infinite;
    }
    @keyframes sweep { to { transform: rotate(360deg); } }
    @keyframes blip { 0%, 100% { opacity: 0.25; transform: scale(0.8); } 50% { opacity: 1; transform: scale(1.25); } }

    .rank {
      display: grid;
      gap: 6px;
      min-width: 210px;
      margin-top: 10px;
      text-align: right;
    }
    .rank-name { font-weight: 760; font-size: 1rem; }
    .rank-emoji { font-size: 1.15rem; margin-right: 4px; }
    .xp-bar { height: 8px; border-radius: 99px; background: var(--surface-soft); border: 1px solid var(--line); overflow: hidden; }
    .xp-fill { height: 100%; border-radius: 99px; background: linear-gradient(90deg, var(--primary), var(--new)); transition: width 0.6s cubic-bezier(.2,.8,.2,1); }
    .xp-note { color: var(--muted); font-size: 0.8rem; }

    .metric { position: relative; overflow: hidden; transition: transform 0.15s ease, box-shadow 0.15s ease; }
    .metric::before { content: ""; position: absolute; inset: 0 0 auto 0; height: 4px; background: var(--accent, var(--primary)); }
    .metric:hover { transform: translateY(-2px); box-shadow: 0 8px 22px rgba(0, 0, 0, 0.08); }
    .metric-icon { float: right; font-size: 1.3rem; opacity: 0.85; }

    .job-row { transition: background 0.15s ease, transform 0.25s ease, opacity 0.25s ease; }
    .job-row:hover { background: color-mix(in srgb, var(--primary) 5%, transparent); }
    .job-row.leaving { opacity: 0; transform: translateX(40px); }
    .marker.fresh { color: #fff; background: linear-gradient(90deg, #f05a28, #f0a54a); border-color: transparent; }

    .confetti {
      position: fixed;
      width: 8px; height: 12px;
      border-radius: 2px;
      pointer-events: none;
      z-index: 50;
      animation: confetti 1.1s cubic-bezier(.15,.7,.4,1) forwards;
    }
    @keyframes confetti {
      to { transform: translate(var(--dx), var(--dy)) rotate(var(--rot)); opacity: 0; }
    }

    .toast {
      position: fixed;
      left: 50%;
      bottom: 26px;
      transform: translate(-50%, 30px);
      opacity: 0;
      padding: 12px 18px;
      border-radius: 99px;
      background: var(--ink);
      color: var(--bg);
      font-weight: 700;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
      transition: transform 0.25s ease, opacity 0.25s ease;
      z-index: 60;
      pointer-events: none;
    }
    .toast.show { transform: translate(-50%, 0); opacity: 1; }

    .empty-art { display: block; font-size: 2.4rem; margin-bottom: 8px; }

    @media (prefers-reduced-motion: reduce) {
      .radar::before, .blip { animation: none; }
      .confetti { display: none; }
    }
    @media (max-width: 860px) { .rank { text-align: left; } }
  </style>
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div class="brand">
        <div class="radar" id="radar" aria-hidden="true"></div>
        <div>
          <h1>Job Hunter</h1>
          <p class="queue-line" id="queue-line">Loading jobs...</p>
        </div>
      </div>
      <div>
        <div class="scan-stamp" id="last-updated">Last scan: loading...</div>
        <div class="rank" id="rank"></div>
      </div>
    </header>

    <section class="metrics" id="summary" aria-label="Job summary"></section>

    <section class="controls" aria-label="Dashboard filters">
      <div class="searchbar">
        <label for="search">Search</label>
        <div class="search-line">
          <input id="search" class="search" type="search" placeholder="Company, title, location, platform">
          <button id="clear-search" class="utility hidden" type="button">Clear</button>
        </div>
      </div>

      <div class="filter-block">
        <div class="filter-label">Status</div>
        <div class="filter-row" id="status-filter"></div>
      </div>

      <div class="filter-block">
        <div class="filter-label">Listings</div>
        <div class="filter-row" id="live-filter"></div>
      </div>
    </section>

    <section class="list-panel">
      <div class="list-head">
        <div>
          <h2 class="list-title" id="list-title">Queue</h2>
          <p class="view-stats" id="view-stats">Preparing list...</p>
        </div>
      </div>
      <div class="job-list" id="board" aria-live="polite"></div>
    </section>
  </main>
  <div class="toast" id="toast" role="status"></div>

  <script>
    const STATUS_ORDER = ["pending", "seen", "applied", "dismissed"];
    const STATUS_LABELS = {
      pending: "Pending",
      seen: "Seen",
      applied: "Applied",
      dismissed: "Dismissed"
    };

    const STATUS_ACTIONS = {
      pending: ["seen", "applied", "dismissed"],
      seen: ["pending", "applied", "dismissed"],
      applied: ["pending", "seen", "dismissed"]
    };

    const ACTION_LABELS = {
      pending: "Pending",
      seen: "Seen",
      applied: "Applied",
      dismissed: "Dismiss"
    };

    const STATUS_FILTERS = [
      { value: "queue", label: "Queue" },
      { value: "pending", label: "Pending" },
      { value: "seen", label: "Seen" },
      { value: "applied", label: "Applied" },
      { value: "all", label: "All" }
    ];

    const LIVE_FILTERS = [
      { value: "live", label: "Live" },
      { value: "closed", label: "Closed" },
      { value: "all", label: "All" }
    ];

    const LIST_TITLES = {
      queue: "Queue",
      pending: "Pending",
      seen: "Seen",
      applied: "Applied",
      all: "All Jobs"
    };

    let state = {
      last_scan_at: null,
      status_counts: {},
      jobs: []
    };

    let activeStatus = "queue";
    let activeLiveFilter = "live";

    const summaryEl = document.getElementById("summary");
    const boardEl = document.getElementById("board");
    const searchEl = document.getElementById("search");
    const clearSearchEl = document.getElementById("clear-search");
    const updatedEl = document.getElementById("last-updated");
    const statusFilterEl = document.getElementById("status-filter");
    const liveFilterEl = document.getElementById("live-filter");
    const viewStatsEl = document.getElementById("view-stats");
    const queueLineEl = document.getElementById("queue-line");
    const listTitleEl = document.getElementById("list-title");

    searchEl.addEventListener("input", render);
    clearSearchEl.addEventListener("click", () => {
      searchEl.value = "";
      render();
      searchEl.focus();
    });

    function formatTimestamp(value) {
      if (!value) return "N/A";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit"
      });
    }

    function escapeHtml(value) {
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function plural(count, word) {
      return `${count} ${word}${count === 1 ? "" : "s"}`;
    }

    function queueCount() {
      const counts = state.status_counts || {};
      return counts.pending || 0;
    }

    function visibleJobCount() {
      return (state.jobs || []).length;
    }

    function visibleLiveCount() {
      return (state.jobs || []).filter((job) => job.is_live).length;
    }

    function visibleClosedCount() {
      return Math.max(0, visibleJobCount() - visibleLiveCount());
    }

    function liveQueueCount() {
      return (state.jobs || []).filter((job) => job.status === "pending" && job.is_live).length;
    }

    function countForStatusFilter(value) {
      const counts = state.status_counts || {};
      if (value === "queue") return queueCount();
      if (value === "all") return visibleJobCount();
      return counts[value] || 0;
    }

    function countForLiveFilter(value) {
      if (value === "live") return visibleLiveCount();
      if (value === "closed") return visibleClosedCount();
      return visibleJobCount();
    }

    function matchesQuery(job, query) {
      if (!query) return true;
      const haystack = [
        job.company,
        job.company_name,
        job.title,
        job.location,
        job.platform,
        job.status
      ].join(" ").toLowerCase();
      return haystack.includes(query);
    }

    function matchesStatusFilter(job) {
      if (activeStatus === "queue") return job.status === "pending";
      if (activeStatus === "all") return true;
      return job.status === activeStatus;
    }

    function matchesLiveFilter(job) {
      if (activeLiveFilter === "live") return !!job.is_live;
      if (activeLiveFilter === "closed") return !job.is_live;
      return true;
    }

    function filteredJobs() {
      const query = searchEl.value.trim().toLowerCase();
      return (state.jobs || []).filter(
        (job) => matchesQuery(job, query) && matchesStatusFilter(job) && matchesLiveFilter(job)
      );
    }

    const RANKS = [
      { min: 0, name: "Rookie Tracker", emoji: "🥾" },
      { min: 1, name: "Scout", emoji: "🧭" },
      { min: 5, name: "Ranger", emoji: "🏹" },
      { min: 15, name: "Pathfinder", emoji: "🗺️" },
      { min: 30, name: "Apex Hunter", emoji: "🦅" },
      { min: 50, name: "Legend of the Hunt", emoji: "👑" }
    ];
    const CHEERS = [
      "Arrow loosed! 🏹",
      "Application launched 🚀",
      "Another one in the bag 🎯",
      "The hunt is on 🔥",
      "Bold move. Respect. 💪",
      "Future you says thanks ✨"
    ];
    const EMPTY_LINES = [
      ["🌲", "The woods are quiet. Run a scan to flush out new jobs."],
      ["🏕️", "Camp is set and the queue is clear. Nice tracking."],
      ["🦉", "Nothing stirring here. Check back after the next scan."]
    ];
    const CONFETTI_COLORS = ["#2bb3a3", "#f0a54a", "#f05a28", "#7d9cf0", "#5cc97d", "#e6c84a"];
    const toastEl = document.getElementById("toast");
    const radarEl = document.getElementById("radar");
    const rankEl = document.getElementById("rank");
    let toastTimer = null;

    function hashString(value) {
      let hash = 2166136261;
      for (let i = 0; i < value.length; i++) {
        hash ^= value.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
      }
      return hash >>> 0;
    }

    function renderRadar() {
      const targets = (state.jobs || []).filter((job) => job.status === "pending" && job.is_live).slice(0, 14);
      radarEl.innerHTML = targets.map((job) => {
        const hash = hashString(job.id || "");
        const angle = (hash % 360) * Math.PI / 180;
        const radius = 14 + (hash >>> 9) % 22;
        const delay = ((hash >>> 4) % 32) / 10;
        const x = 50 + Math.cos(angle) * radius;
        const y = 50 + Math.sin(angle) * radius;
        return `<span class="blip" style="left:${x}%;top:${y}%;animation-delay:${delay}s"></span>`;
      }).join("");
    }

    function renderRank() {
      const applied = (state.status_counts || {}).applied || 0;
      let index = 0;
      RANKS.forEach((rank, i) => { if (applied >= rank.min) index = i; });
      const rank = RANKS[index];
      const next = RANKS[index + 1];
      const progress = next ? (applied - rank.min) / (next.min - rank.min) : 1;
      const note = next
        ? `${applied} applied · ${next.min - applied} more to ${next.name}`
        : `${applied} applied · max rank reached`;
      rankEl.innerHTML = `
        <div class="rank-name"><span class="rank-emoji">${rank.emoji}</span>${escapeHtml(rank.name)}</div>
        <div class="xp-bar"><div class="xp-fill" style="width:${Math.round(progress * 100)}%"></div></div>
        <div class="xp-note">${escapeHtml(note)}</div>
      `;
    }

    function isFresh(job) {
      if (!job.posted_at) return false;
      const posted = new Date(job.posted_at + "T00:00:00");
      return Date.now() - posted.getTime() < 7 * 86400000;
    }

    function showToast(message) {
      toastEl.textContent = message;
      toastEl.classList.add("show");
      clearTimeout(toastTimer);
      toastTimer = setTimeout(() => toastEl.classList.remove("show"), 2200);
    }

    function celebrate(rect) {
      const originX = rect.left + rect.width / 2;
      const originY = rect.top + rect.height / 2;
      for (let i = 0; i < 36; i++) {
        const piece = document.createElement("span");
        const angle = Math.random() * Math.PI * 2;
        const distance = 70 + Math.random() * 150;
        piece.className = "confetti";
        piece.style.left = `${originX}px`;
        piece.style.top = `${originY}px`;
        piece.style.background = CONFETTI_COLORS[i % CONFETTI_COLORS.length];
        piece.style.setProperty("--dx", `${Math.cos(angle) * distance}px`);
        piece.style.setProperty("--dy", `${Math.sin(angle) * distance - 60}px`);
        piece.style.setProperty("--rot", `${Math.random() * 720 - 360}deg`);
        document.body.appendChild(piece);
        setTimeout(() => piece.remove(), 1200);
      }
      showToast(CHEERS[Math.floor(Math.random() * CHEERS.length)]);
    }

    function buildMetric(label, count, note, icon, accent) {
      return `
        <article class="metric" style="--accent:${accent}">
          <div class="metric-label"><span class="metric-icon">${icon}</span>${escapeHtml(label)}</div>
          <div class="metric-count">${count}</div>
          <div class="metric-note">${escapeHtml(note)}</div>
        </article>
      `;
    }

    function renderSummary() {
      const counts = state.status_counts || {};
      summaryEl.innerHTML = [
        buildMetric("On the radar", liveQueueCount(), "Live and unreviewed", "🎯", "var(--new)"),
        buildMetric("Tracked", counts.seen || 0, "Reviewed, not resolved", "👣", "var(--seen)"),
        buildMetric("Applied", counts.applied || 0, "Arrows in flight", "🏹", "var(--applied)"),
        buildMetric("Let go", counts.dismissed || 0, "Dismissed and hidden", "🍃", "var(--dismissed)")
      ].join("");
    }

    function renderFilters() {
      statusFilterEl.innerHTML = STATUS_FILTERS.map((item) => `
        <button
          class="chip ${item.value === activeStatus ? "active" : ""}"
          type="button"
          data-filter-group="status"
          data-filter-value="${item.value}"
          aria-pressed="${item.value === activeStatus ? "true" : "false"}">
          ${escapeHtml(item.label)}<span class="chip-count">${countForStatusFilter(item.value)}</span>
        </button>
      `).join("");

      liveFilterEl.innerHTML = LIVE_FILTERS.map((item) => `
        <button
          class="chip ${item.value === activeLiveFilter ? "active" : ""}"
          type="button"
          data-filter-group="live"
          data-filter-value="${item.value}"
          aria-pressed="${item.value === activeLiveFilter ? "true" : "false"}">
          ${escapeHtml(item.label)}<span class="chip-count">${countForLiveFilter(item.value)}</span>
        </button>
      `).join("");

      document.querySelectorAll("button[data-filter-group]").forEach((button) => {
        button.addEventListener("click", () => {
          if (button.dataset.filterGroup === "status") {
            activeStatus = button.dataset.filterValue;
          } else {
            activeLiveFilter = button.dataset.filterValue;
          }
          render();
        });
      });
    }

    function buildMarkers(job) {
      const statusLabel = STATUS_LABELS[job.status] || job.status || "Pending";
      const markers = [
        `<span class="marker ${escapeHtml(job.status)}">${escapeHtml(statusLabel)}</span>`,
        job.is_live ? `<span class="marker live">Live</span>` : `<span class="marker closed">Closed</span>`,
        job.is_new ? `<span class="marker new">New</span>` : "",
        isFresh(job) ? `<span class="marker fresh">🔥 Fresh</span>` : ""
      ];
      return markers.join("");
    }

    function buildMeta(job) {
      const items = [
        job.platform || "",
        job.posted_at ? `Posted ${job.posted_at}` : "",
        job.first_seen_at ? `First ${formatTimestamp(job.first_seen_at)}` : "",
        job.last_seen_at ? `Seen ${formatTimestamp(job.last_seen_at)}` : ""
      ].filter(Boolean);
      if (!items.length) return "";
      const metaItems = items.map((item) => `<span class="meta-item">${escapeHtml(item)}</span>`).join("");
      return `<div class="meta">${metaItems}</div>`;
    }

    function buildJobRow(job) {
      const encodedId = encodeURIComponent(job.id);
      const openButton = job.url
        ? `<a class="btn btn-open" href="${escapeHtml(job.url)}" target="_blank" rel="noreferrer">Open</a>`
        : "";
      const actionButtons = (STATUS_ACTIONS[job.status] || [])
        .map((status) => `
          <button
            class="btn btn-secondary ${status === "dismissed" ? "btn-dismissed" : ""}"
            type="button"
            data-job-id="${encodedId}"
            data-status="${status}">
            ${escapeHtml(ACTION_LABELS[status])}
          </button>
        `)
        .join("");

      return `
        <article class="job-row">
          <div class="job-main">
            <div class="job-kicker">
              <span class="company">${escapeHtml(job.company_name || job.company || "Unknown company")}</span>
              <span class="markers">${buildMarkers(job)}</span>
            </div>
            <div class="job-title">${escapeHtml(job.title || "Untitled role")}</div>
            <div class="location">${escapeHtml(job.location || "N/A")}</div>
            ${buildMeta(job)}
          </div>
          <div class="actions">
            ${openButton}
            ${actionButtons}
          </div>
        </article>
      `;
    }

    function emptyMessage() {
      if (!(state.jobs || []).length) {
        return EMPTY_LINES[0];
      }
      if (activeStatus === "queue" && !searchEl.value.trim()) {
        return EMPTY_LINES[1];
      }
      return ["🔍", "No tracks match this view. Try a different search."];
    }

    function renderBoard() {
      const jobs = filteredJobs();
      const query = searchEl.value.trim();
      const scopeLabel = activeLiveFilter === "live"
        ? "live"
        : activeLiveFilter === "closed"
          ? "closed"
          : "total";

      boardEl.innerHTML = jobs.length
        ? jobs.map(buildJobRow).join("")
        : (() => {
          const [art, text] = emptyMessage();
          return `<div class="empty"><span class="empty-art">${art}</span>${escapeHtml(text)}</div>`;
        })();

      boardEl.querySelectorAll("button[data-job-id]").forEach((button) => {
        button.addEventListener("click", async () => {
          button.disabled = true;
          const rect = button.getBoundingClientRect();
          const status = button.dataset.status;
          if (status === "dismissed") {
            button.closest(".job-row")?.classList.add("leaving");
            await new Promise((resolve) => setTimeout(resolve, 220));
          }
          try {
            await updateStatus(decodeURIComponent(button.dataset.jobId), status);
            if (status === "applied") celebrate(rect);
          } catch (error) {
            button.disabled = false;
            button.closest(".job-row")?.classList.remove("leaving");
            window.alert(error.message || "Failed to update job status.");
          }
        });
      });

      listTitleEl.textContent = LIST_TITLES[activeStatus] || "Jobs";
      viewStatsEl.textContent = query
        ? `${plural(jobs.length, "job")} found in ${scopeLabel} listings for "${query}".`
        : `${plural(jobs.length, "job")} in ${scopeLabel} listings.`;
      clearSearchEl.classList.toggle("hidden", !query);
    }

    function renderQueueLine() {
      const liveQueue = liveQueueCount();
      if (liveQueue > 0) {
        queueLineEl.textContent = `${plural(liveQueue, "fresh track")} spotted. Time to hunt.`;
        return;
      }
      if (queueCount() > 0) {
        queueLineEl.textContent = `${plural(queueCount(), "unreviewed job")} in the queue, none currently live.`;
        return;
      }
      queueLineEl.textContent = "All quiet in the woods. Queue cleared.";
    }

    function render() {
      renderSummary();
      renderFilters();
      renderBoard();
      renderQueueLine();
      renderRadar();
      renderRank();
      updatedEl.textContent = state.last_scan_at
        ? `Last scan: ${formatTimestamp(state.last_scan_at)}`
        : "Last scan: not run yet";
    }

    async function loadState() {
      const response = await fetch("/api/jobs");
      if (!response.ok) {
        throw new Error("Failed to load dashboard data.");
      }
      state = await response.json();
      render();
    }

    async function updateStatus(jobId, status) {
      const response = await fetch("/api/jobs/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: jobId, status })
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to update job status.");
      }
      state = await response.json();
      render();
    }

    loadState().catch((error) => {
      updatedEl.textContent = error.message || "Failed to load dashboard.";
      queueLineEl.textContent = "Dashboard failed to load.";
    });
  </script>
</body>
</html>
"""


def build_dashboard_handler():
    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            return

        def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_html(self, body: str):
            data = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path in {"/", "/index.html"}:
                self.send_html(DASHBOARD_HTML)
                return
            if self.path == "/api/jobs":
                store = ensure_job_store([])
                self.send_json(dashboard_payload(store))
                return
            if self.path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self):
            if self.path != "/api/jobs/status":
                self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return

            content_length = int(self.headers.get("Content-Length", "0") or 0)
            try:
                payload = json.loads(self.rfile.read(content_length) or "{}")
            except json.JSONDecodeError:
                self.send_json({"error": "Invalid JSON body"}, status=HTTPStatus.BAD_REQUEST)
                return

            job_id = str(payload.get("job_id", "")).strip()
            status = str(payload.get("status", "")).strip()
            if not job_id or not status:
                self.send_json({"error": "job_id and status are required"}, status=HTTPStatus.BAD_REQUEST)
                return

            try:
                set_job_status(job_id, status)
            except ValueError as exc:
                self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            except KeyError:
                self.send_json({"error": "Unknown job id"}, status=HTTPStatus.NOT_FOUND)
                return

            self.send_json(dashboard_payload(ensure_job_store([])))

    return DashboardHandler


def pick_dashboard_port(host: str, preferred_port: int) -> int:
    for port in range(preferred_port, preferred_port + 15):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError("could not find an available dashboard port")


def serve_dashboard(host: str, port: int, open_browser: bool):
    actual_port = pick_dashboard_port(host, port)
    server = ThreadingHTTPServer((host, actual_port), build_dashboard_handler())
    url = f"http://{host}:{actual_port}"
    print_divider("Dashboard")
    print_kv("url", url)
    print_kv("state file", display_path(JOBS_STATE_FILE))
    print("  Press Ctrl+C to stop the dashboard server.")

    if open_browser:
        try:
            webbrowser.open(url, new=2)
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
        print("  Dashboard server stopped.")
    finally:
        server.server_close()


def parse_args():
    parser = argparse.ArgumentParser(description="Scan public job boards for the roles and locations you configure.")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Config file with title/location filters (default: config.yaml, else config.example.yaml).",
    )
    parser.add_argument(
        "--title",
        action="append",
        metavar="PHRASE",
        help="Job title phrase to search for. Repeat for several. Overrides title_phrases in the config.",
    )
    parser.add_argument(
        "--location",
        action="append",
        metavar="PHRASE",
        help="Location phrase to require. Repeat for several. Overrides location_phrases in the config.",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Create config.yaml from config.example.yaml so you can edit your own search terms, then exit.",
    )
    parser.add_argument(
        "--platform",
        choices=list(FETCHERS),
        action="append",
        help="Limit the scan to specific platform(s). Repeat the flag to include multiple platforms.",
    )
    parser.add_argument("--refresh", action="store_true", help="Force refresh company slug caches from GitHub.")
    parser.add_argument("--limit", type=int, default=0, help="Max companies per platform to scan (0 = all).")
    parser.add_argument("--fast", action="store_true", help="Scan greenhouse, lever, and ashby only (skips slower workday, icims, bamboohr).")
    parser.add_argument("--sequential", action="store_true", help="Scan platforms one at a time.")
    parser.add_argument("--all", action="store_true", help="Print all current scan matches at the end of the run.")
    parser.add_argument("--history", action="store_true", help="Print all tracked jobs from the local JSON store and exit.")
    parser.add_argument("--dashboard", action="store_true", help="Serve the local dashboard over HTTP and exit only when stopped.")
    parser.add_argument("--port", type=int, default=DEFAULT_DASHBOARD_PORT, help=f"Dashboard port (default: {DEFAULT_DASHBOARD_PORT}).")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open the dashboard in a browser.")
    parser.add_argument("--no-notify", action="store_true", help="Suppress desktop notifications when new jobs are found.")
    parser.add_argument("--open-first", action="store_true", help="Open the first newly discovered job in your browser.")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.init:
        if CONFIG_PATH.exists():
            print(f"{CONFIG_PATH.name} already exists. Edit it to change your search.")
        else:
            shutil.copyfile(EXAMPLE_CONFIG_PATH, CONFIG_PATH)
            print(f"Created {CONFIG_PATH.name}. Open it and set your job titles and locations.")
        return

    file_events: list[tuple[str, Path]] = []
    ensure_runtime_dirs(file_events)
    cleanup_obsolete_state(file_events)

    if args.dashboard:
        ensure_job_store(file_events)
        print_file_events(file_events)
        serve_dashboard(DEFAULT_DASHBOARD_HOST, args.port, open_browser=not args.no_browser)
        return

    if args.history:
        ensure_job_store(file_events)
        print_file_events(file_events)
        show_history()
        return

    if args.config is None:
        args.config = CONFIG_PATH if CONFIG_PATH.exists() else EXAMPLE_CONFIG_PATH
    if not args.config.exists():
        print(f"Config not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    try:
        config = yaml.safe_load(args.config.read_text()) or {}
    except yaml.YAMLError as exc:
        print(f"Could not parse {args.config}: {exc}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(config, dict):
        print(f"Invalid config format: {args.config}", file=sys.stderr)
        sys.exit(1)

    title_phrases = clean_phrase_list(args.title if args.title else config.get("title_phrases"))
    location_phrases = clean_phrase_list(args.location if args.location else config.get("location_phrases"))
    extras = config.get("extra_companies") or []
    if not title_phrases:
        print(
            "No job titles to search for. Add title_phrases to config.yaml (run `python3 job_hunter.py --init`) "
            "or pass --title \"Your Job Title\".",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.platform:
        platforms = args.platform
    elif args.fast:
        platforms = ["greenhouse", "lever", "ashby"]
    else:
        platforms = list(FETCHERS)
    sequential_platforms = args.sequential or args.platform or args.fast or len(platforms) <= 1
    platform_concurrency = 1 if sequential_platforms else min(2, len(platforms))

    run_lock = acquire_run_lock()
    if run_lock is None:
        print("Another Job Hunter scan is already running. Exiting.", file=sys.stderr)
        return

    print_divider("Job Hunter Scan")
    print_kv("started", datetime.now().strftime("%Y-%m-%d %I:%M:%S %p"))
    print_kv("config", display_path(args.config))
    print_kv("platforms", ", ".join(platforms))
    print_kv("scan mode", "sequential" if sequential_platforms else f"parallel ({platform_concurrency} at a time)")
    print_kv("title phrases", len(title_phrases))
    print_kv("location phrases", len(location_phrases))
    if args.limit > 0:
        print_kv("per-platform limit", args.limit)

    dead_slugs = load_dead_slugs()
    dead_slugs_changed = prune_dead_slugs(dead_slugs)
    dead_slugs_by_platform = {platform: set(dead_slugs.get(platform, {})) for platform in platforms}

    all_matches: list[Job] = []
    scan_stats: list[ScanStats] = []
    slug_fetch_errors: list[str] = []

    def scan_one_platform(platform: str, verbose: bool, show_progress: bool) -> PlatformScanResult:
        platform_file_events: list[tuple[str, Path]] = []
        if verbose:
            print_divider(f"{platform.title()} Platform")
            print_kv("slug cache", display_path(CACHE_DIR / f"{platform}_companies.json"))

        try:
            slugs, cache_state = fetch_slugs(platform, force_refresh=args.refresh)
        except Exception as exc:
            slug_error = f"{platform}: {exc.__class__.__name__}: {exc}"
            return PlatformScanResult(platform, None, None, None, None, slug_error, [])

        cache_file = CACHE_DIR / f"{platform}_companies.json"
        if cache_state not in {"reused", "reused-stale"}:
            platform_file_events.append((cache_state, cache_file))
        if cache_state == "reused-stale":
            if verbose:
                print_kv("slug source", "cached fallback after refresh failure")

        extra_slugs = clean_slugs(
            [
                item.get("slug", "")
                for item in extras
                if isinstance(item, dict) and item.get("platform") == platform
            ]
        )
        slugs = list(dict.fromkeys(slugs + extra_slugs))
        if verbose:
            print_kv("companies available", len(slugs))

        skipped_stale = 0
        if not args.refresh:
            known_dead = dead_slugs_by_platform.get(platform, set())
            original_count = len(slugs)
            slugs = [slug for slug in slugs if slug not in known_dead]
            skipped_stale = original_count - len(slugs)

        if args.limit > 0:
            slugs = slugs[: args.limit]

        if verbose:
            print_kv("companies queued", len(slugs))
            print_kv("cached dead slugs", skipped_stale)
        if not slugs:
            stats = ScanStats(platform=platform, total=0, skipped_stale=skipped_stale)
            if verbose:
                print_kv("result", "nothing to scan")
            return PlatformScanResult(platform, [], stats, set(), cache_state, None, platform_file_events)

        if verbose:
            print("  scanning progress updates appear while requests complete...")
        matches, stats, stale_found = scan_platform(
            platform,
            slugs,
            title_phrases,
            location_phrases,
            show_progress=show_progress,
        )
        stats.skipped_stale = skipped_stale
        if verbose:
            print_kv("summary", format_scan_summary(stats))
        return PlatformScanResult(platform, matches, stats, stale_found, cache_state, None, platform_file_events)

    def record_scan_result(result: PlatformScanResult, print_errors: bool = True):
        nonlocal dead_slugs_changed
        file_events.extend(result.file_events)
        if result.slug_error:
            slug_fetch_errors.append(result.slug_error)
            if print_errors:
                print_kv(f"{result.platform} slug fetch", result.slug_error)
            return
        if result.stale_found:
            platform_dead = dead_slugs.setdefault(result.platform, {})
            timestamp = now_iso()
            for slug in result.stale_found:
                if platform_dead.get(slug) != timestamp:
                    platform_dead[slug] = timestamp
                    dead_slugs_changed = True
        stats = result.stats
        if stats is None:
            return
        if print_errors and stats.sample_errors:
            print("  sample request errors:")
            for error in stats.sample_errors:
                print(f"    {error}")
        scan_stats.append(stats)
        all_matches.extend(result.matches or [])

    try:
        if sequential_platforms:
            for platform in platforms:
                result = scan_one_platform(platform, verbose=True, show_progress=True)
                record_scan_result(result)
        else:
            print_divider("Platform Scans")
            print("  Parallel scan deck online. Worker progress is folded into the finish rows below.")
            print("  Legend: # ok | - stale/skipped | ! request errors")
            with ThreadPoolExecutor(max_workers=platform_concurrency) as pool:
                futures = {
                    pool.submit(scan_one_platform, platform, False, False): platform
                    for platform in platforms
                }
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    completed += 1
                    record_scan_result(result, print_errors=False)
                    print_platform_completion(completed, len(platforms), result)
    finally:
        if run_lock is not None:
            run_lock.close()

    scan_timestamp = now_iso()
    if dead_slugs_changed:
        save_dead_slugs(dead_slugs, file_events)

    scanned_platforms = {stats.platform for stats in scan_stats}
    scanned_companies = {
        checked_company_key(stats.platform, slug)
        for stats in scan_stats
        for slug in stats.successful_slugs
    }
    # Load the store only now, under the lock, so dashboard status changes made during the scan are kept.
    with job_store_lock():
        store = ensure_job_store(file_events)
        visible_matches, dismissed_matches = split_visible_and_dismissed_matches(store, all_matches)
        new_jobs, known_matches = apply_scan_results(
            store,
            visible_matches,
            scan_timestamp,
            scanned_platforms,
            scanned_companies,
            dismissed_matches,
        )
        save_job_store(store, file_events)

    report_file = append_daily_report(new_jobs, scan_timestamp)
    if report_file is not None:
        file_events.append(("updated", report_file))

    total_found = len(visible_matches)
    new_count = len(new_jobs)
    dismissed_hidden_count = len(dismissed_matches)
    total_ok = sum(stats.successful_fetches for stats in scan_stats)
    total_stale = sum(stats.stale_slugs + stats.skipped_stale for stats in scan_stats)
    total_request_errors = sum(stats.request_errors for stats in scan_stats)
    total_slug_fetch_errors = len(slug_fetch_errors)
    unreliable_platforms = [stats.platform for stats in scan_stats if scan_stats_unreliable(stats)]

    print_divider("Run Summary")
    print_kv("matches this run", total_found)
    print_kv("new jobs", new_count)
    print_kv("already known", known_matches)
    print_kv("hidden dismissed", dismissed_hidden_count)
    print_kv("slug fetch failures", total_slug_fetch_errors)
    print_kv("request failures", total_request_errors)
    total_partial = sum(stats.partial_fetches for stats in scan_stats)
    if total_partial:
        print_kv("partially read boards", total_partial)
    if unreliable_platforms:
        print_kv("unreliable platforms", ", ".join(unreliable_platforms))

    print_store_snapshot(store)
    print_file_events(file_events)

    if total_found == 0 and (
        total_slug_fetch_errors > 0 or (total_ok == 0 and (total_stale > 0 or total_request_errors > 0))
        or unreliable_platforms
    ):
        print_divider("Run Verdict")
        print("  Scan did not reach enough live job boards to trust this as a clean no-match result.")
        if total_slug_fetch_errors > 0:
            print(f"  {total_slug_fetch_errors} company slug fetches failed.")
        if total_stale > 0:
            print(f"  {total_stale} company slugs were skipped or marked stale.")
        if total_request_errors > 0:
            print(f"  {total_request_errors} live requests failed.")
        if unreliable_platforms:
            print(
                "  Platforms above the request failure threshold: "
                f"{', '.join(unreliable_platforms)} "
                f"(>{MAX_REQUEST_FAILURE_RATIO:.0%})."
            )
        sys.exit(2)

    if new_jobs:
        print_match_list("New Jobs", new_jobs)
    elif not visible_matches:
        print_divider("Result")
        print("  No matching jobs found in this run.")

    if args.all and visible_matches:
        print_match_list(
            "All Current Matches",
            sorted(visible_matches, key=lambda job: (job.platform, job.company.casefold(), job.title.casefold())),
        )

    if new_jobs and not args.no_notify:
        notify_new_jobs(new_jobs)

    if new_jobs and args.open_first:
        open_job_in_browser(new_jobs[0])

    print_divider("Next Step")
    print("  Review all tracked jobs with: python3 job_hunter.py --dashboard")


if __name__ == "__main__":
    main()
