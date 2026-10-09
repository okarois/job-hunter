import csv
import io
import json
import sys
import tempfile
import time
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import requests

import job_hunter


class JobHunterHelpersTests(unittest.TestCase):
    def test_job_key_falls_back_when_url_is_missing(self):
        job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="",
            platform="lever",
        )

        self.assertEqual(
            job_hunter.job_key(job),
            "lever|acme|IT Support Specialist|Remote",
        )

    def test_company_display_name_and_iso_date(self):
        self.assertEqual(job_hunter.company_display_name("acme-corp_ny"), "Acme Corp Ny")
        self.assertEqual(job_hunter.iso_date("2026-09-09T04:35:19-04:00"), "2026-09-09")
        self.assertEqual(job_hunter.iso_date(None), "")
        self.assertRegex(job_hunter.iso_date(1782214185805), r"^\d{4}-\d{2}-\d{2}$")

    def test_clean_phrase_list_accepts_strings_lists_and_junk(self):
        self.assertEqual(job_hunter.clean_phrase_list("Data Analyst"), ["Data Analyst"])
        self.assertEqual(job_hunter.clean_phrase_list([" A ", "", "A", 5]), ["A", "5"])
        self.assertEqual(job_hunter.clean_phrase_list(None), [])
        self.assertEqual(job_hunter.clean_phrase_list({"a": 1}), [])

    def test_example_config_is_valid_and_has_titles(self):
        import yaml

        config = yaml.safe_load(job_hunter.EXAMPLE_CONFIG_PATH.read_text())
        self.assertTrue(job_hunter.clean_phrase_list(config["title_phrases"]))
        self.assertTrue(job_hunter.clean_phrase_list(config["location_phrases"]))

    def test_title_matching_requires_whole_words(self):
        phrases = job_hunter.normalize_phrases(["IT Support", "IT Administrator", "IT Specialist", "Help Desk"])

        for title in ["IT Support Specialist", "Sr. IT Specialist", "Help Desk: Tier 1", "Desktop/IT Support"]:
            self.assertTrue(job_hunter.matches_normalized_phrase(title, phrases), title)
        for title in ["Audit Support Associate", "Credit Administrator", "Recruit Specialist"]:
            self.assertFalse(job_hunter.matches_normalized_phrase(title, phrases), title)

    def test_checked_company_key_normalizes_workday_composite_slug(self):
        self.assertEqual(
            job_hunter.checked_company_key("workday", "acme|wd1|acmecareers"),
            ("workday", "acme"),
        )
        self.assertEqual(
            job_hunter.checked_company_key("greenhouse", "acme"),
            ("greenhouse", "acme"),
        )

    def test_prune_dead_slugs_removes_expired_entries(self):
        now = datetime.now()
        dead_slugs = {
            "greenhouse": {
                "fresh": now.isoformat(),
                "stale": (now - timedelta(days=job_hunter.DEAD_SLUG_TTL_DAYS + 1)).isoformat(),
                "broken": "not-a-date",
            }
        }

        changed = job_hunter.prune_dead_slugs(dead_slugs)

        self.assertTrue(changed)
        self.assertEqual(list(dead_slugs["greenhouse"]), ["fresh"])

    def test_scan_platform_tracks_stale_slugs_and_errors(self):
        def fake_fetcher(_session, slug, _limiter):
            if slug == "dead":
                raise job_hunter.DeadSlugError(slug)
            if slug == "broken":
                raise requests.ConnectionError("boom")
            return [
                job_hunter.Job(
                    company=slug,
                    title="IT Support Specialist",
                    location="Remote",
                    url=f"https://example.com/{slug}",
                    platform="greenhouse",
                )
            ]

        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(job_hunter.FETCHERS, {"greenhouse": fake_fetcher}, clear=False))
            stack.enter_context(mock.patch.object(job_hunter, "get_session", return_value=object()))
            matches, stats, stale = job_hunter.scan_platform(
                "greenhouse",
                ["dead", "broken", "good"],
                ["IT Support"],
                ["Remote"],
            )

        self.assertEqual(len(matches), 1)
        self.assertEqual(stats.successful_fetches, 1)
        self.assertEqual(stats.stale_slugs, 1)
        self.assertEqual(stats.request_errors, 1)
        self.assertEqual(stale, {"dead"})
        self.assertEqual(len(stats.sample_errors), 1)
        self.assertEqual(stats.successful_slugs, {"good"})

    def test_scan_platform_keeps_bounded_in_flight_work(self):
        pending = 0
        max_pending = 0
        pending_lock = job_hunter.threading.Lock()
        real_executor = job_hunter.ThreadPoolExecutor

        class RecordingExecutor(real_executor):
            def submit(self, fn, *args, **kwargs):
                nonlocal pending, max_pending
                with pending_lock:
                    pending += 1
                    max_pending = max(max_pending, pending)

                def wrapped(*wrapped_args, **wrapped_kwargs):
                    nonlocal pending
                    try:
                        return fn(*wrapped_args, **wrapped_kwargs)
                    finally:
                        with pending_lock:
                            pending -= 1

                return super().submit(wrapped, *args, **kwargs)

        def fake_fetcher(_session, slug, _limiter):
            time.sleep(0.01)
            return [
                job_hunter.Job(
                    company=slug,
                    title="IT Support Specialist",
                    location="Remote",
                    url=f"https://example.com/{slug}",
                    platform="greenhouse",
                )
            ]

        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(job_hunter.FETCHERS, {"greenhouse": fake_fetcher}, clear=False))
            stack.enter_context(mock.patch.dict(job_hunter.WORKERS, {"greenhouse": 2}, clear=False))
            stack.enter_context(mock.patch.object(job_hunter, "ThreadPoolExecutor", RecordingExecutor))
            stack.enter_context(mock.patch.object(job_hunter, "get_session", return_value=object()))
            matches, stats, stale = job_hunter.scan_platform(
                "greenhouse",
                [f"company-{index}" for index in range(12)],
                ["IT Support"],
                ["Remote"],
            )

        self.assertEqual(len(matches), 12)
        self.assertEqual(stats.successful_fetches, 12)
        self.assertEqual(stale, set())
        self.assertLessEqual(max_pending, 4)

    def test_fetch_slugs_falls_back_to_cached_list_when_refresh_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            cache_dir = tmp / ".cache"
            cache_dir.mkdir()
            cache_file = cache_dir / "greenhouse_companies.json"
            cache_file.write_text(json.dumps(["cached-co", "", "cached-co"]))

            session = mock.Mock()
            session.get.side_effect = requests.ConnectionError("network down")

            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(job_hunter, "CACHE_DIR", cache_dir))
                stack.enter_context(mock.patch.object(job_hunter, "get_session", return_value=session))
                slugs, cache_state = job_hunter.fetch_slugs("greenhouse", force_refresh=True)

        self.assertEqual(slugs, ["cached-co"])
        self.assertEqual(cache_state, "reused-stale")

    def test_build_session_retries_idempotent_workday_post_requests(self):
        session = job_hunter.build_session()

        retry = session.adapters["https://"].max_retries

        self.assertIn("POST", retry.allowed_methods)

    def test_apply_scan_results_preserves_status_and_marks_missing_jobs_not_live(self):
        existing_job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="https://example.com/acme",
            platform="greenhouse",
        )
        old_job_id = job_hunter.job_key(existing_job)
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-08T10:00:00",
            "jobs": {
                old_job_id: {
                    "id": old_job_id,
                    "platform": "greenhouse",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "Remote",
                    "url": "https://example.com/acme",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "applied",
                    "status_updated_at": "2026-04-05T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                },
                "https://example.com/old": {
                    "id": "https://example.com/old",
                    "platform": "greenhouse",
                    "company": "oldco",
                    "title": "Desktop Support",
                    "location": "Remote",
                    "url": "https://example.com/old",
                    "first_seen_at": "2026-04-02T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "seen",
                    "status_updated_at": "2026-04-03T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                },
            },
        }

        fresh_job = job_hunter.Job(
            company="beta",
            title="Technical Support Engineer",
            location="Remote",
            url="https://example.com/beta",
            platform="greenhouse",
        )

        new_jobs, known_matches = job_hunter.apply_scan_results(
            store,
            [existing_job, fresh_job],
            "2026-04-09T12:00:00",
            {"greenhouse"},
        )

        self.assertEqual(known_matches, 1)
        self.assertEqual([job.url for job in new_jobs], ["https://example.com/beta"])
        self.assertEqual(store["jobs"][old_job_id]["status"], "applied")
        self.assertEqual(store["jobs"][old_job_id]["last_seen_at"], "2026-04-09T12:00:00")
        self.assertFalse(store["jobs"]["https://example.com/old"]["is_live"])

    def test_apply_scan_results_only_closes_jobs_for_successfully_checked_companies(self):
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-08T10:00:00",
            "jobs": {
                "https://example.com/acme": {
                    "id": "https://example.com/acme",
                    "platform": "greenhouse",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "Remote",
                    "url": "https://example.com/acme",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "pending",
                    "status_updated_at": "2026-04-01T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                },
                "https://example.com/broken": {
                    "id": "https://example.com/broken",
                    "platform": "greenhouse",
                    "company": "broken",
                    "title": "Desktop Support",
                    "location": "Remote",
                    "url": "https://example.com/broken",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "pending",
                    "status_updated_at": "2026-04-01T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                },
            },
        }

        job_hunter.apply_scan_results(
            store,
            [],
            "2026-04-09T12:00:00",
            {"greenhouse"},
            {("greenhouse", "acme")},
        )

        self.assertFalse(store["jobs"]["https://example.com/acme"]["is_live"])
        self.assertTrue(store["jobs"]["https://example.com/broken"]["is_live"])

    def test_apply_scan_results_closes_workday_jobs_for_checked_composite_slug(self):
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-08T10:00:00",
            "jobs": {
                "https://example.com/workday-old": {
                    "id": "https://example.com/workday-old",
                    "platform": "workday",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "New York, NY",
                    "url": "https://example.com/workday-old",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "pending",
                    "status_updated_at": "2026-04-01T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                }
            },
        }

        job_hunter.apply_scan_results(
            store,
            [],
            "2026-04-09T12:00:00",
            {"workday"},
            {job_hunter.checked_company_key("workday", "acme|wd1|acmecareers")},
        )

        self.assertFalse(store["jobs"]["https://example.com/workday-old"]["is_live"])
        self.assertEqual(store["jobs"]["https://example.com/workday-old"]["last_checked_at"], "2026-04-09T12:00:00")

    def test_split_visible_and_dismissed_matches_uses_status_and_fingerprint(self):
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-08T10:00:00",
            "jobs": {
                "https://example.com/old": {
                    "id": "https://example.com/old",
                    "platform": "greenhouse",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "Remote",
                    "url": "https://example.com/old",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "dismissed",
                    "status_updated_at": "2026-04-08T10:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-08T10:00:00",
                }
            },
        }
        exact = job_hunter.Job("acme", "IT Support Specialist", "Remote", "https://example.com/old", "greenhouse")
        same_role_new_url = job_hunter.Job(
            "acme",
            "IT Support Specialist",
            "Remote",
            "https://example.com/new",
            "greenhouse",
        )
        visible_role = job_hunter.Job("acme", "Desktop Support", "Remote", "https://example.com/desktop", "greenhouse")

        visible, dismissed = job_hunter.split_visible_and_dismissed_matches(
            store,
            [exact, same_role_new_url, visible_role],
        )

        self.assertEqual([job.url for job in visible], ["https://example.com/desktop"])
        self.assertEqual([job.url for job in dismissed], ["https://example.com/old", "https://example.com/new"])

    def test_apply_scan_results_tracks_dismissed_matches_without_reopening_them(self):
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-08T10:00:00",
            "jobs": {
                "https://example.com/old": {
                    "id": "https://example.com/old",
                    "platform": "greenhouse",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "Remote",
                    "url": "https://example.com/old",
                    "first_seen_at": "2026-04-01T10:00:00",
                    "last_seen_at": "2026-04-08T10:00:00",
                    "status": "dismissed",
                    "status_updated_at": "2026-04-08T10:00:00",
                    "is_live": False,
                    "last_checked_at": "2026-04-08T10:00:00",
                }
            },
        }
        dismissed_match = job_hunter.Job(
            "acme",
            "IT Support Specialist",
            "Remote",
            "https://example.com/new-url",
            "greenhouse",
        )

        new_jobs, known_matches = job_hunter.apply_scan_results(
            store,
            [],
            "2026-04-09T12:00:00",
            {"greenhouse"},
            {("greenhouse", "acme")},
            [dismissed_match],
        )

        self.assertEqual(new_jobs, [])
        self.assertEqual(known_matches, 0)
        self.assertNotIn("https://example.com/new-url", store["jobs"])
        self.assertTrue(store["jobs"]["https://example.com/old"]["is_live"])
        self.assertEqual(store["jobs"]["https://example.com/old"]["status"], "dismissed")
        self.assertEqual(store["jobs"]["https://example.com/old"]["last_seen_at"], "2026-04-09T12:00:00")

    def test_dashboard_payload_marks_new_jobs_and_counts_statuses(self):
        store = {
            "schema_version": job_hunter.STATE_SCHEMA_VERSION,
            "last_scan_at": "2026-04-09T12:00:00",
            "jobs": {
                "one": {
                    "id": "one",
                    "platform": "greenhouse",
                    "company": "acme",
                    "title": "IT Support Specialist",
                    "location": "Remote",
                    "url": "https://example.com/one",
                    "first_seen_at": "2026-04-09T12:00:00",
                    "last_seen_at": "2026-04-09T12:00:00",
                    "status": "pending",
                    "status_updated_at": "2026-04-09T12:00:00",
                    "is_live": True,
                    "last_checked_at": "2026-04-09T12:00:00",
                },
                "two": {
                    "id": "two",
                    "platform": "lever",
                    "company": "beta",
                    "title": "Desktop Support",
                    "location": "Remote",
                    "url": "https://example.com/two",
                    "first_seen_at": "2026-04-08T12:00:00",
                    "last_seen_at": "2026-04-09T12:00:00",
                    "status": "dismissed",
                    "status_updated_at": "2026-04-09T12:00:00",
                    "is_live": False,
                    "last_checked_at": "2026-04-09T12:00:00",
                },
            },
        }

        payload = job_hunter.dashboard_payload(store)

        self.assertEqual(payload["status_counts"]["pending"], 1)
        self.assertEqual(payload["status_counts"]["dismissed"], 1)
        self.assertEqual(payload["status_counts"]["live"], 1)
        self.assertEqual(payload["status_counts"]["closed"], 1)
        self.assertEqual([job["id"] for job in payload["jobs"]], ["one"])
        first_job = payload["jobs"][0]
        self.assertEqual(first_job["id"], "one")
        self.assertTrue(first_job["is_new"])


class JobHunterMainTests(unittest.TestCase):
    def workspace_patches(self, tmp: Path):
        return [
            mock.patch.object(job_hunter, "CACHE_DIR", tmp / ".cache"),
            mock.patch.object(job_hunter, "REPORTS_DIR", tmp / "reports"),
            mock.patch.object(job_hunter, "JOBS_STATE_FILE", tmp / ".cache" / "jobs_state.json"),
            mock.patch.object(job_hunter, "DEAD_SLUGS_FILE", tmp / ".cache" / "dead_slugs.json"),
            mock.patch.object(job_hunter, "LOCK_FILE", tmp / ".cache" / "job_hunter.lock"),
        ]

    def test_main_scan_persists_jobs_and_removes_obsolete_files(self):
        job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="https://example.com/job",
            platform="greenhouse",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text(
                "title_phrases:\n"
                "  - IT Support\n"
                "location_phrases:\n"
                "  - Remote\n"
                "extra_companies: []\n"
            )
            cache_dir = tmp / ".cache"
            cache_dir.mkdir()
            (cache_dir / "seen.json").write_text("{}")
            (cache_dir / "pending_jobs.json").write_text("[]")

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", return_value=(["acme"], "reused")))
                stack.enter_context(
                    mock.patch.object(
                        job_hunter,
                        "scan_platform",
                        return_value=(
                            [job],
                            job_hunter.ScanStats(platform="greenhouse", total=1, successful_fetches=1, matches=1),
                            set(),
                        ),
                    )
                )
                stack.enter_context(mock.patch.object(job_hunter, "notify_new_jobs"))
                stdout = stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
                stack.enter_context(
                    mock.patch.object(
                        sys,
                        "argv",
                        [
                            "job_hunter.py",
                            "--config",
                            str(config_path),
                            "--platform",
                            "greenhouse",
                            "--no-notify",
                        ],
                    )
                )

                job_hunter.main()

            store = json.loads((tmp / ".cache" / "jobs_state.json").read_text())
            records = list(store["jobs"].values())
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["status"], "pending")
            self.assertFalse((tmp / ".cache" / "seen.json").exists())
            self.assertFalse((tmp / ".cache" / "pending_jobs.json").exists())

            report_files = sorted((tmp / "reports").glob("*.csv"))
            self.assertEqual(len(report_files), 1)
            with report_files[0].open(newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(rows[0]["company"], "acme")
        self.assertIn("jobs_state.json", stdout.getvalue())

    def test_main_full_scan_runs_platforms_concurrently_and_aggregates_results(self):
        job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="https://example.com/job",
            platform="greenhouse",
        )
        started_platforms = []
        progress_flags = []
        started_lock = job_hunter.threading.Lock()
        both_started = job_hunter.threading.Event()

        def fake_fetch_slugs(platform, force_refresh=False):
            return ([f"{platform}-slug"], "reused")

        def fake_scan_platform(platform, slugs, title_phrases, location_phrases, show_progress=True):
            with started_lock:
                started_platforms.append(platform)
                progress_flags.append(show_progress)
                if len(started_platforms) == 2:
                    both_started.set()
            both_started.wait(timeout=1)
            if platform == "greenhouse":
                stats = job_hunter.ScanStats(platform="greenhouse", total=1, successful_fetches=1, matches=1)
                stats.successful_slugs = {"greenhouse-slug"}
                return [job], stats, set()
            stats = job_hunter.ScanStats(platform="lever", total=1, stale_slugs=1)
            return [], stats, {"lever-slug"}

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text(
                "title_phrases:\n"
                "  - IT Support\n"
                "location_phrases:\n"
                "  - Remote\n"
                "extra_companies: []\n"
            )

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(
                    mock.patch.dict(job_hunter.FETCHERS, {"greenhouse": object(), "lever": object()}, clear=True)
                )
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", side_effect=fake_fetch_slugs))
                stack.enter_context(mock.patch.object(job_hunter, "scan_platform", side_effect=fake_scan_platform))
                stack.enter_context(mock.patch.object(job_hunter, "notify_new_jobs"))
                stdout = stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
                stack.enter_context(
                    mock.patch.object(
                        sys,
                        "argv",
                        ["job_hunter.py", "--config", str(config_path), "--no-notify"],
                    )
                )

                job_hunter.main()

            store = json.loads((tmp / ".cache" / "jobs_state.json").read_text())
            dead_slugs = json.loads((tmp / ".cache" / "dead_slugs.json").read_text())

        self.assertTrue(both_started.is_set())
        self.assertEqual(set(started_platforms), {"greenhouse", "lever"})
        self.assertEqual(progress_flags, [False, False])
        self.assertEqual(len(store["jobs"]), 1)
        self.assertIn("lever-slug", dead_slugs["lever"])
        self.assertIn("Platform Scans", stdout.getvalue())
        self.assertNotIn("Greenhouse Platform", stdout.getvalue())
        self.assertIn("matches this run", stdout.getvalue())
        self.assertIn("new jobs                 1", stdout.getvalue())

    def test_main_sequential_preserves_platform_order(self):
        call_order = []

        def fake_fetch_slugs(platform, force_refresh=False):
            call_order.append(("fetch", platform))
            return ([f"{platform}-slug"], "reused")

        def fake_scan_platform(platform, slugs, title_phrases, location_phrases, show_progress=True):
            call_order.append(("scan", platform))
            stats = job_hunter.ScanStats(platform=platform, total=1, successful_fetches=1)
            stats.successful_slugs = set(slugs)
            return [], stats, set()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text("title_phrases:\n  - IT Support\nextra_companies: []\n")

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(
                    mock.patch.dict(
                        job_hunter.FETCHERS,
                        {"greenhouse": object(), "lever": object(), "ashby": object()},
                        clear=True,
                    )
                )
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", side_effect=fake_fetch_slugs))
                stack.enter_context(mock.patch.object(job_hunter, "scan_platform", side_effect=fake_scan_platform))
                stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
                stack.enter_context(
                    mock.patch.object(
                        sys,
                        "argv",
                        ["job_hunter.py", "--config", str(config_path), "--sequential", "--no-notify"],
                    )
                )

                job_hunter.main()

        self.assertEqual(
            call_order,
            [
                ("fetch", "greenhouse"),
                ("scan", "greenhouse"),
                ("fetch", "lever"),
                ("scan", "lever"),
                ("fetch", "ashby"),
                ("scan", "ashby"),
            ],
        )

    def test_main_keeps_dashboard_status_changes_made_during_scan(self):
        job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="https://example.com/job",
            platform="greenhouse",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text("title_phrases:\n  - IT Support\nlocation_phrases:\n  - Remote\n")
            state_file = tmp / ".cache" / "jobs_state.json"
            state_file.parent.mkdir()
            record = job_hunter.job_record_from_job(job, "2026-01-01T09:00:00")
            state_file.write_text(json.dumps({"schema_version": 1, "last_scan_at": None, "jobs": {job.url: record}}))

            def scan_while_user_applies(*_args, **_kwargs):
                job_hunter.set_job_status(job.url, "applied")
                return [job], job_hunter.ScanStats(platform="greenhouse", total=1, successful_fetches=1, matches=1), set()

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", return_value=(["acme"], "reused")))
                stack.enter_context(mock.patch.object(job_hunter, "scan_platform", side_effect=scan_while_user_applies))
                stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
                stack.enter_context(
                    mock.patch.object(
                        sys, "argv", ["job_hunter.py", "--config", str(config_path), "--platform", "greenhouse", "--no-notify"]
                    )
                )

                job_hunter.main()

            store = json.loads(state_file.read_text())

        self.assertEqual(store["jobs"][job.url]["status"], "applied")

    def _run_main(self, tmp, argv):
        with ExitStack() as stack:
            for patcher in self.workspace_patches(tmp):
                stack.enter_context(patcher)
            stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", return_value=(["acme"], "reused")))
            scan = stack.enter_context(
                mock.patch.object(
                    job_hunter,
                    "scan_platform",
                    return_value=([], job_hunter.ScanStats(platform="greenhouse", total=1, successful_fetches=1), set()),
                )
            )
            stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
            stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
            stack.enter_context(mock.patch.object(sys, "argv", ["job_hunter.py", "--platform", "greenhouse", "--no-notify", *argv]))
            job_hunter.main()
        return scan

    def test_title_and_location_flags_override_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "config.yaml"
            config_path.write_text("title_phrases:\n  - Config Title\nlocation_phrases:\n  - Config Place\n")
            scan = self._run_main(
                tmp,
                ["--config", str(config_path), "--title", "Nurse Practitioner", "--title", "RN", "--location", "Denver"],
            )

        args = scan.call_args.args
        self.assertEqual(args[2], ["Nurse Practitioner", "RN"])
        self.assertEqual(args[3], ["Denver"])

    def test_config_phrases_are_used_without_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "config.yaml"
            config_path.write_text("title_phrases: Barista\nlocation_phrases: []\n")
            scan = self._run_main(tmp, ["--config", str(config_path)])

        self.assertEqual(scan.call_args.args[2], ["Barista"])
        self.assertEqual(scan.call_args.args[3], [])

    def test_missing_titles_exit_with_helpful_message(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "config.yaml"
            config_path.write_text("location_phrases:\n  - Remote\n")
            with self.assertRaises(SystemExit) as raised:
                self._run_main(tmp, ["--config", str(config_path)])

        self.assertEqual(raised.exception.code, 1)

    def test_init_creates_config_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            target = tmp / "config.yaml"
            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(job_hunter, "CONFIG_PATH", target))
                stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch.object(sys, "argv", ["job_hunter.py", "--init"]))
                job_hunter.main()
                target.write_text("title_phrases:\n  - Mine\n")
                job_hunter.main()
            kept = target.read_text()

        self.assertIn("Mine", kept)

    def test_main_keeps_dismissed_matches_hidden_on_future_scans(self):
        dismissed_job = job_hunter.Job(
            company="acme",
            title="IT Support Specialist",
            location="Remote",
            url="https://example.com/job",
            platform="greenhouse",
        )
        job_id = job_hunter.job_key(dismissed_job)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text(
                "title_phrases:\n"
                "  - IT Support\n"
                "location_phrases:\n"
                "  - Remote\n"
                "extra_companies: []\n"
            )
            cache_dir = tmp / ".cache"
            cache_dir.mkdir()
            job_hunter.save_json(
                cache_dir / "jobs_state.json",
                {
                    "schema_version": job_hunter.STATE_SCHEMA_VERSION,
                    "last_scan_at": "2026-04-08T10:00:00",
                    "jobs": {
                        job_id: {
                            "id": job_id,
                            "platform": "greenhouse",
                            "company": "acme",
                            "title": "IT Support Specialist",
                            "location": "Remote",
                            "url": "https://example.com/job",
                            "first_seen_at": "2026-04-08T10:00:00",
                            "last_seen_at": "2026-04-08T10:00:00",
                            "status": "dismissed",
                            "status_updated_at": "2026-04-08T10:00:00",
                            "is_live": True,
                            "last_checked_at": "2026-04-08T10:00:00",
                        }
                    },
                },
            )

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", return_value=(["acme"], "reused")))
                stats = job_hunter.ScanStats(platform="greenhouse", total=1, successful_fetches=1, matches=1)
                stats.successful_slugs = {"acme"}
                stack.enter_context(
                    mock.patch.object(job_hunter, "scan_platform", return_value=([dismissed_job], stats, set()))
                )
                stack.enter_context(mock.patch.object(job_hunter, "notify_new_jobs"))
                stdout = stack.enter_context(mock.patch("sys.stdout", new=io.StringIO()))
                stack.enter_context(mock.patch("sys.stderr", new=io.StringIO()))
                stack.enter_context(
                    mock.patch.object(
                        sys,
                        "argv",
                        [
                            "job_hunter.py",
                            "--config",
                            str(config_path),
                            "--platform",
                            "greenhouse",
                            "--no-notify",
                        ],
                    )
                )

                job_hunter.main()

            store = json.loads((tmp / ".cache" / "jobs_state.json").read_text())
            report_files = sorted((tmp / "reports").glob("*.csv"))

        self.assertEqual(store["jobs"][job_id]["status"], "dismissed")
        self.assertTrue(store["jobs"][job_id]["is_live"])
        self.assertEqual(report_files, [])
        self.assertIn("hidden dismissed         1", stdout.getvalue())
        self.assertNotIn("New Jobs", stdout.getvalue())

    def test_set_job_status_updates_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            cache_dir = tmp / ".cache"
            cache_dir.mkdir()
            job_id = "https://example.com/job"
            store = {
                "schema_version": job_hunter.STATE_SCHEMA_VERSION,
                "last_scan_at": "2026-04-09T12:00:00",
                "jobs": {
                    job_id: {
                        "id": job_id,
                        "platform": "greenhouse",
                        "company": "acme",
                        "title": "IT Support Specialist",
                        "location": "Remote",
                        "url": job_id,
                        "first_seen_at": "2026-04-09T12:00:00",
                        "last_seen_at": "2026-04-09T12:00:00",
                        "status": "pending",
                        "status_updated_at": "2026-04-09T12:00:00",
                        "is_live": True,
                        "last_checked_at": "2026-04-09T12:00:00",
                    }
                },
            }

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                job_hunter.save_json(tmp / ".cache" / "jobs_state.json", store)
                updated = job_hunter.set_job_status(job_id, "applied")

            persisted = json.loads((tmp / ".cache" / "jobs_state.json").read_text())

        self.assertEqual(updated["status"], "applied")
        self.assertEqual(persisted["jobs"][job_id]["status"], "applied")

    def test_slug_fetch_failure_exits_unreliable_instead_of_clean_no_match(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            config_path = tmp / "companies.yaml"
            config_path.write_text("title_phrases:\n  - IT Support\nextra_companies: []\n")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with ExitStack() as stack:
                for patcher in self.workspace_patches(tmp):
                    stack.enter_context(patcher)
                stack.enter_context(mock.patch.object(job_hunter, "fetch_slugs", side_effect=RuntimeError("slug fetch failed")))
                stack.enter_context(mock.patch("sys.stdout", new=stdout))
                stack.enter_context(mock.patch("sys.stderr", new=stderr))
                stack.enter_context(
                    mock.patch.object(
                        sys,
                        "argv",
                        [
                            "job_hunter.py",
                            "--config",
                            str(config_path),
                            "--platform",
                            "greenhouse",
                            "--no-notify",
                        ],
                    )
                )

                with self.assertRaises(SystemExit) as exc:
                    job_hunter.main()

        self.assertEqual(exc.exception.code, 2)
        self.assertIn("did not reach enough live job boards", stdout.getvalue())
        self.assertNotIn("No matching jobs found in this run.", stdout.getvalue())

    def test_removed_cron_flag_is_rejected_by_argparse(self):
        with mock.patch.object(sys, "argv", ["job_hunter.py", "--install-cron"]):
            with self.assertRaises(SystemExit) as exc:
                job_hunter.parse_args()
        self.assertEqual(exc.exception.code, 2)


class FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, content=b"", headers=None):
        self.status_code = status_code
        self._json = json_data
        self.content = content
        self.headers = headers or {"Content-Type": "application/json"}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class FakeSession:
    def __init__(self, get_responses=None, post_responses=None):
        self._get = list(get_responses or [])
        self._post = list(post_responses or [])
        self.get_calls = []
        self.post_calls = []

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return self._get.pop(0)

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return self._post.pop(0)


class BambooHRFetcherTests(unittest.TestCase):
    def _limiter(self):
        return job_hunter.RateLimiter(0)

    def test_parses_result_list_and_flattens_location(self):
        session = FakeSession(get_responses=[
            FakeResponse(json_data={
                "result": [
                    {
                        "id": 101,
                        "jobOpeningName": "IT Support Specialist",
                        "location": {"city": "Melville", "state": "NY"},
                    },
                    {
                        "id": 102,
                        "jobOpeningName": "Sysadmin",
                        "location": "Remote",
                    },
                    {
                        "id": 103,
                        "jobOpeningName": "Help Desk",
                        "location": None,
                    },
                ]
            })
        ])

        jobs = job_hunter.fetch_bamboohr(session, "acme", self._limiter())

        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0].title, "IT Support Specialist")
        self.assertEqual(jobs[0].location, "Melville, NY")
        self.assertEqual(jobs[0].url, "https://acme.bamboohr.com/careers/101")
        self.assertEqual(jobs[0].platform, "bamboohr")
        self.assertEqual(jobs[1].location, "Remote")
        self.assertEqual(jobs[2].location, "")

    def test_json_content_type_check_is_case_insensitive(self):
        session = FakeSession(get_responses=[
            FakeResponse(
                json_data={"result": []},
                headers={"Content-Type": "Application/JSON; charset=utf-8"},
            )
        ])

        jobs = job_hunter.fetch_bamboohr(session, "acme", self._limiter())

        self.assertEqual(jobs, [])

    def test_html_content_type_is_dead_slug(self):
        session = FakeSession(get_responses=[
            FakeResponse(
                json_data={"result": []},
                headers={"Content-Type": "text/html; charset=utf-8"},
            )
        ])
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_bamboohr(session, "inactive", self._limiter())

    def test_404_is_dead_slug(self):
        session = FakeSession(get_responses=[FakeResponse(status_code=404)])
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_bamboohr(session, "missing", self._limiter())

    def test_302_redirect_to_login_is_dead_slug(self):
        # Inactive BambooHR tenants 302 /careers/list to /login.php.
        session = FakeSession(get_responses=[
            FakeResponse(
                status_code=302,
                headers={"Location": "https://inactive.bamboohr.com/login.php"},
            )
        ])
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_bamboohr(session, "inactive", self._limiter())
        # Confirm the fetcher disabled redirects so requests did not follow the 302.
        self.assertEqual(session.get_calls[0][1].get("allow_redirects"), False)

    def test_uses_canonical_careers_id_url_without_view_segment(self):
        session = FakeSession(get_responses=[
            FakeResponse(json_data={
                "result": [{"id": 58, "jobOpeningName": "IT Sys Admin", "location": "NY"}]
            })
        ])
        jobs = job_hunter.fetch_bamboohr(session, "lct", self._limiter())
        self.assertEqual(jobs[0].url, "https://lct.bamboohr.com/careers/58")


class ICIMSFetcherTests(unittest.TestCase):
    SITEMAP_XML = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        b'<url><loc>https://careers-acme.icims.com/jobs/intro</loc></url>'
        b'<url><loc>https://careers-acme.icims.com/jobs/intro/</loc></url>'
        b'<url><loc>https://careers-acme.icims.com/jobs/9620/it-support-specialist/job</loc></url>'
        b'<url><loc>https://careers-acme.icims.com/jobs/9721/desktop%20support%20analyst/job</loc></url>'
        b'<url><loc>https://careers-acme.icims.com/about</loc></url>'
        b'</urlset>'
    )
    JOB_PAGE_NY = (
        b'<html><script type="application/ld+json">'
        b'{"@context":"https://schema.org","@type":"JobPosting",'
        b'"jobLocation":{"@type":"Place","address":{"@type":"PostalAddress",'
        b'"addressLocality":"New York","addressRegion":"NY"}}}'
        b'</script></html>'
    )
    JOB_PAGE_REMOTE = (
        b'<html><script type="application/ld+json">'
        b'{"@context":"https://schema.org","@type":"JobPosting","jobLocationType":"TELECOMMUTE"}'
        b'</script></html>'
    )

    def _limiter(self):
        return job_hunter.RateLimiter(0)

    def test_sitemap_extracts_titles_and_hydrates_locations(self):
        session = FakeSession(get_responses=[
            FakeResponse(content=self.SITEMAP_XML, headers={"Content-Type": "application/xml"}),
            FakeResponse(content=self.JOB_PAGE_NY, headers={"Content-Type": "text/html"}),
            FakeResponse(content=self.JOB_PAGE_REMOTE, headers={"Content-Type": "text/html"}),
        ])

        jobs = job_hunter.fetch_icims(session, "acme", self._limiter())

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].title, "IT Support Specialist")
        self.assertEqual(jobs[0].url, "https://careers-acme.icims.com/jobs/9620/it-support-specialist/job")
        self.assertEqual(jobs[0].location, "New York, NY")
        self.assertEqual(jobs[0].platform, "icims")
        self.assertEqual(jobs[1].title, "Desktop Support Analyst")
        self.assertEqual(jobs[1].location, "Remote")
        self.assertEqual(len(session.get_calls), 3)

    def test_scan_platform_applies_location_filter_to_icims_page_locations(self):
        session = FakeSession(get_responses=[
            FakeResponse(content=self.SITEMAP_XML, headers={"Content-Type": "application/xml"}),
            FakeResponse(content=self.JOB_PAGE_NY, headers={"Content-Type": "text/html"}),
        ])

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(job_hunter, "get_session", return_value=session))
            stack.enter_context(mock.patch.dict(job_hunter.MAX_RPS, {"icims": 0}, clear=False))
            matches, stats, stale = job_hunter.scan_platform(
                "icims",
                ["acme"],
                ["IT Support"],
                ["New York, NY"],
            )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].location, "New York, NY")
        self.assertEqual(stats.successful_fetches, 1)
        self.assertEqual(stale, set())
        # The non-matching Desktop Support title did not need a page hydration request.
        self.assertEqual(len(session.get_calls), 2)

    def test_404_is_dead_slug(self):
        session = FakeSession(get_responses=[FakeResponse(status_code=404)])
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_icims(session, "missing", self._limiter())

    def test_malformed_xml_is_dead_slug(self):
        session = FakeSession(get_responses=[
            FakeResponse(content=b"<not-xml", headers={"Content-Type": "text/html"})
        ])
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_icims(session, "broken", self._limiter())


class WorkdayFetcherTests(unittest.TestCase):
    def _limiter(self):
        return job_hunter.RateLimiter(0)

    def _page(self, titles, total):
        return FakeResponse(json_data={
            "total": total,
            "jobPostings": [
                {
                    "title": title,
                    "locationsText": "New York, NY",
                    "externalPath": f"/job/NY/{title.replace(' ', '-')}/R-{idx}",
                }
                for idx, title in enumerate(titles)
            ],
        })

    def test_workday_marks_board_incomplete_when_page_cap_is_hit(self):
        def page(offset):
            response = mock.Mock(status_code=200)
            response.json.return_value = {
                "total": 1000,
                "jobPostings": [{"title": f"Job {offset + i}", "externalPath": f"/job/{offset + i}"} for i in range(20)],
            }
            return response

        session = mock.Mock()
        session.post.side_effect = lambda *_a, **kw: page(kw["json"]["offset"])
        with mock.patch.object(job_hunter, "WORKDAY_MAX_PAGES", 2):
            jobs = job_hunter.fetch_workday(session, "acme|wd1|site", self._limiter())

        self.assertEqual(len(jobs), 40)
        self.assertFalse(jobs.complete)

    def test_partial_board_is_not_counted_as_checked(self):
        partial = job_hunter.JobList([job_hunter.Job("acme", "IT Support", "Remote", "https://x/1", "workday")])
        partial.complete = False
        with mock.patch.dict(job_hunter.FETCHERS, {"workday": lambda *_a, **_k: partial}):
            matches, stats, _ = job_hunter.scan_platform("workday", ["acme|wd1|site"], ["IT Support"], [], show_progress=False)

        self.assertEqual(len(matches), 1)
        self.assertEqual(stats.successful_slugs, set())
        self.assertEqual(stats.partial_fetches, 1)

    def test_invalid_slug_format_is_dead_slug(self):
        session = FakeSession()
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_workday(session, "only-one-part", self._limiter())
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_workday(session, "a||c", self._limiter())
        with self.assertRaises(job_hunter.DeadSlugError):
            job_hunter.fetch_workday(session, "a|notwd|c", self._limiter())

    def test_paginates_and_stops_when_offset_reaches_total(self):
        session = FakeSession(post_responses=[
            self._page(["IT Support Specialist"] * job_hunter.WORKDAY_PAGE_SIZE, total=25),
            self._page(["Help Desk Analyst"] * 5, total=25),
        ])

        jobs = job_hunter.fetch_workday(session, "acme|wd1|acmecareers", self._limiter())

        self.assertEqual(len(jobs), 25)
        self.assertEqual(len(session.post_calls), 2)
        first_url, first_kwargs = session.post_calls[0]
        self.assertEqual(
            first_url,
            "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/acmecareers/jobs",
        )
        self.assertEqual(first_kwargs["json"]["offset"], 0)
        self.assertEqual(session.post_calls[1][1]["json"]["offset"], job_hunter.WORKDAY_PAGE_SIZE)
        self.assertEqual(jobs[0].company, "acme")
        self.assertEqual(jobs[0].platform, "workday")
        self.assertTrue(jobs[0].url.startswith(
            "https://acme.wd1.myworkdayjobs.com/acmecareers/job/NY/"
        ))

    def test_mid_pagination_total_divergence_breaks(self):
        session = FakeSession(post_responses=[
            self._page(["A"] * job_hunter.WORKDAY_PAGE_SIZE, total=100),
            self._page(["B"] * job_hunter.WORKDAY_PAGE_SIZE, total=42),
        ])

        jobs = job_hunter.fetch_workday(session, "acme|wd1|site", self._limiter())

        # First page accepted, second page's divergent total triggers break.
        self.assertEqual(len(jobs), job_hunter.WORKDAY_PAGE_SIZE)
        self.assertEqual(len(session.post_calls), 2)

    def test_first_page_permanent_error_is_dead_slug(self):
        for status_code in job_hunter.WORKDAY_DEAD_STATUS_CODES:
            session = FakeSession(post_responses=[FakeResponse(status_code=status_code)])
            with self.assertRaises(job_hunter.DeadSlugError):
                job_hunter.fetch_workday(session, "acme|wd1|site", self._limiter())

    def test_first_page_throttle_is_request_error_not_dead_slug(self):
        session = FakeSession(post_responses=[FakeResponse(status_code=429)])

        with self.assertRaises(requests.HTTPError):
            job_hunter.fetch_workday(session, "acme|wd1|site", self._limiter())

    def test_external_path_without_leading_slash_and_missing_location(self):
        session = FakeSession(post_responses=[
            FakeResponse(json_data={
                "total": 1,
                "jobPostings": [
                    {
                        "title": "IT Support Specialist",
                        "externalPath": "job/NY/IT-Support/R-1",
                    }
                ],
            })
        ])

        jobs = job_hunter.fetch_workday(session, "acme|wd1|site", self._limiter())

        self.assertEqual(jobs[0].location, "")
        self.assertEqual(jobs[0].url, "https://acme.wd1.myworkdayjobs.com/site/job/NY/IT-Support/R-1")


class FetchersRegistryTests(unittest.TestCase):
    def test_new_platforms_registered(self):
        for platform in ("bamboohr", "icims", "workday"):
            self.assertIn(platform, job_hunter.FETCHERS)
            self.assertIn(platform, job_hunter.SLUG_URLS)
            self.assertIn(platform, job_hunter.WORKERS)
            self.assertIn(platform, job_hunter.MAX_RPS)
            self.assertIn(platform, job_hunter.REQUEST_TIMEOUT)

    def test_scan_platform_runs_with_new_fetchers(self):
        for platform in ("bamboohr", "icims", "workday"):
            def fake_fetcher(_session, slug, _limiter, platform=platform):
                return [
                    job_hunter.Job(
                        company=slug,
                        title="IT Support Specialist",
                        location="",
                        url=f"https://example.com/{platform}/{slug}",
                        platform=platform,
                    )
                ]

            with ExitStack() as stack:
                stack.enter_context(mock.patch.dict(job_hunter.FETCHERS, {platform: fake_fetcher}, clear=False))
                stack.enter_context(mock.patch.object(job_hunter, "get_session", return_value=object()))
                matches, stats, stale = job_hunter.scan_platform(platform, ["acme"], ["IT Support"], [])

            self.assertEqual(len(matches), 1)
            self.assertEqual(stats.successful_fetches, 1)
            self.assertEqual(stats.matches, 1)
            self.assertEqual(stale, set())


if __name__ == "__main__":
    unittest.main()
