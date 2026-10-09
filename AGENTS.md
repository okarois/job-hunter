# Instructions for AI coding agents (Codex, Claude Code, etc.)

This repo is **Job Hunter**, a local CLI that scans public job boards and tracks matches. Read this file
before running anything. The human's goal is usually "find me jobs for X in Y".

## Set up (once)

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python job_hunter.py --init     # creates config.yaml (git-ignored)
```

Python 3.10 or newer is required. If `python3 --version` is older, install a newer Python first.

## Ask the user before searching

You need two things from the user. Do not guess them:

1. **Job titles** they want, for example "Product Designer", "Data Analyst".
2. **Locations** they want, for example "Remote", "Chicago". An empty list matches every location.

Put them in `config.yaml` under `title_phrases` and `location_phrases`. Phrases are matched as whole
words, ignoring case and punctuation, so `IT Support` matches "Sr. IT Support Engineer" but not
"Audit Support". Suggest a few close variants of each title ("Designer", "Product Designer") because
companies word titles differently.

For a one-off search without editing the file, use flags (they override the config):

```bash
.venv/bin/python job_hunter.py --fast --title "Product Designer" --location Remote
```

## Commands

| Goal | Command |
| --- | --- |
| Quick daily scan (Greenhouse, Lever, Ashby) | `.venv/bin/python job_hunter.py --fast` |
| Scan every supported job site (slow the first time) | `.venv/bin/python job_hunter.py` |
| Scan one site | `.venv/bin/python job_hunter.py --platform lever` |
| Try it on a small sample | `.venv/bin/python job_hunter.py --fast --limit 100` |
| Print every current match | add `--all` |
| List everything tracked | `.venv/bin/python job_hunter.py --history` |
| Open the dashboard | `.venv/bin/python job_hunter.py --dashboard` |

The first full scan can take 30 to 60 minutes because dead company IDs are discovered and cached.
Prefer `--fast` or `--limit` while testing, and run long scans in the background.

## Reading results

- New matches print under **New Jobs** and are appended to `reports/YYYY-MM-DD.csv`.
- Everything is stored in `.cache/jobs_state.json`. Statuses: `pending`, `seen`, `applied`, `dismissed`.
- The dashboard at `http://127.0.0.1:8765` is how the user reviews and updates statuses.
- Exit code `2` means the scan could not reach enough job boards to trust a "no results" answer. Tell
  the user and retry later instead of reporting zero jobs.

## Rules

- Everything stays local. Never upload `.cache/`, `reports/`, or `config.yaml`.
- Do not apply to jobs for the user. This tool only finds and tracks them.
- Keep scans polite: do not raise the worker counts or rate limits in `job_hunter.py`.
- Run `.venv/bin/python -m unittest` after any code change.
