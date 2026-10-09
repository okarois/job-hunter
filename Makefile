.PHONY: setup test lint scan dashboard

PYTHON ?= .venv/bin/python

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dev.txt

test:
	$(PYTHON) -m unittest -v

lint:
	$(PYTHON) -m ruff check .

scan:
	$(PYTHON) job_hunter.py --fast

dashboard:
	$(PYTHON) job_hunter.py --dashboard
