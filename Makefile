# lakebase-solutions — local dev/test gate.
# GitHub Actions is disabled at the org level for this repo, so the test suite
# runs locally (see CONTRIBUTING.md). `make check` is the pre-push gate.

PYTHON ?= python3
VENV   := .venv
BIN    := $(VENV)/bin

.PHONY: check test lint venv install-hooks uninstall-hooks clean

## check: lint + tests (run this before every push)
check: lint test

## test: run the pytest suite in an isolated venv (no Databricks workspace needed)
test: $(VENV)/.installed
	$(BIN)/pytest -q

## lint: byte-compile all sources to catch syntax errors (zero extra deps)
lint: $(VENV)/.installed
	$(BIN)/python -m compileall -q bootstrap tests core modules conftest.py deploy.py

## venv: (re)create the venv and install dev deps when requirements change
$(VENV)/.installed: requirements-dev.txt
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install -q --upgrade pip
	$(BIN)/pip install -q -r requirements-dev.txt
	touch $@

venv: $(VENV)/.installed

## install-hooks: enable the repo-local pre-push test gate (chains Databricks hooks)
install-hooks:
	git config core.hooksPath .githooks
	@echo "Installed repo-local hooks (.githooks). Databricks secret-scan hooks are chained."
	@echo "VERIFY secret scanning still fires (see CONTRIBUTING.md) before relying on this."

## uninstall-hooks: revert to the global Databricks hooks path
uninstall-hooks:
	git config --unset core.hooksPath || true
	@echo "Reverted to global git hooks path."

## clean: remove venv and caches
clean:
	rm -rf $(VENV) .pytest_cache **/__pycache__
