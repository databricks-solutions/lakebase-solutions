# Contributing

## Branching & pull requests

`main` is protected. **Work on your own branch and open a Pull Request to merge —
do not commit directly to `main`.** Direct pushes to `main` are restricted to
maintainers (currently: `chase.marler@databricks.com`); everyone else's changes
land via reviewed PRs to keep `main` deployable.

```
git switch -c <your-name>/<short-topic>   # branch off main
# ...make changes, then run the test gate below...
git push -u origin <branch>               # push your branch (never `main`)
# open a Pull Request into main; a maintainer reviews + merges
```

If you drive this repo with an AI coding agent, the same rule is written into
[`AGENTS.md`](AGENTS.md) so the agent branches by default. Branch protection
enforces it server-side as well.

## Test gate — run before every push

GitHub Actions is **disabled at the org level** for this repo, so tests run
locally, not in cloud CI. The `.github/workflows/ci.yml` is kept in place and
will start working automatically if the org enables Actions. Until then, the
gate is:

```
make check      # lint (byte-compile) + pytest
```

`make test` builds an isolated `.venv` (first run installs
`requirements-dev.txt`) and runs the suite (manifest schema, DAG ordering,
orchestrator control flow, deploy-notebook import) with **no** Databricks
workspace required.

## Optional: auto-enforce the gate on push

```
make install-hooks
```

This sets `core.hooksPath` to `.githooks/` **for this repo only**. Git honors a
single hooks path, and the Databricks secret-scan hooks are installed at the
*global* `core.hooksPath` (`~/.databricks/githooks`) — so the repo-local hooks
**chain** to them:

- `.githooks/pre-commit` and `.githooks/commit-msg` delegate to the Databricks
  secret-scan hooks (commit-time scanning preserved).
- `.githooks/pre-push` runs the Databricks secret-scan push hook **first**, then
  `make test` (push blocked if either fails).

> ⚠️ **Verify after installing.** Secret scanning is security-critical on this
> public-facing repo. After `make install-hooks`, confirm it still fires: stage a
> dummy secret in a scratch file and try to commit — the Databricks pre-commit
> hook should block it — then delete the scratch file. If scanning does *not*
> fire, run `make uninstall-hooks` and fall back to running `make check`
> manually.

Uninstall / revert to the global Databricks hooks: `make uninstall-hooks`.

## Adding a module

See [`docs/MODULE_AUTHORING.md`](docs/MODULE_AUTHORING.md). Drop a folder under
`modules/` with a `module.yaml`; discovery + the dependency DAG pick it up with
**no edits to the deploy notebook**.

## Deployment

See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) — immutable, workspace-run (commit → push →
`databricks repos update` → run the deploy notebook).
