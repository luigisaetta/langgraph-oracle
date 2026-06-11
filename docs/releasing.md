# Publishing to PyPI

This project is prepared for PyPI publication through GitHub Actions and PyPI Trusted Publishing.

The workflow builds the source distribution and wheel, checks both with Twine, and publishes without storing a PyPI API token in GitHub secrets.

## Local Release Checks

Install release tooling:

```bash
conda run -n langgraph-oracle python -m pip install -e ".[release]"
```

Build the distributions:

```bash
conda run -n langgraph-oracle python -m build
```

Validate package metadata:

```bash
conda run -n langgraph-oracle python -m twine check dist/*
```

The generated `dist/` directory is ignored by Git.

## Trusted Publishing Setup

Configure trusted publishing once on PyPI.

For TestPyPI:

1. Create or log into a TestPyPI account.
2. Create the `langgraph-oracle` project manually if needed.
3. Add a trusted publisher with:
   - Owner: `luigisaetta`
   - Repository name: `langgraph-oracle`
   - Workflow name: `publish.yml`
   - Environment name: `testpypi`

For PyPI:

1. Create or log into a PyPI account.
2. Create the `langgraph-oracle` project manually if needed.
3. Add a trusted publisher with:
   - Owner: `luigisaetta`
   - Repository name: `langgraph-oracle`
   - Workflow name: `publish.yml`
   - Environment name: `pypi`

## Publishing to TestPyPI

Use TestPyPI before the first real release.

1. Push all release changes to `main`.
2. Open GitHub Actions.
3. Select the `publish` workflow.
4. Click **Run workflow**.
5. Select `testpypi`.
6. Run the workflow.

Install from TestPyPI in a clean environment:

```bash
python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  langgraph-oracle
```

The extra PyPI index is needed because dependencies such as `langgraph` and `oracledb` may not exist on TestPyPI.

## Publishing to PyPI

Before publishing:

- Ensure tests, formatting, linting, and docs build pass.
- Ensure `pyproject.toml` has the intended version.
- Create a Git tag matching the version, for example `v0.1.0`.
- Create a GitHub release from that tag.

Publishing flow:

1. Go to GitHub Releases.
2. Draft a new release.
3. Choose or create the version tag.
4. Publish the release.
5. The `publish` workflow runs automatically.
6. The package is uploaded to PyPI through Trusted Publishing.

## Versioning

Update the package version in `pyproject.toml` before each release.

Use semantic versioning while the public API stabilizes:

- Patch: bug fixes and documentation-only package changes.
- Minor: backwards-compatible features.
- Major: breaking API changes.

## Rollback Notes

PyPI does not allow replacing a file for the same version.

If a release is broken:

1. Yank the broken release on PyPI if appropriate.
2. Fix the issue.
3. Publish a new version.
