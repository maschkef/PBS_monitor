# Release Process Guide

This document outlines the necessary steps and considerations when preparing and publishing a new release for the **PBS Monitor**.

## 1. Pre-Release Checklist

Before creating a new git tag, ensure the following codebase updates are made:

### A. Bump the Version
The application relies on the `VERSION` file to power the update checker.
- Open the `VERSION` file in the repository root.
- Update the version string (e.g., `0.2.14-beta` or `1.0.0`).
- Ensure no trailing newlines or extra spaces exist.

### B. Configuration Changes (`config.json`)
If your new release introduces new configuration options:
- Add the new keys and their `_comment_...` descriptions to `alerting/config.json.example`.
- Add the new keys with default values to `DEFAULT_CONFIG` in `alerting/monitor.py`.
- *Note:* You do not need to write manual migration code for the config. The `auto_migrate_config` function will automatically merge new fields from the `.example` file into the user's active `config.json` on their next startup.

### C. State Schema Changes (`state.json`)
If your new release changes the structure of the runtime state:
- Increment the `STATE_VERSION` integer in `alerting/monitor.py`.
- Add a new migration block inside the `migrate_state(state)` function in `alerting/normalization.py` to seamlessly convert older state schemas to the new format.

### D. Update Documentation
Ensure that all relevant documentation accurately reflects the changes in this release:
- Update the English (`README.md`) and German (`README_DE.md`) readme files if features, setup instructions, or configurations have changed.
- Update the technical markdown files in the `docs/` folder if architectural or functional changes were made.

### E. Run Tests
Ensure no regressions were introduced.
```bash
python3 -m pytest tests/
```

## 2. Commit and Tag

Once the codebase is ready and tested, commit your changes.

```bash
git add .
git commit -m "chore: bump version to 1.0.0"
```

Create an annotated git tag. The GitHub Actions workflow relies on tags starting with `v` (e.g., `v1.0.0`).
```bash
git tag -a v1.0.0 -m "Release v1.0.0"
```

## 3. Push and Publish

Push your changes and the new tag to GitHub:
```bash
git push origin main
git push origin v1.0.0
```

## 4. Automated CI/CD Pipeline

Once the tag is pushed, the `.github/workflows/docker-publish.yml` workflow will automatically trigger. Here is what happens in the background:

1. **Docker Images Built:** The workflow builds both the `alerting` and `webui` Docker images.
2. **Tagging Strategy:**
   - The specific version tag (e.g., `:v0.2.14-beta` or `:v1.0.0`) is always applied.
   - The `:edge` tag is updated to point to this newest release, making it ideal for testing bleeding-edge features.
   - The `:latest` tag is **only** updated if the tag does not contain `beta`, `alpha`, or `rc`. This protects stable users from experimental builds.
3. **GitHub Release:** A GitHub Release is drafted automatically.
   - For pre-releases (e.g. `v0.2.14-beta`), it will be marked as a "Pre-release" in the GitHub UI.
   - Release assets (like `docker-compose.yml`, `.env.example`, and a quickstart script) are dynamically generated and attached to the release for easy user downloads.

## 5. Post-Release

- Review the automatically generated GitHub Release.
- Edit the Release Notes on GitHub to provide a human-readable changelog (the update checker in the Web UI links directly to this page).
