# GitHub Actions Workflows

This directory contains CI/CD workflows for the project.

## Workflows

### Unit Tests (`test.yml`)

Runs automated unit tests on every push and pull request.

**Triggers:**
- Push to `main` or `develop` branches
- Push to branches matching `scum-*` pattern (e.g., `scum-123-feature-name`)
- Pull requests to `main` or `develop` branches

**What it does:**
1. Checks out the code
2. Sets up Python 3.13
3. Installs `uv` package manager
4. Installs project dependencies and test dependencies
5. Runs pytest with coverage reporting
6. Generates test summary

**Requirements:**
- Python 3.13
- All tests must pass for the workflow to succeed

**Coverage:**
- Current coverage: 100% for KlineMapper and MessageProcessor
- Coverage reports are generated in terminal format

## Adding Status Badge to README

Add this badge to your README.md to show test status:

```markdown
![Tests](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/test.yml/badge.svg)
```

Replace `YOUR_USERNAME` and `YOUR_REPO` with your GitHub username and repository name.

## Local Testing

To run the same tests locally before pushing:

```bash
# Run all tests
.venv/bin/python -m pytest tests/ -v

# Run with coverage
.venv/bin/python -m pytest tests/ --cov=src --cov-report=term-missing

# Run specific test file
.venv/bin/python -m pytest tests/mappers/test_kline_mapper.py -v
```

## Troubleshooting

### Tests fail in CI but pass locally

1. Check Python version matches (3.13)
2. Ensure all dependencies are in `pyproject.toml`
3. Check for environment-specific issues (paths, etc.)

### Workflow doesn't trigger

1. Ensure `.github/workflows/test.yml` exists
2. Check that you're pushing to `main` or `develop` branch
3. Verify workflow file syntax is correct
