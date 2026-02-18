# GitHub Actions Badge

Add this to the top of your `README.md`:

```markdown
# Project Name

![Tests](https://github.com/USERNAME/REPO_NAME/actions/workflows/test.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.13-blue.svg)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)

Your project description...
```

## Replace placeholders:

- `USERNAME`: Your GitHub username
- `REPO_NAME`: Your repository name

## Example:

If your GitHub repo is `https://github.com/johndoe/crypto-trading`, use:

```markdown
![Tests](https://github.com/johndoe/crypto-trading/actions/workflows/test.yml/badge.svg)
```

The badge will automatically update to show:
- ✅ Green "passing" when tests pass
- ❌ Red "failing" when tests fail
- ⚪ Gray "no status" when workflow hasn't run yet
