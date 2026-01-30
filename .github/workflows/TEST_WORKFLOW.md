# Testing PyPI Release Workflow Guide

## How to Test Workflow on build/cicd Branch

### Method 1: Via GitHub Actions Page (Recommended)

1. **Ensure workflow file is pushed to target branch**
   ```bash
   # Switch to build or cicd branch
   git checkout build

   # Ensure workflow file is up to date
   git pull origin build
   ```

2. **Manually trigger on GitHub Actions page**
   - Visit: `https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml`
   - Click "Run workflow" button in the top right
   - **Important**: Select the branch to test (build or cicd) in the dropdown menu
   - Set parameters:
     - test_mode: Keep default `true` (test mode, does not publish to PyPI)
     - version_suffix: Optional, e.g., `test1`
   - Click the green "Run workflow" button

3. **Version Number Format**
   - Test version format: `0.0.0.dev20250130142035.build.test1`
   - Includes timestamp, branch name, and suffix for easy identification

### Method 2: Using GitHub CLI (Command Line)

```bash
# Install GitHub CLI (if not already installed)
brew install gh  # macOS

# Login
gh auth login

# Trigger workflow on build branch
gh workflow run pypi-release.yml \
  --ref build \
  -f test_mode=true \
  -f version_suffix=test1

# Check run status
gh run list --workflow=pypi-release.yml
```

## Test Mode vs Official Release

### Test Mode (test_mode=true)
- ✅ Build Python package
- ✅ Upload to TestPyPI
- ❌ Do not upload to PyPI
- ❌ Do not create GitHub Release
- Version number includes branch name, e.g.: `0.0.0.dev20250130142035.build.test1`

### Official Release (Push tag)
- ✅ Build Python package
- ✅ Upload to TestPyPI
- ✅ Upload to PyPI
- ✅ Create GitHub Release
- Version number uses tag name, e.g.: `1.0.3`

## Common Questions

### Q: Why does the workflow triggered on the build branch run code from the main branch?
A: You need to explicitly select the build branch in the "Run workflow" dropdown menu.

### Q: How to verify which branch's workflow is being used?
A: Check the "Set version from tag or manual trigger" step in the workflow run logs, which will display:
- `🧪 Manual test trigger on branch: build`
- `📦 Test version: 0.0.0.dev20250130142035.build.test1`

### Q: What to do if TestPyPI upload fails?
A: TestPyPI can be unstable at times. The workflow has `continue-on-error: true` set, so it won't affect the overall process.

## Branch Strategy Recommendations

1. **main branch**: Stable version, for official releases
2. **build branch**: CI/CD testing, verify build process
3. **cicd branch**: Integration testing, verify complete release process
4. **feature branch**: Feature development, does not trigger workflow

## View Test Results

```bash
# View package on TestPyPI
# https://test.pypi.org/project/mirobody/

# Install test version
pip install -i https://test.pypi.org/simple/ mirobody==0.0.0.dev20250130142035.build.test1
```