# GitHub Actions Workflows

This project contains two main GitHub Actions workflows for automating package building, testing, and release.

## 📦 Workflow Descriptions

### 1. PyPI Release (`pypi-release.yml`)

**Trigger Conditions**: Push a tag with version format (e.g., `1.0.0`, `2.1.3`, `1.0.0-beta1`)

**Features**:
- Automatically extract version number from tag name
- Build Python package (wheel and source distribution)
- Run package validation tests
- Publish to TestPyPI
- **Intelligent Release Notes Generation**:
  - Automatically categorize commit history (features/fixes/improvements/documentation)
  - Extract and link related Pull Requests
  - Display contributor information and release statistics
  - Include detailed installation instructions
  - Support both English and Chinese commit message recognition
- Create GitHub Release with build artifacts
- Support multiple Python versions
- Automatically identify pre-release versions (alpha/beta/rc)

### 2. Test Build (`test-build.yml`)

**Trigger Conditions**:
- Manual trigger (can specify test version number)
- Pull Request to main/master/develop branch

**Features**:
- Test build on multiple Python versions (3.8-3.12)
- Test package import on multiple operating systems (Linux, macOS, Windows)
- Validate package installation and import
- Generate test reports

## 🚀 Usage Guide

### Release a New Version

1. **Prepare Release**
   ```bash
   # Ensure all changes are committed
   git add .
   git commit -m "Prepare for release"
   git push
   ```

2. **Run Test Build** (optional but recommended)
   - Visit the Actions page
   - Select "Test Build" workflow
   - Click "Run workflow"
   - Enter test version number (e.g., `1.0.0-test`)
   - Wait for tests to complete

3. **Create and Push Tag**
   ```bash
   # Create version tag
   git tag 1.0.0
   # Or create annotated tag (recommended)
   git tag -a 1.0.0 -m "Release version 1.0.0"

   # Push tag to GitHub
   git push origin 1.0.0
   ```

4. **Monitor Release Process**
   - Visit Actions page to check workflow run status
   - Check GitHub Releases page
   - Verify package on TestPyPI

> 💡 **Tip**: See [Intelligent Release Notes Generation Guide](RELEASE_NOTES_GUIDE.md) to learn how to optimize automatically generated release notes.

### Manual Test Build

1. Visit the repository's Actions tab
2. Select "Test Build" workflow
3. Click "Run workflow"
4. Enter test version number (optional)
5. Click "Run workflow" button

## ⚙️ Configuration Requirements

### Required Secrets

Configure the following secrets in GitHub repository settings:

- `TEST_PYPI_API_TOKEN`: API token for TestPyPI
  - How to obtain:
    1. Visit https://test.pypi.org/
    2. Login to your account
    3. Go to Account Settings → API tokens
    4. Create new token (scope: entire account or specific project)
    5. Add in repository Settings → Secrets → Actions

### Optional Configuration

To publish to official PyPI, add:
- `PYPI_API_TOKEN`: API token for official PyPI

## 📝 Version Number Specifications

Supported version number formats:
- Standard versions: `1.0.0`, `2.3.1`
- Pre-release versions: `1.0.0-alpha`, `1.0.0-beta1`, `1.0.0-rc2`

Version numbers must comply with PEP 440 specification.

## 🔧 Troubleshooting

### Common Issues

1. **TestPyPI Upload Failed**
   - Check if `TEST_PYPI_API_TOKEN` is correctly configured
   - Confirm package name is not taken on TestPyPI
   - Check if version number already exists

2. **Build Failed**
   - Check error messages in Actions logs
   - Test locally by running `python -m build`
   - Check `pyproject.toml` configuration

3. **GitHub Release Creation Failed**
   - Confirm tag format is correct
   - Check repository permission settings

## 📊 Workflow Status Badges

You can add status badges to your README:

```markdown
[![PyPI Release](https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml/badge.svg)](https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml)
[![Test Build](https://github.com/thetahealth/mirobody/actions/workflows/test-build.yml/badge.svg)](https://github.com/thetahealth/mirobody/actions/workflows/test-build.yml)
```

## 🔄 Updating Workflows

After modifying workflow files:
1. Commit changes to repository
2. Workflows will automatically use the latest version
3. It's recommended to verify changes on a test branch first

## 📚 Related Links

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Python Packaging Guide](https://packaging.python.org/)
- [TestPyPI](https://test.pypi.org/)
- [PEP 440 - Version Identification and Dependency Specification](https://www.python.org/dev/peps/pep-0440/)
