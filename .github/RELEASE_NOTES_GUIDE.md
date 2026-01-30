# 🚀 Intelligent Release Notes Generation Guide

This guide explains how to use the automatic intelligent release notes generation feature for the mirobody project.

## ✨ Features

Our GitHub Actions automatically generates detailed release notes based on the following information:

### 📝 Automatically Included Content

1. **Tag Message** - If using annotated tags
2. **Categorized Commit History**:
   - 🆕 New Features (feat:, feature:, add:, 新增, 功能)
   - 🐛 Bug Fixes (fix:, bug:, 修复, bugfix)
   - 🔧 Improvements (improve:, update:, 优化, 改进)
   - 📚 Documentation (docs:, doc:, 文档)
   - 🔨 Other Changes
3. **Related Pull Requests** - Automatically identified and linked
4. **Contributor List** - Automatically compiled
5. **Release Statistics** - Commit count, file changes, etc.
6. **Installation Instructions** - Multiple installation methods
7. **Package Details** - Version, date, platform support, etc.

## 📋 Usage

### 1. Standard Release Process

```bash
# 1. Ensure all changes are committed
git add .
git commit -m "feat: add new health analysis feature"
git push

# 2. Create an annotated tag (recommended)
git tag -a 1.0.0 -m "Major release: Add health analysis and improved performance

New Features:
- Health data analysis module
- Performance optimization
- New API interfaces

Bug Fixes:
- Fix data import issue
- Resolve memory leak

Thanks to all contributors for their efforts!"

# 3. Push the tag to trigger automatic release
git push origin 1.0.0
```

### 2. Quick Release (Simple Tag)

```bash
# Create a simple tag
git tag 1.0.1
git push origin 1.0.1
```

## 🎯 Tips for Optimizing Release Notes

### 1. Use Standardized Commit Message Format

```bash
# Recommended commit message format
git commit -m "feat: add user authentication system"
git commit -m "fix: resolve login timeout issue"
git commit -m "docs: update API documentation"
git commit -m "improve: optimize database queries"
```

### 2. Supported English and Chinese Keywords

- **New Features**: `feat:`, `feature:`, `add:`, `新增`, `功能`
- **Bug Fixes**: `fix:`, `bug:`, `修复`, `bugfix`
- **Improvements**: `improve:`, `update:`, `优化`, `改进`
- **Documentation**: `docs:`, `doc:`, `文档`

### 3. Reference PRs in Commit Messages

```bash
git commit -m "feat: add health metrics tracking (#123)"
git commit -m "fix: resolve data sync issue (closes #456)"
```

### 4. Use Annotated Tags

```bash
# Detailed release message
git tag -a 2.0.0 -m "🎉 Major Release v2.0.0

## Major Updates
- Brand new user interface design
- Multi-language support
- 50% performance improvement

## Breaking Changes
- API v1 is deprecated, please upgrade to v2
- Configuration file format has changed

## Migration Guide
Please refer to the documentation for upgrade instructions: https://docs.mirobody.com/migration"
```

## 📊 Example of Generated Release Notes

When you push a tag, the system will automatically generate release notes in a format similar to this:

```markdown
## 🚀 What's New in v1.0.0

### 📝 Release Message
Major release with health analysis features and performance improvements.

### 📋 Changes Since v0.9.0

#### 🆕 New Features
- feat: add health data analysis module
- feat: implement real-time monitoring dashboard
- add: support for multiple data sources

#### 🐛 Bug Fixes
- fix: resolve memory leak in data processing
- bugfix: handle edge cases in authentication

#### 🔧 Improvements
- improve: optimize database query performance by 40%
- update: enhance error handling and logging

#### 📚 Documentation
- docs: add comprehensive API reference
- docs: update installation guide

#### 🔗 Related Pull Requests
- [Add health analysis module](https://github.com/thetahealth/mirobody/pull/123) #123
- [Fix memory leak issue](https://github.com/thetahealth/mirobody/pull/124) #124

#### 👥 Contributors
- Zhang San <zhang@example.com>
- Li Si <li@example.com>

#### 📊 Release Statistics
- **Commits**: 25
- **Files Changed**: 47
- **Period**: 2024-01-15 → 2024-01-30

## 📦 Installation

### From TestPyPI:
```bash
pip install -i https://test.pypi.org/simple/ mirobody==1.0.0
```

### From GitHub Release:
Download the wheel file below and install:
```bash
pip install mirobody-1.0.0-py3-none-any.whl
```

## 🔍 Package Details
- **Version**: 1.0.0
- **Release Date**: 2024-01-30 14:30:00 UTC
- **Python Support**: 3.8+
- **Platforms**: Linux, macOS, Windows
```

## 🔧 Advanced Configuration

### Custom Categorization Rules

If you need to modify the categorization rules, you can edit the grep patterns in [.github/workflows/pypi-release.yml](.github/workflows/pypi-release.yml):

```bash
# Example: Add new feature keywords
git log --oneline --grep="feat:" --grep="feature:" --grep="add:" --grep="新功能"
```

### Pre-release Versions

The system will automatically identify pre-release versions and mark them as prerelease:

- `1.0.0a1` - Alpha version
- `1.0.0b1` - Beta version
- `1.0.0rc1` - Release Candidate
- `1.0.0.dev1` - Development version

## 🚨 Important Notes

1. **First Release**: If there are no previous tags, the system will display the last 10 commits
2. **PR Information**: PR numbers need to be included in commit messages (e.g., #123)
3. **GitHub CLI**: If the gh command is available on the runner, more detailed PR information will be retrieved
4. **Tag Format**: Supports semantic versioning format (1.0.0, 2.1.3, etc.)

## 🎯 Best Practices

1. **Regular Releases**: Recommended to release a minor version every 2-4 weeks
2. **Clear Commits**: Use descriptive commit messages
3. **Documentation Sync**: Ensure README and documentation are in sync with code
4. **Testing Validation**: Run the test-build workflow before release
5. **Version Planning**: Follow Semantic Versioning (SemVer)

## 📞 Support

If you have any questions:
1. Check [GitHub Actions logs](https://github.com/thetahealth/mirobody/actions)
2. Submit an [Issue](https://github.com/thetahealth/mirobody/issues)
3. Refer to [Workflow Documentation](.github/workflows/README.md)