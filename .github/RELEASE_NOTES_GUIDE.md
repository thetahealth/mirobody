# 🚀 智能 Release Notes 生成指南

本指南介绍如何使用 mirobody 项目的智能 release notes 自动生成功能。

## ✨ 功能特性

我们的 GitHub Actions 会自动根据以下信息生成详细的 release notes：

### 📝 自动包含的内容

1. **Tag 消息** - 如果使用带注释的 tag
2. **分类的提交记录**:
   - 🆕 新功能 (feat:, feature:, add:, 新增, 功能)
   - 🐛 Bug 修复 (fix:, bug:, 修复, bugfix)
   - 🔧 改进 (improve:, update:, 优化, 改进)
   - 📚 文档 (docs:, doc:, 文档)
   - 🔨 其他变更
3. **相关 Pull Requests** - 自动识别并链接
4. **贡献者列表** - 自动统计
5. **发布统计** - 提交数、文件变更数等
6. **安装说明** - 多种安装方式
7. **包详情** - 版本、日期、平台支持等

## 📋 使用方法

### 1. 标准发布流程

```bash
# 1. 确保所有更改已提交
git add .
git commit -m "feat: add new health analysis feature"
git push

# 2. 创建带注释的 tag（推荐）
git tag -a 1.0.0 -m "Major release: Add health analysis and improved performance

新功能：
- 健康数据分析模块
- 性能优化
- 新的 API 接口

Bug 修复：
- 修复数据导入问题
- 解决内存泄漏

感谢所有贡献者的努力！"

# 3. 推送 tag 触发自动发布
git push origin 1.0.0
```

### 2. 快速发布（简单 tag）

```bash
# 创建简单 tag
git tag 1.0.1
git push origin 1.0.1
```

## 🎯 优化 Release Notes 的技巧

### 1. 使用规范的提交消息格式

```bash
# 推荐的提交消息格式
git commit -m "feat: add user authentication system"
git commit -m "fix: resolve login timeout issue"
git commit -m "docs: update API documentation"
git commit -m "improve: optimize database queries"
```

### 2. 支持的中英文关键词

- **新功能**: `feat:`, `feature:`, `add:`, `新增`, `功能`
- **Bug修复**: `fix:`, `bug:`, `修复`, `bugfix`
- **改进**: `improve:`, `update:`, `优化`, `改进`
- **文档**: `docs:`, `doc:`, `文档`

### 3. 在提交消息中引用 PR

```bash
git commit -m "feat: add health metrics tracking (#123)"
git commit -m "fix: resolve data sync issue (closes #456)"
```

### 4. 使用带注释的 tag

```bash
# 详细的 release 消息
git tag -a 2.0.0 -m "🎉 Major Release v2.0.0

## 主要更新
- 全新的用户界面设计
- 支持多语言
- 性能提升 50%

## 破坏性更改
- API v1 已弃用，请升级到 v2
- 配置文件格式已更改

## 迁移指南
请参考文档进行升级：https://docs.mirobody.com/migration"
```

## 📊 生成的 Release Notes 示例

当您推送 tag 后，系统会自动生成类似以下格式的 release notes：

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

## 🔧 高级配置

### 自定义分类规则

如果需要修改分类规则，可以编辑 `.github/workflows/pypi-release.yml` 中的 grep 模式：

```bash
# 例如：添加新的功能关键词
git log --oneline --grep="feat:" --grep="feature:" --grep="add:" --grep="新功能"
```

### 预发布版本

系统会自动识别预发布版本并标记为 prerelease：

- `1.0.0a1` - Alpha 版本
- `1.0.0b1` - Beta 版本
- `1.0.0rc1` - Release Candidate
- `1.0.0.dev1` - Development 版本

## 🚨 注意事项

1. **首次发布**: 如果没有之前的 tag，系统会显示最近 10 个提交
2. **PR 信息**: 需要在提交消息中包含 PR 编号 (如 #123)
3. **GitHub CLI**: 如果 runner 上有 gh 命令，会获取更详细的 PR 信息
4. **标签格式**: 支持语义化版本格式 (1.0.0, 2.1.3 等)

## 🎯 最佳实践

1. **定期发布**: 建议每 2-4 周发布一次小版本
2. **清晰的提交**: 使用描述性的提交消息
3. **文档同步**: 确保 README 和文档与代码同步
4. **测试验证**: 发布前运行 test-build workflow
5. **版本规划**: 遵循语义化版本控制 (SemVer)

## 📞 支持

如有问题，请：
1. 查看 [GitHub Actions 日志](https://github.com/thetahealth/mirobody/actions)
2. 提交 [Issue](https://github.com/thetahealth/mirobody/issues)
3. 参考 [工作流程文档](.github/workflows/README.md)