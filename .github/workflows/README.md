# GitHub Actions Workflows

本项目包含两个主要的 GitHub Actions 工作流程，用于自动化包的构建、测试和发布。

## 📦 工作流程说明

### 1. PyPI Release (`pypi-release.yml`)

**触发条件**: 推送符合版本格式的 tag（如 `1.0.0`, `2.1.3`, `1.0.0-beta1`）

**功能**:
- 自动从 tag 名称提取版本号
- 构建 Python 包（wheel 和 source distribution）
- 运行包验证测试
- 发布到 TestPyPI
- **智能生成 Release Notes**:
  - 自动分类提交记录（功能/修复/改进/文档）
  - 提取并链接相关 Pull Requests
  - 显示贡献者信息和发布统计
  - 包含详细的安装说明
  - 支持中英文提交消息识别
- 创建 GitHub Release，附带构建产物
- 支持多种 Python 版本
- 自动识别预发布版本（alpha/beta/rc）

### 2. Test Build (`test-build.yml`)

**触发条件**:
- 手动触发（可指定测试版本号）
- Pull Request 到 main/master/develop 分支

**功能**:
- 在多个 Python 版本（3.8-3.12）上测试构建
- 在多个操作系统（Linux, macOS, Windows）上测试包导入
- 验证包的安装和导入
- 生成测试报告

## 🚀 使用指南

### 发布新版本

1. **准备发布**
   ```bash
   # 确保所有更改已提交
   git add .
   git commit -m "Prepare for release"
   git push
   ```

2. **运行测试构建**（可选但推荐）
   - 访问 Actions 页面
   - 选择 "Test Build" workflow
   - 点击 "Run workflow"
   - 输入测试版本号（如 `1.0.0-test`）
   - 等待测试完成

3. **创建并推送 tag**
   ```bash
   # 创建版本 tag
   git tag 1.0.0
   # 或创建带注释的 tag（推荐）
   git tag -a 1.0.0 -m "Release version 1.0.0"

   # 推送 tag 到 GitHub
   git push origin 1.0.0
   ```

4. **监控发布过程**
   - 访问 Actions 页面查看工作流程运行状态
   - 检查 GitHub Releases 页面
   - 验证 TestPyPI 上的包

> 💡 **提示**: 查看 [智能 Release Notes 生成指南](RELEASE_NOTES_GUIDE.md) 了解如何优化自动生成的发布说明。

### 手动测试构建

1. 访问仓库的 Actions 标签页
2. 选择 "Test Build" workflow
3. 点击 "Run workflow"
4. 输入测试版本号（可选）
5. 点击 "Run workflow" 按钮

## ⚙️ 配置要求

### 必需的 Secrets

在 GitHub 仓库设置中配置以下 secrets：

- `TEST_PYPI_API_TOKEN`: TestPyPI 的 API token
  - 获取方式：
    1. 访问 https://test.pypi.org/
    2. 登录账号
    3. 进入 Account Settings → API tokens
    4. 创建新 token（scope: 整个账号或特定项目）
    5. 在仓库 Settings → Secrets → Actions 中添加

### 可选配置

如果要发布到正式 PyPI，需要添加：
- `PYPI_API_TOKEN`: 正式 PyPI 的 API token

## 📝 版本号规范

支持的版本号格式：
- 标准版本：`1.0.0`, `2.3.1`
- 预发布版本：`1.0.0-alpha`, `1.0.0-beta1`, `1.0.0-rc2`

版本号必须符合 PEP 440 规范。

## 🔧 故障排除

### 常见问题

1. **TestPyPI 上传失败**
   - 检查 `TEST_PYPI_API_TOKEN` 是否正确配置
   - 确认包名在 TestPyPI 上未被占用
   - 检查版本号是否已存在

2. **构建失败**
   - 查看 Actions 日志中的错误信息
   - 本地运行 `python -m build` 测试
   - 检查 `pyproject.toml` 配置

3. **GitHub Release 创建失败**
   - 确认 tag 格式正确
   - 检查仓库权限设置

## 📊 工作流程状态徽章

可以在 README 中添加状态徽章：

```markdown
[![PyPI Release](https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml/badge.svg)](https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml)
[![Test Build](https://github.com/thetahealth/mirobody/actions/workflows/test-build.yml/badge.svg)](https://github.com/thetahealth/mirobody/actions/workflows/test-build.yml)
```

## 🔄 更新工作流程

修改工作流程文件后：
1. 提交更改到仓库
2. 工作流程会自动使用最新版本
3. 建议先在测试分支上验证更改

## 📚 相关链接

- [GitHub Actions 文档](https://docs.github.com/en/actions)
- [Python Packaging 指南](https://packaging.python.org/)
- [TestPyPI](https://test.pypi.org/)
- [PEP 440 - 版本标识和依赖规范](https://www.python.org/dev/peps/pep-0440/)
