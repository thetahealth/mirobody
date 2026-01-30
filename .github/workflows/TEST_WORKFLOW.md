# 测试 PyPI Release Workflow 说明

## 如何在 build/cicd 分支测试 workflow

### 方法一：通过 GitHub Actions 页面（推荐）

1. **确保 workflow 文件已推送到目标分支**
   ```bash
   # 切换到 build 或 cicd 分支
   git checkout build

   # 确保 workflow 文件是最新的
   git pull origin build
   ```

2. **在 GitHub Actions 页面手动触发**
   - 访问: `https://github.com/thetahealth/mirobody/actions/workflows/pypi-release.yml`
   - 点击右上角 "Run workflow" 按钮
   - **重要**: 在下拉菜单中选择要测试的分支（build 或 cicd）
   - 设置参数:
     - test_mode: 保持默认 `true`（测试模式，不发布到 PyPI）
     - version_suffix: 可选，如 `test1`
   - 点击绿色 "Run workflow" 按钮

3. **版本号格式**
   - 测试版本格式: `0.0.0.dev20250130142035.build.test1`
   - 包含了时间戳、分支名和后缀，便于识别

### 方法二：使用 GitHub CLI（命令行）

```bash
# 安装 GitHub CLI（如果未安装）
brew install gh  # macOS

# 登录
gh auth login

# 在 build 分支上触发 workflow
gh workflow run pypi-release.yml \
  --ref build \
  -f test_mode=true \
  -f version_suffix=test1

# 查看运行状态
gh run list --workflow=pypi-release.yml
```

## 测试模式 vs 正式发布

### 测试模式（test_mode=true）
- ✅ 构建 Python 包
- ✅ 上传到 TestPyPI
- ❌ 不上传到 PyPI
- ❌ 不创建 GitHub Release
- 版本号包含分支名，如: `0.0.0.dev20250130142035.build.test1`

### 正式发布（推送 tag）
- ✅ 构建 Python 包
- ✅ 上传到 TestPyPI
- ✅ 上传到 PyPI
- ✅ 创建 GitHub Release
- 版本号使用 tag 名，如: `1.0.3`

## 常见问题

### Q: 为什么在 build 分支触发的 workflow 运行的是 main 分支的代码？
A: 需要在 "Run workflow" 下拉菜单中明确选择 build 分支。

### Q: 如何验证正在使用哪个分支的 workflow？
A: 查看 workflow 运行日志中的 "Set version from tag or manual trigger" 步骤，会显示:
- `🧪 Manual test trigger on branch: build`
- `📦 Test version: 0.0.0.dev20250130142035.build.test1`

### Q: TestPyPI 上传失败怎么办？
A: TestPyPI 有时不稳定，workflow 已设置 `continue-on-error: true`，不会影响整体流程。

## 分支策略建议

1. **main 分支**: 稳定版本，用于正式发布
2. **build 分支**: CI/CD 测试，验证构建流程
3. **cicd 分支**: 集成测试，验证完整发布流程
4. **feature 分支**: 功能开发，不触发 workflow

## 查看测试结果

```bash
# 查看 TestPyPI 上的包
# https://test.pypi.org/project/mirobody/

# 安装测试版本
pip install -i https://test.pypi.org/simple/ mirobody==0.0.0.dev20250130142035.build.test1
```