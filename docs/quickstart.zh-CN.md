# 快速开始

**[English](quickstart.md)** · **中文**

用一条命令体验健康数据标准化，或部署完整应用，处理文档并查询健康记录。

| 用途 | 安装方式 | 环境要求 |
| --- | --- | --- |
| 在终端或 Python 中标准化数据 | [使用 Python 库](#a--the-library) | uv 或 Python 3.12+；无需 API Key |
| 在浏览器中试用应用 | [Codespaces](#try-it-in-codespaces) | GitHub 账号和模型 API Key |
| 在本机运行应用和模型 | [本地 Docker](#run-it-100-locally) | Docker Compose；16 GB 内存，其中至少 8 GB 分配给 Docker |
| 开发和调试源码 | [源码安装](#c--a-checkout) | Python 3.12+、支持 pgvector 的 Postgres，以及模型 API Key 或本地模型服务 |

安装前也可以先打开 [Mirobody 网页演示](https://chat.mirobody.cn/demo?ref=github-quickstart&lang=zh-cn)，查看合成健康档案和问答效果。

<a id="a--the-library"></a>

<a id="a--这个库"></a>

## A · 使用 Python 库

[安装 uv](https://docs.astral.sh/uv/getting-started/installation/) 后，执行：

```bash
uvx --python 3.12 mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

uv 会按需下载 Python 和依赖。首次下载完成后，名称和单位解析均可离线使用，无需模型、API Key 或数据库。

<p align="center">
  <img src="images/resolve-demo.zh-CN.gif" alt="中文与日文的血红蛋白名称匹配同一 LOINC 编码，宽泛类别不予编码" width="880">
</p>

`血红蛋白` 和 `ヘモグロビン` 都会匹配 LOINC `718-7`。`血脂` 是一类指标的统称，会保留为未解析，避免误用某项具体检验的编码。

在 Python 3.12+ 项目中，先安装：

```bash
pip install mirobody
```

```python
from mirobody import resolve, resolve_reading

resolve("血红蛋白").loinc  # '718-7'
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2'
```

解析读数时请提供单位，因为单位可能影响编码选择。基础库仅依赖 numpy，也可以导入 Apple 健康导出文件：

```bash
mirobody import apple ~/Downloads/export.zip
```

如需从 PDF、照片或电子表格中提取读数，执行 `pip install 'mirobody[parse]'`，并配置模型 API Key。

接入 MCP 客户端时，运行 `uvx --python 3.12 mirobody mcp`，即可通过 stdio 使用离线词表工具。文档工具 `standardize_report` 则需要 `[parse]` 扩展和模型 API Key。主诉与诊断编码、单位转换及 FHIR 输出的用法见 [standardization.zh-CN.md](standardization.zh-CN.md)，可执行示例见 [`examples/`](../examples/README.md)。

<a id="b--the-stack"></a>

<a id="b--整套服务"></a>

## B · 部署完整应用

完整应用包含网页客户端、服务端、后台任务进程，以及支持 pgvector 的 Postgres。可以选择云端试用或本地部署。

<a id="try-it-in-codespaces"></a>

### 在 Codespaces 体验

[![在 GitHub Codespaces 中打开](https://github.com/codespaces/badge.svg)](https://codespaces.new/thetahealth/mirobody?quickstart=1)

创建 Codespace，等待终端中的自动配置完成。打开终端输出的 HTTPS 设置链接，输入自己的模型 API Key，例如 [OpenRouter](https://openrouter.ai/keys) 或 [OpenAI](https://platform.openai.com/api-keys)。设置页会发送真实请求验证 Key。`18060` 端口默认仅向自己转发。

应用和记录保存在 GitHub 云端，需要模型处理的文档和问题会发送给所选模型服务商。试用时请使用仓库提供的合成数据。费用分别按 [Codespaces 计费规则](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces) 和模型服务商的收费规则计算。用完后停止 Codespace，下次打开会自动重新启动应用。

<a id="run-it-100-locally"></a>

### 100% 在本机运行

安装 Git、OpenSSL 和 Docker Compose 后，执行：

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

主机建议配备 16 GB 内存，至少分配 8 GB 给 Docker，无需显卡或模型 API Key。Windows 用户请在 WSL 2 终端执行。首次启动会下载镜像和模型权重，之后文档识别和模型处理均在本机完成。CPU 生成回答可能需要几分钟；Mac 和 GPU 的配置方法见 [local-models.zh-CN.md](local-models.zh-CN.md)。

脚本会生成密钥并写入 `.env`，自动选择本地模型，最后输出应用链接。预构建镜像已包含 LOINC 词表，因此 Docker 部署不需要 Git LFS。

**如需使用云端模型，** 直接执行 `./deploy.sh`，不启用本地 profile。打开 `http://localhost:18060` 的设置链接，输入模型 API Key。记录保存在本机，需要模型处理的文档和问题发送给所选服务商。

### 登录并上传示例

在「邮箱验证码」标签使用 `you@mirobody.ai`、验证码 `111111` 登录，无需配置邮件服务。可以先问「我的胆固醇有什么变化？」，再到「数据」页上传 [`demo/upload/`](../demo/) 中的合成文件。这些文件未预先导入，上传后可验证完整的文档处理流程。

**如需保存真实数据，请在首次启动前设置 `SEED_DEMO_DATA=false`。** 否则应用会创建两个使用公开登录信息的示例账号。

也可以在 `.env` 中设置 `OPENROUTER_API_KEY`、`OPENAI_API_KEY`、`GOOGLE_API_KEY` 或 `ANTHROPIC_API_KEY`；使用 OpenAI 兼容网关时，通过 `<PROVIDER>_BASE_URL` 指定地址。[`config.llm.yaml`](../config.llm.yaml) 仅声明各条目读取的变量名，密钥应保存在环境变量中。修改 `.env` 后执行 `docker compose up -d`，`restart` 不会重新加载配置。

检查当前模型配置，并向各模型发送测试请求：

```bash
docker compose exec mirobody mirobody doctor --probe
```

<a id="c--a-checkout"></a>

<a id="c--一份检出"></a>

## C · 从源码开发

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[app]'
```

启动支持 pgvector 的 Postgres，也可以使用已有数据库：

```bash
docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pw \
    -e POSTGRES_DB=mirobody \
    pgvector/pgvector:pg17
```

```bash
mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody
```

在另一个终端检查服务状态：

```bash
curl -fsS http://127.0.0.1:18090/api/health
```

返回内容包含 `version`。在仓库根目录运行 `dev`，即可同时访问 `frontend/` 中的网页客户端和 API；Python 安装包本身不包含网页客户端。

`dev` 默认生成临时密钥。如需重启后保留会话和加密配置，请设置固定的 `JWT_KEY` 和 `CONFIG_ENCRYPTION_KEY`。同时请设置 `PG_ENCRYPTION_KEY`：不设置时，数据库中加密存储的记录字段使用 `config.yaml` 里公开的占位密钥（见 SECURITY.md）。正式部署使用 `serve`，它读取随包配置和 `config.{ENV}.yaml`，并要求使用持久化密钥。

Debian 或 Ubuntu 也可直接运行 `scripts/install_native.sh`，安装 PostgreSQL、pgvector 和 Python 环境，无需 Docker；然后通过 `scripts/start_native.sh` 启动 worker 和端口 18060 上的服务端。

<a id="when-it-does-not-come-up"></a>

<a id="没有出现时"></a>

## 启动问题排查

| 现象 | 检查方法 |
| --- | --- |
| 没有可用模型 | 打开 `./deploy.sh` 输出的设置链接完成配置，再执行 `mirobody doctor`。 |
| 新克隆的源码无法查询 LOINC | 执行 `git lfs pull`，下载词表数据。 |
| `dev` 找不到 Postgres | 使用 `--pg-url`，或设置 `PG_URL` / `DATABASE_URL`。`serve` 使用 `PG_HOST`、`PG_PORT`、`PG_USER`、`PG_PASSWORD` 和 `PG_DBNAME`。 |
| 设备不同步 | 检查 worker 日志和 Postgres 连接。 |
| Docker Hub 无法访问 | `deploy.sh` 会说明情况并通过镜像站重试；也可在 `.env` 中设置 `DOCKER_MIRROR=<registry>/` 指定镜像站。 |

<a id="next"></a>

## 下一步

详细部署步骤见[自部署指南](https://docs.mirobody.ai/zh/self-host)，主要功能见 [walkthrough.zh-CN.md](walkthrough.zh-CN.md)，Garmin、Oura 或 Whoop 的接入方式见 [provider-setup.zh-CN.md](provider-setup.zh-CN.md)。开发者可继续阅读[代码结构](repository-layout.zh-CN.md)和[数据处理流程](pipeline.md)（英文）。
