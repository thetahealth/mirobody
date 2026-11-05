<a id="在自己的机器上运行无需任何模型-key"></a>

# 本地模型部署

**[English](local-models.md)** · **中文** · **[日本語](local-models.ja.md)**

将模型部署在自己的机器上，即可在本地完成文档识别和健康问答，无需模型 API Key。首次启动需要下载模型权重，之后会复用缓存。

Mirobody 通过 [llama.cpp](https://github.com/ggml-org/llama.cpp) 的 OpenAI 兼容接口 `llama-server` 调用模型：一个模型负责问答和摘要，GLM-OCR-0.9B 负责识别报告图片与扫描页。

<a id="quick-start"></a>

<a id="快速开始"></a>

## 选择部署方式

| 平台 | 模型服务 | 环境要求 |
| --- | --- | --- |
| 无显卡的 Windows、Linux 或 macOS | Docker Compose，使用 `local-cpu` | 16 GB 内存，至少 8 GB 分配给 Docker |
| Apple 芯片 Mac | 在主机直接运行 `llama-server` | 默认模型需要 16 GB 内存 |
| 配备 NVIDIA 显卡的 Linux 或 Windows | Docker Compose，使用 `local` | 兼容显卡；Linux 需安装 NVIDIA Container Toolkit |
| 配备 Intel、AMD 或 NVIDIA 显卡的 Windows | 在主机运行 Vulkan 版 `llama-server` | Windows 本地源码目录及对应的 llama.cpp 版本 |

CPU 生成回答可能需要几分钟。Mac 建议在主机直接运行 llama.cpp，以使用 GPU；Mac 上的 Docker 容器无法使用该 GPU。

<a id="choose-a-size"></a>

<a id="选一个大小"></a>

## 选择模型

| 设置选项 | 问答模型 | 模型下载量，含 OCR | 硬件要求 |
| --- | --- | --- | --- |
| **Small**，默认轻量方案 | MiniCPM5-2B，Q8_0 | 约 4.1 GB | 16 GB 内存，无需显卡 |
| **Large**，大模型方案 | Qwen3.8-27B，IQ3_S | 约 14.5 GB | 32 GB 内存的 Mac，或 24 GB 显存的 NVIDIA 显卡 |

两种方案都使用 GLM-OCR 识别文档。对话中上传照片时，MiniCPM5-2B 接收 OCR 文字，Qwen3.8-27B 还可以直接识别图片。模型能力对比见 [选择模型](model-choice.zh-CN.md)，详细测量见[基准测试记录](../benchmarks/local_models/README.md)。

<a id="start-the-models"></a>

<a id="启动模型"></a>

## 启动模型服务

以下命令均在仓库根目录执行，即 `deploy.sh` 所在目录。预设使用相对路径，包括 `docker/minicpm5-2b.jinja`。`--models-max 2` 用于同时保留问答模型和 OCR 模型；切换问答模型时，会卸载之前的模型。

### Docker CPU 部署

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

脚本会启动应用与模型，将 `COMPOSE_PROFILES` 保存到 `.env`，并将 `LOCAL_BASE_URL` 和 `LOCAL_OCR_BASE_URL` 设为 `http://llama:8080/v1`。首次下载时可用 `docker compose logs -f llama_cpu` 查看进度。

已有部署可以执行 `docker compose --profile local-cpu up -d`，并在 `.env` 中加入 `COMPOSE_PROFILES=local-cpu`，确保以后执行 `docker compose down`、再执行 `up` 时仍会启动模型服务。

### Apple 芯片 Mac

在主机直接启动 llama.cpp：

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

### Docker NVIDIA GPU 部署

Linux 先安装 NVIDIA Container Toolkit；Windows 使用 Docker Desktop 的 WSL 2 后端，然后执行：

```bash
COMPOSE_PROFILES=local ./deploy.sh
```

### Windows 原生模型服务

winget 官方包使用 Vulkan，支持 x64 和 arm64 上的 Intel、AMD、NVIDIA 显卡。在 PowerShell 中执行：

```powershell
winget install --id ggml.llamacpp
git clone https://github.com/thetahealth/mirobody.git C:\src\mirobody
cd C:\src\mirobody
llama-server --models-preset docker\local-models.ini --port 8080 --models-max 2
```

请使用 Windows 本地磁盘目录，不要以 `\\wsl.localhost\...` UNC 路径作为工作目录。这里的源码与应用在 WSL 中的源码应保持同一发布版本。只使用 CPU 时，将预设改为 `docker\local-models-cpu.ini`。

使用 CUDA 版时，请选择支持当前显卡的 llama.cpp 发行版，并安装配套运行库。

Mirobody 应用本身仍在 Docker Desktop 中运行。打开 **Settings → Resources → WSL integration**，在 WSL 终端中克隆仓库并执行 `./deploy.sh`。如果 Docker 提示 `permission denied`，执行 `sudo usermod -aG docker $USER`，然后在 Windows 中执行 `wsl --shutdown`。

Linux 使用其他显卡，或希望不通过 Docker 启动模型时，可安装对应的 [llama.cpp 发行版](https://github.com/ggml-org/llama.cpp/releases)，使用相同的 `llama-server` 命令。

<a id="point-mirobody-at-them"></a>

<a id="让-mirobody-指向它们"></a>

## 连接 Mirobody

打开 `./deploy.sh` 输出的设置链接，或进入**设置 → 模型**，选择 **100% 在本机运行**。Mirobody 会依次检查 `host.docker.internal:8080`、`llama:8080` 和 `127.0.0.1:8080`。选择模型后，页面会验证服务是否提供该模型、加载模型并显示进度。选择结果会加密保存在数据库中。

也可以直接在 `.env` 中指定地址：

```bash
LOCAL_BASE_URL=http://host.docker.internal:8080/v1
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
```

使用 `local` 或 `local-cpu` Compose profile 时，两个地址均填写 `http://llama:8080/v1`。如果应用和模型都直接运行在主机上，则均填写 `http://127.0.0.1:8080/v1`。

`.env` 中的云端模型 Key 优先级更高，存在这些 Key 时，设置页会阻止选择本地模式。请删除相应 Key，或明确设置 `DEFAULT_MODEL=local`、`UTILS_VISION_MODEL=local-utils` 和 `UTILS_TEXT_MODEL=local-utils`。本地部署脚本不会将 shell 中的云端 Key 复制到 `.env`，发现文件中已有 Key 时会提示。

修改 `.env` 后执行 `docker compose up -d`；仅重启不会重新加载配置。

<a id="check-it"></a>

<a id="检查一下"></a>

## 验证连接

```bash
docker compose exec mirobody mirobody doctor --probe
```

该命令会向选定模型发送真实请求，检查工具调用、结构化抽取、图片识别和 OCR。

<a id="when-it-does-not-work"></a>
<a id="without-a-gpu"></a>

<a id="不工作时"></a>

<a id="没有显卡时"></a>

## 常见问题

| 现象 | 处理方法 |
| --- | --- |
| 找不到模型服务 | 确认服务已在端口 8080 启动。Linux 还需检查下方的主机网络配置。 |
| 首次提问等待较久 | 模型可能仍在下载。查看 `docker compose logs -f llama_cpu`；GPU 服务查看 `llama`。 |
| Docker 内存不足 | 在 Docker Desktop 中分配至少 8 GB 内存，或执行 `colima start --memory 8`。CPU 使用 `docker/local-models-cpu.ini`，GPU 使用 `docker/local-models.ini`。 |
| 预设文件报错 `Is a directory` | 确认 Docker 能访问源码目录。Colima 默认共享主目录，可将源码放在其中，或将路径加入 `mounts`。 |
| 无法从 Hugging Face 下载 | Compose 在 `.env` 中设置 `HF_ENDPOINT=https://<mirror>`；直接运行 `llama-server` 时，在启动它的 shell 中设置。 |
| 系统代理导致本地请求失败 | 在发起请求的 shell 中设置 `export NO_PROXY=localhost,127.0.0.1`。WSL 继承 Windows 代理时也需如此处理。 |

模型缓存在当前 Compose 项目的 `mirobody_models` 卷中。更换源码目录或 `COMPOSE_PROJECT_NAME` 可能创建新的缓存并重新下载。镜像固定使用 llama.cpp b11429；如需其他构建或镜像站，可在 `.env` 中设置 `LLAMA_CPU_IMAGE` 或 `LLAMA_IMAGE`，再执行 `docker compose up -d`。

**Linux 主机网络：** `host.docker.internal` 指向 Docker 网桥，主机上的模型服务需要监听该地址，例如 `--host 172.17.0.1`，实际地址可用 `ip -4 addr show docker0` 查询。监听 `0.0.0.0` 会将无需认证的模型服务开放到网络。Mac 的 Docker Desktop 和 Colima 可以连接主机默认的 `127.0.0.1` 监听地址。Compose 中的模型服务通过内部网络通信，无需发布主机端口。应用本身也默认只绑定 `127.0.0.1`；改为 `MIROBODY_BIND=0.0.0.0` 前，请先阅读 [SECURITY.md](../SECURITY.md)（英文），尤其注意示例账号的公开验证码 `111111`。

<a id="other-models"></a>
<a id="other-servers"></a>

<a id="其他模型"></a>

<a id="其他服务"></a>

## 更换模型或服务

将模型加入 llama.cpp 预设后，在设置页选择，或设置：

```bash
LOCAL_MODEL=qwen3.8-27b
LOCAL_OCR_MODEL=glm-ocr
```

模型服务必须提供所选模型。其他 OpenAI 兼容服务也可通过相同的地址和模型名配置接入，使用 `curl <address>/v1/models` 查看模型列表。`local` 条目使用 `temperature: 0`，其他模型专用参数填写在预设中。

云端模型使用 `config.llm.yaml` 中各条目的 `model_env` 变量，例如 `OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5.5`。设置页保存新模型前会发送真实请求验证。

使用 Ollama 时，请在 `ollama serve` 前设置 `OLLAMA_CONTEXT_LENGTH=65536`，避免 Mirobody 的提示词触发 `exceed_context_size_error`。Ollama 0.34.1 及之后的 `glm-ocr` 存在无法正常结束的问题（[issue](https://github.com/ollama/ollama/issues/18609)），问题解决前请继续使用 llama.cpp 处理文档 OCR。

<a id="the-document-reader"></a>
<a id="what-each-model-can-read-in-a-photo"></a>
<a id="things-that-behave-differently-from-a-hosted-model"></a>
<a id="how-a-document-is-read-whichever-model-reads-the-rest"></a>

<a id="读文档的模型"></a>

<a id="每个模型能从照片里读出什么"></a>

<a id="和托管模型不一样的地方"></a>

<a id="不管谁读剩下的部分文档都这样被读"></a>

## 文档、照片与计算

GLM-OCR 用于识别文字和表格，无法判断餐食、皮疹等场景。默认问答模型接收照片的 OCR 文字，无法提取文字时会要求用户描述图片。Qwen3.8-27B 等视觉模型可以直接识别照片，但餐食热量仍只能粗略估算。

本地与云端模型使用相同的文档处理流程：长报告按页处理，可识别的表格优先按规则抽取，其余内容交给文本模型。处理流程见 [处理流程参考](pipeline.md)（英文），其他 OCR 模型及其配置见 [文档识别](model-choice.zh-CN.md#the-document-reader-glm-ocr-09b)。

本地模型通过 `code_exec` 对查询记录进行计算：应用能连接 Docker 服务时用 Python，否则在应用内用 JavaScript。配置方法见[聊天中的计算](python-analysis.zh-CN.md)。
