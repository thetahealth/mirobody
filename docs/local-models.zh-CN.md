# 在自己的机器上运行，无需任何模型 Key

**[English](local-models.md)** · **中文**

Mirobody 用到的每一个模型，都能跟服务运行在同一台机器上。这样一来，你的记录、文档和问题都留在那台机器上；只有模型权重会被下载，且只下载一次。

**本地模型的运行时是 [llama.cpp](https://github.com/ggml-org/llama.cpp)。** Mirobody 自己不运行任何模型：`llama-server` 通过 OpenAI 兼容的 API 提供服务，Mirobody 调用它。两个模型，两份工作：**GLM-OCR-0.9B** 把报告照片和扫描页读成文字和表格，另一个模型**回答问题**，并写标题、摘要和日记条目。两个都不需要认得编码：表格的行按表头读，不用模型，② 转译会离线给它们编码。

<a id="quick-start"></a>

## 快速开始

| 你的机器 | 运行 | 第一次回答 |
| --- | --- | --- |
| 任何装了 Docker、没有显卡的电脑（Windows、Linux、macOS） | `COMPOSE_PROFILES=local-cpu ./deploy.sh` | CPU 上要几分钟（[没有显卡时](#without-a-gpu)） |
| 用 Apple 芯片的 Mac | `brew install llama.cpp`，然后在 Mac 上运行 `llama-server`（[见下](#start-the-models)） | 16 GB Apple 芯片笔记本的 GPU 上约 30 秒 |
| Linux 或带 NVIDIA 显卡的 Windows | `COMPOSE_PROFILES=local ./deploy.sh` | 用 GPU |

默认大小需要 16 GB 内存，Docker 的虚拟机至少要分到其中 8 GB。首次启动会从 Hugging Face 下载约 4.1 GB 的模型，另外还有容器镜像。要不要完全在本机运行，以及小号在同一套评测上跟云端模型比起来如何，见 [model-choice.zh-CN.md](model-choice.zh-CN.md)。

<a id="choose-a-size"></a>

## 选一个大小

设置页提供两种大小，数字如下。下载量指的是预设会拉取的 GGUF 文件，包含读文档的模型；内存指的是两个模型都加载、正在回答时 `llama-server` 占用的最大值。

| 大小 | 回答 | 下载量 | 内存 | 每次回答 | 对话里的照片 | 在评测上 |
| --- | --- | --- | --- | --- | --- | --- |
| **小号**，默认 | MiniCPM5-2B，Q8_0 | 4.1 GB | 7.6 GB | 中位数 18 秒，48 GB 的 Apple 芯片主机，用它的 GPU | 读它的 OCR 文字 | 两次分别过了 24 道题里的 23 道和 24 道（得分 229 和 225/248），140 个印刷行存对 132 个，31 条日记条目写出 26 条 |
| **大号** | Qwen3.8-27B，IQ3_S（ISTA-DASLab GSQ-RCO） | 14.5 GB | 20.8 GB | 约 2 分钟（中位数 138 秒），48 GB 的 Apple 芯片主机，用它的 GPU | 直接看 | 24 道题通过 22 道（得分 240/248），印刷行 140 行读对 139 行，日记 31 条对 29 条 |

小号能运行在任何 16 GB 内存、没有显卡的电脑上；旁边的服务再占约 1 GB。大号要 32 GB 内存的 Mac，或者 24 GB 显存的 NVIDIA 显卡。大号那一行是 2026-10-08 在较早的 harness 上测的，当时小号（还是 Q4_K_M）在同一台主机、同一份记录上得 229 分，大号 240 分，每个回答的耗时约为大号的九分之一（见 [model-choice.zh-CN.md](model-choice.zh-CN.md)）。2026-10-10 起小号换成 Q8_0：在 1.5.5 的 harness 上 Q4_K_M 两次得 212 和 194，而 Q8_0 在 CPU 上读提示词还更快（[没有显卡时](#without-a-gpu)）。另外测过并放弃了两个回答模型：MiniCPM5-1B 两种采样都只得 109/248；Qwen3.5-9B 在 16 GB 内存上和服务放不到一起。

两种大小都用 GLM-OCR 读文档、按规则读表格，所以同一份化验单读出来的读数是一样的。变的是问题答得好不好、多快，以及对话里的照片是被直接看（大号）还是只读它的 OCR 文字（小号）。这些数字背后的评测、以及怎么重新运行，见 [`benchmarks/local_models/`](../benchmarks/local_models/README.md)。

<a id="start-the-models"></a>

## 启动模型

按自己的机器选一行命令。每一行都提供同一份预设，[`docker/local-models.ini`](../docker/local-models.ini)；要在 `mirobody` 文件夹（也就是放着 `deploy.sh` 的那个）里启动。`--models-max 2` 让内存里只留一个回答模型和一个读文档的模型，所以在设置页换一个大小会先卸载前一个。

**任何装了 Docker、不需要显卡的电脑**（Windows、Linux 或 macOS）。最慢的一条路线，但除了 Docker 什么都不需要。首次启动会下载约 4.8 GB：4.1 GB 的模型加上约 0.7 GB 的镜像（llama.cpp 的 CPU 镜像约 310 MB，应用约 230 MB，Postgres 约 160 MB），解包后约占 2 GB（在 Linux x86 上分别是 850、640 和 460 MB）。

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh     # 服务加上 llama.cpp 的 CPU 镜像，运行在旁边
```

`deploy.sh` 会把 `COMPOSE_PROFILES` 写进 `.env`，这样之后的 `docker compose up -d` 会留住这个模型服务，并把两个本地条目都指向它（`LOCAL_BASE_URL` 和 `LOCAL_OCR_BASE_URL`，都是 `http://llama:8080/v1`），所以设置页不用再问什么。你 shell 里导出的厂商 Key 不会写进 `.env`；如果 `.env` 里已经有一把，会把它指出来，因为那家厂商会被优先使用。第一个问题要等模型下载完：`docker compose logs -f llama_cpu` 能看到下载进度。

在一套正在运行的服务上，`docker compose --profile local-cpu up -d` 会在应用旁边启动这个服务（设置页显示的就是这一行）。同时也把 `COMPOSE_PROFILES=local-cpu` 加进 `.env`，否则一次 `docker compose down` 会把它移除，下一次 `up` 就不会带上它。

**macOS**（Apple 芯片）。Mac 上的容器用不了它的 GPU，所以 llama.cpp 要运行在 Mac 本身上，设置页会找到它：

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

**Linux，或者装了 Docker Desktop 并带 NVIDIA 显卡的 Windows**，跟应用放在一起（Linux 需要 NVIDIA Container Toolkit；CUDA 镜像约 3 GB）：

```bash
COMPOSE_PROFILES=local ./deploy.sh         # 服务加上运行在 CUDA 镜像上的 `llama` 服务
```

**Windows**，要用任何显卡（Intel、AMD 或 NVIDIA，经 Vulkan；没有显卡就用 CPU）。winget 的 `ggml.llamacpp` 是 llama.cpp 官方的 Vulkan 发行版，有 x64 和 arm64 两种。在 PowerShell 里，从 WSL 里的检出目录进入（`Ubuntu` 是 `wsl -l` 里那个发行版的名字），这样预设里的相对路径才能解析：

```powershell
winget install --id ggml.llamacpp
cd \\wsl.localhost\Ubuntu\home\<you>\mirobody
llama-server --models-preset docker\local-models.ini --port 8080 --models-max 2
```

设置页会通过 Docker Desktop 的 `host.docker.internal` 去找它，这个地址在 Mac 上能连到主机 `127.0.0.1` 上的服务（用 colima 测过）。**这条路线我们还没有在 Windows 上运行过**；如果页面找不到服务，请改用上面那条 Docker 命令，它不需要任何主机网络配置。

**用别的显卡（AMD、Intel）的 Linux**，或者模型不想用 Docker：找一个对应的 [llama.cpp 发行版](https://github.com/ggml-org/llama.cpp/releases)构建，用同一行 `llama-server` 命令。

**Mirobody 本身在 Windows 上**运行在 Docker Desktop 里（WSL 2 后端）：打开一个 WSL 终端（Ubuntu），在里面克隆仓库并运行 `./deploy.sh`，和在 Linux 上一样。脚本在每一份检出里都保持 LF 换行。

选好大小之后，第一个问题要等它下载完；之后再启动就读缓存了。如果连不上 huggingface.co，把下载指向一个镜像站：给 compose 服务用的话在 `.env` 里写 `HF_ENDPOINT=https://<mirror>`，给 `llama-server` 用的话在启动它之前的 shell 里写。被下载的只有模型本身；关于你的任何信息都不会被发出去。

<a id="point-mirobody-at-them"></a>

## 让 Mirobody 指向它们

在设置页上（`./deploy.sh` 会打印它的链接；之后可以在**设置 → 模型**里找到），选择**100% 在本机运行**。Mirobody 会依次在 `host.docker.internal:8080`、compose 的 `llama:8080`、`127.0.0.1:8080` 寻找这个服务，列出它提供的模型，检查它是否提供你选的那两个（默认是预设里的，除非你另外选了别的），让它加载，并显示加载进度。这个选择会加密存进数据库。

或者在 `.env` 里写两行，再 `docker compose up -d`（`restart` 不会重新读取 `.env`）：

```bash
LOCAL_BASE_URL=http://host.docker.internal:8080/v1        # 应用在 Docker 里，模型在主机上
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
# 用 local 或 local-cpu profile 时：两者都写 http://llama:8080/v1
# 应用和模型都在主机上时：两者都写 http://127.0.0.1:8080/v1
```

**`.env` 里的厂商 Key 优先级更高**，所以只要 `.env` 里有一把，设置页就会拒绝本机这个选项。要强制使用本地模型，请设置 `DEFAULT_MODEL=local`、`UTILS_VISION_MODEL=local-utils` 和 `UTILS_TEXT_MODEL=local-utils`。

<a id="check-it"></a>

## 检查一下

```bash
docker compose exec mirobody mirobody doctor --probe
```

`doctor` 显示每个环节用的是哪个条目；`--probe` 会经产品自己的代码给每一个发送一次真实请求（一次工具调用、一次按 schema 的回答、一张渲染出来的图、OCR 的各遍），并检查每个服务运行的是不是条目里写的那个模型。

<a id="when-it-does-not-work"></a>

## 不工作时

| 你看到的 | 怎么办 |
| --- | --- |
| 设置页找不到服务 | 启动上面某一行命令；如果 `llama-server` 运行在主机上，保持 `--port 8080`。在 Linux 上，见下面的网络说明。 |
| 设置页拒绝**100% 在本机运行** | `.env` 里的厂商 Key 优先级更高：把它删除，或者按[让 Mirobody 指向它们](#point-mirobody-at-them)里说的设置那三个变量。 |
| 第一个问题等了很久 | 模型正在下载：`docker compose logs -f llama_cpu`（GPU 服务看 `llama`）。 |
| 模型服务的日志里出现 `failed to initialize router models: ... Is a directory` | Docker 看不到这份检出，在预设该在的位置挂载了一个空目录。Colima 默认只共享你的主目录：把检出放在它下面，或者把这个路径加进 colima 的 `mounts`。 |
| Docker 内存不够时模型服务会停 | 给 Docker 的虚拟机至少 8 GB：在 Docker Desktop 的设置里，或者用 `colima start --memory 8`。 |
| 改了 `.env` 没有效果 | 运行 `docker compose up -d`；`docker compose restart` 不会重新读取 `.env`。 |

**Linux 上的网络配置。** `compose.yaml` 给 Linux 上的 Docker Engine 映射了 `host.docker.internal`，并把它排除在 `HTTP_PROXY` 之外。在 Mac 上（Docker Desktop 或 colima），这个名字连到的是主机的回环地址，所以 `llama-server` 保持默认的 `127.0.0.1` 就行。在 Linux 上它指向的是 Docker 的网桥，所以主机上的服务要监听在那个地址上：给它网桥地址（`--host 172.17.0.1`，从 `ip -4 addr show docker0` 获取），不要用 `0.0.0.0`：那会把这个不需要 Key 的服务开放给你网络上的每一台机器。`local` 和 `local-cpu` 这两个服务都不需要这么做：应用在 compose 自己的网络上就能连到它们，并且它们不发布端口。应用本身只发布在 `127.0.0.1` 上；`MIROBODY_BIND=0.0.0.0` 会把它开放给你的网络，而演示账号的验证码 `111111` 在那种情况下是公开的（先看 [SECURITY.md](../SECURITY.md)（英文），了解要先改什么）。

<a id="without-a-gpu"></a>

## 没有显卡时

上面那些耗时都是 Apple 芯片的，llama.cpp 在那上面用 GPU 运行。只用 CPU 时一次回答要几分钟，具体多久看处理器。下面两组测量都是在 llama.cpp 的 CPU 镜像里（`local-cpu` profile）、用 4 个 vCPU 运行的小号，时间是 2026-10-07：

| | 16 GB 的 Apple 芯片笔记本，运行在 colima 的 arm64 虚拟机里 | Intel Xeon Gold 5220R，Linux x86（一次外部评测） |
| --- | --- | --- |
| MiniCPM5-2B 读提示词 | 每秒约 50 个 token | 没有测 |
| MiniCPM5-2B 写回答 | 每秒约 18 个 token | 每秒 3 到 7 个 |
| 第一次回答 | 2–3 分钟（一个 6.8k token 的提示词在 137 秒内答完） | 最长约 15 分钟，含模型下载 |
| 一张报告照片 | GLM-OCR 读一页约 17 秒，两遍加起来 | 最长约 9 分钟，直到上传处理完 |
| 一页 PDF | 没有测 | 最长约 12 分钟，直到上传处理完 |

- 后续轮次会复用服务器的提示词缓存，只读新增的部分；一次新对话要重新读一遍整个提示词（两次测量里分别是 6.8k 和 8.4k token）。预设给每个模型两个槽位（`np = 2`），所以后台写的标题或摘要会跟回答抢 CPU 核心。
- 两个模型都加载后在容器里约占 6.0 GiB，所以 Docker 的虚拟机至少需要 8 GB 内存。Docker Desktop 默认分配电脑内存的一半，16 GB 的机器上就是 8 GB；colima 默认只给 2 GB，除非指定 `--memory 8`。
- 产品的工具调用探测（`doctor --probe`）通过了这个服务，两个模型下载之后都从它的缓存卷加载。
- 这份缓存是某一个 compose 项目的 `mirobody_models` 卷：换一份检出，或者换一个 `COMPOSE_PROJECT_NAME`，模型就要重新下载。
- CPU 镜像的日志里会出现 `warning: no usable GPU found, --gpu-layers option will be ignored`：预设要的是 GPU，`local` 服务用的就是这个设置，CPU 版本会把它忽略。

**没有显卡时哪个文件读得最快。** 同一个 CPU 镜像、4 个 vCPU，运行在一台 48 GB Apple 芯片主机的 colima 虚拟机里，每个容器只放一个模型，每个提示词都是冷读（2026-10-10）：

| MiniCPM5-2B | 读 1.8k token 的提示词 | 读 10.6k token 的提示词 | 写回答（短 / 读完 10.6k 之后） | 10.6k 提示词到答完 | 内存 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q4_K_M | 每秒 94 个 token | 每秒 45 个 | 每秒 36 / 17 个 | 239 秒 | 3.2 GB |
| Q8_0 | 142 | 52 | 37 / 17 | 210 秒 | 3.6 GB |
| Q4_0，从 OpenBMB 的 F16 自己量化 | 150 | 52 | 42 / 18 | 209 秒 | 3.1 GB |

- Q8_0 读提示词比 Q4_K_M 快，写得一样快，内存多 0.4 GB。在 CPU 上，一次回答的大部分时间花在读提示词上。
- 提示词越长读得越慢，10.6k token 时只有 1.8k 时的三分之一，所以服务器的提示词缓存能复用多少，和选哪个文件一样要紧。
- MiniCPM5-1B 读得大约快一倍（短提示词每秒 229 到 390 个 token），但在评测的问题上只得 109/248，2B 是 194 到 229：当不了一个大小。

这两个 compose 服务运行的是 llama.cpp 的 b11429 构建版，也是这次评测使用的版本：`local-cpu` 用的是 `ghcr.io/ggml-org/llama.cpp:server-b11429`，`local` 用的是 `server-cuda-b11429`。在 `.env` 里设置 `LLAMA_CPU_IMAGE` 和 `LLAMA_IMAGE` 可以换掉它们，再 `docker compose up -d`：可以换成更新的构建版，或者在连不上 ghcr.io 的地方，换成镜像它的某个 registry 上的同一个标签（`LLAMA_CPU_IMAGE=<mirror>/ggml-org/llama.cpp:server-b11429`）。

<a id="other-models"></a>

## 其他模型

回答模型默认是 `minicpm5-2b`，除非你选了大号（`qwen3.8-27b`）；读文档的模型是 `glm-ocr`：这些都是 `docker/local-models.ini` 里的小节，由 `config.llm.yaml` 里的 `local` 条目去要。设置页会写下你选的大小；在 `.env` 里同样的设置是一行：

```bash
LOCAL_MODEL=qwen3.8-27b        # 大号：回答问题、写标题和摘要
LOCAL_OCR_MODEL=glm-ocr        # 读报告照片和扫描页
```

`local` 条目发送 `temperature: 0`。MiniCPM5-2B 的模型卡给思考模式的是 `temperature 1.0, top_p 0.95, min_p 0`；在评测上按它跑三次得 182 到 200 分（满分 248），temperature 0 两次是 194 和 212，丢的分在工具没给过的数字和医学说法上（[基准](../benchmarks/local_models/README.md#sampling-thinking-two-more-models-and-what-post-training-is-for-2026-10-10)）。你加进来的模型也用这个温度；它的模型卡里其余的参数写进它在预设里的小节。

要运行别的模型，把它的小节加进预设（或者用别的方式提供服务），然后在设置页上选它（页面会列出服务提供的模型），或者在 `.env` 里写上它的名字。这样选出来的模型也会被同样地检查：服务必须真的提供它。挑出这两个模型之前测过的候选都在 [local-models-roadmap.zh-CN.md](local-models-roadmap.zh-CN.md) 里。看不了图的模型会从它的服务那里被检测出来，照片会以文字的形式发给它，而不是图片本身。

厂商 Key 也是同样的方式：设置页会在 Key 旁边显示模型，也可以换一个名字（比如 `OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5.5`），保存前会用一次真实请求验证。每个条目对应的变量就是它在 `config.llm.yaml` 里的 `model_env`。

<a id="other-servers"></a>

### 其他服务

任何 OpenAI 兼容的服务都能用：设置那两个地址，以及它提供的模型名（用 `curl <address>/v1/models` 查），可以在设置页上设置，也可以写成 `LOCAL_MODEL` 和 `LOCAL_OCR_MODEL`。

**Ollama。** `ollama pull qwen3.8:27b`（17 GB）在大号早先那组题（8 道题各问两遍）上全部答对，而且最快。有两件事要知道：

- Ollama 会按 GPU 内存设置上下文长度，显存低于 24 GB 时是 4,096 个 token，会把 Mirobody 的提示词截断却不报错。请在 `ollama serve` 之前设置 `OLLAMA_CONTEXT_LENGTH=65536`。
- 在 0.34.1 及之后的版本里，它的 `glm-ocr` 读完一页之后不会停下来（[ollama/ollama#18609](https://github.com/ollama/ollama/issues/18609)）。在这个问题修好之前，文档部分请继续用 llama.cpp。

**Ternary Bonsai 2 27B** 是同一个 Qwen3.8-27B 压缩到 6.6 GB 的版本，答得一样好，但目前只有 PrismML 的 [llama.cpp 分支](https://github.com/PrismML-Eng/llama.cpp)能运行它。一旦上游 llama.cpp 支持它，它就是大号的候选，下载量减半。

<a id="the-document-reader"></a>

## 读文档的模型

两种大小都用 GLM-OCR-0.9B 读文档。上游 llama.cpp 支持的三个小型 OCR 模型，都在合成报告上走过了产品完整的抽取路径（[`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md)）：303 个印刷行里，GLM-OCR 按印刷值存对了 283 个（去掉生成器的「SYNTHETIC SAMPLE」水印、和真实报告一样时是 302 个），没有一个是页面上没印的；PaddleOCR-VL-1.6 是 278（282）个，其中 12（8）个是页面上没印的；MinerU2.5-Pro 是 279（299）个，其中 3（4）个是没印的。301 个手写行里，三个模型分别存对了 99、62 和 55 个（去掉水印是 219、136 和 171 个）。GLM-OCR 仍是默认；完整的表格和原因见 [model-choice.zh-CN.md](model-choice.zh-CN.md#the-document-reader-glm-ocr-09b)。

PaddleOCR-VL-1.6 以 `[paddleocr-vl]` 的名字在预设里（Apache-2.0，带视觉投影器共 1.8 GB），从 1.5.4 起产品能读懂它答案里的 OTSL 表格和 LaTeX 单位。它的提示词和 GLM-OCR 不一样，所以光设置 `LOCAL_OCR_MODEL=paddleocr-vl` 还不够：`config.llm.yaml` 里的 `local-ocr` 条目需要同时改两处：

```yaml
  local-ocr:
    model: paddleocr-vl           # 预设里的那个小节
    ocr_prompts:
      text: "OCR:"                # 它自己的任务提示词，来自它的模型卡片
      tables: "Table Recognition:"
```

源码安装读的是检出目录里的文件；Docker 镜像带的是自己那一份，所以要把改过的文件挂载给 `mirobody` 和 `mirobody_worker`，写进 `compose.override.yaml`（[model-choice.zh-CN.md](model-choice.zh-CN.md#health-data-on-openrouter) 里有示例），再 `docker compose up -d` 并 `doctor --probe`。预期它的 OCR 文字里会多出一些行，在手写文档上会有几遍循环到 token 上限（28 页里有 7 页），以及页面上没印的行。

<a id="what-each-model-can-read-in-a-photo"></a>

## 每个模型能从照片里读出什么

GLM-OCR 只读印刷的文字和表格。它说不出一张照片拍的是什么：一餐饭、一块皮疹或者一个场景都在它能力之外。Mirobody 只发它官方的提示词（`Text Recognition:` 和 `Table Recognition:`，也就是 `local-ocr` 条目的 `ocr_prompts`）。它按 JSON 做信息抽取的提示词测过但没有用：在一张血压显示屏、一张中文营养成分表和一张 FDA 标签上，它每次都把值填错了字段（128/91 的读数把收缩压填成 76，营养成分表的 NRV% 那一列被当成能量「3」），问到一张没有文字的餐食照片上是什么菜时，它编了一个出来。

对每种照片，这意味着什么（48 GB 的 Apple 芯片主机，2026-09-30）：

| 照片 | GLM-OCR（文字提示） | Qwen3.8-27B（大号，能看图） |
| --- | --- | --- |
| 化验单、营养成分表、FDA 标签 | 每一行都读对 | 能读 |
| 监护仪显示 128/91、脉搏 76 | 三个数字，没有标签 | 「128/91 mmHg，脉搏 76」 |
| 一盘菜 | 什么都没有（没有文字） | 给出热量区间和推理依据，但菜名认错 |

大号能看图，所以对话里的照片会以图片的形式到达它，一餐饭能被粗略估算：预期得到一个区间，而不是一个确切的数。默认大小做不到（MiniCPM5-2B；任何没有带 `mmproj` 提供服务的模型都一样）：这会从它的服务那里被检测出来，agent 收到的是照片的 OCR 文字，并被告知这就是全部内容；问到一餐饭时，它会说看不到这张照片，并问吃了什么。

<a id="things-that-behave-differently-from-a-hosted-model"></a>

## 和托管模型不一样的地方

- **读提示词的时候不会有任何流式输出。** 一轮新增 6.6k token 的对话，在 48 GB 的主机上等第一个字节要一分钟以上，在 Apple 芯片笔记本的 4 个 CPU 核心上要两分钟以上，处理器更慢时还要更久（[没有显卡时](#without-a-gpu)）。`local` 条目允许 600 秒的静默（`stream_chunk_timeout`）；库默认的 120 秒会让一轮长对话失败。
- **加载后的第一轮是最慢的一轮。** 服务器会缓存它读过的提示词，所以之后的轮次只读变化的部分。
- **一次回复可能全是推理过程。** 回复里既没有答案文字也没有工具调用时，agent 会再问一次；如果第二次也是空的，对话会说它没有答案，而不是显示一条空消息。

<a id="how-a-document-is-read-whichever-model-reads-the-rest"></a>

## 不管谁读剩下的部分，文档都这样被读

这些规则在用厂商 Key 时同样成立：厂商的模型只会从表格规则剩下的部分里抽取读数，不过文件的标题和摘要仍然由它从文档文字里写出来。

- **一份长报告按页读。** 超过 3,000 字符、带页眉的文字会一页一个请求地发给模型，两页并发，再把各页的读数拼起来：一次性发整份时，MiniCPM5-2B 对一本 7 页体检册的 78 行一个都没读出来，按页发则全部读出来了。日志类文件的行保留它们印出来的日期。
- **表格按表头读。** 规则认得的表头（项目名称 / 结果 / 参考值 / 单位，Analyte / Result / Unit，或者 CSV 的第一行）下面的行，只要看起来像读数，并且页面的另一份拷贝（文字层，或者 OCR 的文字提示那一遍）显示同样的值，就会按印刷值存下，标为 `rules:table@v1`。病人信息会被跳过。OCR 返回的表格会按报告印刷的样子来读：空单元格挪了位置的行会按内容重新排好，拆成两行或两个单元格的表头会被合并，一张表第一个表头之上的行会沿用它，没有表头的页面在不含糊的情况下按单元格内容来判断类型。没读到的行、表格之外带着数字或某个发现的文字，以及没有表格的文档，都交给文本模型去读，但不包括规则已经读出的行在文字提示那一遍里的重复内容；对同一个印刷行，规则的结果优先于模型的结果，在词表归为同一条序列的任何名字下都是如此。
- **任何 OCR 模型的答案都按同样的方式读。** 答案里的 OTSL 表格（PaddleOCR-VL、MinerU）会变成 HTML，它的 LaTeX（`\(\mu mol/L\)`）会变成排版出来的字符，一行被重复到 token 上限的内容只保留一份；每一遍的上限是 8,192 个 token。
