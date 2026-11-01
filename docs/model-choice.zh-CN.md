# 选择模型

[English](model-choice.md) · **中文**

选择模型前，先确定健康数据能否交给外部服务处理，再考虑本机硬件和图像理解需求。
Mirobody 支持全本地、云端和混合部署。对话、文档提取、健康日记可以分别指定模型，
不必让同一个模型承担所有任务。

## 部署方案

| 需求 | 选择 | 硬件或服务要求 |
| --- | --- | --- |
| 所有模型处理都在本机完成 | MiniCPM5-2B 回答问题，GLM-OCR-0.9B 识别文档 | 16 GB 内存，无须显卡 |
| 数据留在本机，同时直接理解图像 | Qwen3.8-27B 回答问题，GLM-OCR-0.9B 识别文档 | 32 GB 内存的 Mac，或 24 GB 显存的 NVIDIA 显卡 |
| 不在本机部署推理服务 | 配置云端模型 | 模型 API Key；相应任务的数据会发送给服务商 |
| 文档图片留在本机，对话使用云端模型 | 本地 GLM-OCR 配合云端对话 | 本地 OCR 服务和模型 API Key |

设置页可直接选择全本地运行或填写云端 API Key；混合方案需要修改 `.env`。
仅使用 CPU 时，一次回答可能需要数分钟，具体取决于硬件、模型及对话长度。

## 本地模型

轻量配置是默认选项，采用 MiniCPM5-2B 的 Q8_0 权重。增强配置采用
Qwen3.8-27B 的 IQ3_S 权重，并加载视觉投影模型。两种配置都使用
GLM-OCR-0.9B 识别文档。

模型由 [llama.cpp](https://github.com/ggml-org/llama.cpp) 的 `llama-server`
提供推理服务；Mirobody 负责连接，不直接加载权重。预设会在首次使用时从 Hugging Face
下载模型。各操作系统的安装、启动及 GPU/CPU 配置见[本地运行](local-models.zh-CN.md)。

<a id="the-document-reader-glm-ocr-09b"></a>

### 文档识别与图片理解

GLM-OCR 将报告中的文字和表格识别出来，规则提取能够识别的表格行，其余文本交由模型处理。
轻量对话模型只能读取照片的 OCR 文字，无法判断图像外观；增强模型可以直接分析图片，
但仍可能认错菜品或给出不准确的热量估计。

## 云端处理的数据

使用云端模型可以减少本地推理的硬件需求。配置 API Key 后，在模型菜单中选择可用条目；
实际可用的模型取决于所配置的服务。

| 数据或任务 | 全本地 | 云端模型 | 本地 OCR + 云端对话 |
| --- | --- | --- | --- |
| 报告照片、扫描页 | 本地识别 | 发送给视觉模型 | 本地识别 |
| 文档文字 | 本地处理 | 规则提取表格后，其余文字发送给模型；前 3,000 个字符还用于生成标题和摘要 | 发送的文字范围相同 |
| 问题及 agent 查询到的数据行 | 留在本机 | 发送给对话服务商 | 发送给对话服务商 |
| 对话中的图片附件 | 按模型能力使用 OCR 或本地视觉 | 对话模型支持图片时发送 | 对话模型支持图片时仍会发送 |
| 术语和单位标准化 | 离线完成 | 离线完成 | 离线完成 |

本地 OCR 只保证文档图片在本机处理，不能保证文档内容不外传。
对话图片与上传文件的处理路径也不同，需要分别检查模型设置。

<a id="health-data-on-openrouter"></a>

### OpenRouter 的托管方

同一个模型可以由不同托管方提供，OpenRouter 也可能在请求失败后切换托管方。
数据保留条款和支持的参数应按实际托管方核对。已公布的评测使用账号级零数据保留设置，
并为每个模型固定托管方、关闭回退。仅在设置页填入 Key，或设置 `OPENROUTER_CHAT_MODEL`，
都不会固定托管方。

高级路由在模型条目的 `extra_body` 中配置 `provider.order` 和 `allow_fallbacks: false`。
[评测配置](../benchmarks/local_models/refs/)保留了对应示例。这些选项控制路由和数据保留条件，
云端处理本身仍会接收完成任务所需的数据。

## 混合部署

先启动提供 GLM-OCR 的本地模型服务，再设置：

```bash
OPENROUTER_API_KEY=sk-or-...
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
UTILS_VISION_MODEL=local-utils
```

即使已配置云端 Key，`LOCAL_OCR_BASE_URL` 也会让报告图片交由本地 OCR 处理。
`UTILS_VISION_MODEL=local-utils` 则覆盖无法提取文字的文件，例如作为文件上传的餐食照片：
这些文件只在本机生成描述，或不生成描述，不再交给云端视觉模型。

对话图片还要单独处理。云端对话条目需设置 `supports_image: false`，只接收 OCR 文字。
此方案不能通过设置页完成，需要直接编辑 `.env` 和模型配置。

## 切换与检查

首次启动时，打开 `./deploy.sh` 打印的设置链接；之后可进入**设置 → 模型**。
填写 Key 和模型名，或选择**100% 在本机运行**及所需配置。验证通过后，选择会加密保存并立即生效。

使用环境变量时，`LOCAL_MODEL`、`LOCAL_OCR_MODEL` 指定本地模型；
`OPENROUTER_CHAT_MODEL`、`OPENROUTER_UTILS_MODEL` 指定 OpenRouter 使用的模型。
`DEFAULT_MODEL`、`UTILS_TEXT_MODEL`、`UTILS_VISION_MODEL`、`UTILS_OCR_MODEL` 则选择各任务的配置条目。
修改 `.env` 后运行 `docker compose up -d`，仅重启容器不会重新读取它。
更多设置见[配置](https://docs.mirobody.ai/zh/configuration)和 [`config.llm.yaml`](../config.llm.yaml)。

检查当前模型及其能力：

```bash
docker compose exec mirobody mirobody doctor --probe
```

探针检查流式对话的工具调用，以及文档提取、视觉和 OCR，并报告对话模型是否显示了思考。
按需思考的模型在探针这道简短的问题上可能不思考，所以这只是一条提示，不算失败。探针还会核对本地模型名称。
通过检查说明这些能力可用，不代表回答质量或响应速度达到某个水平。

## 思考过程与最终答案

对话会通过受支持的模型接口请求思考内容。`AGENT_THINKING` 默认为 `medium`，
模型条目中启用的 `thinking_level` 可以覆盖它；文档、OCR 等任务保留各自设置。
部分端点不返回可见思考，探针会报告这一情况。执行面板展示返回的思考和工具活动，
最终答案在对应模型步骤结束后显示；系统不会补写端点未提供的推理。


## 评测资料

测试方法、完整结果与复现命令见[评测说明](../benchmarks/local_models/README.md)、
[历史结果](../benchmarks/local_models/HISTORY.md)和 [OCR 评测](../benchmarks/local_ocr/README.md)。
评测使用合成记录，结果仅适用于所记录的版本、硬件和服务商。
