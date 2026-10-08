# 怎么选模型

[English](model-choice.md) · **中文**

Mirobody 在三件事上调用模型：在对话里回答问题，把文档读成读数，把一句日记拆成条目。
用哪个模型做这三件事，决定了两件事，这一页帮你把两件事放在一起选：

- **谁读你的健康数据**：只有你自己的电脑，还是按某家模型厂商的条款交给它；
- **回答要多好**，以及要为此付出多少速度和费用。

这里的每个数字都出自同一套评测：通过产品自己的 API、在同一份合成记录上跑；
两次运行之间除了模型还有别的不同时，结果下面的说明会写出来（[怎么测的](#怎么测的怎么复现)）。

## 简短的回答

| 你的情况 | 选 | 什么会离开你的电脑 |
| --- | --- | --- |
| 什么都不能出去；一台普通电脑（16 GB 内存，没有显卡） | **本机，小号**：MiniCPM5-2B 回答问题，GLM-OCR-0.9B 读文档，都跑在 llama.cpp 上 | 什么都没有 |
| 什么都不能出去；32 GB 内存的 Mac 或 24 GB 显存的显卡 | **本机，大号**：Qwen3.8-27B 回答问题、能看照片，GLM-OCR-0.9B 读文档 | 什么都没有 |
| 要最好的回答 | **Claude Sonnet 5.5**、**Claude Opus 5.5** 或 **Gemini 3.8 Flash**，经 OpenRouter，开零数据保留：在这套评测上打平，Gemini 最便宜 | 你的问题、agent 读到的数据行、文档的文字 |
| 要最低的成本，或最快的回答 | **GPT-6 Luna**（每次回答最便宜）或 **DeepSeek V4.1 Flash**（开放权重，最快），同样的方式 | 同上 |
| 文档图片留在本机，回答交给云端 | **混合**：llama.cpp 上的 GLM-OCR 读每一张照片和每一页，云端模型回答 | 同上，但没有页面图片 |

## 三种方式，各有什么离开这台机器

首次设置页提供前两种；第三种是 `.env` 里的几行（[见下](#混合文档在本机读回答交给云端)）。

| | 100% 在本机运行 | 一把模型 key | 本机读文档，云端回答 |
| --- | --- | --- | --- |
| 文档的照片和扫描页 | 在本机由 GLM-OCR 读 | 发给厂商的视觉模型 | 在本机由 GLM-OCR 读 |
| 文档的文字 | 在本机读 | 发给厂商，表格规则在本机读出的行除外；无论如何，开头 3,000 个字符会发去生成标题和摘要 | 同左 |
| 你的问题，以及 agent 为回答而读的数据行 | 留在本机 | 发给厂商 | 发给厂商 |
| 对话里打开的照片 | 在本机读：小号读它的 OCR 文字，大号直接看 | 对话模型能看图时发出去 | 对话模型能看图时发出去 |
| 名称到编码、单位到 UCUM（② 转译） | 在本机，离线，用包里自带的数据 | 同左 | 同左 |
| 模型权重 | 从 Hugging Face 下载一次 | 没有 | GLM-OCR，1.4 GB，下载一次 |

「发给厂商」指按该厂商的条款处理；经 OpenRouter 时，是按它转发到的那家托管方的条款
（[OpenRouter 上的健康数据](#openrouter-上的健康数据)）。README 的
[什么留在你的机器上](../README.zh-CN.md#什么留在你的机器上) 是整套服务的同一张表。

## 本机：两种大小，由 llama.cpp 提供服务

**本地模型由 [llama.cpp](https://github.com/ggml-org/llama.cpp) 的 `llama-server`
提供服务；Mirobody 自己不跑模型。** 用预设 [`docker/local-models.ini`](../docker/local-models.ini)
启动一个服务，就同时提供读文档的模型和任一个回答模型，每个模型第一次被用到时从 Hugging Face
下载。Windows、Linux、macOS 各自的启动命令见 [local-models.md](local-models.md)（英文）。

| | 小号，默认 | 大号 |
| --- | --- | --- |
| 回答问题 | MiniCPM5-2B，Q4_K_M，纯文本 | Qwen3.8-27B，IQ3_S（ISTA-DASLab GSQ-RCO），带视觉投影 |
| 读文档 | GLM-OCR-0.9B | GLM-OCR-0.9B |
| 下载量，含读文档的模型 | 3.0 GB | 14.5 GB |
| 内存，两个模型都加载 | 回答时最多 5.7 GB | 回答时最多 20.8 GB |
| 能跑在 | 任何 16 GB 内存的电脑，不要显卡：Windows、Linux、macOS | 32 GB 内存的 Mac，或 24 GB 显存的 NVIDIA 显卡 |
| 每次回答中位数 | 29 秒，16 GB 的 Apple 芯片笔记本，用 GPU | 约 2 分钟（138 秒），48 GB 的 Apple 芯片主机，用 GPU |
| 对话里的照片 | 读它的 OCR 文字：它看不见图 | 直接看 |
| 评测结果 | 24 道题过了 19 道（评分 248 分得 215），140 个印刷行全部存对，31 条日记条目写出 22 条 | 24 道题过了 22 道（评分 248 分得 240），140 个印刷行存对 139 个，31 条日记条目写出 29 条；同一台机器、同一份记录上小号得 229 分 |

回答耗时是在 Apple 芯片上测的，llama.cpp 在那里用 GPU 跑。没有显卡时以分钟计，具体多久看处理器。
在 llama.cpp 的 CPU 镜像里、4 个 vCPU 上（2026-10-07），MiniCPM5-2B 在 16 GB 的 Apple 芯片笔记本（colima 的 arm64 虚拟机）上
每秒写约 18 个 token，第一个回答 2–3 分钟；在 Intel Xeon Gold 5220R 上（外部评测）每秒写 3 到 7 个，第一个回答
连下载最长约 15 分钟，一张化验单照片最长约 9 分钟，一页 PDF 最长约 12 分钟。之后的轮次会复用服务器的提示词缓存；
两个模型加载后约占 6.0 GiB，所以 Docker 至少要分到 8 GB 内存（[local-models.md](local-models.md#without-a-gpu)，英文）。

在下面的评测里，在最终版代码上，小号 24 道题过了 19 道（Claude Code 评分 248 分得 215），12 份文档的 140 个印刷行
全部存对，单位和范围都和印的一样，31 条日记条目写出 22 条，没有一个回答超时。它是经过两轮 harness 改动做到的，
每一项都记在 [CHANGELOG](../CHANGELOG.md) 里：

| 小号 | 评分 | 通过 | 印刷行 | 日记 |
| --- | ---: | ---: | ---: | ---: |
| 1.5.4 改动之前 | 190 / 248 | 16 / 24 | 45 / 140 | 0 / 31 |
| 第一轮之后 | 209 / 248 | 19 / 24 | 140 / 140 | 24 / 31 |
| 最终版代码 | 215 / 248 | 19 / 24 | 140 / 140 | 22 / 31 |

第一轮：日记请求里加了两个示范回答，长报告逐页读，记录和日志带上每行自己的日期，表格规则认得大多数报告印的表头，
关键词跨拼写和单位都能找回读数，JSON schema 全部封闭，循环输出有上限，表格读剩的文字带上它所需的上下文，
`view="stats"` 报读数自己的那一天。第二轮：印在两页上的同一个读数只存一次，印出的标记和单位分开，页面的打印日期
不再当作读数的日期，原生 PDF 的表格直接从文字层读，被截断的回答先说明再列行，本地模型的一次回答最多写 6,144 个 token。
它仍然丢分的地方：几个平均值算错（7 月的睡眠、8 月的体重、5 月的舒张压），一张体重图调了 15 次工具后还是空的，
一份体检总结只读了前 200 行就停了，报告没印范围却说「在正常范围内」，还有一个基因型叫错了。

<a id="the-document-reader-glm-ocr-09b"></a>

### 读文档的模型：GLM-OCR-0.9B

两种大小读文档的方式都一样：GLM-OCR 把照片或扫描页转成文字和表格，表格的行按表头读，不用模型，
规则读剩下的交给回答模型。三个由上游 llama.cpp 支持的小 OCR 模型，在合成报告上走完了这整条路径
（[`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md)，Mirobody fcfbf78，读剩余部分的是 MiniCPM5-2B）：

| 按印刷值存对的行 | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro 1.2B |
| --- | ---: | ---: | ---: |
| 印刷：26 页 303 行，带生成器的水印 | **283**（93.4%） | 278（91.7%） | 279（92.1%） |
| … 去掉水印，和真实报告一样 | **302**（99.7%） | 282（93.1%） | 299（98.7%） |
| 页面上没印的行（带水印 / 去水印） | **0 / 0** | 12 / 8 | 3 / 4 |
| 手写：28 页 301 行（带水印 / 去水印） | **99 / 219** | 62 / 136 | 55 / 171 |
| 每个印刷页的 OCR 秒数 | 12.3 | 13.1 | 14.8 |
| 下载量，模型加投影 | 1.43 GB | 1.82 GB | 1.24 GB |
| 许可 | MIT | Apache-2.0 | Apache-2.0 |

读文档的仍是 GLM-OCR：带不带水印都存对最多行，一行页面上没印的都没有，最快，产品也不用改。
PaddleOCR-VL-1.6 的 OCR 文字里装下的最多（印刷行的 99%，另两个 85%），但它输出的 LaTeX 和表格标记要产品
另行清理，28 个手写页里有 7 页循环到 token 上限，还存了页面上没印的行。它作为可选项留在预设里
（[怎么切换](local-models.md#the-document-reader)，英文）。手写页上三个模型都读出了约 95% 的值；
行是在之后丢的：规则读剩的部分由小号回答模型来读。

### 照片

GLM-OCR 只读印刷的文字和表格，别的一概不会。小号看不见图：对话里的照片以它的 OCR 文字送达；问到一盘菜，
它会说看不到这张照片，并问吃了什么。大号直接看照片，对一盘菜给出热量区间和依据，菜名可能认错
（[每个模型能从照片里读出什么](local-models.md#what-each-model-can-read-in-a-photo)，英文）。

## 云端：一个开放权重的基线，四个闭源参照

五个云端模型用同一套服务、同样的题目、文档和日记句子、同样的评分，和小号一起跑过：

| 模型 | 权重 | 经由 | 托管方 |
| --- | --- | --- | --- |
| DeepSeek V4.1 Flash | 开放 | OpenRouter，零数据保留，不回退 | Together |
| Claude Sonnet 5.5 | 闭源 | 同上 | Google Vertex |
| Claude Opus 5.5 | 闭源 | 同上 | Google Vertex |
| Gemini 3.8 Flash | 闭源 | 同上 | Google Vertex |
| GPT-6 Luna | 闭源 | 同上 | Azure |

跑法让它们和小号之间只差在回答模型：文档在本机由 GLM-OCR 和表格规则读，云端模型读规则剩下的 OCR 文字
（只存一次，所以每个模型读的是同一份文字）；对话模型配置为看不见图（`supports_image: false`），
照片以它的 OCR 文字送达，和小号一样；提取和日记每个模型各用一个新账号。这就是下面的
[混合](#混合文档在本机读回答交给云端)，并且所有图片都留在本机。

### 结果

| | 跑在 | 问答：评分 | 问答：通过 | 每次回答中位数 | 存对的印刷行，12 份文档 | 日记条目，15 句 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| MiniCPM5-2B，小号 | 16 GB 的 Apple 芯片笔记本 | 215 / 248 | 19 / 24 | 29 秒 | 140 / 140 | 22 / 31 |
| MiniCPM5-2B，小号 | 48 GB 的 Apple 芯片主机 | 229 / 248 | 20 / 24 | 15 秒 | 140 / 140 | 24 / 31 |
| Qwen3.8-27B，大号 | 48 GB 的 Apple 芯片主机 | 240 / 248 | 22 / 24 | 138 秒 | 139 / 140 | 29 / 31 |
| DeepSeek V4.1 Flash | Together | 245 / 248 | 23 / 24 | 4.2 秒 | 138 / 140 | 29 / 31 |
| Claude Sonnet 5.5 | Google Vertex | 247 / 248 | 23 / 24 | 9.0 秒 | 140 / 140 | 30 / 31 |
| Claude Opus 5.5 | Google Vertex | 247 / 248 | 23 / 24 | 14.9 秒 | 139 / 140 | 29 / 31 |
| Gemini 3.8 Flash | Google Vertex | 247 / 248 | 23 / 24 | 14.9 秒 | 140 / 140 | 31 / 31 |
| GPT-6 Luna | Azure | 237 / 248 | 21 / 24 | 9.7 秒 | 140 / 140 | 31 / 31 |

- **评分**是 Claude Code 按一份写好的评分表打的分：对不对、有没有出处、是否按报告印的范围判断、
  是否用提问的语言、有没有用，要图时还有图，每项 0 到 2 分。**通过**是五项自动检查全过：答完了、
  调对了工具、每个期望的事实都在、用提问的语言、图能解析。
- **印刷行**数的是和文档印出的某一行对得上的读数。此外每个模型都存了对不上任何一行的读数：云端模型 47 到 49 条，
  小号 9 条。这些还没有逐条看过；在更早的一次 DeepSeek 运行里，其中 40 条来自那本 7 页的体检册。
  表格规则放在前面之后（[见下](#表格规则在前面时)），云端模型的降到 26–41 条。
- **48 GB 主机上的两行** 是 2026-10-08 在 7372b096 上先后跑的，读的是当时载入的记录（`qa5`），llama.cpp 是较旧的 b11269。
  其余各行所用的 16 GB 机器装不下大号；小号在它旁边一起跑，两者在同一台机器、同一份记录上可比
  （[评测](../benchmarks/local_models/README.md#small-and-large-on-one-machine-2026-10-08)，英文）。
- **记录和提交。** 16 GB 笔记本上那行小号全部在 958fae5 上跑，读的记录（`qa4`）经最终版流水线载入，和今天用户的记录一样。
  云端模型读的是在 490a0e1 上载入的记录（`qa3`，其中两份文档在 321aa2c 上重读过），之后没有重新载入。
  DeepSeek、Sonnet 和 Luna 的文档和日记在 4e3c06f 上读，20 道题在 490a0e1 上答，读那两份重读文档的 4 道题在
  321aa2c 上答；Opus 和 Gemini 的每一部分都在 321aa2c 的应用代码上跑。

这张表说明：

- **三个模型并列第一**：Claude Sonnet 5.5、Claude Opus 5.5 和 Gemini 3.8 Flash 都是 248 分得 247，丢的是同一分，
  都丢在月视图的平均值上（这是流水线的问题，不是模型的）。一套 24 道题排不出它们的先后；价格和速度能分开它们：
  Gemini 每次回答 $0.012，Sonnet 约 $0.029，Opus $0.045；三个里 Sonnet 回答最快（中位数 9.0 秒，另两个 14.9 秒）。
- **DeepSeek V4.1 Flash** 是开放权重的基线，回答最快（中位数 4.2 秒），比第一少 2 分，140 个印刷行漏了 2 个。
- **GPT-6 Luna** 每次回答最便宜（$0.0009），存对了每个印刷行、写出了每条日记条目。它丢的 11 分里，
  有只在一年之内搜一个症状、把血脂的问题推回给用户而不去查读数、说一个印出来的范围不存在。
- **MiniCPM5-2B** 在一台 16 GB 的笔记本上、什么都不发出去，也存对了每个印刷行，单位和范围都和印的一样，
  问答比并列第一的三个少 32 分。
- **Qwen3.8-27B**，也就是大号，比并列第一的三个少 7 分，在同一台机器上比小号多 11 分，每次回答的时间约是小号的九倍。
  它丢的 8 分里有 3 分，是因为说了一个工具没返回过的 SLCO1B1 位点，这是它 24 个回答里唯一编出来的值。

### 表格规则在前面时

从 c396b4f 起，不管配置的是什么模型，表格规则都会读每一份上传的文件，云端模型只读规则剩下的部分。
四个参照在这版代码上重新读了那 12 份文档，用的是同一份存好的 OCR 文字，每个都在新账号里、经各自固定的托管方：

| 四个云端模型，12 份文档 | 之前 | 规则在前面 |
| --- | --- | --- |
| 找到的印刷行，共 140 | 138–140 | 138–140 |
| 范围和印的一样 | 133–136 | 138–139 |
| 对不上任何印刷行的读数 | 约 48（Gemini 47） | 26–28（Gemini 41） |
| 发给厂商提取读数那次调用的文档文字 | 27,184 个字符 | 16,910 个（−37%） |

12 份文档里有 5 份（两份化验单 PDF、那份表格、那张照片和那份复印件）现在完全不用模型。这个比较测到了什么、没测到什么：

- **这不是「规则关」对「规则开」。** 参照的配置里一直留着本地 OCR 的路由，所以之前那几次规则也在跑，
  只是当时认得的表头少。这个比较测的是更广的表头词汇。Gemini 之前那次已经用上了它，所以它的两次几乎没有差别。
- **规则读每一份上传最要紧的地方，这里没有测**：用厂商 key、又没有本地 OCR 模型的部署，在 c396b4f 之前那里没有任何表格是按规则读的。
- **生成标题和摘要的那次调用仍然把每份文档的前 3,000 个字符发给厂商**，不管有没有规则（12 份一共 12,200 个字符）。

### 花多少钱

| | 每次回答 | 100 份这样的文档 |
| --- | ---: | ---: |
| 本机，任一种大小 | 每次调用不花钱 | 每次调用不花钱 |
| GPT-6 Luna | $0.0009 | 约 $0.31 |
| DeepSeek V4.1 Flash | $0.0017 | 约 $0.23 |
| Gemini 3.8 Flash | $0.012 | 约 $2.1 |
| Claude Sonnet 5.5 | 约 $0.029 | 约 $6.6 |
| Claude Opus 5.5 | $0.045 | 约 $8.5 |

每次回答的费用，是 OpenRouter 对 24 道题的收费（等记账稳定后读的：OpenRouter 在回答之后最多几分钟才记上一次请求的费用），
除以 24；Sonnet 的是估计值，因为它的问答和另一个模型在同一把 key 上重叠了。每百份文档的费用，是在表格规则放在前面时
读这 12 份文档的收费，乘以 100/12（Opus 的是它自己那次运行里的提取部分，那版代码发出的文档文字相同）：
对这类文档（单页化验单、CSV、表格、扫描件和照片、一本 7 页体检册）而言，而且它们的 OCR 是在本机做的。
如果页面图片也交给厂商读，还要加上它的视觉调用。这把 key 也被别的工作共用，所以每个数字都是上限。
这次评测所有云端运行合计花了 $7.55。

<a id="health-data-on-openrouter"></a>

## OpenRouter 上的健康数据

OpenRouter 是一把 key 用所有模型。不另外指定时，它会把请求发给提供该模型的任何一家托管方，一家失败就换另一家。
对健康数据，这有两重问题：每家托管方对保留什么数据各有条款，而同一个模型的各家托管方提供的东西并不一样。
以下是 2026-10-06 的实测，还没有记进 `benchmarks/`：

- **精度。** OpenRouter 上 `deepseek/deepseek-v4.1-flash` 的端点列表（`/api/v1/models/deepseek/deepseek-v4.1-flash/endpoints`）
  列了 32 家托管方。很多家提供的是量化版，标着 `fp4`（OpenInference、Sail Research、Decart）或 `fp8`
  （Morph、DeepInfra、AtlasCloud、Novita 等）；好几家没列 `structured_outputs`，DeepSeek 自己的那家列了
  `response_format`，没列 `structured_outputs`。
- **一家把按 schema 的回答清空的托管方。** 对一份家庭体重记录的 OCR 文字（12 行，每行带日期）发提取请求，带
  `response_format: json_schema`，被转到了 InferenceNet（响应里的 `provider` 字段），返回 333 个字符，
  `"indicators": []` 排在 `content_type` 前面。同一个请求不带 `response_format`、同一家托管方，返回了全部 12 行和各自的日期。
  在一次对照检查里，4 种 schema 写法各试 5 次，DeepSeek 在每种写法下（包括原样不改的 schema）都是 5 次里有 3 次
  读出 demo 血脂 CSV 的全部 5 行，而每一次空结果都来自 InferenceNet：问题出在转发，不在 schema。
- **它让一次评测付出的代价。** 第一次跑 DeepSeek V4.1 Flash 时没有固定托管方，用的是自带的按 schema 的条目，
  140 个印刷行只存对 86 个，有六份文档一个读数都没有。最可能的原因是托管方；生成器的水印在那家托管方上让情况更糟
  （去掉那一行，一张化验单从 0 行变成 6 行全对）。固定到 Together、在之后的提交上重跑，同一个模型在带水印的情况下
  存对了 140 个里的 138 个。

所以评测和这份指南都做两件事：

1. **账号开零数据保留。** 在 OpenRouter 账号设置里只允许不保留数据的托管方。这样 OpenRouter 遇到不提供零数据保留的
   托管方会拒绝，而不是转发过去：2026-10-06 它就以这个设置拒绝了 DeepSeek 自己的 API（「ZDR violation (account settings), Paid model training violation」），
   所以 DeepSeek V4.1 Flash 跑在 Together 上。`DEEPSEEK_API_KEY` 用的就是这同一个 API。
2. **每个模型固定一家托管方，不回退。** `provider.order` 指定托管方，`allow_fallbacks: false` 让 OpenRouter
   不去试别家。没有用 `require_parameters: true`：OpenRouter 没给 GPT-6 Luna 列出 temperature 参数，
   带上它会返回 404。每次运行前，固定的托管方都先答过一个和条目本身同样形状的请求，temperature 和 JSON 格式都在
   （每个参照的 `meta.json` 里的 `host_probe`）。Claude Opus 5.5 和 Gemini 3.8 Flash 只有 Google 的托管方通过：
   `anthropic`、`azure` 和 `amazon-bedrock` 对带 temperature 的 JSON schema 请求都没给 Opus 可用的端点，Gemini 的
   `google-ai-studio` 被零数据保留排除。

托管方写在 `config.llm.yaml` 条目的 `extra_body` 里，Mirobody 会原样发出。评测给 Together 上的
DeepSeek V4.1 Flash 用的条目是这样的（另外四个只换模型和托管方，也没有 DeepSeek 专有的那几行）：

```yaml
MODELS:                           # 加在已有条目旁边
  deepseek-zdr:                   # 在对话里回答问题
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: deepseek/deepseek-v4.1-flash
    supports_image: false         # 照片以 OCR 文字送达；没有图片出去
    extra_body:
      provider: {order: [together], allow_fallbacks: false}
  deepseek-zdr-utils:             # 读文档文字、日记、标题
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: deepseek/deepseek-v4.1-flash
    supports_image: false
    chat: false
    response_format: json_object  # DeepSeek 接受 JSON 模式，不接受 schema
    extra_body:
      reasoning: {enabled: false}
      provider: {order: [together], allow_fallbacks: false}
```

然后在 `.env` 里把路由指过去：`DEFAULT_MODEL=deepseek-zdr` 和 `UTILS_TEXT_MODEL=deepseek-zdr-utils`。
GPT-6 Luna 的工具条目不要 DeepSeek 那两行，改带 `reasoning_effort: none`（和自带的 `openai-utils` 一样）；
Gemini 3.8 Flash 的 `extra_body` 带 `reasoning: {effort: low, exclude: true}`（和自带的 `openrouter-utils` 一样：它的推理关不掉）；
Sonnet 5.5 和 Opus 5.5 的条目除了托管方什么都不加。评测挂载的文件在
[`benchmarks/local_models/refs/`](../benchmarks/local_models/refs/)，由 `overlays.py` 从自带的 `config.llm.yaml` 生成。

从源码安装时读的是检出目录里的 `config.llm.yaml`。Docker 镜像里有自己的一份，所以要在 `compose.yaml` 旁边的
`compose.override.yaml` 里把你的那份挂载上去（Compose 两个文件都读；这个文件被 gitignore，已经有的话把这几行加进去），
然后 `docker compose up -d`：

```yaml
services:
  mirobody:
    volumes:
      - ./config.llm.yaml:/app/config.llm.yaml:ro
  mirobody_worker:
    volumes:
      - ./config.llm.yaml:/app/config.llm.yaml:ro
```

在设置页粘贴的 key、或在 `OPENROUTER_CHAT_MODEL` 里写的模型名，都不固定托管方：OpenRouter 按你账号的设置转发。

## 按情况选

### 隐私优先，一台普通电脑

小号：`./deploy.sh`，再在它给出的页面上选**100% 在本机运行**。16 GB 内存，不要显卡，下载 3.0 GB 模型和约 0.7 GB 镜像。
在 Apple 芯片笔记本的 GPU 上预计每次回答约 29 秒；只用 CPU 时第一个回答要几分钟，Apple 芯片笔记本的核上 2–3 分钟，4 vCPU 的 x86 服务器上最长约 15 分钟。化验单的每个印刷行都能存对，大多数问题答得对（24 道过了 19 道）；最弱的是要它自己算
平均值或图表的时间范围的时候。关于你的一切都不离开这台机器。

### 隐私优先，一台大内存的机器

大号，32 GB 内存的 Mac 或 24 GB 显存的 NVIDIA 显卡：在同一个页面上选它。48 GB 的主机上每次回答约 2 分钟，
评分 248 分得 240（同一台机器上小号 229），也是本地唯一能看照片的大小。

### 要最好的回答

Claude Sonnet 5.5、Claude Opus 5.5 或 Gemini 3.8 Flash，经 OpenRouter，开零数据保留并固定托管方（[见上](#openrouter-上的健康数据)）。
三个都是 248 分得 247；一套 24 道题上的并列不是排名，能分开它们的是价格和速度。Gemini 3.8 Flash 每次回答 $0.012、
每百份文档约 $2.1，也是 OpenRouter key 的工具环节默认用的模型（`openrouter-utils`）。Sonnet 约 $0.029 和 $6.6，
三个里回答最快。Opus $0.045 和 $8.5。

### 要最低的成本

Azure 上的 GPT-6 Luna 或 Together 上的 DeepSeek V4.1 Flash，同样的方式：每次回答分别是 $0.0009 和 $0.0017，
每百份文档约 $0.31 和 $0.23。Luna 每个印刷行都存对了；DeepSeek 回答最快，权重开放。

### 混合：文档在本机读，回答交给云端

GLM-OCR 留在你的机器上，回答交给云端模型。每张报告照片、每个扫描页都在本机读；到厂商那里的是文字：
你的问题、agent 读的数据行，表格规则没读走的文档文字，以及每份文档用来生成标题和摘要的前 3,000 个字符。上面的云端参照就是这样测的。
`llama-server` 跑着预设（只会用到 `glm-ocr`）时，在 `.env` 里写：

```bash
OPENROUTER_API_KEY=sk-or-...
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1   # UTILS_OCR_MODEL 在这里读每一张照片和每一页
UTILS_VISION_MODEL=local-utils                           # 没有文字的图片不发出去生成描述
```

只要设了 `LOCAL_OCR_BASE_URL`，不管 `.env` 里有哪把 key，`UTILS_OCR_MODEL` 都会从视觉模型手里接过报告照片和页面。
没有第三行时，读不出文字的文件（比如作为文件上传的一张菜的照片）仍会发给厂商的视觉模型去生成描述；有了它，
这样的文件要么在本机描述，要么不描述。对话里打开的照片会送到能看图的对话模型；上面固定托管方的条目写了
`supports_image: false`，照片就以 OCR 文字送达。设置页不提供这种方式：`.env` 里有 key 时它不让选本机。

## 怎么切换

- **设置页。** `./deploy.sh` 会打印它的链接；之后在「设置 › 模型」。粘贴一把 key（旁边显示它会用的模型，
  可以改成别的名字，用一次真实请求验证），或选**100% 在本机运行**再选大小。选择加密存储，不用重启就生效。
- **`.env`。** 写 key，再按每个 `config.llm.yaml` 条目的 `model_env` 变量写模型：OpenRouter 的 key 对应
  `OPENROUTER_CHAT_MODEL`（对话）和 `OPENROUTER_UTILS_MODEL`（文档、日记、标题）；本地服务对应
  `LOCAL_MODEL`（`minicpm5-2b` 或 `qwen3.8-27b`）和 `LOCAL_OCR_MODEL`（`glm-ocr`）。`DEFAULT_MODEL`、
  `UTILS_TEXT_MODEL`、`UTILS_VISION_MODEL`、`UTILS_OCR_MODEL` 指定每个环节用哪个条目。然后
  `docker compose up -d`：`restart` 不会重新读 `.env`。
- **不改任何文件就用 GPT-6 Luna。** 有 OpenRouter 的 key 时，对话的模型菜单里也有 GPT-6 Luna（`gpt` 条目）；
  `DEFAULT_MODEL=gpt` 把它设为默认。和所有自带条目一样，它不固定托管方。
- **检查：**

  ```bash
  docker compose exec mirobody mirobody doctor --probe
  ```

  它列出每个环节用的条目，经产品自己的代码给每个条目发一次真实请求（一次工具调用、一次按 schema 的回答、
  一张图、OCR 的各遍），并检查每个本地服务跑的是不是条目里写的那个模型。

## 怎么测的，怎么复现

- **记录。** [mirobody-gen](https://github.com/thetahealth/mirobody-gen)，Mirobody 的合成记录生成器，正在开源，
  版本 248df0f，`--seed 7 --people 6`：结果确定，同一个提交和种子生成逐字节相同的文件。每个期望答案都由它的
  真值算出，不手写。里面没有任何真人的数据。
- **题目。** 24 道题（中英文各 12 道），关于四个人，每道题在一个新会话里通过产品的 HTTP API 问一次；12 份文档
  （带文字层的 PDF、CSV、XLSX、扫描件、手机照片、复印件、截图、一本 7 页体检册、一份门诊病历、一份家庭记录），
  逐行对照文档印出的内容打分；15 句日记，对照每句话说出的条目打分。
- **评分。** 自动检查（`score.py`），加上 Claude Code 对照期望答案、按上面的评分表读每一份对话记录；两者都保留，
  每个分数都附理由（`grades.json`），可以互相核对。
- **OCR。** 同一次生成里的 28 个印刷页（303 行，另有两份家庭记录的 57 行），以及 mirobody-gen 手写版生成的
  28 个手写页（301 行；b4a8c50，`--seed 42 --people 18`），每页都走产品自己的提取路径。

复现（每一步和每个选项见 [`benchmarks/local_models/`](../benchmarks/local_models/README.md) 和
[`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md)）：

```bash
B=benchmarks/local_models; G=~/mirobody-gen-seed7
mirobody-gen build --seed 7 --people 6 --out $G --render     # 在 mirobody-gen 的检出目录里
python $B/cases.py --corpus $G
python $B/load.py --corpus $G --qa
python $B/run.py --size small --corpus $G
python $B/run.py --ref deepseek-v4.1-flash-v2 --corpus $G     # 一个云端参照；准备步骤见它的 README
python $B/report.py summary

python benchmarks/local_ocr/run.py --model glm-ocr-q8 --corpus <corpus> --models-dir <models>
python benchmarks/local_ocr/extract.py --model glm-ocr-q8 --corpus <corpus>
```

这些数字说明不了的：

- **合成数据，一个种子，跑一次。** 一份记录、24 道题、12 份文档，每个模型跑一次。差一两道题不算结论，
  llama.cpp 的批处理也不能逐位复现。
- **水印。** 生成的每一页都印着「SYNTHETIC SAMPLE — GENERATED DATA, NOT A REAL PATIENT RECORD」，小模型会照字面理解；
  真实报告上没有它，所以 OCR 那张表两种都给。
- **手写来自字体。** 手写页是用手写字体渲染、再扫描或拍照的，不是人写的。
- **只有一个评分者**，Claude Code，题目也是它写的。评分表和每个分数的理由都公开，读者可以重新评。
- **速度只在两台机器上。** 本地耗时是一台 16 GB 的笔记本和一台 48 GB 的主机用 GPU 跑的；上面只用 CPU 的数字是在 4 个 vCPU 上另测的。云端的取决于托管方当晚的负载。
- **GPT-6.1 Sol 没有纳入比较**：即使固定到 Azure，OpenRouter 也一直对它上游限流（24 道题里有 9 道要重试最多四轮，
  其中一道始终没有答上；140 个文档行有 50 个始终没有存下），而价格约是 GPT-6 Luna 的 20 倍
  （每百万 token $2/$10，对 $0.10/$0.50）。
- **云端模型是在看不见图的条件下测的**，读的是本地 OCR 的文字。让它们自己看页面图片，可能读得更多，
  但更贵，图片也会发出去。

## 1.6.0 预告：Mirobody 自己的模型

Mirobody 1.6.0 会发布自己的模型：足够小、足够快，能在普通电脑上跑，针对 Mirobody 自己的工具和文档做了后训练，
和上面的模型一样由 llama.cpp 提供服务。有了它，一台 16 GB 内存、没有显卡的电脑就能私密地跑完整个流程：
读文档、回答问题、写日记，用的是为这套 harness 做的模型，而不是凑合适配来的。

它就是 [local-models-roadmap.zh-CN.md](local-models-roadmap.zh-CN.md#再训练) 里的计划，建立在小号今天用的两个模型上：

- **回答模型**，从 MiniCPM5-2B（Apache-2.0）后训练：用在真实 harness 里跑出、且每项检查都通过的运行来训练，
  再以「回答里每个数是否在记录里」为奖励继续训练；
- **读文档的模型**，从 GLM-OCR-0.9B（MIT）后训练：用加了手机拍摄畸变的渲染报告，学会写出每一行的名称、值、
  单位、范围、标记和日期。

两个合计约 3 GB，和今天的小号相当。只有它通过了和被替换的模型同样的评测（这一页和
[`benchmarks/`](../benchmarks/README.md) 里的这一套），才会替换默认，评测结果和它一起发布。
