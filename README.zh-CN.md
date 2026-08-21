<div align="center">

# 🚀 Mirobody

**AI 原生的健康数据引擎——收集、标准化，并针对检验报告、穿戴设备与基因数据进行推理。**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/healthmemoryarena)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)

**[📚 文档](https://docs.mirobody.ai/)** · **[💬 云端聊天服务——chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 API 平台——platform.mirobody.ai](https://platform.mirobody.ai/)**

**[English](README.md)** · **简体中文** · **[繁體中文](README.zh-TW.md)** · **[日本語](README.ja.md)**

*血液检验、穿戴设备、基因数据、影像资料——全都零散破碎，彼此互不兼容。
在 AI 能真正理解你的健康状况之前，得先有人把这些信号统一成一种 AI
真正读得懂的标准格式。这正是这个引擎在做的事。*

<img src="docs/images/where-your-data-comes-from.zh-CN.svg" alt="从穿戴设备到饭菜照片 —— 一种标准格式，AI 可直接读取。" width="920">

</div>

这个引擎做三件事，整个代码库（包括「贡献方式」一节）也完全按照这三个阶段来组织——正是[文档](https://docs.mirobody.ai/en/api-reference/)里用的同一套 **C · S · A**：

| 阶段 | 含义 | 位置 |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| **① 收集** | 把信号拉进来：3 家设备提供者 + 一个 SQL 数据源 · 7 种文件格式 · Apple Health | [`pulse/`](mirobody/pulse/) |
| **② 标准化** | 一套标准：把任意一条读数解析成标准代码（LOINC · SNOMED CT · RxNorm），统一单位，落地到 FHIR 认可的码制 | [`indicator/`](mirobody/indicator/) |
| **③ 解答** | 推理：agent 通过虚拟文件系统读取*原始文件*，并用图表和引用来源作答 | [`agent/`](mirobody/agent/) |

---

## ⚡ 60 秒试一下

指标解析是这个引擎的正门，不需要 key、不需要配置、不需要联网：

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン" "空腹血糖(GLU)"
```

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   任何语言，同一个码
resolve("total cholesterol").loinc                     # '2093-3'  [质量/体积]
resolve_reading("total cholesterol", "5.0", "mmol/L")   # '14647-2' [摩尔/体积]
resolve_reading("total cholesterol", "193", "mg/dL")    # '2093-3'  单位决定了码
resolve("血脂").resolved                                 # False    这是类别，不是一次观测
```

**手上有数值和单位就一起传进去。** LOINC 把单位**和**结果类型编进了身份，所以同一个
名字会解析到不同的码——把一笔 mmol/L 的结果归到 mg/dL 的码下，就是一个序列悄悄混进
两种单位的原因。`resolve` 宁可弃权也不猜：`""` 是值得再看一眼的空缺，`"refused"`
才是答案。
→ [引擎参考](https://docs.mirobody.ai/en/engine/) ·
[指标](https://docs.mirobody.ai/en/concepts/indicators/)

---

## 标准化这一层到底是什么

不是查表——这是相邻的开源项目都没有的那部分：

- **概念图谱**：440,961 个节点 · 22,044,110 条跨词表边 · **595,746 个来源 id**
  蒸馏成标准概念（LOINC · SNOMED CT · RxNorm 桥接）。
- **49,253 个多语言别名**（中文 22,578 · 日本語 16,809 · 另 5 种：de·es·fr·ko·ru）。
  `hemoglobin`、`血红蛋白`、`血紅素`、`ヘモグロビン` 全部落到 LOINC 718-7。
- **繁體中文是两个问题，分两套处理。** 字形折叠是机械的（随包 3,336 字的
  zh-Hant → zh-Hans 表）；词汇不是——台湾临床用词不同，把 `血紅素` 折叠会得到
  HbA1c 的码。这类词按繁体拼写单独收录，收录行永远压过折叠。
- **单位**归一到约 310 个 UCUM 家族，含量纲分析、按 LOINC 码索引的摩尔质量桥接，
  以及对 `%` 与 `10*9/L` 的明确拒绝。300 个标准 pulse 指标。
- **这个说法我们是量出来的，不是断言的。**
  [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 用体检实际会开的
  套组给离线解析器打分，按报告实际打印的写法，覆盖英文、简体中文、繁體中文、日本語，
  外加平台 API 教的可穿戴词汇。**今天 197/197；写出来那天是 32/94。** 它评的是
  **临床**正确性：把 `血红蛋白` 答成 HbA1c 的码算失败，而 `血脂` 必须解析为空。

```bash
pytest mirobody/test_engine_coverage.py -s   # 离线，约一秒
```

→ [标准化](https://docs.mirobody.ai/en/api-reference/standardization/) ·
[架构](https://docs.mirobody.ai/en/concepts/architecture/) ·
[数据流](https://docs.mirobody.ai/en/concepts/data-flow/)

---

## 📊 基准 —— 我们不说「相信我们」，我们把评测开源出来

我们的健康 AI 基准是 Hugging Face 上同类里**下载量最高的**（各 4,000+）：

| 基准 | 测什么 | 下载量 |
| --- | --- | --- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) | 事件驱动的纵向健康 agent——100 个合成用户、10,000 个问题、程序化标准答案（[arXiv:2604.02834](https://arxiv.org/abs/2604.02834)） | 4,800+ |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | 医疗幻觉 | 4,500+ |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | 有害医疗建议 | 4,300+ |

用 **[mirobody-eval](https://github.com/thetahealth/mirobody-eval)** 一条命令复现
任何一个；它同时也能给部署灌入合成（不含 PHI）轨迹数据。

---

## 🚀 把整套跑起来

```bash
git clone https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs pull          # 引擎的数据包，`resolve` 需要它
./deploy.sh           # Postgres + pgvector、Redis、服务、worker
```

然后打开 **http://localhost:18080**。服务启动时会打印它接受的账号——随包那个是
`caregiver@mirobody.ai`，验证码 `111111`，名字就是角色：你以照护者身份登录，
读的是别人的记录。

没有邮件服务？不需要。登录页默认落在**密码**，把邮箱验证码留作第三个标签：

```bash
curl -X POST localhost:18080/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

要对话需要一个 LLM key；embedding key 是可选的。
→ [Docker 部署](https://docs.mirobody.ai/en/deployment/docker/) ·
[配置](https://docs.mirobody.ai/en/configuration/) ·
[本地 Python 环境](https://docs.mirobody.ai/en/development/setup/)

### 👨‍👩‍👧 一个会回答、然后向你要文件的 demo

`SEED_DEMO_DATA` 默认开启，所以你一进来关爱圈里就已经有一位合成用户：
**Demo (synthetic)**，两年跨度 244 个指标，五份 agent 可以 `read_file` 的文档。
你自己什么都没有；那份记录是她的。

<div align="center">
<img src="docs/images/your-care-circle.zh-CN.svg" alt="你自己没有数据；你读的是她的记录。" width="820">
</div>

**从提问开始，而不是从数据表开始。** 在 Ask 页问：

> *「她最近一次的 LDL 是多少？和一年前比怎么样？」*

```
2024-04-16   3.4 mmol/L
2024-10-15   3.2
2025-04-15   3.1
```

……然后它会主动指出最近一次面板已经一年多了、值得再查一次。这就是后半段的引子。

**接着给它一个文件。** `mirobody/demo/lab_report_2025-10-15.pdf` 是她**下一次**的
面板，刻意从灌入数据里留出——所以上传它不是空操作。拖到 Data 页，看着 ① 收集 和
② 标准化 干活：十二个分析物带着单位出来、每一个解析到码、LDL 序列多出第四个点。
再问一遍，答案就变了。

所有数值都是合成的——由 [mirobody-eval](https://github.com/thetahealth/mirobody-eval)
为 ESL-Bench 生成后固化在仓里，所以灌数据不需要联网、不需要 key。要让部署承载真实
数据，把 `SEED_DEMO_DATA` 设为 false。

---

## 🧩 扩展它

五个目录键指向插件根目录；丢一个文件进去，重启即可。工具会同时成为 agent 工具和
MCP 工具，不需要额外接线。

| 你想要 | 丢进 | 文档 |
| --- | --- | --- |
| 一个新工具 | `mirobody/agent/tools/` | [添加工具](https://docs.mirobody.ai/en/tools/adding-tools/) |
| 一个 Agent Skill（SKILL.md） | `mirobody/agent/skills/` | [Skills](https://docs.mirobody.ai/en/tools/skills/) |
| 一整个 agent | `mirobody/agent/` | [Agents](https://docs.mirobody.ai/en/tools/agents/) |
| 一个设备 provider | `mirobody/pulse/providers/` | [Provider 接入](https://docs.mirobody.ai/en/development/provider-integration/) |
| 别人的 MCP 服务 | Settings → MCP | [MCP 集成](https://docs.mirobody.ai/en/tools/mcp-integration/) |

agent 拥有的每个工具同时通过 `/mcp` 对外提供，按用户门控。
→ [内置工具](https://docs.mirobody.ai/en/tools/built-in/) ·
[MCP 服务](https://docs.mirobody.ai/en/api-reference/mcp-servers/)

---

## 🔌 从你自己的代码里用

| 接口 | 适合 | 文档 |
| --- | --- | --- |
| `pip install mirobody` | 解析和文件解析，不需要服务 | [引擎](https://docs.mirobody.ai/en/engine/) |
| HTTP API | 你的应用对接一个部署 | [API 总览](https://docs.mirobody.ai/en/api-reference/overview/) · [数据](https://docs.mirobody.ai/en/api-reference/data/) |
| MCP | Claude、Cursor 或任何 MCP 客户端读取用户记录 | [MCP 服务](https://docs.mirobody.ai/en/api-reference/mcp-servers/) |
| Backbone 模式 | 你自己的 agent，我们的数据层 | [Backbone](https://docs.mirobody.ai/en/api-reference/backbone-mode/) |

不确定选哪个？→ [如何选 API](https://docs.mirobody.ai/en/api-reference/choose-your-api/)

---

## 🏗️ 仓库怎么摆的

```
mirobody/
├── pulse/       ① 收集     —— provider、文件解析、聚合
├── indicator/   ② 标准化   —— 解析器、单位、分类（无 DB、无网络）
├── agent/       ③ 回答     —— DeepAgent、工具、skills、chat
├── mcp/         MCP 服务
├── schema/      DDL，开发环境启动时重放
└── demo/        关爱圈 fixture
```

**一条机器强制的规则**：`indicator/` 永不导入 agent 层，所以
`pip install mirobody` 是 207 MB、89 个包，看不到任何框架——加上 `[agents]` 几乎
翻三倍，到 597 MB、168 个包。两条 import-linter 契约守着这条线，`lint-imports`
会让构建失败。

→ [架构](https://docs.mirobody.ai/en/concepts/architecture/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

## 📚 文档

更深的内容都在 **[docs.mirobody.ai](https://docs.mirobody.ai/)** —— 50 个页面，
英文与简体中文。

| | |
| --- | --- |
| [快速开始](https://docs.mirobody.ai/en/quickstart/) · [安装](https://docs.mirobody.ai/en/installation/) · [自托管](https://docs.mirobody.ai/en/self-host/) | 先跑起来 |
| [指标](https://docs.mirobody.ai/en/concepts/indicators/) · [Provider](https://docs.mirobody.ai/en/concepts/providers/) · [文件处理](https://docs.mirobody.ai/en/concepts/file-processing/) | 三个阶段怎么工作 |
| [API 参考](https://docs.mirobody.ai/en/api-reference/) · [流式](https://docs.mirobody.ai/en/api-reference/streaming/) · [函数调用](https://docs.mirobody.ai/en/api-reference/function-calling/) | 基于它开发 |
| [贡献](https://docs.mirobody.ai/en/development/contributing/) · [环境搭建](https://docs.mirobody.ai/en/development/setup/) | 参与开发 |

仓内、面向贡献者：[CONTRIBUTING.md](CONTRIBUTING.md) ·
[docs/roadmap.md](docs/roadmap.md) · [SECURITY.md](SECURITY.md)

---

## 🤝 贡献

杠杆最高的贡献是一个解析器答错的词。跑 `mirobody resolve "<词>"`，如果答案错了
或是空的，就往 [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv)
加一行，再往 [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 加一个
用例——覆盖率分数就是评审。

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [贡献指南](https://docs.mirobody.ai/en/development/contributing/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

<div align="center">

**[📚 文档](https://docs.mirobody.ai/)** · **[💬 Chat](https://chat.mirobody.ai/)** · **[🔌 平台](https://platform.mirobody.ai/)** · **[🧪 Eval](https://github.com/thetahealth/mirobody-eval)**

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>
