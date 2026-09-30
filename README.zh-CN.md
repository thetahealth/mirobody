<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/mirobody-icon-dark.svg">
  <img src="docs/images/mirobody-icon.svg" alt="Mirobody" width="72">
</picture>

# Mirobody

**自托管的 AI 健康数据引擎：化验单、穿戴设备和基因文件汇成一份记录，由一个每句话都有出处的智能体来回答。**

**[English](README.md)** · **中文**

[![PyPI](https://img.shields.io/pypi/v/mirobody?label=PyPI&color=3775A9)](https://pypi.org/project/mirobody/)
[![Docker Hub](https://img.shields.io/docker/v/thetahealth4mirobody/mirobody?label=Docker%20Hub&logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/thetahealth4mirobody/mirobody)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/thetahealth/mirobody?style=social)](https://github.com/thetahealth/mirobody/stargazers)

**[▶ 在线 Demo，免注册](https://chat.mirobody.ai/demo)** · **[📚 文档](https://docs.mirobody.ai/zh/self-host)** · **[🐳 Docker Hub](https://hub.docker.com/r/thetahealth4mirobody/mirobody)** · **[☁ 云端 API](https://platform.mirobody.cn/)**

</div>

---

去年体检写 `A1c`，今年医院写 `HbA1c`，换家机构又成 `糖化血红蛋白`。
一项检查三个名字，读不懂，也比不了。Mirobody 把任何来源、任何格式、任何语言的健康数据读进来，
每一个值都落到同一套标准上，再在这份记录上回答你的问题，每个数字都能追回它出自的那份文件。
我的血压这几年怎么变？妈妈的糖尿病指标好转了吗？孩子历年体检有什么变化？
全部跑在你自己的机器上，用你自己选的模型 key。

<p align="center">
  <img src="docs/images/ask-own-demo.zh-CN.gif"
       alt="用中文问胆固醇怎么变的，agent 找到三份用不同写法记录同一项检查的文件，把它们解析成同一个码，并画出趋势" width="880">
</p>

<p align="center"><em>三份文件，三种写法，同一个码。智能体把三份都找出来，画出趋势，
并说明每个数字来自哪份文件。</em></p>

## 三条命令跑起来

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh                          # 拉取镜像；Postgres、服务端、worker 起来 → http://localhost:18060
echo 'OPENROUTER_API_KEY=sk-or-...' >> .env && docker compose up -d   # 一把模型 key，打开抽取和问答
```

只需要 Docker：不用 Python，不用 Node.js，不用 GPU，也不用 Git LFS。
镜像 `thetahealth4mirobody/mirobody:1.5.3` 自带词表和 Web 客户端。
`deploy.sh` 会把数据库、签名与加密密钥写进 `.env`，
连不上 Docker Hub 时改用 `docker.1ms.run` 镜像源，镜像完全拉不到时才用当前检出在本机构建。
（要提 PR 的话，去掉 `--depth 1`。）

1. **登录。** 在「邮箱验证码」页签用 `you@mirobody.ai`、验证码 `111111` 登录。
   `SEED_DEMO_DATA` 默认打开，一开始就有两个账号，合计 **2,019 条读数**：
   你自己，和把记录以只读方式共享给你的 `mom@mirobody.ai`。
   要存真实数据，首次启动前把它设成 `false`。
2. **把一份文件拖到 Data 页。** [`demo/upload/`](demo/) 里放着四份种子数据故意没写进库的文件：
   一份化验单 PDF、一张打印报告的手机照片、一个表格、另一家化验所导出的 CSV。
   每一项分析物都带着数值、单位和编码被抽出来，并链回它来源的那一页。
3. **提问。**「我的胆固醇怎么变的？」会把带着这一项的文件全都找出来，不管化验所怎么写，
   画出趋势，并说明每个数字出自哪份文件。同一个问题问到共享给你的那份记录上，
   答案就是另一个人的，而且那份数据你只能看，不能改。

<p align="center">
  <img src="docs/images/upload-demo.zh-CN.gif"
       alt="把化验单 PDF 拖到 Data 页；分析物被抽取出来，带着 LOINC 码出现在指标表里" width="880">
</p>

<p align="center">
  <img src="docs/images/ask-circle-demo.zh-CN.gif"
       alt="同一个问题问到共享记录上；答案来自另一个人的文件" width="880">
</p>

**用哪把 key。** 下面任何一把都能把整套跑通：
[OpenRouter](https://openrouter.ai/keys)（`OPENROUTER_API_KEY`）、
[OpenAI](https://platform.openai.com/api-keys)（`OPENAI_API_KEY`）、
[Gemini](https://aistudio.google.com/apikey)（`GOOGLE_API_KEY`）、
[Anthropic](https://platform.claude.com/settings/keys)（`ANTHROPIC_API_KEY`）、
DeepSeek（`DEEPSEEK_API_KEY`）、DashScope（`DASHSCOPE_API_KEY`），
或者任何 OpenAI 兼容网关（`<PROVIDER>_BASE_URL`）。
聊天用哪个模型、报告照片交给谁读、指标交给谁抽、向量交给谁算，
是 [`config.llm.yaml`](config.llm.yaml) 里的四行；那个文件写的是变量名（`api_key: OPENROUTER_API_KEY`），
不是密钥本身。`docker compose exec mirobody mirobody doctor` 会列出每一环选中了什么。
没有 key 也能登录、浏览种子记录、画图；抽取、记录和智能体等一把 key。

→ [自部署指南](https://docs.mirobody.ai/zh/self-host) ·
[配置](https://docs.mirobody.ai/zh/configuration) ·
[部署到服务器](https://docs.mirobody.ai/zh/deployment/production) ·
[排障](https://docs.mirobody.ai/zh/troubleshooting) ·
[从 1.5.2 升级](docs/backup-restore.md#upgrading-from-152)

## 你会得到什么

- **所有来源，一份记录。** Garmin、Oura、Whoop 直接对接；任何写进 Apple Health 的手环、
  戒指、体重秤一并进来；PDF、照片、表格、导出文件，23 种文件类型，按类型读取。源文件原样留下。
- **全家人一份记录。** 邀请伴侣、父母，或者添加一个完全不会自己登录的孩子，
  全家的健康档案放在一处。共享叫**关爱圈**：邀请制，默认关闭，
  一个账号要读到不属于自己的记录，只有 `resolve_subject` 这一条路。
- **用自己的话记下感受。** 在「记录」里写一句 `昨晚开始头疼，血压150/95，没发烧，每天早晚吃二甲双胍500mg`，
  它会记下一条头疼、两条血压读数，各自落在标准码上，把二甲双胍加入用药清单；
  你说了没有的发烧，不会被记进去。拆句的是你选的模型，编码来自词表，从不来自模型。
- **绝不编码。** 每一条读数要么落进一套确定的体系，要么明说没解析出来并留下你的原话。
  基于真实报告开发和测试，英文、中文、日文、俄文的写法都认。
- **基因数据，只说测到了什么。** 把 23andMe、AncestryDNA、MyHeritage、FTDNA、WeGene 的原始导出或
  VCF 传上来，就能按 rsID、基因或染色体区间查。489 个药物基因位点会对照两版参考基因组逐一核对，
  其余位点原样保存。问到用药，它会说明 CPIC 相关位点哪些测到、哪些缺失、哪些没读出来，
  但不推断代谢型，更不建议调整用药。[基因数据怎么处理](docs/genetics.zh-CN.md)。
- **智能体只在编码过的数据上推理，而且不一定得是我们这一个。** 分钟到月的趋势画成图；
  基线和变化量一次调用算出；跨化验所、跨文件、跨设备直接比较，因为底下是同一个码。
  每个工具同时挂在 `/mcp` 上，按用户鉴权，Claude Desktop、Cursor 或你自己的 loop 都能用。
- **你的模型，你的 key，你的数据。** 模型调用走你选的那家，其余一切留在你自己的机器上。

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/your-care-circle-dark.zh-CN.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/your-care-circle.zh-CN.svg">
    <img src="docs/images/your-care-circle.zh-CN.svg" alt="一个人如何读到另一个人的健康记录：请求经过 resolve_subject，它要求双方成员关系都已接受、且对方自己打开了 health_access 开关，然后要么返回按请求裁剪过的权限，要么抛出 403" width="920">
  </picture>
</p>

→ [四分钟完整演示](docs/walkthrough.zh-CN.md) ·
[`examples/06_care_circle_rules.py`](examples/06_care_circle_rules.py) 能离线打印整张共享决策表 ·
[设备接入](docs/provider-setup.zh-CN.md)：Garmin、Oura、Whoop

## 收集 · 转译 · 智能体

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/collect-translate-agent-dark.zh-CN.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/collect-translate-agent.zh-CN.svg">
  <img src="docs/images/collect-translate-agent.zh-CN.svg" alt="收集、转译、智能体：三个阶段，从左到右" width="920">
</picture>
</p>

一条读数从进门到被引用，要走三步。每一步都留下痕迹，所以最后那个答案，
你能一路查回到它出自的那一页：

| 阶段 | 做什么 | 在哪 |
| --- | --- | --- |
| **① 收集 Collect** | 化验单、穿戴设备、手机照片、基因文件、记录里的一句话，都收进来。源文件原样留下，每一条读数都能指回它被读出来的那一页。 | [`collect/`](mirobody/collect/) |
| **② 转译 Translate** | 一个名字解析成一个码，一个单位统一到 UCUM，全程离线、结果确定。`A1c`、`HbA1c`、`糖化血红蛋白` 在这一层变成同一项检查（LOINC），`头疼` 和 `headache` 也成了同一条主诉（ICPC-3）。 | [`engine/`](mirobody/engine/) · [`translate/`](mirobody/translate/) |
| **③ 智能体 Agent** | 在编码后的记录上提问。按分钟、小时、天、周、月给出趋势，一次调用就能算出计数、最小值、最大值、均值和变化量；同一个码，跨化验所、跨设备直接比较。图表画在回复里，用药记录和基因型数据也读得了，每个数字都说明出自哪份文件。 | [`agent/`](mirobody/agent/) |

## 单独试一下引擎

一条命令，五种写法，不用 key，不用联网；用 `uvx`，连装都省了。
看它认出哪几个、又在哪一个上停下来：

```bash
uvx mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

<p align="center">
  <img src="docs/images/resolve-demo.zh-CN.gif"
       alt="mirobody resolve：血红蛋白和 ヘモグロビン 落在同一个 LOINC 码上，血脂 是一次故意的弃答" width="880">
</p>

`血红蛋白` 和 `ヘモグロビン`，两种语言，同一个码 718-7。`血脂` 说的是一个类别，
不是一项具体检查，所以它解析成**空**：一个猜错的码会把两项不同的检查画进同一条趋势里，
所以解析器宁可空着，也不猜。

```python
from mirobody.engine import resolve, resolve_reading, standardize_reading

resolve("血红蛋白").loinc                                     # '718-7'   任何语言，同一个码
resolve("total cholesterol").loinc                          # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2' [Moles/volume]：单位决定码
resolve_reading("total cholesterol", "193", "mg/dL").loinc   # '2093-3'
resolve("中性粒细胞百分比").loinc                               # '26511-6' Neutrophils/Leukocytes
resolve_reading("中性粒细胞", "62 %", None).loinc              # '26511-6' 百分比……
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc           # '26499-4' ……和绝对值是两个码
resolve("血脂").resolved                                     # False    一个类别，不是一项检查
standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # '718-7'  同一个答案，写成 FHIR Observation
```

主诉和诊断有自己的一条轴，ICPC-3，规则一样：给一个码，或者明说不编，绝不猜。

```python
from mirobody.translate import resolve_symptom, resolve_condition

resolve_symptom("头疼").code           # 'NS01'         头痛；'headache'、'頭痛'、'головная боль' 答的是同一个码
resolve_symptom("疼").outcome         # 'refused'      太笼统，不编码；原话留下，码不瞎猜
resolve_condition("2型糖尿病").code    # 'TD72'         2 型糖尿病
resolve_condition("糖尿病").outcome    # 'needs-input'  哪一型？问你，不替你定
```

**有数值和单位，就一起传进来。** 单位不一样，就是两项不同的检查，LOINC 把这件事写进了码本身的定义里，
所以同一个名字按设计会对应好几个码。`uvx mirobody mcp` 把同一套词表通过 stdio 提供给任何 MCP 客户端：
读数转成带码的 FHIR Observation，主诉编到 ICPC-3，单位归到 UCUM。不要 key，不要数据库。

→ [标准化详解](docs/standardization.zh-CN.md) ·
[作为库使用](https://docs.mirobody.ai/zh/quickstart#a--the-library) ·
[`examples/`](examples/README.md)

## 隐私

除了你自己选的那个模型，以及你连上的设备厂商，没有任何数据离开你的机器。
读一张报告照片、从 PDF 里抽指标、拆开记录里的一句话、回答提问，这四件事调用模型；
连上的 Garmin、Oura、Whoop 走它们自己的 API，只取它们记下的数据。
**② 转译这一层完全在本地**：名字对到码、单位换算成 UCUM，查的是随包发布的词表，
不用 key，不联网，不跑模型。你的记录存在你自己的 Postgres 里，容器是你自己起的，
这里不会把任何使用情况报给谁。落库加密还没覆盖到每一个字段；要把它接到一个你控制不了的网络上，
先过一遍 [SECURITY.md](SECURITY.md)，服务端到底会往外发哪些请求，也写在那里。

## 每一个数字，都可以复现

出处都附在后面：一条可以跑的命令，或者一个公开的数据集。

| 说法 | 怎么验 |
| --- | --- |
| **296/296**：常规体检会打印的那些项目，按报告上真正的印法写，覆盖英文、中文（简体和繁体）、日文、俄文和爱沙尼亚文 | [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) 跑一下就会把分数打出来 |
| **328 个 UCUM 单位**，带量纲分析和摩尔质量换算，百分比和绝对值之间明确拒绝换算 | [标准化详解](docs/standardization.zh-CN.md) |
| **316 项标准设备指标**；13 家可穿戴厂商逐字段读过一遍：447 个字段里 289 个落到码，每一个都带置信度和它出自哪份厂商文档，71 个明确不落码并写明原因 | [设备对照表](docs/device-crosswalk.md) |
| **13 条读数和 26 条主诉说法**，五种语言，各自该落到哪个 LOINC/UCUM 或 ICPC-3 编码，包括宁可不编也不瞎猜的那些 | [`benchmarks/health_records`](benchmarks/health_records/README.md) |
| **一份基因型标准答案，十种文件格式**：1000 Genomes 公开样本在 12 个基因上的 13 个位点，五家厂商的导出格式，两版 VCF 及其 gzip、BGZF、ZIP | [`benchmarks/genomics`](benchmarks/genomics/README.md)，不联网也能跑 |
| **三个公开基准**，数据集公开：长期健康 agent、医疗幻觉、有害医疗建议 | [mirobody-eval](https://github.com/thetahealth/mirobody-eval) · [数据集](https://huggingface.co/mirobody) · [arXiv:2604.02834](https://arxiv.org/abs/2604.02834) |
| 包会自己说清楚是哪份词表在回答你：`mirobody.BUNDLE_VERSION` 是 `loinc-2.83+2026.09.17-aacb2c715b56`，发行版本、切分日期，加一份对词表内容算出来的摘要 | `python -c "import mirobody; print(mirobody.BUNDLE_VERSION)"` |
| **`pip install mirobody` 只装 2 个包**，只依赖 numpy | `pip install mirobody && pip list` |

这套引擎驱动着 **[Theta Wellness](https://www.thetahealth.ai/)**：一款已经上线、
注册用户 5,000+ 的个人健康产品。

## 使用和扩展

| 你想要 | 这样做 |
| --- | --- |
| 在自己代码里做离线解析和单位换算 | `pip install mirobody`：不用 key，不用联网，两个包 |
| 把一份文件变成读数 | `pip install 'mirobody[parse]'`：PDF、图片、Excel、Word、PowerPoint、文本都行；只有扫描件才会送到视觉模型 |
| 在 Claude Desktop、Cursor 或自己的 loop 里用这些工具 | 设置 → MCP：每个 agent 工具同时挂在 `/mcp` 上，按用户鉴权 |
| 在任意 MCP 客户端里做编码，不起服务 | `uvx mirobody mcp`（stdio）：读数转成带码的 FHIR Observation，主诉编到 ICPC-3，还有单位工具；不要 key，不要数据库 |
| 让自己的应用对接一套部署 | 走 HTTP API，打到你自己跑的那套上：[自部署 HTTP API](https://docs.mirobody.ai/zh/http-api) |
| 加一个工具或一个设备数据源 | 往 `mirobody/agent/tools/` 或 `mirobody/collect/providers/` 丢个文件重启，或者 `pip install` 一个声明了 `mirobody.providers` / `mirobody.tools` / `mirobody.agents` entry point 的包 |
| 换掉自带的 agent 框架 | `pip install 'mirobody[agent]'` 拿中间件和虚拟文件系统后端；或者把 `AGENT_DIRS` 指向自己的目录，整体替换自带的 agent |
| 同一套引擎，不自己起服务 | [Mirobody Cloud](https://docs.mirobody.ai/zh/api-reference/overview)，托管的 `/v1` API |

→ [MCP 接入](https://docs.mirobody.ai/zh/tools/mcp-integration) ·
[添加工具](https://docs.mirobody.ai/zh/tools/adding-tools) ·
[智能体](https://docs.mirobody.ai/zh/tools/agents) ·
[接入自己的 agent](CONTRIBUTING.md#-bringing-your-own-agent)

## 贡献

最大的贡献，是找出一个解析器答错的词。拿你自己报告上的写法跑一下 `mirobody resolve "<词>"`，
主诉用 `resolve_symptom`；如果答案错了，或者是空的，
[提个 issue](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)，
或者往 [`resolver_overrides.tsv`](mirobody/res/loinc/resolver_overrides.tsv) 加一行，
再往 [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) 加一个用例；
主诉说法加进 [`benchmarks/health_records/cases.json`](benchmarks/health_records/cases.json)。
覆盖率分数，就是评审。

```bash
pip install -e '.[app,test]'
pytest -q                                                            # 随克隆发布的门禁
python -m unittest benchmarks.health_records.test_cases              # LOINC/UCUM 与 ICPC-3 判定，五种语言
python -m unittest discover -s benchmarks/genomics -p 'test_*.py'    # 一份基因型标准答案，十种文件格式
lint-imports && ruff check mirobody examples
```

→ [CONTRIBUTING.md](CONTRIBUTING.md) · [`benchmarks/`](benchmarks/README.md) ·
[AGENTS.md](AGENTS.md)（给编码智能体看的） ·
[仓库结构](docs/repository-layout.zh-CN.md) · [路线图](docs/roadmap.md) ·
[CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md)

## 文档，以及这套设计参考过的项目

**[docs.mirobody.ai](https://docs.mirobody.ai/zh/self-host)**，中英双语：
[自部署](https://docs.mirobody.ai/zh/self-host)、
[快速开始](https://docs.mirobody.ai/zh/quickstart)、
[完整演示](https://docs.mirobody.ai/zh/walkthrough)，以及托管版的
[API 参考](https://docs.mirobody.ai/zh/api-reference)。
文档站的开源部分在每次发版时直接从本仓库的 [`docs/`](docs/README.md) 生成，
所以不会和仓库里的命令走偏。

本项目的设计参考了以下标准与项目，特此鸣谢：
[HL7 FHIR](https://hl7.org/fhir/)、
[Regenstrief Institute](https://www.regenstrief.org/)（[LOINC](https://loinc.org/)）、
[UCUM](https://ucum.org/)、[ICPC-3](https://icpc-3.info/)（WONCA）、
[CPIC](https://cpicpgx.org/)、[OHDSI OMOP](https://www.ohdsi.org/)、
[Open Wearables](https://github.com/the-momentum/open-wearables)、
[Open mHealth](https://github.com/openmhealth/schemas) / IEEE 1752、
[wearipedia](https://github.com/Stanford-Health/wearipedia)、
[dlt](https://github.com/dlt-hub/dlt) / [Airbyte](https://github.com/airbytehq/airbyte-python-cdk) / [Singer](https://github.com/meltano/sdk)，
以及 [deepagents](https://github.com/langchain-ai/deepagents) 和 LangChain。
随包分发的术语许可在 [`LICENSE-3RD-PARTY`](LICENSE-3RD-PARTY) 里。

<div align="center">

<a href="https://www.star-history.com/#thetahealth/mirobody&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
  </picture>
</a>

*如果它替你读懂了一份报告，点个 star，下一个人就更容易找到它。
基本每周都有新版本，[Watch](https://github.com/thetahealth/mirobody/subscription) 一下就能第一时间收到。*

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>

<!-- mcp-name: ai.thetahealth/mirobody -->
