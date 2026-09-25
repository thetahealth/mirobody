<div align="center">

# Mirobody

**自托管的 AI 原生健康数据引擎：任何来源，一套标准，每个答案都有出处。**

**[English](README.md)** · **中文**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)
[![GitHub stars](https://img.shields.io/github/stars/thetahealth/mirobody?style=social)](https://github.com/thetahealth/mirobody/stargazers)

**[📚 文档](https://docs.mirobody.ai/)** · **[▶ 在线 Demo（免注册）](https://chat.mirobody.ai/demo)** · **[🔌 API 平台](https://platform.mirobody.ai/)**

</div>

---

去年体检写 `A1c`，今年医院写 `HbA1c`，换家机构又成 `糖化血红蛋白`。一项检查三个名字，读不懂，也比不了。
Mirobody 把不同来源、格式、表述的健康信息，规整成一套语言、一个体系，再由 AI 在这份记录上回答你的问题，
每个指标都有出处，可追溯、可对比、可展示。我的血压这几年怎么变？妈妈的糖尿病指标好转了吗？
宝宝历年体检有什么变化？全部自托管，数据就在你自己手中。

<p align="center">
  <img src="docs/images/ask-own-demo.zh-CN.gif"
       alt="用中文问胆固醇怎么变的，agent 找到三份用不同写法记录同一项检查的文件，把它们解析成同一个码，并画出趋势" width="880">
</p>

<p align="center"><em>三份文件，三种写法，同一个 LOINC 码。agent 把三份都找出来，
汇总出趋势，并说明每个数字来自哪份文件。</em></p>

## Mirobody 能做什么

- **把全家人的记录合成一份。** 用关爱圈邀请伴侣、父母，甚至是一个完全不会自己登录的孩子，管理家庭的健康档案。
- **所有来源，照单全收。** Garmin、Oura、Whoop 直接对接；任何写进 Apple Health 的手环、
  戒指、体重秤，也一并进来。PDF、照片、表格、导出文件，23 种文件类型，Mirobody 都能看懂。
- **0 幻觉，可追溯。** 每一条健康数据都会落进一套确定的指标体系：要么给出一个确定的编码，
  要么明说没解析出来，绝不自己编一个。基于真实报告开发和测试，英文、中文、日文的写法都认。
- **Agent 只在编码过的数据上推理。** 按分钟、小时、天、周、月给出趋势，并画成图；一次调
  用就能算出基线和变化了多少；因为统一到了同一套标准（LOINC/UCUM），跨化验所、跨文件、跨设备
  可以直接比较；用药记录和基因型数据也读得了。
- **一台笔记本就能跑。** 四个容器，常驻 791 MiB，空载 CPU 占用不到 5%。不用
  GPU，不用 Node.js。
- **你的模型，你的 key，你的数据。** 模型调用走你自己选的那个模型，其余的一切都留
  在你自己的机器上。

## 60 秒试一下

一条命令，五种写法，看它认出哪几个、又会在哪一个上停下来。不用 key，不用配置，
不用联网；用 `uvx`，连装都省了：

```bash
uvx mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

<p align="center">
  <img src="docs/images/resolve-demo.zh-CN.gif"
       alt="mirobody resolve：血红蛋白和 ヘモグロビン 落在同一个 LOINC 码上，血脂 是一次故意的弃答" width="880">
</p>

`血红蛋白` 和 `ヘモグロビン`，两种语言，同一个码 718-7。`血脂` 说的是一个类别，
不是一项具体检查，所以它解析不出结果：认不出来的词，解析器宁可空着，也不猜一个
码给你。一个猜错的码，会把两项不同的检查画进同一条趋势里。

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   any language, one code
resolve("total cholesterol").loinc                     # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")   # '14647-2' [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")    # '2093-3'  the unit picks the code

resolve("中性粒细胞百分比").loinc                          # '26511-6' Neutrophils/Leukocytes
resolve_reading("中性粒细胞", "62 %", None).loinc          # '26511-6' a percentage...
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc       # '26499-4' ...and a count are two codes
resolve("血脂").resolved                                 # False    a category, not an observation
```

**有数值和单位，就一起传进来。** 单位不一样，就是两项不同的检查，LOINC 把这件事
写进了码本身的定义里，所以同一个名字按设计会对应好几个码。
→ [引擎参考](https://docs.mirobody.ai/zh/engine/) · [指标](https://docs.mirobody.ai/zh/concepts/indicators/)

## 收集 · 转译 · 智能体

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/collect-translate-agent-dark.zh-CN.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/collect-translate-agent.zh-CN.svg">
  <img src="docs/images/collect-translate-agent.zh-CN.svg" alt="收集、转译、智能体：三个阶段，从左到右" width="920">
</picture>
</p>

一项指标从进门到被引用，要走三步。每一步都留下痕迹，所以最后那个答案，你能一路
查回到它出自的那一页：

| 阶段                        | 做什么                                                                                                                                                                                                                     | 在哪                                                                                                               |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **① 收集 Collect**   | 化验单、穿戴设备、手机照片、基因文件，都收进来。源文件原样留下，每一项指标都能指回它被读出来的那一页。                                                                                                                     | [`collect/`](mirobody/collect/)                                                                                   |
| **② 转译 Translate** | 一个名字解析成一个码，一个单位统一到 UCUM，全程离线、结果确定。`A1c`、`HbA1c`、`糖化血红蛋白` 在这一层变成同一项检查。                                                                                               | [`engine/`](mirobody/engine/) · [`translate/`](mirobody/translate/) |
| **③ 智能体 Agent**   | 在编码后的记录上提问。按分钟、小时、天、周、月给出趋势，一次调用就能算出计数、最小值、最大值、均值和变化量；同一个码，跨化验所、跨设备直接比较。图表画在回复里，用药记录和基因型数据也读得了，每个数字都说明出自哪份文件。 | [`agent/`](mirobody/agent/)                                                                                       |

① 记下来源怎么写，② 判断它到底是什么，③ 在这个基础上作答。跨化验所比一个数字、
画一条三年的趋势、算一个基线，靠的都是 ② 给出的那个码。

**智能体不一定得是我们这一个。** 它用的每一个工具同时挂在 `/mcp` 上，按用户鉴权。
Claude Desktop、Cursor，或者你自己写的 loop，用的是同一套工具、同一份记录，问出
来的是同一批指标。

Garmin、Oura、Whoop 用你自己在厂商那边申请的凭证接入，步骤写在
[接入指南](docs/provider-setup.zh-CN.md)里。Apple Health 走的是另一条路：数据由手机上
的客户端交过来，所以任何牌子的手环、戒指、体重秤，只要写进了 Apple Health，就
一起进到你的记录里，这边什么都不用对接。

## 隐私

除了你自己选的那个大模型，没有任何数据离开你的机器。读一张体检报告照片、从 PDF
里抽出指标、回答你的提问，这几件事都要调用它；用哪家、用哪个模型，由你 `.env`
里那一把 key 说了算。

**② 转译**这一层完全在本地：名字对到码、单位换算成 UCUM，查的是随包发布的词表，
不用 key，不联网，不跑模型，也不用 GPU。你的记录存在你自己的 Postgres 里，容器
是你自己起的，这里不会把任何使用情况报给谁。

**一把 key，也是你唯一要保管的秘密。** 把
[DeepSeek key](https://platform.deepseek.com/api_keys)（`DEEPSEEK_API_KEY`）或
[DashScope key](https://dashscope.console.aliyun.com/apiKey)（`DASHSCOPE_API_KEY`）
写进 `compose.yaml` 旁边的 `.env`，`docker compose restart`，就跑起来了。
[OpenRouter](https://openrouter.ai/keys)（`OPENROUTER_API_KEY`）、
[OpenAI](https://platform.openai.com/api-keys)（`OPENAI_API_KEY`）、Anthropic、
Google，或者任何 OpenAI 兼容网关，单独一家都能把整套跑通。聊天用哪个模型、报告
照片交给谁读、指标交给谁抽、向量交给谁算，是 [`config.llm.yaml`](config.llm.yaml)
里的四行；那个文件写的是变量名（`api_key: OPENROUTER_API_KEY`），不是密钥本身。
`mirobody doctor` 会把每一环选中了什么列出来，缺了什么也直接告诉你怎么补。

快速上手用的那几个密钥都是占位符，落库加密也还没覆盖到每一个字段。要把它接到一
个你控制不了的网络上，先过一遍 [SECURITY.md](SECURITY.md)：服务端到底会往外发哪
些请求，也写在那里。

## 🚀 从头到尾跑一遍

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull   # 解析器的 LOINC 词表，13 MB；新克隆下来是个指针文件，直到你跑这条命令
./deploy.sh                       # Postgres + pgvector、Redis、服务端、worker → http://localhost:18060
```

（`--depth 1` 跳过历史里那些已经被替换掉的前端构建产物；要提 PR 就去掉它。）

有两件事 `deploy.sh` 会拦下来，并把修法写在报错里：一台机器同时只能跑一份，
因为 `compose.yaml` 固定了这套栈的子网，再起一份要改 `mirobody_network` 的网段；
另外，拒绝 named volume 的 Docker（rootless、受限环境）要改用 bind mount，
`compose.override.yaml.example` 就是为这个准备的。

用 `you@mirobody.ai`、验证码 `111111` 登录，不需要邮件服务。注册自己的账号也
只要一个请求：

```bash
curl -X POST localhost:18060/password/register -H 'Content-Type: application/json' \
     -d '{"email":"me@example.com","password":"at-least-8-chars"}'
```

`SEED_DEMO_DATA` 默认打开，所以一开始就有两个账号，合计 **2,019 条指标**：你
自己，和把记录以只读方式共享给你的 `mom@mirobody.ai`。要存真实数据，把它设成
`false`，这两个账号就不会建。**设置 → 添加成员**，收录的是一个完全不会自己登录
的人：父母，孩子，你替他们保管这份记录。

把一份文件拖到 Data 页，看它变成指标。[`demo/upload/`](demo/) 里放着四份种子
数据故意没写进库的文件：一份化验单 PDF、一张打印报告的手机照片、一个表格、另
一家化验所导出的 CSV。每一项分析物都会带着数值、单位和 LOINC 码被抽出来，并链
回它来源的那一页。

<p align="center">
  <img src="docs/images/upload-demo.zh-CN.gif"
       alt="把化验单 PDF 拖到 Data 页；分析物被抽取出来，带着 LOINC 码出现在指标表里" width="880">
</p>

问一句胆固醇怎么变的，agent 会把带着这一项的文件全都找出来：一份化验所写的是
`Cholesterol, Total`，另外几份写的是 `Total Cholesterol-TC`，两种写法同为
**14647-2**。它把趋势画出来，并说明每个数字出自哪份文件：
`4.60 → 4.45 → 4.38 mmol/L`。改问基线或者月均值，同一个工具给的是在整条记录上
汇总出来的结果，而不是模型自己把一行行数字加起来。

同一个问题问到共享给你的那份记录上，答案就是另一个人的，而且那份数据你只能看，
不能改。这种共享叫**关爱圈**：邀请制，默认关闭，权限严格控制。

<p align="center">
  <img src="docs/images/ask-circle-demo.zh-CN.gif"
       alt="同一个问题问到共享记录上；答案来自另一个人的文件" width="880">
</p>

这三件事各是一道检查，而且都收在同一个函数里。
一个账号要读到不属于自己的记录，只有 `resolve_subject` 这一条路；
光是同在一个圈子里，什么也读不到。

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/your-care-circle-dark.zh-CN.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/your-care-circle.zh-CN.svg">
    <img src="docs/images/your-care-circle.zh-CN.svg" alt="一个人如何读到另一个人的健康记录：请求经过 resolve_subject，它要求双方成员关系都已接受、且对方自己打开了 health_access 开关，然后要么返回按请求裁剪过的权限，要么抛出 403" width="920">
  </picture>
</p>

→ [四分钟完整演示](docs/walkthrough.zh-CN.md) ·
[`examples/06_care_circle_rules.py`](examples/06_care_circle_rules.py) 能离线打印整张共享决策表 ·
[Docker 部署](https://docs.mirobody.ai/zh/deployment/docker/) ·
[配置](https://docs.mirobody.ai/zh/configuration/)

## 每一个数字，都可以复现

出处都附在后面：一条可以跑的命令，或者一个公开的数据集。

- 常规体检会打印的那些项目，**296/296**，覆盖英文、中文（简体和繁体）、日文、俄文和爱沙尼亚文。
  这套用例是故意挑的最不讨巧的一组：日常的几个套餐，按报告上真正的印法写，
  也正是每个新用户第一分钟会去试的那些词。
  [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) 跑一下
  就会把分数打出来。
- **13 家可穿戴厂商，逐字段读过一遍**：447 个字段里 289 个落到 LOINC 码，每一个
  都带置信度和它出自哪份厂商文档；另有 71 个量明确不落码，写明原因而不是猜。
  表在[设备对照表](docs/device-crosswalk.md)。
- **三个公开基准**，数据集公开，各自一条命令可复现：长期健康 agent、医疗幻觉、
  有害医疗建议。
  [mirobody-eval](https://github.com/thetahealth/mirobody-eval) ·
  [数据集](https://huggingface.co/mirobody) ·
  [arXiv:2604.02834](https://arxiv.org/abs/2604.02834)。
- **包会自己说清楚是哪份词表在回答你**：`mirobody.BUNDLE_VERSION` →
  `loinc-2.83+2026.09.17-aacb2c715b56`，发行版本、切分日期，加一份对词表内容
  算出来的摘要。
- **316 项标准设备指标**，328 个带量纲分析的 UCUM 单位。完整数字，以及
  LOINC 2.83 的切法留下了什么、丢掉了什么，都写在[标准化详解](docs/standardization.zh-CN.md)里。
- **`pip install mirobody` 只装 2 个包**，只依赖 numpy。

这套引擎驱动着 **[Theta Wellness](https://www.thetahealth.ai/)**：一款已经
上线、注册用户 5,000+ 的个人健康产品。

## 🔌 使用和扩展

| 你想要                                               | 这样做                                                                                                                                                                                          |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 在自己代码里做离线解析和单位换算                     | `pip install mirobody`：不用 key，不用联网，两个包                                                                                                                                            |
| 把一份文件变成指标                                   | `pip install 'mirobody[parse]'`：PDF、图片、Excel、Word、PowerPoint、文本都行；只有扫描件才会送到视觉模型                                                                                     |
| 在 Claude Desktop、Cursor 或自己的 loop 里用这些工具 | 设置 → MCP：每个 agent 工具同时挂在`/mcp` 上，按用户鉴权                                                                                                                                     |
| 让自己的应用对接一套部署                             | 走 HTTP API，打到你自己跑的那套上：你的应用，你的数据层                                                                                                                                         |
| 加一个工具或一个设备数据源                           | 往`mirobody/agent/tools/` 或 `mirobody/collect/providers/` 丢个文件重启，或者 `pip install` 一个声明了 `mirobody.providers` / `mirobody.tools` / `mirobody.agents` entry point 的包 |
| 换掉自带的 agent 框架                                | `pip install 'mirobody[agent]'` 拿中间件和虚拟文件系统后端；或者把 `AGENT_DIRS` 指向自己的目录，整体替换自带的 agent                                                                        |

→ [API 总览](https://docs.mirobody.ai/zh/api-reference/overview/) ·
[MCP 接入](https://docs.mirobody.ai/zh/tools/mcp-integration/) ·
[添加工具](https://docs.mirobody.ai/zh/tools/adding-tools/) ·
[接入自己的 agent](CONTRIBUTING.md#-bringing-your-own-agent)

## 🤝 贡献

最大的贡献，是找出一个解析器答错的词。拿你自己报告上的写法跑一下
`mirobody resolve "<词>"`；如果答案错了，或者是空的，
[提个 issue](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)，
或者往 [`resolver_overrides.tsv`](mirobody/res/loinc/resolver_overrides.tsv) 加一行，
再往 [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) 加一
个用例：覆盖率分数，就是评审。

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [CONTRIBUTING.md](CONTRIBUTING.md) · [本地 Python 环境搭建](https://docs.mirobody.ai/zh/development/setup/) ·
[仓库结构](docs/repository-layout.zh-CN.md) ·
[路线图](docs/roadmap.md) · [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md)

## 📚 文档，以及这套设计参考过的项目

**[docs.mirobody.ai](https://docs.mirobody.ai/)**，中英双语，从
[快速上手](https://docs.mirobody.ai/zh/quickstart/)或
[API 参考](https://docs.mirobody.ai/zh/api-reference/)开始看；
快速上手也随代码一起发布，就是 [`docs/quickstart.md`](docs/quickstart.md)，
所以它不会和仓库里的命令走偏；给贡献者的文档在 [`docs/`](docs/README.md)。

本项目的设计参考了以下标准与项目，特此鸣谢：
[HL7 FHIR](https://hl7.org/fhir/)、
[Regenstrief Institute](https://www.regenstrief.org/)（[LOINC](https://loinc.org/)）、
[UCUM](https://ucum.org/)、[OHDSI OMOP](https://www.ohdsi.org/)、
[Open Wearables](https://github.com/the-momentum/open-wearables)、
[Open mHealth](https://github.com/openmhealth/schemas) / IEEE 1752、
[wearipedia](https://github.com/Stanford-Health/wearipedia)、
[dlt](https://github.com/dlt-hub/dlt) / [Airbyte](https://github.com/airbytehq/airbyte-python-cdk) / [Singer](https://github.com/meltano/sdk)，
以及 [deepagents](https://github.com/langchain-ai/deepagents) 和 LangChain。随包
分发的术语许可在 [`LICENSE-3RD-PARTY`](LICENSE-3RD-PARTY) 里。

<div align="center">

<a href="https://www.star-history.com/#thetahealth/mirobody&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
  </picture>
</a>

*如果它替你读懂了一份报告，点个 star，下一个人就更容易找到它。
基本每周都有新版本 —— [Watch](https://github.com/thetahealth/mirobody/subscription) 一下就能第一时间收到。*

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>
