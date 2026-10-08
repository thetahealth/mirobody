# 快速开始

**[English](quickstart.md)** · **中文**

**想看到完整产品，从 Docker 开始。** 两条命令就能启动带着一份合成示例数据的网页应用；剩下的交给一把模型 Key，或者本地模型：

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh
```

这页剩下的部分是给只想要其中一部分的开发者看的。下面三条路是可选项，不是步骤：

| 你想要 | 去哪 | 需要 | 模型 Key |
| --- | --- | --- | --- |
| 在自己的代码里解析名称和单位 | [A · 这个库](#a--the-library) | Python 3.12 | 不需要 |
| 运行完整产品，并带有数据 | [B · 整套服务](#b--the-stack) | 带 Compose 的 Docker | 一把，或者用 llama.cpp 上的本地模型则不需要 |
| 改代码并看到效果 | [C · 一份检出](#c--a-checkout) | Python + 一个带 pgvector 的 Postgres | 一把，或者一个本地模型服务 |

要用托管的 API Key 和 `/v1` 接口，请用另一份 [云端快速开始](https://docs.mirobody.ai/zh/api-reference/quickstart/)。这一页讲的是开源引擎和它自己的命令。

<a id="a--the-library"></a>

## A · 这个库

两个包，唯一的依赖是 numpy。没有 Key、没有网络、没有数据库，你的机器上也不会运行任何模型：解析器是建在 LOINC 上的词法索引，不是大模型。

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

用 [uv](https://docs.astral.sh/uv/) 的话什么都不用安装：`uvx --python 3.12 mirobody resolve ...`。`--python 3.12` 很重要：换成更老的默认解释器，uv 会选到 1.2 之前的版本。

<p align="center">
  <img src="images/resolve-demo.zh-CN.gif" alt="mirobody resolve：血红蛋白 和 ヘモグロビン 落到同一个 LOINC 码上，还有一次刻意的弃答" width="880">
</p>

`血脂` 说的是一个类别，不是一项具体的观测，所以它什么都解析不到。这是故意的设计：一个错的码会把两种不同的检验放进同一条趋势线。

Python 里也一样。单位是一条读数身份的一部分，所以它能改变码：

```python
from mirobody.engine import resolve, resolve_reading, standardize_reading

resolve("血红蛋白").loinc                                     # '718-7'   任何语言，同一个编码
resolve("total cholesterol").loinc                          # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2' [Moles/volume]：单位决定编码
resolve_reading("中性粒细胞", "62 %", None).loinc              # '26511-6' 一个百分比……
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc           # '26499-4' ……和一个计数，是两个编码
standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # '718-7'  作为一条 FHIR Observation
```

主诉和诊断有自己的轴，ICPC-3，规则一样：要么给出一个编码，要么给出明确的拒绝理由，不猜。

```python
from mirobody.translate import resolve_symptom, resolve_condition

resolve_symptom("头疼").code           # 'NS01'         头痛；'headache' 和 '頭痛' 得到同一个结果
resolve_symptom("疼").outcome         # 'refused'      太宽泛，编不了码；原话被保留
resolve_condition("2型糖尿病").code    # 'TD72'         2型糖尿病
resolve_condition("糖尿病").outcome    # 'needs-input'  哪一型？会问，不会猜
```

读一份厂商导出的文件不需要其他依赖；`zipfile` 和 `xml.etree` 都是标准库：

```bash
mirobody import apple ~/Downloads/export.zip
```

剩下的部分：`pip install 'mirobody[parse]'` 能把一份 PDF、照片或表格变成读数（这一步会调用模型，所以需要一把 Key），`pip install 'mirobody[app]'` 安装服务端。以上内容都不需要它们。`[agent]` 是把 agent harness 作为库使用，`[test]` 是测试套件。

不用整套服务，`uvx --python 3.12 mirobody mcp` 就能通过 stdio 把词表交给一个 MCP 客户端，离线、不需要 Key。它唯一那个读整份文档的工具，`standardize_report`，还需要 `[parse]` 这个 extra 和一把模型 Key。[standardization.zh-CN.md](standardization.zh-CN.md) 详细讲了 ② 转译，[`examples/`](../examples/README.md) 里有可以直接运行的脚本。

<a id="b--the-stack"></a>

## B · 整套服务

带 pgvector 的 Postgres、服务端和后台任务，连同已经播种好的示例记录。

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh                       # → 在 http://localhost:18060 打印设置链接
```

`deploy.sh` 会生成密钥写进 `.env`，并拉取预构建镜像；镜像里已经带着 LOINC 词表，所以 Docker 用户不需要 Git LFS。生成密钥需要 OpenSSL。这时候还没有模型，它会打印一个首次设置页的链接：在那里粘贴**一把**模型 Key，或者选择**100% 在本机运行**。要在 Docker 里跟服务一起运行模型，换成 `COMPOSE_PROFILES=local-cpu ./deploy.sh` 启动；[local-models.zh-CN.md](local-models.zh-CN.md) 有 GPU 和 Mac 的路线，以及各自需要什么。

在「邮箱验证码」标签用 `you@mirobody.ai`、验证码 `111111` 登录；不涉及任何邮件服务商。`SEED_DEMO_DATA` 默认打开，所以两个账号下已经有 2,019 条读数。如果打算保存真实数据，请在第一次启动前把它设为 `false`，这样两个账号都不会被创建。[`demo/upload/`](../demo/) 里的四份文件是刻意没有播种进去的，所以把其中一份拖到 Data 页走的是真实路径。

一把 Key 也可以不填在设置页，而是写进 `.env`：`OPENROUTER_API_KEY`、`OPENAI_API_KEY`、`GOOGLE_API_KEY`、`ANTHROPIC_API_KEY`，或者通过 `<PROVIDER>_BASE_URL` 指向任何 OpenAI 兼容网关。写完之后运行 `docker compose up -d`；`restart` 不会重新读取 `.env`。[`config.llm.yaml`](../config.llm.yaml) 里写的是每个条目读取哪个变量名（`api_key: OPENROUTER_API_KEY`），从不写密钥本身。要查看每个模型环节选用了哪个条目：

```bash
docker compose exec mirobody mirobody doctor          # 加上 --probe 会给每一个发送一次真实请求
```

[自部署指南](https://docs.mirobody.ai/zh/self-host) 按步骤讲解整个过程，[walkthrough.zh-CN.md](walkthrough.zh-CN.md) 带你把运行中的服务走一遍。

<a id="c--a-checkout"></a>

## C · 一份检出

`serve` 是部署的形态：它读取随包的配置和任何 `config.{ENV}.yaml` 覆盖层，并要求持久的密钥。`dev` 在一个进程里运行同一个服务端，用的是内存里的配置和临时生成的密钥：

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[app]'
```

如果手头还没有带 pgvector 的 Postgres，先启动一个：

```bash
docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pw \
    -e POSTGRES_DB=mirobody \
    pgvector/pgvector:pg17
```

然后启动 API；如果用的是已有的数据库，替换掉连接地址。再开一个终端检查它的健康检查端点：

```bash
mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody
```

```bash
curl -fsS http://127.0.0.1:18090/api/health
```

返回内容里带一个 `version` 字段。请用 pgvector 而不是普通 Postgres：schema 首次启动时会建一个 vector 列。

**`dev` 提供的是 API，不是网页客户端。** 构建好的客户端放在仓库根目录的 `frontend/`，在包之外，所以一份检出里有它，`pip install` 里没有。`dev` 是用来运行 API、MCP 接口和 agent 的；要用界面，请用 B。

`dev` 生成的密钥是每次运行各不相同的：会话和加密过的配置值在重启后不会保留。要保留它们，请在环境变量里设置 `JWT_KEY` 和 `CONFIG_ENCRYPTION_KEY`。

<a id="when-it-does-not-come-up"></a>

## 没有出现时

| 你看到的 | 原因 |
| --- | --- |
| `mirobody doctor` 显示 `keys present : none`，每个环节都是 `--` | 没有模型 Key，也没有本地模型服务。① 收集和 ② 转译仍然能用；抽取和回答不行。打开 `./deploy.sh` 打印出来的链接。 |
| 在一份新克隆上查 LOINC 会报错 | `git lfs pull` 还没有运行过，词表还只是一个指针占位文件。 |
| `mirobody dev` 退出并要求提供 Postgres | 给出 `--pg-url`，或者在环境变量里设 `PG_URL` / `DATABASE_URL`。 |
| 服务端启动了，但设备同步一直没有运行 | 检查后台任务的日志和 Postgres 连接；设备拉取和任务状态都用 Postgres。 |
| `mirobody serve` 连不上 Postgres | 为那个数据库设置 `PG_HOST`、`PG_PORT`、`PG_USER`、`PG_PASSWORD` 和 `PG_DBNAME`。 |

<a id="next"></a>

## 下一步

[walkthrough.zh-CN.md](walkthrough.zh-CN.md) 看运行中的服务是什么样子 ·
[local-models.zh-CN.md](local-models.zh-CN.md) 把所有模型都留在自己的机器上 ·
[pipeline.md](pipeline.md)（英文）看一条读数经历了什么 ·
[repository-layout.zh-CN.md](repository-layout.zh-CN.md) 看代码地图 ·
[provider-setup.zh-CN.md](provider-setup.zh-CN.md) 打开 Garmin、Oura 或 Whoop。
