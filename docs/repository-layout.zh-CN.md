# 仓库结构

**[English](repository-layout.md)** · **中文**

三个阶段（① 收集 Collect、② 转译 Translate、③ 智能体 Agent）各自住在哪里，以及
这套代码出货的两种形态。1.4.1 从 README 挪到这里，README 只留一行地图。

```
mirobody/
├── engine/      正门：resolve()、resolve_reading() 和 parse_file()
├── units/       UCUM 单位、unit_family、换算
├── lexical.py   表层折叠 + 认识 CJK 的分词器
├── bundle.py    构建期：轴表和别名来源
├── res/         随包发布的 LOINC 词表、res/catalog/metrics.tsv
├── kernel/      健康数据「是什么意思」，全是纯函数：
│                metrics · series · quality · overlay · meds ·
│                query · tools · ops · connect · sink · events ·
│                evidence · memory · decoders/
├── documents/   文件变成文本，按类型分：PDF 文本层、只对扫描件做 OCR、Office、纯文本   [parse]
├── collect/     ① 收集 Collect   provider、文件解析、落库、读取（Postgres）
├── translate/   ② 转译 Translate 纯函数缝（fold · parse · local_day · series · code ·
│                devices）、指标目录、单位、aggregate/、derive/
├── agent/       ③ 智能体 Agent   models/ fs/ wire/ middleware/ tools/ chat/
├── mcp/         MCP 服务端
├── server/      HTTP 应用：路由、鉴权、自带的前端
├── utils/       调用方自己接的机制：config、db、sse、net、llm_output、prompts、log
├── user/        身份与关爱圈：谁可以读谁的记录
└── schema/      DDL，开发环境启动时重放

demo/            关爱圈演示数据，和 frontend/ 并排      检出会带这两个目录，
frontend/        自带的前端                            pip install 不会
```

从 `engine/` 到 `kernel/` 这一段就是**库**的全部：只依赖 numpy，两个包。

**两种形态，诉求正好相反。** 发到 PyPI 的那个是一个**库**，小到没人需要为它操心：
`pip install mirobody` 装下来是 **2 个包、67 MB**，即上面 `documents/` 那行以上的
部分，外加 numpy。`[parse]` 补上文件读取，`[agent]` 把 agent 框架当库用；`[app]`
是全部，而唯一会装它的是 `requirements.txt`，因为 Docker 那条路是
`git clone && ./deploy.sh`，从来不是 pip install。

**这些边界是机器守的，不是写在文档里的。** 四条 import-linter 契约压着分层：库这
一层除了 numpy 什么都不 import，engine 永远不 import agent 层，`lint-imports` 不
过就构建失败。另一道闸门 `scripts/check_wheel_data.py` 负责把词表构建流程和 v2
语义管线（19,000 行，装了包的人一行也跑不了）挡在产物外面。

→ [架构](https://docs.mirobody.ai/zh/concepts/architecture/) ·
[CONTRIBUTING.md](../CONTRIBUTING.md)
