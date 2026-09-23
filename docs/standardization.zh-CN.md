# 标准化详解

**[English](standardization.md)** · **中文**

README 里 **② 转译 Translate（标准化）** 那一阶段的长版本：随包发布的词表到底是
什么、它刻意不做什么、切自哪个 LOINC 版本、为什么停在那里，以及那个默认关闭的
语义层。这里每一个精确数字都和 README 引用的是同一个，而 README 里的数字由它们
来源的产物守着，这一页跟着它走。

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/collect-translate-agent-dark.zh-CN.svg">
  <source media="(prefers-color-scheme: light)" srcset="images/collect-translate-agent.zh-CN.svg">
  <img src="images/collect-translate-agent.zh-CN.svg" alt="收集、转译、智能体：三个阶段，从左到右" width="920">
</picture>
</p>

## 这一层提供什么

这里说的标准化不是一张查找表，而是一套完整的术语归一系统：

- **63,416 个码**，由一条规则一趟从 LOINC 2.83 切出：实验室与临床观察，
  叙述与文档量表丢掉，面板只留化验子类和生命体征子类。
  **692,577 条折叠过的称谓**指向它们，来自组分、长名与短名、相关名，
  以及发行版自带的 21 份语言变体。
- **英文为主，中文在旁。** 22,578 条中文别名，加上本项目自己人工收录的那些，
  排在索引前面，所以 `hemoglobin`、`血红蛋白`、`血紅素` 都落在 LOINC 718-7 上。
  其他语言走 LOINC 自己发布的那 21 份语言变体（它们本来就是别名索引的输入），
  而不是我们自己的文件：日文那份是 UMLS 派生的，
  另外五份机器派生的（de·es·fr·ko·ru）追溯不到它们声称的语言变体。
  两者都已删除，见 `LICENSE-3RD-PARTY`。
- **繁體中文 是两个问题，也就当两个问题处理。** 字形折叠是机械的（随包发布一张
  3,336 字的 zh-Hant → zh-Hans 对照表）；用词不是：台湾的习惯用词不一样，`血紅素`
  折叠过去会撞上 HbA1c 的码。这类词按繁体写法单独收录，而人工收录的那一行永远压
  过折叠的结果。
- **单位**按 331 个 UCUM 单位归一到 59 个 PROPERTY 家族，带量纲分析，
  有一座按 LOINC 码索引的摩尔质量桥，并且对 `%` 和 `10*9/L` 这种情况明确拒绝混算。
  316 项标准设备指标。
- **这里全是词法的，弃答是我们要守住的天花板。** 词表不认识的词返回 `unresolved`，
  不返回最近邻。1.4.x 在旁边挂过一层默认关闭的余弦召回，1.5.0 把它删了。它**不会
  弃答**：碰到从没见过的词，会用「答对了」的那种置信度把最近邻交给你——在 LOINC
  矩阵上实测，胡话能拿 0.78，而真实的指标名低到 0.56，两个区间重叠，没有阈值能把
  它们分开。它也从来没跑起来过：矩阵是 108,248 行 × 1024 维、只对某一对（供应商，
  模型）有效，而且从未发布，所以 `get_index()` 在 wheel 安装态和源码树里**都返回
  None**（两边都实测过）。一个没人能打开的开关，挡在一个我们本来也不敢信的答案前面。
  想要更高的召回，诚实的杠杆是在 `res/loinc/resolver_overrides.tsv` 里加一行人工词条。
- **我们量这个说法，而不是断言它。**
  [`test_engine_coverage.py`](../mirobody/tests/test_engine_coverage.py) 拿常规体检
  会出现的那些套餐去考离线解析器，词按报告上真正的印法写，覆盖英文、简体中文、
  繁體中文和日本語，外加平台 API 教给我们的那套穿戴设备词汇。**今天是 213/213；
  刚写出来那天是 32/94。** 它判的是**临床**正确：把 `血红蛋白` 答成 HbA1c 的码算
  错，`血脂` 则必须解析不出结果。

```bash
pytest mirobody/tests/test_engine_coverage.py -s   # 离线，大约一秒
```

### 两个语义索引，以及哪一个是白送的

上面那个矩阵属于**可下载语料**那一层：LOINC 的行被嵌入一次，由你用自己的 embedding
模型建。部署里还有第二个索引，和它毫无关系，而且是白送的：应用内的指标搜索嵌入的
是**你自己的指标名**，不是 LOINC 语料。worker 的 `IndicatorSyncTask` 会在每次入库
时写 `th_series_dim.embedding_qwen3_8b`，查询就是拿它来匹配的。它需要
`mirobody worker` 在跑（`./deploy.sh` 会起），以及一个 embedding 供应商：可以是跑
聊天的那把 OpenAI 兼容 key，也可以是任何挂在 `/v1/embeddings` 后面、通过
`<PROVIDER>_BASE_URL` 指过去的自托管模型。这两个索引都不改变 `resolve()` 的答案。

### 切的是哪个 LOINC，覆盖了什么，没覆盖什么

随包发布的词表切自 **LOINC 2.83**，而且这件事由包在运行时自己说，不是写在一句会
过期的注释里：

```python
>>> import mirobody; mirobody.BUNDLE_VERSION
'loinc-2.83+2026.09.17-aacb2c715b56'
```

发行版本、切分日期，加一份对词表自身成员算出来的摘要，于是「构建期用的词表」和
「运行时 `pip` 钉住的那个」可以被断言为同一份语料，而光看包版本号你永远不知道这
一点。[LOINC 的许可](https://loinc.org/license/)要求每一份拷贝都带版本号；
`res/loinc/fhir_loinc_bundle.NOTICE` 带了，`scripts/stamp_bundle_version.py --check`
负责让这个戳保持诚实。

**这一刀切下了什么。** 2.83 的 99,737 个 ACTIVE 码里留 63,416 个，按规则切而不是
手挑（`translate_build/loinc_cut.py`）：CLASSTYPE 是实验室或临床；从来不承载读数的
CLASS 族丢掉（问卷、文档、放射、行政）；SCALE_TYP 为 `Doc`/`Nar`/`-`/`Set`/`Multi`
的丢掉；体格检查类只在 `Qn` 或 `Ord` 时留；面板留化验那几个子类，外加生命体征与个人
健康记录那几个。**只删行，不改行**——这是[许可](https://loinc.org/license/)第 3 条的
要求；153 行带着别人版权声明的，删掉而不是原样复制。

1.4.x 停在 2.82 的理由已经不存在了：当时轴表和语料来自不同来源，靠折叠后的
`LONG_COMMON_NAME` 接在一起，于是 2.83 改掉 2,842 个名字就断了 3,486 条链接。现在
两者从同一个发行版一趟切出，根本没有那个接缝。随之而来的两件事以前做不到——轴表现在
带 `TIME_ASPCT`，没有它点尿蛋白和 24 小时尿蛋白会共用一个序列键；也带 `CLASS`，于是
一个 10,045 行、需要手工重新生成的门禁文件可以退休了。

**实测，7,354 条真实报告写法。** 覆盖率仍是 0.963，错答率从 0.035 降到 0.025——口径
是「金标准码在切法内」的 6,780 条。另外 362 条期望的是叙述、文档或体检所见类的码，
或者是 2.83 自己退役的码：解析器现在对这些弃答而不是作答，这正是这一刀的目的，不是
回归。1.4.x 还答得出的 650 个 DISCOURAGED / DEPRECATED 码，随 `STATUS` 门一起没了。

**LOINC 覆盖的穿戴设备世界比大多数人以为的多。** 它不只有化验套餐：`BDYWGT.*` 管
身体成分（`101685-6` 骨量、`73964-9` 肌肉量、`101684-9` 体水分百分比），
`HRTRATE.*` 把静息心率（`40443-4`）和随手一测区分开，还有步数（`41950-7`）、睡眠
分期（`93831-6` 深睡、`93830-8` 浅睡）、HRV SDNN（`112429-6`）、最大摄氧量峰值和
爬升高度的码。它停在哪里：厂商的复合指标。Garmin 的 Body Battery 和压力分数没有
码，这是对的，因为那是一家公司的公式，不是一项测量。

**一个词表覆盖了什么，和我们在它上面的召回率，是两回事**，而这个差距是我们的，不是
LOINC 的：`Body bone mass` 在这里能解析到 `101685-6`，但中文的 `骨量` 会解析到一个
牙科体积的码，因为没有别名把它路由过去。
[`res/loinc/resolver_overrides.tsv`](../mirobody/res/loinc/resolver_overrides.tsv) 就是干这个用
的：人写下的一行，永远压过索引里的一次表层匹配。

→ [loinc.org](https://loinc.org/) · [许可](https://loinc.org/license/) ·
[发布说明](https://loinc.org/kb/)。下载免费，但需要注册账号，这也是为什么这里发布
的是派生词表，而不是源发行版。

→ [标准化](https://docs.mirobody.ai/zh/api-reference/standardization/) ·
[架构](https://docs.mirobody.ai/zh/concepts/architecture/) ·
[数据流](https://docs.mirobody.ai/zh/concepts/data-flow/)
