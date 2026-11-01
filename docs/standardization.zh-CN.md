# 标准化详解

**[English](standardization.md)** · **中文**

README 里 **② 标准化 Standardize** 那一阶段的长版本：报告上印的一行读数，
怎样一步步、一个模块一个模块地变成有编码、可比较的读数；随包发布的词表到底是什么、
它刻意不做什么、切自哪个 LOINC 版本。面向读者的简版是 [site/standardization.zh-CN.md](site/standardization.zh-CN.md)。词表相关的数字出自 `res/loinc/fhir_loinc_bundle.NOTICE`，由词表版本戳守着。

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/collect-standardize-agent-dark.zh-CN.svg">
  <source media="(prefers-color-scheme: light)" srcset="images/collect-standardize-agent.zh-CN.svg">
  <img src="images/collect-standardize-agent.zh-CN.svg" alt="收集、标准化、智能体：三个阶段，从左到右" width="920">
</picture>
</p>

## 从报告上的一行，到一条可比较的读数

不管从哪里来，每条读数都经过同一个写入口 `collect/observations.py`（`ingest()`），
在一个事务里完成。它调用的各步在 `mirobody/standardize/`、`mirobody/engine/` 和 `mirobody/units/` 里，不碰数据库、时钟和模型，所以同样的输入在任何机器上得到同样的编码。
下面用同一项检查的三份报告做例子：`LDL-C 2.48 mmol/L`、`低密度脂蛋白胆固醇 96 mg/dL`、
`LDL Cholesterol 101 mg/dL`。

### 0. 读数从哪里来

| 来源 | 模块 | 哪里用到模型 |
| --- | --- | --- |
| 化验单、照片、表格 | `collect/files/services/indicator_extractor.py`：`table_indicators.py` 的表格规则先读所有像读数的行（按表头文字对列，从不按位置）；规则读不了的才交给模型。`indicator_store.py` 把读到的行交给 `ingest()` | 读页面。提示词（`prompts/file_indicator_extract.py`）要求名称照报告原样抄、不翻译，因为第 5 步就拿它去查。从不让模型给编码 |
| 穿戴设备、Apple Health | `kernel/decoders/` 把每个厂商字段换成 `kernel/metrics.py` 规定的存储单位；从厂商数据到入库、再到按天汇总和选源的完整路线，见 [pipeline.md](pipeline.md) 第 1–8 段（英文） | 不用 |
| 健康日记里的一句话 | `collect/sentence.py` 把 `头疼，血压150/95` 拆成一条主诉和两条读数；每一段都必须引用原句，否定、假设和说别人的话都不写入 | 拆句和判断类型。编码来自第 5 步 |
| HTTP API | 按报告原样传名称、数值和单位 | 不用 |

### 1. 原样保留（`observations.prepare`）

`name_text`、`value_text`、`unit_text`、`ref_text`、`flag_text` 按原样存。推导出的列都放在旁边，
不替换原文。`th_observation` 只追加不修改：更正是新写一行，指向被更正的那一行。

### 2. 折叠名称（`standardize/fold.py`，`name_key`）

NFKC、统一大小写、拉丁词之间留一个空格、汉字旁不留空格、去掉末尾标点：
`白细胞 计数` → `白细胞计数`，`ＨＤＬ－Ｃ` → `hdl-c`。它不翻译、不去标本词、不展开缩写，
这些都算编码决定，另行记录。所有写入和读取都用这一个函数：多一个差一条规则的折叠，
同一个序列就会被拆成两条。

### 3. 判断数值类型（`standardize/parse.py`，`parse_value`）

数值分为 `quantity`（一定带数字）、`ordinal`（`++`）、`nominal`（`阴性`）、`narrative` 和 `absent`。比较符单独存（`<0.5` 记为 0.5 和 `<`），单位由 `mirobody.units` 规范成 UCUM，
写在数值格里的单位也算报告印的单位。`120/80` 仍是 narrative：它是两条读数，应由抽取拆开，
不能当成一个 120。

### 4. 拦下不可能的值（`kernel/quality.py`）

`time_gate` 拒收没有开始时间、结束早于开始、时间跨度长得不合理、或者比现在晚一天以上的读数；
`value_gate` 拒收非有限数，以及超出 0–100 的百分比。被拒的读数在 `Report` 里按原因计数。
不补默认值：没有时间就是没有时间，绝不记成今天。

### 5. 编码（`standardize/code.py`，`code`）

`observations.coding_for` 按类型选词表：主诉和诊断走 ICPC-3（`standardize/icpc3.py`），
笔记从不编码，测量交给 `code()`，按这个先后顺序决定：

1. **已确认的别名**（`th_coding_alias`），本人的优先于全局的。别名定下是哪项检查；
   报告印的单位仍可以在这项检查的质量浓度和摩尔浓度编码之间切换。
2. **设备目录**，只用于设备批次。目录里的厂商字段名是普通词（那里的 `cholesterol` 指饮食摄入），
   所以用户自己输入的名称从不查它。
3. **词法解析器** `engine.resolve_reading`，只接受 `lexical` 方法给出的结果。

解析器内部（`engine/resolver.py`）：

- **类别名称拒绝编码。** 在 `resolver_overrides.tsv` 里目标是 `!unresolved` 的词（`血脂`、`lipid panel`）直接拒绝；所有表面变体都要检查，`lipid_panel` 绕不过去。
- **候选键，越具体越先试。** 先查别名表，再查索引：`resolver_overrides.tsv`、
  `aliases_src/zh_curated.tsv` 和 `zh.tsv` 是「这个词就是这个概念」的明确陈述，
  而索引命中只是所有表面字符串相同的 LOINC 行。然后试表面变体（全角、各种横线、
  `snake_case`，见 `lexical.surface_variants`）。末尾的缩写只在它重复主体时才去掉（`总胆固醇 TC`）；它补充了标本或限定时保留（`Protein CSF`、`Glucose OGTT`、`胆固醇 HDL`），这几个词因此解析不出，而不是悄悄答成去掉限定词后那项检查的编码。
  量词（`计数`、`count`）最后才去，所以只会把「查不到」变成「查到」。
- **`名称(缩写)` 两半必须一致。** 两半分别解析。同一项检查：用主体（`空腹血糖(GLU)` → 1558-6）。
  两项不同的检查：拒绝，并写明两个编码，留给人选（`血糖(HbA1c)` 分别是 2339-0 和 4548-4）。
- **在候选里挑一个**（`_pick`）：参考 LOINC 的 `COMMON_TEST_RANK`，普通血清、血浆、全血优先于脐血、毛细血管血，`COMPONENT` 完全一致的优先，但不能把人身上的测量换成器官或设备的测量（`weight` 不能变成结石重量）。
- **由读数挑具体编码**（`variant_for_reading`）。LOINC 把 PROPERTY 和 SCALE 写进了编码，
  所以单位所属的性质族、数值的类型，决定选同一 `COMPONENT`、同一标本下的哪个兄弟编码，
  没有方法限定的优先。结果有这几种：

| 结果 | 例子 | 编码 |
| --- | --- | --- |
| `agreed` | `LDL Cholesterol 101 mg/dL` | 13457-7，和名称给出的一致 |
| `switched` | `LDL-C 2.48 mmol/L` | 22748-8（摩尔）；只看名称是 13457-7 |
| `switched` | `尿糖 阴性` | 2349-9（定性，`Ord`）；只看名称是 2350-7（`Qn`） |
| `unit-unrecognized` | `glucose 5.6 mmol/xyz` | 用名称给出的编码，并标明单位不认识 |
| `axis-conflict` | `albumin 30 mg/24h` | 不编码：这个单位只在尿液里对得上白蛋白，单位只能选性质，不能换标本 |

回到 `code()`，还有两道关：数字不会被归到定性编码下；一项按单位区分编码的检查，
没有可用单位的数字不会被编码（`FPG 4.76` 要等 mg/dL 或 mmol/L）。

每条读数最后只有一个结果，写进 `th_coding_current`、`th_coding_history` 和 `th_coding_decision`，存在原始行旁边：

| 结果 | 记录了什么 |
| --- | --- |
| `coded` | 编码、所属序列、词表版本、规则，以及佐证（`name`、`property`、`scale`、`alias`） |
| `needs-input` | `engine:no-match`、`name:ambiguous`、`unit:conflict`、`unit:missing`、`unit:unrecognized`、`scale:<类型>-vs-<量表>` |
| `refused` | `engine:refused`（类别名称）、`alias:not-standard`、笔记 |

`decision_id` 是折叠后的名称、单位、数值类型、词表版本和规则的哈希：折叠结果相同的读数共用一个决定，
换新词表后的重新编码，就是在冻结的抽取结果上重放一遍 `code()`。

不连数据库时，库函数给出同样的决定，形式是 FHIR R4 Observation：报告原名留在 `code.text`，
只有解析器给出编码时才有 `coding`，扩展里记着编码是怎么得来的（`engine/observation.py`，`standardize_reading("LDL-C", "2.48", "mmol/L")`）：

```json
{
  "resourceType": "Observation",
  "status": "final",
  "code": {
    "text": "LDL-C",
    "coding": [{"system": "http://loinc.org", "code": "22748-8",
                "display": "Cholesterol in LDL [Moles/volume] in Serum or Plasma"}]
  },
  "valueQuantity": {"value": 2.48, "unit": "mmol/L",
                    "system": "http://unitsofmeasure.org", "code": "mmol/L"},
  "extension": [{
    "url": "https://mirobody.ai/fhir/StructureDefinition/coding-decision",
    "extension": [
      {"url": "method", "valueString": "lexical"},
      {"url": "evidence", "valueString": "name"},
      {"url": "evidence", "valueString": "property"},
      {"url": "evidence", "valueString": "scale"},
      {"url": "property", "valueString": "SCnc"},
      {"url": "scale", "valueString": "Qn"},
      {"url": "system", "valueString": "Ser/Plas"},
      {"url": "unitRecognized", "valueBoolean": true},
      {"url": "bundle", "valueString": "loinc-2.83+2026.10.27-126ecba69782"},
      {"url": "rejectedCode", "valueString": "13457-7"},
      {"url": "rejectedReason", "valueString": "the name alone gives 13457-7 (MCnc/Qn)"}
    ]
  }]
}
```

### 6. 归入序列（`standardize/series.py`，`units.canonicalize`）

一个 LOINC 编码比「能画在一条线上」的范围更窄，所以序列键折叠了两条轴：

    series_id = loinc:COMPONENT|SYSTEM|TIME|SCALE|dim(PROPERTY)

METHOD 合并，质量浓度和摩尔浓度折叠成同一个量纲，这和 LOINC 自己的分组（LG55-6、LG100-4）
是同一条规则。22748-8 和 13457-7 都是 `loinc:cholesterol.in ldl|Ser/Plas|Pt|Qn|QCnc`。
数值通过存在原值旁边的一对规范值对齐，摩尔质量按 LOINC 编码取：2.48 mmol/L 是 0.00248 mol/L，
96 mg/dL 是 0.002483 mol/L，101 mg/dL 是 0.002612 mol/L。没编码的读数也有序列，
`local:<name_key>|<单位>`，不会和别的名称合并。

### 7. 读出来（`collect/query.py`）

目录列的是序列，不是报告上的原名。统计时，序列只有一种单位就用原值算；所有读数有同一个规范单位就用规范值算；
否则不给统计值（白蛋白的 g/L 和 umol/L 没有摩尔质量可以换算）。原始行同时带着两种值。
回答会引用它所依据的行（[answers.md](answers.md)（英文），`kernel/citations.py`）。

### 8. 改主意

`observations.confirm_alias`（接口是 `POST /api/v1/health-indicators/confirm-code`，
见 [file-processing.md](file-processing.md)（英文））记下一个报告名称的含义，并重新编码所有带这个名称的行。
升级后运行 `mirobody recode`，会对已存的读数重放第 5 步。无论哪种，原始行都不改，
历史表记下旧编码和原因。

## 这一层提供什么

这里说的标准化不是一张查找表，而是一套完整的术语归一系统：

- **63,416 个码**，由一条规则一趟从 LOINC 2.83 切出：实验室与临床观察，
  叙述与文档量表丢掉，面板只留化验子类和生命体征子类。
  **1,023,273 条折叠过的称谓**指向它们，来自组分、长名与短名、相关名、显示名和大众名，以及发行版自带的 21 份语言变体（`res/loinc/fhir_loinc_bundle.NOTICE`）。
- **英文为主，中文在旁。** 22,578 条中文别名，加上本项目自己人工收录的那些，
  排在索引前面，所以 `hemoglobin`、`血红蛋白`、`血紅素` 都落在 LOINC 718-7 上。
  其他语言走 LOINC 自己发布的那 21 份语言变体（它们本来就是别名索引的输入），
  而不是我们自己的文件：日文那份是 UMLS 派生的，
  另外五份机器派生的（de·es·fr·ko·ru）追溯不到它们声称的语言变体。
  两者都已删除，见 `LICENSE-3RD-PARTY`。
- **繁體中文 是两个问题，也就当两个问题处理。** 字形折叠是机械的（随包发布一张 3,336 字的 zh-Hant → zh-Hans 对照表）；用词不是：台湾的习惯用词不一样，`血紅素`
  折叠过去会撞上 HbA1c 的码。这类词按繁体写法单独收录，而人工收录的那一行永远压过折叠的结果。
- **主诉和诊断落在 ICPC-3 上。** `resolve_symptom("头疼")` 得到 `NS01`，
  `resolve_condition("高血压")` 得到 `KD73`：ICPC-3 的 S、D 两个组件共 1,218 个码原样随包，
  英文、中文、日语、俄语说法是我们自己写的别名（繁體中文走同一套字形折叠，台湾用词如「氣喘」按繁体原文单独收录）。
  两套索引分开，主诉不会落到诊断上；拿不准就弃答并给原因，不猜。
  在 200 条真实问诊里的中英文主诉上测过，金标准由三个互相独立的模型标注（三种不同的模型，看不到解析器）：
  可计分的 142 条症状里没有一个错码，编上码的约五分之一，其余弃答。日语和俄语的说法是新加的，还没有做过同样的测评。
- **单位**按 330 个 UCUM 单位归一到 59 个 PROPERTY 家族，带量纲分析，
  有一座按 LOINC 码索引的摩尔质量桥，并且对 `%` 和 `10*9/L` 这种情况明确拒绝混算。
  902 个设备与实验室指标目录条目。
- **这里全是词法的，弃答是我们要守住的天花板。** 词表不认识的词返回 `unresolved`，
  不返回最近邻。1.4.x 在旁边挂过一层默认关闭的余弦召回，1.5.0 把它删了。它**不会弃答**：碰到从没见过的词，会用「答对了」的那种置信度把最近邻交给你——在 LOINC
  矩阵上实测，胡话能拿 0.78，而真实的指标名低到 0.56，两个区间重叠，没有阈值能把它们分开。它也从来没跑起来过：矩阵是 108,248 行 × 1024 维、只对某一对（供应商，
  模型）有效，而且从未发布，所以 `get_index()` 在 wheel 安装态和源码树里**都返回 None**（两边都实测过）。一个没人能打开的开关，挡在一个我们本来也不敢信的答案前面。
  想要更高的召回，诚实的杠杆是在 `res/loinc/resolver_overrides.tsv` 里加一行人工词条。
- **我们量这个说法，而不是断言它。**
  [`test_engine_coverage.py`](../mirobody/tests/test_engine_coverage.py) 拿常规体检会出现的那些套餐去考离线解析器，词按报告上真正的印法写，覆盖英文、简体中文、
  繁體中文、日本語、俄文、爱沙尼亚文和土耳其文，外加平台 API 教给我们的那套穿戴设备词汇。
  **今天是 345/345；刚写出来那天是 32/94。**
  它判的是**临床**正确：把 `血红蛋白` 答成 HbA1c 的码算错，`血脂` 则必须解析不出结果。

```bash
pytest mirobody/tests/test_engine_coverage.py -s   # 离线，大约一秒
```

### 运行时不做向量检索

`mirobody.bundle` 把词表开放给构建期工具，你愿意的话可以用自己的模型把 LOINC 各行做成向量、
建一个自己的召回索引，但 `resolve()` 和应用都不读它。问到某人自己的读数时走 `query_health_indicators`：对这个人自己的序列（报告上印的名字，以及写入时存在每条读数旁的 LOINC 显示名和编码）做词法排序，找不到再退回离线解析器给出的编码。整条路径都不需要 embedding 供应商。

### 切的是哪个 LOINC，覆盖了什么，没覆盖什么

随包发布的词表切自 **LOINC 2.83**，而且这件事由包在运行时自己说，不是写在一句会过期的注释里：

```python
>>> import mirobody; mirobody.BUNDLE_VERSION
'loinc-2.83+2026.10.27-126ecba69782'
```

发行版本、切分日期，加一份对词表自身成员算出来的摘要，于是「构建期用的词表」和「运行时 `pip` 钉住的那个」可以被断言为同一份语料，而光看包版本号你永远不知道这一点。[LOINC 的许可](https://loinc.org/license/)要求每一份拷贝都带版本号；
`res/loinc/fhir_loinc_bundle.NOTICE` 带了，`scripts/stamp_bundle_version.py --check`
负责让这个戳保持诚实。

**这一刀切下了什么。** 2.83 的 99,737 个 ACTIVE 码里留 63,416 个，按规则切而不是手挑（`standardize_build/loinc_cut.py`）：CLASSTYPE 是实验室或临床；从来不承载读数的 CLASS 族丢掉（问卷、文档、放射、行政）；SCALE_TYP 为 `Doc`/`Nar`/`-`/`Set`/`Multi`
的丢掉；体格检查类只在 `Qn` 或 `Ord` 时留；面板留化验那几个子类，外加生命体征与个人健康记录那几个。**只删行，不改行**——这是[许可](https://loinc.org/license/)第 3 条的要求；153 行带着别人版权声明的，删掉而不是原样复制。

1.4.x 停在 2.82 的理由已经不存在了：当时轴表和语料来自不同来源，靠折叠后的 `LONG_COMMON_NAME` 接在一起，于是 2.83 改掉 2,842 个名字就断了 3,486 条链接。现在两者从同一个发行版一趟切出，根本没有那个接缝。随之而来的两件事以前做不到——轴表现在带 `TIME_ASPCT`，没有它点尿蛋白和 24 小时尿蛋白会共用一个序列键；也带 `CLASS`，于是一个 10,045 行、需要手工重新生成的门禁文件可以退休了。

**实测，7,354 条真实报告写法。** 在「金标准码在切法内」的 6,992 条上，
覆盖率 0.951，错答率 0.025。另外 362 条里，
330 条期望的是切法丢掉的码（叙述、文档或体检所见类），
24 条期望的是 2.83 自己退役的码，8 条本就不该有码：
解析器对其中 273 条弃答而不是作答，这正是这一刀的目的，不是回归。
1.4.x 还答得出的 DISCOURAGED / DEPRECATED 码，随 `STATUS` 门一起没了。

**LOINC 覆盖的穿戴设备世界比大多数人以为的多。** 它不只有化验套餐：`BDYWGT.*` 管身体成分（`101685-6` 骨量、`73964-9` 肌肉量、`101684-9` 体水分百分比），
`HRTRATE.*` 把静息心率（`40443-4`）和随手一测区分开，还有步数（`41950-7`）、睡眠分期（`93831-6` 深睡、`93830-8` 浅睡）、HRV SDNN（`112429-6`）、最大摄氧量峰值和爬升高度的码。它停在哪里：厂商的复合指标。Garmin 的 Body Battery 和压力分数没有码，这是对的，因为那是一家公司的公式，不是一项测量。

**一个词表覆盖了什么，和我们在它上面的召回率，是两回事**，而这个差距是我们的，不是 LOINC 的：`Body bone mass` 在这里能解析到 `101685-6`，但中文的 `骨量` 解析不出结果，
因为没有别名把它路由过去。
[`res/loinc/resolver_overrides.tsv`](../mirobody/res/loinc/resolver_overrides.tsv) 就是干这个用的：人写下的一行，永远压过索引里的一次表层匹配。

→ [loinc.org](https://loinc.org/) · [许可](https://loinc.org/license/) ·
[发布说明](https://loinc.org/kb/)。下载免费，但需要注册账号，这也是为什么这里发布的是派生词表，而不是源发行版。

→ [数据管线](pipeline.md) · [操作演示](walkthrough.md) ·
[演示数据](../demo/README.md)
