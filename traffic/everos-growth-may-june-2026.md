# EverOS 2026 年 5—6 月涨星复盘

调研日期：2026-09-28。目的：解释 [EverMind-AI/EverOS](https://github.com/EverMind-AI/EverOS) 在 2026 年 5—6 月的增长，并为 Mirobody 制定可验证的推广实验。EverOS 早期名称为 EverMemOS。

## 一句话判断

5—6 月的增长更像**场景化产品叙事 + 多渠道开发者内容 + 榜单放大 + 再传播**组成的连续过程。5 月以 Claude Code 插件和 20 多个案例被看见；6 月以“本地 Markdown 记忆层”和开发者使用经验再次被介绍。公开资料无法证明任何单一文章、活动或版本贡献了多少 Star。

## Star 时间线

[GitHub 官方 Star History API](https://docs.github.com/en/rest/activity/starring#get-repository-star-history)的匿名每日计数，截至 2026-09-28 合计 **13,242** Star。该接口日界线不保证与 UTC 对齐；事件对照应容许约一天错位。

| 时段 | 新增 Star | 观察 |
| --- | ---: | --- |
| 2025 年 11 月 | 894 | EverMemOS 开源与首轮媒体传播。 |
| 2025 年 12 月 | 411 | 有传播，但不是累计增长的主因。 |
| 2026 年 5 月 | **2,368** | 两次短期高峰。 |
| 2026 年 6 月 | **3,320** | 月内后半段持续加速。 |
| **5—6 月合计** | **5,688** | 占截至调研日累计 Star 的约 **43.0%**。 |

| 涨星波段 | 新增 Star | 同期可核实的事件 | 归因强度 |
| --- | ---: | --- | --- |
| **5 月 19—20 日** | **429**，其中 20 日 290 | [GitHubDaily 的公众号文章](https://mp.weixin.qq.com/s/PUoTCGguCDC4MJGDaxE2Qg)于 5 月 20 日刊发；[可读转载](https://haogit.com/article/17)展示文章以 Claude Code “跨会话失忆”切入，具体介绍插件、一行安装、记忆面板和 20 多个案例。5 月 19 日已新增 139，说明热度先于当天文章。 | **较强的时间与内容匹配**；不能独占归因。 |
| **5 月 29—31 日** | **757**，其中 30 日 393，为该阶段最高单日 | [IT 咖啡馆 5 月 30 日视频](https://www.bilibili.com/video/BV1sQVe6kECA/)将 EverOS 列入“Github 一周热点”；[Trendshift](https://trendshift.io/repositories/25109)记录同日首次进入 Python 日榜第 4、全语言第 7；[Changelog Nightly](https://nightly.changelog.com/2026/05/30/email-day.html)亦收录。视频页面搜索快照显示近 10 万播放。 | **较强的同日匹配**；榜单可能既是涨星结果，也是后续曝光来源。 |
| **6 月 17—28 日** | **2,112**；6 月 23—28 日占 1,238 | 6 月 17 日[快速开始文档改为验证第一条真实记忆](https://github.com/EverMind-AI/EverOS/commit/9c7c9d73)；[公众号原发、后同步到腾讯云的介绍](https://developer.cloud.tencent.com/article/2697493?policyId=1003)标注原发于 6 月 18 日；[开发者的 Codex 实践分享](https://yichen.ai/writing/everos-memory-rebuild/)发表于 6 月 19 日；[1.1.0 版本](https://github.com/EverMind-AI/EverOS/releases/tag/v1.1.0)发布于 6 月 24 日，增加知识库与反思功能。 | **连续多事件吻合**；这波上涨在 1.1.0 前已开始，不能由单一版本解释。 |

对照：5 月 1—18 日新增 750，约每日 42；6 月 3—16 日新增 780，约每日 56。波段和基线的差距显示关注集中，但这些基线中也含其他推广活动，不能直接把差额计算成某渠道转化。日报告仅是 Star 创建计数，不能证明安装、活跃使用或商业价值。

## 为什么内容容易被反复传播

1. **把基础设施包装成读者已经遇到的具体问题。** [5 月 20 日文章](https://haogit.com/article/17)没有先讲复杂的记忆架构，而是从 Claude Code / Codex 忘记历史决策和 Bug 修复切入，再引出插件。读者可将“长期记忆”立即映射到自己的工作流。其文末明确引导关注 GitHub 和 Star。
2. **案例供应充足。** EverMind 的 [Memory Genesis Competition](https://evermind.ai/activities)在 2026 年 1—4 月募集了官方自报的 420 多名参与者、70 多个展示项目、20 个 Demo 和 8 万美元以上奖金。5 月[开发者渠道自荐](https://github.com/ruanyf/weekly/issues/10029)和公众号文章都反复强调“20+ 真实 Use Cases”。赛事与案例库为作者提供截图、故事和不同受众的切入口；但赛事本身不是 5—6 月涨星的直接证据。
3. **两种传播叙事相互补足。** 一种是“给 Claude Code 装记忆”，适合大众开发者；另一种是“Markdown 为权威数据、SQLite + LanceDB 做索引”，适合工程读者。[6 月 18 日原发的项目介绍](https://developer.cloud.tencent.com/article/2697493?policyId=1003)和 [6 月 19 日实践分享](https://yichen.ai/writing/everos-memory-rebuild/)展示了后一种叙事。统一仓库接住不同入口。
4. **技术可信度与传播资源共同起作用。** [4 月品牌升级及公测公告](https://www.prnewswire.com/news-releases/the-awakening-moment-for-agents-everos-brand-upgrade-and-public-beta-launches-the-era-of-self-evolving-memory-302741954.html)、[5 月 CEO 采访](https://www.pingwest.com/a/313462)、论文与评测材料提供技术故事及媒体素材。官网赛事页还列有专门的营销和 DevRel 负责人。这里能确认传播投入与内容供给，不能推算它们的单独转化率。
5. **榜单和 Star 数又被写回标题。** 5 月 30 日登榜后，后续内容以“8.3K Star”“8.7K Star”等作为社会证明；[6 月公众号标题及日期的聚合记录](https://post.smzdm.com/p/a5rn74zl/)可见这一点。这能提高点击意愿，也容易造成“已有热度 → 更多曝光 → 更多热度”的反馈环。聚合页并非微信原文，标题存在但阅读量与头条位置未核实。

一个重要的版本边界：当前主分支的[根提交](https://github.com/EverMind-AI/EverOS/commit/518b8eca855fcd9310afa7c254edf6c6a8f2a938)日期为 2026-06-05，父提交为空；但仓库在 2025-10 已创建，5 月已有 issue、宣传与 Star。故**不能拿今天的代码或 README 倒推 5 月时读者看到的产品**。6 月 3 日的 [1.0.0 发布](https://github.com/EverMind-AI/EverOS/releases/tag/v1.0.0)是新一轮产品化，不是这个 GitHub 仓库首次获得关注。

## 公众号线索与边界

- 2025 年最接近“12 月头条”记忆的文章是「深思圈」[《2025 AI 记忆系统大横评》微信原文](https://mp.weixin.qq.com/s/CvOirakfA-aWwWLDY3ikQw)：[EverMind 官网引用](https://evermind.ai/zh/blogs/top-ai-memory-systems-benchmarked-in-2026)将原发时间记为 **2025-11-28**；[品玩同题文章](https://www.pingwest.com/w/309492)发表于 **2025-12-01**。12 月 1 日新增 108 Star。尚无公众号后台记录证明该文占头条位或带来多少外链点击。
- 真正落在 2026 年 5—6 月增长窗口里的公众号线索包括 GitHubDaily 5 月 20 日的[文章](https://mp.weixin.qq.com/s/PUoTCGguCDC4MJGDaxE2Qg)和 cxuanAI 6 月 18 日的[原发文章转载](https://developer.cloud.tencent.com/article/2697493?policyId=1003)。6 月 24—25 日另有[聚合页列出的多篇公众号文章](https://post.smzdm.com/p/a5rn74zl/)，但原文、推送位置和阅读数据未独立核实。
- WeChat 链接在本次调研环境中无法直接读取。上述微信原文的存在、日期和内容分别依赖原发布方引用或可读转载。没有 EverOS 仓库的历史 referrer、公众号外链点击和文章阅读曲线，任何“某篇爆文带来 N Star”的说法都超出现有证据。

## 对 Mirobody 的推广建议

**2026-09-28 用户纠正后的主线：个人自托管用户优先，开发者其次。** 首轮社区反馈把 Mirobody 类比为“开源版蚂蚁阿福”，同时追问健康数据会不会外传。1.5.4 推出的是**一整套本地健康数据系统**：上传报告与设备数据、本地抽取、确定性标准化、本地问询、有出处的回答、完整档案导出与恢复。可感知利益是数据和模型运行由用户掌控，断网仍能完成这些任务，换部署也能带走规整结果。标准化是使“带走后仍可读、可比、可追溯”成立的内部机制，不适合作为文章第一句。

“全本地健康 Agent”本身并非空白：[HiMe](https://github.com/thinkwee/HiMe)已有 Apple Watch 本地同步和自托管 Agent，[OpenHealth](https://github.com/OpenHealthForAll/open-health)支持本地解析与 Ollama，[HealthLog](https://github.com/MBombeck/HealthLog)有自托管记录、可选本地模型及 FHIR API。[Open Wearables](https://github.com/the-momentum/open-wearables)提供统一可穿戴 API，但其 Garmin/Oura/WHOOP 等连接走厂商云 OAuth。[Fasten OnPrem](https://github.com/fastenhealth/fasten-onprem)能导入 FHIR Bundle，可作为独立接收方验证互操作性。因此 Mirobody 应证明一条**可复验的具体路径**，而非宣称自己是唯一的本地健康助手。

### 推荐的首发系统演示

先预下载并核验本地模型与镜像，随后断开公网。向 Mirobody 上传一份**合成的**体检报告图片或 PDF，由 1.5.4 计划选定的 **GLM-OCR-0.9B 在本地识别文档**，交给 ② Translate 确定性定码和单位，保存指标及原件引用。再把 Apple Watch 的测试数据从 iPhone Health 导出：演示前按 [Apple 的说明](https://www.apple.com/in/legal/privacy/data/en/health-app/)关闭 iCloud Health 同步，只采集这套测试设备的新记录；使用 Health App 的[官方 XML 导出](https://support.apple.com/guide/iphone/share-your-health-data-iph5ede58c3d/ios)，经 USB 等本地文件传输导入同一档案。由本地 **Bonsai-27B** Agent 回答“近一个月的心率、步数、睡眠记录各自有什么变化，报告里的指标又提供了什么独立信息”，逐项指向原始来源；检索需要的嵌入按计划使用本地 Qwen3-Embedding-0.6B。最后导出 1.5.3 的档案，在一台干净的 1.5.4 实例中恢复。这是**上传、标准化、问询、迁移的一条完整系统链路**；Apple Health 只是一个设备来源，且一次性导入不冒充实时同步，也不能证明设备历史数据过去从未上过云。模型选型的同机探针见[本地 Agent 复评](../benchmarks/local_agent/README.md)；公开演示以本节版本门禁及完整离线端到端验证通过为前提。

这个故事的可传播画面是**报告上传 → 本地 GLM-OCR → 标准化档案 ← Apple Health → 本地 Bonsai 问询 → 下载档案 → 换机器恢复**。汇总只陈述各指标的趋势与数据覆盖；不做“健康分数”、诊断，也不把睡眠、运动和某次化验的同向变化说成因果关系。设备指标不应为求整齐而误合并：本仓 [device crosswalk](../docs/device-crosswalk.md)已列出 HRV 的 SDNN/RMSSD、相对/绝对温度、睡眠阶段等不能直接混算的情况。未匹配的设备指标在导出中保留原名、来源及未映射状态。

### 外部开发者项目的顺序

| 优先级 | 项目 | 为什么适合 | 主要门槛 |
| --- | --- | --- | --- |
| **P0，1.5.4 发布门禁** | **Apple Health `export.zip` → 自托管档案入库**。Apple 官方支持[全量 XML 导出](https://support.apple.com/guide/iphone/share-your-health-data-iph5ede58c3d/ios)；本仓已有流式解码器，写入需接现有 `collect/observations.py`。 | 用户无需另购指定设备，Apple Watch 是更容易理解的首发例子；复用现成解码路径。 | 当前 CLI 只打印统计或写 JSONL，**不会写进实例**；真实 ZIP 大，须限流和逐批导入。仅对关闭 iCloud Health 后的新测试数据声明零厂商云路径。 |
| **P1，Android 社区扩展** | **Gadgetbridge ZIP → Mirobody 适配器**；先支持一个经实机验证的无厂商 App 配对设备。 | Gadgetbridge 核心[不持有 Android 网络权限](https://gadgetbridge.org/basics/topics/internet/)，其[ZIP 导出含 SQLite](https://gadgetbridge.org/internals/development/data-management/)，很适合严格自托管人群。GitHub 旧仓[已归档并迁往 Codeberg](https://github.com/Freeyourgadget/Gadgetbridge)，但官网[2026 年 8 月仍在发布 0.93.0](https://gadgetbridge.org/blog/category/releases/)。 | Android-only；需处理其导出数据库版本和具体设备能力。首发不以它为前置。 |
| **P1，开发者可信度** | **Mirobody 档案 → 新实例恢复 + FHIR 子集导入 Fasten OnPrem**。 | 把“规整后可迁移”变成独立软件也能读取的事实，不依赖标准化排行榜。 | 全档案恢复和跨产品 FHIR 导入是两项不同的验收；Fasten 不保证接收 Mirobody 的所有原件与内部元数据。 |
| **P2，第二个硬件故事** | 标准蓝牙血压计，如厂商文档确认实现 [Blood Pressure Profile 的 A&D UA-651BLE](https://www.aandd.co.jp/products/medical/hhc/hhc-humerus/ua651ble/)。 | 一次测量可现场进入本机，有强烈“设备没有上云”的可见性；[蓝牙 Blood Pressure Service](https://www.bluetooth.com/wp-content/uploads/Files/Specification/HTML/BLS_v1.1.1/out/en/index-en.html)有公开格式。 | 蓝牙兼容、配对和测量归属要逐型号验证；只产出少数指标，不如穿戴设备适合做综合概览。 |

### 1.5.4 的可判门禁

1. **完整本地系统。** 预下载模型和镜像后断网；上传合成报告，由 GLM-OCR 本地抽取、② Translate 标准化并保存，再由 Bonsai 本地问询并给出带原件出处的回答。Apple Watch 作为第二种输入：演示前关闭 iCloud Health 同步，只取随后产生的测试数据，经 Health App 导出 ZIP 并以本地文件路径进入同一实例。最后导出档案。网络抓包和模型调用轨迹验证 Mirobody 运行阶段外部请求为零，写明手机和电脑各自测试边界。不要用“100% 隐私”替代可测量的主张。
2. **入库可信。** 用户先明确选择档案归属；ZIP 只读取 `export.xml` 并限制压缩及解压大小。同一离线导出重复导入不增加重复读数；时间、单位、`sourceName`/设备信息与原始记录定位保留；不支持或无法安全对照的字段明示数量与原因。所有观察写入经过现有 `collect/observations.py`。
3. **档案可搬。** 用户确认 1.5.3 的全量规整指标导出会完成；1.5.4 在其基础上完成导入。全档案在干净实例恢复后，指标数、文件数、可打开原件数和来源关联与原机一致；FHIR 子集再用外部系统验证。当前本仓 1.5.2 工作树还不能作为自托管导出已交付的证据，发布时以实际分支门禁为准。
4. **本地模型可用。** GLM-OCR 的本地抽取质量、Bonsai 的实际工具问询和本地嵌入都要在端到端任务中测。[推理开启的同机复评](../benchmarks/local_agent/README.md)里，Bonsai 能用两张各标单位的图回答混单位趋势，但单题约 144 秒；较小的 MiMo IQ3_M 与 Q4_K_M 在 12 次扩展探针调用内仍没有交付这道图题。首次传播应展示完整系统实际交互延迟，不能只剪出最终成功画面。当前 OCR 只在两份样本测过、嵌入尚未实测、专用模型分发依赖 fork，都是公开演示前要关掉的风险。

**传播顺序：** 首篇用短视频或动图展示整套系统在断网后上传报告、由 GLM-OCR 本地抽取、标准化、由 Bonsai 本地问询、导出和换机恢复；Apple Health 是第二个输入镜头。第二篇由外部开发者独立复现系统或做设备适配；技术附录再解释 FHIR/LOINC、设备 crosswalk 和为什么有些指标不能乱比。用 Star 衡量发现度，用断网演示完成率、成功导入设备数据的人数、成功恢复档案的次数及外部集成反馈判断真实采用。所有公开素材只用合成数据或在用户明确同意后对真实数据脱敏。

### 当前 Mirobody 基线

2026-09-28 从本仓库有权限的 [GitHub Traffic API](https://docs.github.com/en/rest/metrics/traffic)读取过去 14 天：**374 unique visitors、446 unique cloners**；首页 295 unique visitors，中文 README 119 unique visitors；来源榜中的 GitHub 68、Google 31、知乎链接 11、mirobody.ai 11、X 的 `t.co` 8（均为该来源 unique visitors，来源之间可能重叠）。仓库约 **1,341 Star**。这些是后续实验的基线，不能与 EverOS 的 2026 年历史 referrer 比较，因为我们没有对方同期后台流量数据。

### 复核数据

`gh api -H 'X-GitHub-Api-Version: 2026-03-10' 'repos/EverMind-AI/EverOS/stargazers/history?per_page=30&page=1'` 与 `page=2`，逐周展开 `days` 后按日期求和。GitHub [接口说明](https://docs.github.com/en/rest/activity/starring#get-repository-star-history)明确只返回匿名计数；[Traffic API](https://docs.github.com/en/rest/metrics/traffic)仅保留最近 14 天。上述逐日与分段数字来自 2026-09-28 的查询，后续若取消 Star，历史计数可能略变。
