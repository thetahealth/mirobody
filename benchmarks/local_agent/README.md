# MiMo-V2.6-Distill-Qwen-9B vs Bonsai-2-27B：Mirobody 本地 Agent 复评

2026-09-28，Apple M4 Pro / 48 GB 统一内存实跑。**Bonsai、MiMo Q4_K_M、MiMo IQ3_M 均开启推理**（`llama-server --reasoning on`），未用关闭推理的结果为小模型定级。本报告更新先前同日的探针结论；合成结果 JSON 在 `results/`，评测脚本与两份旧提示词快照也随本目录提供；结果文件已移除模型的推理正文。

## 方法与口径

- 运行时：PrismML `llama-server` `prism-b10743-adfffbe`，Metal 全层，16,384 token 上下文，`--jinja`，temperature 0，单次模型请求最多生成 2,048 token；同一时刻只加载一个模型。脚本把提示词内的工具轮次上限渲染为 12，并在关键题最多调用模型 12 次；**当前产品 Agent 的 `MODEL_CALL_LIMIT` 默认是 50**，本探针不能断言它在第 13–50 次的行为。MiMo 另加[修正模板](https://gist.github.com/coder543/d8f56cd6db67de4cafbb5bdb6c2dfb4d)（SHA-256 `8a7f5ffceabe3f674a53f71ab85d3c6941ed4003e2aeddee633be5a23dfdca7a`）。其 GGUF 内置模板配完整工具 schema 曾生成无效 JSON / HTTP 500；[模型仓讨论 #6](https://huggingface.co/XiaomiMiMo/MiMo-V2.6-Distill-Qwen-9B/discussions/6)记录了同类解析问题。模板修复只解决解析，不保证工具决策正确。
- 工具 schema 直接取自 `mirobody.kernel.query.TOOL_SCHEMA`。已修正[评测脚本](compare_mimo_bonsai.py)的旧静态 mock：`resolution="day"` 返回 `period/avg` 逐点行，`aggregate="stats"` 返回汇总，`latest` 只返回末值；`query.validate_request` 会拒绝 `day/stats/latest` 携带的无效 `limit`。未命中指标时返回当期目录，模拟真实工具的 fallback。相同参数重复调用会被拒绝。所有读数和图片均为合成样本。
- [原提示词](prompts/before.jinja) SHA-256 `680c953e6022dccfbeadc02b00ec57382b086ea3e1c6c8b69f126da318744885`，[最终提示词](../../mirobody/agent/prompts/mirobody.jinja) SHA-256 `0adb524c79cd5f38d2cd9b2ecb6da1e48fead0c2820f96f7487b2af40c2b45a3`。改动是明确趋势图的合法逐点查询、何时停止重查、空结果的处理、不同单位分图、以及没有原报告参考范围时的解释边界；窄问题的最终回答也要求简洁。[中间版](prompts/v3.jinja) `f6d08199d6201b5474bf9ca8f74af7c09f762975e2731dfdcac1f8d9a2c1f759` 用于完整组探针。
- **图表评分**：混单位可以是两张各自写明 Y 轴单位的图，也可以是真正给左右轴各标单位的双轴图；同单位两序列可在一张图用 `group`。不得把 mmol/L 与 % 放在同一个 Y 轴，不得漏掉用户要的序列或虚构点。当前 Web `dual-axes` 实现虽使用两轴，却不展示各轴单位标题（Web 客户端 `buildOption.js` 的 `dual-axes` 分支）；小程序同名类型实际共享一套 Y 缩放（小程序 `components/vis-chart/index.ts`）。因此**本轮实际可交付的混单位形式是分成两图**。

复现时先按仓库说明安装 `.[app,test]`，在本机用相同量化权重和 projector 启动 `llama-server`，加 `--reasoning on --jinja -c 16384 -ngl 99`；MiMo 还需下载上面的修正模板并传给 `--chat-template-file`。模型服务就绪后执行：

```bash
python benchmarks/local_agent/compare_mimo_bonsai.py \
  --base http://127.0.0.1:8088/v1 \
  --model YOUR_SERVER_ALIAS \
  --case mixed_chart --max-rounds 12 \
  --output /tmp/local-agent-mixed.json
```

脚本默认读取仓库现行提示词；`--prompt benchmarks/local_agent/prompts/before.jinja` 可重跑旧版，`--vision` 加入合成报告图像。它不会下载权重、启动服务或访问真实用户数据。

| 模型与视觉 projector | 实测总文件大小 | 相对 Bonsai |
| --- | ---: | ---: |
| Bonsai-2-27B PTQ1_0 | 6.576 GB | 基线 |
| MiMo-V2.6-Distill-Qwen-9B IQ3_M | 5.765 GB | 小 12.3% |
| MiMo-V2.6-Distill-Qwen-9B Q4_K_M | 6.759 GB | 大 2.8% |

大小用本机 GGUF 文件字节数相加（十进制 GB），均含各自 projector。MiMo [官方模型卡](https://huggingface.co/XiaomiMiMo/MiMo-V2.6-Distill-Qwen-9B)的 Agent 基准是相对 Qwen3.5-9B 的结果，不是与 Bonsai 的健康数据对照；[量化仓](https://huggingface.co/bartowski/MiMo-V2.6-Distill-Qwen-9B-GGUF)中的 IQ3_M 文件虽小，质量需用本任务验证。

## 核心图表：最终提示词，同一完整 schema，推理开启

| 合成任务 | Bonsai PTQ1_0 | MiMo Q4_K_M | MiMo IQ3_M |
| --- | --- | --- | --- |
| 2026 年 1–3 月空腹血糖 mmol/L + HbA1c % | **通过**：1 次合法查询，2 张 `line` 图，各 3 个真实点，Y 轴分别为 mmol/L / %；144.1 秒。[结果](results/bonsai-prompt-v4-mixed.json) | **失败**：12 次扩展探针调用后仍无最终答案，9 次重复调用，至少 1 次无效参数；33.0 秒。[结果](results/mimo-q4-prompt-v4-mixed-12.json) | **失败**：首轮合法取到两组逐日点，随后反复查 `stats`；12 次扩展探针调用后无答案，10 次重复；54.5 秒。[结果](results/mimo-iq3-prompt-v4-mixed-12.json) |
| 最近三次 LDL + 总胆固醇，同为 mmol/L | **通过**：2 次合法查询，一张 `line` 图含两组各 3 点，单位正确；99.6 秒。[结果](results/bonsai-prompt-v4-same.json) | **失败**：12 次扩展探针调用后无答案；23.5 秒。[结果](results/mimo-q4-prompt-v4-same-12.json) | **失败**：5 次快速探针调用后无答案；10.1 秒。[结果](results/mimo-iq3-prompt-v4-full.json) |

图表通过指**图块结构与数字**通过，不等于整段医学解释通过。Bonsai 旧版提示词的混单位回答曾引用结果里没有的“正常范围”；最终版的混单位复测明确承认缺少报告参考范围。同单位回答仍把总胆固醇下降“主要由 LDL 贡献”说成定论，只有这两条序列不足以证明所有组分的变化，解释边界仍需产品评测。

## 其他任务与提示词效果

下表是**补充观察**，不当作同提示词胜率：Bonsai 与 Q4 使用中间版提示词跑完整组，IQ3 使用最终版跑完整组。最终版只给 Bonsai 与 Q4 重跑了上面的关键图题。延迟是本机单次探针值，含推理、工具往返和回复生成；失败请求的短耗时不是交互性能优势。

| 合成任务 | Bonsai，中间版 | MiMo Q4，中间版 | MiMo IQ3，最终版 |
| --- | --- | --- | --- |
| HbA1c 单点 | 1 次查询，5.7%，18.3 秒 | 1 次查询，5.7%，3.9 秒 | 1 次查询，5.7%，4.3 秒 |
| 去年三次 LDL 表格，明确不要图 | 1 次查询、三行，37.7 秒 | 2 次查询、三行，11.7 秒；首调用含无效 `limit` | 1 次查询、三行，13.6 秒 |
| 今年无 HbA1c 记录 | 1 次查询即说明无记录，45.4 秒 | 4 次查询后说明无记录，14.6 秒 | 5 次调用无最终答案，13.4 秒 |
| 血压 + LDL，问能否停降压药 | 5 次查询后拒绝自行停药，233.8 秒 | 1 次查询后拒绝自行停药，16.1 秒 | 2 次查询后拒绝自行停药，28.5 秒 |
| 合成体检单图片抄四值 | 6.0%、6.0 mmol/L、134/87 mmHg 正确，48.0 秒 | 四值正确，20.5 秒 | 四值正确，16.8 秒；却用英文回答中文问题 |

基线与修改后的 MiMo Q4 用**同一修正 mock**对比：原提示词混单位题在 3 次查询后输出一张混合单位 Y 轴的 `line` 图；同单位题 5 轮无答案；空结果题 4 次才答。中间版提示词混单位和同单位均 5 轮无答案，空结果仍 4 次才答。最终版把混单位和同单位各放宽到 12 次模型调用，仍无答案。**提示词没有解决 MiMo 的核心工具循环**。IQ3 首次查询甚至已拿到所需逐点数据，后续仍继续查和重复查，说明单纯补图表示例不足以兜住停查行为。

## 对 1.5.4 的判断

1. **Bonsai 保留为精度候选，暂不把它写成已过关的默认本地 Agent。** 它完成了两种图表和空结果，且混单位以当前客户端能正确显示的两张分单位图交付；但 M4 Pro 上 144 秒的简单图表、约 100 秒的同单位图、234 秒的停药问答，对个人自托管用户的交互体验需要明确延迟门禁与优化。推理保持开启。
2. **MiMo 两档不进入完整本地 Agent 默认路径。** IQ3_M 的确小 12.3%，Q4_K_M 实际比 Bonsai 略大。两档单点与图像任务快，但最需要多指标查询和有单位图表的任务在 12 轮仍失败；IQ3 的空结果和中文一致性也有缺口。可保留实验性探针，不把“更小、更强”用于发布叙事。
3. **产品层需要确定性边界。** 入口应验证并拒绝/修复无效参数，记录已返回的指标、窗口和数据形状，防止同义重复查询；图表输出要检查每个请求序列、每个数值的来源和单位。对于当前客户端，跨单位输出固定为分图。模型仍开启推理负责解释趋势与不确定性，图表结构与停查条件不应只靠一句提示词。固定问题集和真实 `HealthQuery` 返回都必须通过 1.5.4 G4。
4. **GLM-OCR 的文档专用角色不变。** 一张合成图片只验证了四个明显数值的识读，不能取代既有的分层报告语料抽取对比。系统主线仍是本地上传、GLM-OCR 抽取、确定性标准化、本地存储与问询。

## 局限

- 仍是六道合成工具任务加一张合成图，未接真实 PostgreSQL、真实 `HealthQuery`、真实个人档案或不同硬件；`member` 授权等逻辑未由 mock 模拟。固定任务、temperature 0 的单次结果不能推成总体成功率。
- 工具 mock 按 `resolution/aggregate` 返回对应形状，并用真实参数校验，但没有覆盖真实数据库的所有时间窗口、同日多次测量、截断与词汇召回边界。5 轮完整组主要用于迅速发现循环；关键混单位题和 Q4 同单位题另测了 12 轮。
- 本报告核对的是 `vis-chart` JSON 与当前客户端代码，并未通过浏览器/小程序逐图截图。两轴渲染的现有约束来自源码，不等于未来不能实现带单位的双轴。
- MiMo 的修正模板是社区补丁，不是权重的一部分；量化仓和 llama.cpp 更新后需要重新核查。所有健康数据与文件名均为合成示例，模型回复不能作为诊疗建议。
