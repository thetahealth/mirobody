# 图片素材

**[English](README.md)** · **中文**

这个目录里的全部素材随仓库以 [Apache-2.0](../../LICENSE) 提供，可以直接用在文章、演讲和帖子里。

所有应用画面都来自 [`demo/`](../../demo/README.md) 里的两个合成演示账号：数值是生成的，没有任何一张图展示真人的记录。画面里出现的 LOINC 编码和检查名称来自 LOINC，术语许可见 [`LICENSE-3RD-PARTY`](../../LICENSE-3RD-PARTY)（英文）。

文件名里不带语言的是英文版，`.zh-CN` 是简体中文，`.ja` 是日文。每张表都写明了现有的语言版本：表里没有的版本就是还没有做，不是你的检出少了文件。

## 可以直接发帖的截图

静态图，1600 × 900（16:9），简体中文界面。五张页面截图是 1.6.1 版页面在浏览器里的原样，窗口宽 880 到 1056 px，再裁成 16:9，所以放进信息流、只有约 500 px 宽时，画面主体仍然看得清。它们来自用 `./deploy.sh` 启动的一套服务：首次设置页是在还没有配置模型时截的；其余几张是在填了一个模型 Key、并把 `demo/upload/` 里的四份 2026 年文件分别上传到对应账号之后截的（`you` 上传 `you_annual_checkup_2026-05.pdf` 和 `you_lipid_panel_2026-08.csv`，`mom` 上传 `mom_physical_2026-06.jpg` 和 `mom_clinic_visit_2026-07.xlsx`）。`you` 更早三年的三份文件是后来加的，不在这些画面里；把它们也传上去，`you` 的胆固醇记录会比画面里多。

| 文件 | 画面内容 | 对应 README 里的说法 | 语言 |
| --- | --- | --- | --- |
| [`ask-own-shot.zh-CN.png`](ask-own-shot.zh-CN.png) | 「询问」页对「我的胆固醇这几年是怎么变化的？」的回答：agent 把三份报告里的四项血脂画成一张趋势图，下面是表格，并点开了其中一个带虚线的数值。弹窗里是报告上的原始写法（`Total Cholesterol-TC`，4.45 mmol/L）、参考范围，以及读出这个数值的那份 PDF。 | 「不同医院，不同写法。放进同一条趋势，回看每个数值的来源。」 | 中文 |
| [`upload-shot.zh-CN.png`](upload-shot.zh-CN.png) | `you` 上传两份 2026 年文件之后，「指标」页里总胆固醇的抽屉。两份报告把这项检查写成 `Total Cholesterol-TC`，第三份写成 `Cholesterol, Total`；抽屉里是同一项指标：一个单位、一个 LOINC 编码（14647-2）、三条记录，以及趋势图的开头。 | 「② 标准化：……解析为同一个编码，单位统一到同一标准」和「跨医院比较报告。」 | 中文 |
| [`ask-circle-shot.zh-CN.png`](ask-circle-shot.zh-CN.png) | 「询问」页把「帮他问」切到妈妈的档案，登录的演示账号对这份档案只有查看权限：画面里是问题、回答的开头，以及她三份报告里总胆固醇的趋势图。 | 「照顾家人。邀请家人共享他们自己的档案，或替不登录的长辈代管一份档案。」 | 中文 |
| [`resolve-shot.zh-CN.png`](resolve-shot.zh-CN.png) | `uvx --python 3.12 mirobody resolve 血红蛋白 ヘモグロビン 血脂` 和它打印的结果，排成一个终端窗口。两种写法都得到 LOINC 718-7，「血脂」保持未解析。输出是这条命令（1.6.1 版）自己打印的，逐字未改、没有加颜色，按 90 列终端的方式折行。窗口下面的两行是说明文字，不是输出。 | 「`血红蛋白` 和 `ヘモグロビン` 都得到 `718-7`。`血脂` 保持未解析，因为它是一个类别，不是一项具体检查。」 | 说明文字是中文；输出本身不分语言 |
| [`setup-shot.zh-CN.png`](setup-shot.zh-CN.png) | 还没有配置模型的部署打开的首次设置页：用一个 API Key 接入模型服务，或者用 llama.cpp 100% 在本机运行、不需要 Key；两种选择在速度、隐私和硬件要求上各意味着什么。 | 「应用运行在哪里、模型运行在哪里，可以分别选择。」 | 中文 |
| [`journal-shot.zh-CN.png`](journal-shot.zh-CN.png) | 「数据 › 记录」里刚保存完 `昨晚开始头疼，血压150/95，没发烧，每天早晚吃二甲双胍500mg` 的样子：血压变成两条带编码的读数（8480-6 和 8462-4），头疼对上 ICPC-3 的 NS01，二甲双胍进了用药清单，「没发烧」没有被记下。保存之后输入框会清空，所以这句话本身不在画面里：发帖时请把它写在图片旁边。 | 「记健康日记。……『没发烧』不会被记成发烧。」 | 中文 |

## 动图

GIF 动图，宽 880 px（终端那一张是 916）。README 和各篇指南嵌入的就是它们；上面的截图是同一批页面的静态画面。

| 文件 | 画面内容 | 出现在哪里 | 语言 |
| --- | --- | --- | --- |
| [`ask-own-demo.gif`](ask-own-demo.gif)<br>[`ask-own-demo.zh-CN.gif`](ask-own-demo.zh-CN.gif)<br>[`ask-own-demo.ja.gif`](ask-own-demo.ja.gif) | 问胆固醇怎么变化：agent 实时显示每一步，把三份用不同写法记录同一项检查的报告画成一条趋势，点开数字下的虚线就能看到读数和它出自的文件。 | README 顶部，配的是「不同医院，不同写法。放进同一条趋势，回看每个数值的来源。」；完整演示 | English · 中文 · 日本語 |
| [`ask-circle-demo.gif`](ask-circle-demo.gif)<br>[`ask-circle-demo.zh-CN.gif`](ask-circle-demo.zh-CN.gif) | 同一个问题问到妈妈共享的档案上；agent 从一份提问者只能查看的档案里作答。 | 完整演示，README 的「照顾家人」一节会把读者带到这里 | English · 中文 |
| [`upload-demo.gif`](upload-demo.gif)<br>[`upload-demo.zh-CN.gif`](upload-demo.zh-CN.gif)<br>[`upload-demo.ja.gif`](upload-demo.ja.gif) | 在「数据」页上传化验单 PDF；处理完后，「指标」列表用通俗的名称列出每项检查，点开一项可以看到标本、测量类型和 LOINC 编码。 | 完整演示。对应「跨医院比较报告。」 | English · 中文 · 日本語 |
| [`journal-demo.gif`](journal-demo.gif)<br>[`journal-demo.zh-CN.gif`](journal-demo.zh-CN.gif) | 写一句话，头疼编码为 NS01，血压编码为 8480-6 和 8462-4，二甲双胍进入用药清单；「没发烧」不记录。 | 完整演示。对应「记健康日记。」 | English · 中文 |
| [`resolve-demo.gif`](resolve-demo.gif)<br>[`resolve-demo.zh-CN.gif`](resolve-demo.zh-CN.gif)<br>[`resolve-demo.ja.gif`](resolve-demo.ja.gif) | 终端里的 `mirobody resolve`：血红蛋白的中文和日文写法得到同一个 LOINC 编码，宽泛的类别保持未解析。 | README 的「一条命令体验标准化引擎」；快速开始 | English · 中文 · 日本語 |
| [`setup-demo.gif`](setup-demo.gif)<br>[`setup-demo.zh-CN.gif`](setup-demo.zh-CN.gif)<br>[`setup-demo.ja.gif`](setup-demo.ja.gif) | 先是 `./deploy.sh` 打印的内容，然后是首次设置页：在 OpenRouter 的 Key 旁边修改模型名；再选择 100% 在本机运行，页面找到 llama.cpp 服务，两个模型都已就绪。 | README 的「100% 本地部署」 | English · 中文 · 日本語 |

## 示意图

SVG，分别为浅色和深色页面绘制。关爱圈示意图另有一套给手机看的竖版，以及给不支持 SVG 的地方用的 PNG（1840 × 1272）。

| 文件 | 画面内容 | 出现在哪里 | 语言 |
| --- | --- | --- | --- |
| [`collect-standardize-agent.svg`](collect-standardize-agent.svg)<br>[`collect-standardize-agent.zh-CN.svg`](collect-standardize-agent.zh-CN.svg)<br>[`collect-standardize-agent.ja.svg`](collect-standardize-agent.ja.svg)<br>深色：[`collect-standardize-agent-dark.svg`](collect-standardize-agent-dark.svg)<br>[`collect-standardize-agent-dark.zh-CN.svg`](collect-standardize-agent-dark.zh-CN.svg)<br>[`collect-standardize-agent-dark.ja.svg`](collect-standardize-agent-dark.ja.svg) | 三个阶段：收集报告、设备数据和基因数据；统一名称与单位，让不同来源可比较；在整份记录上提问，每个回答都能追溯来源。 | README 的「收集 · 标准化 · 智能体」；标准化和流程两篇指南 | English · 中文 · 日本語 |
| [`care-circle-sharing.svg`](care-circle-sharing.svg)<br>[`care-circle-sharing.zh-CN.svg`](care-circle-sharing.zh-CN.svg)<br>[`care-circle-sharing.ja.svg`](care-circle-sharing.ja.svg)<br>深色：[`care-circle-sharing-dark.svg`](care-circle-sharing-dark.svg)<br>[`care-circle-sharing-dark.zh-CN.svg`](care-circle-sharing-dark.zh-CN.svg)<br>[`care-circle-sharing-dark.ja.svg`](care-circle-sharing-dark.ja.svg) | 邀请家人，由本人决定是否允许查看或编辑自己的档案，再查询获准的档案；不自行登录的家人可先由你代管，接管后由本人重新决定你的权限。 | README 的「照顾家人」，配的是「加入关爱圈本身不会共享任何健康数据。」；完整演示 | English · 中文 · 日本語 |
| [`care-circle-sharing-mobile.svg`](care-circle-sharing-mobile.svg)<br>[`care-circle-sharing-mobile.zh-CN.svg`](care-circle-sharing-mobile.zh-CN.svg)<br>[`care-circle-sharing-mobile.ja.svg`](care-circle-sharing-mobile.ja.svg)<br>深色：[`care-circle-sharing-mobile-dark.svg`](care-circle-sharing-mobile-dark.svg)<br>[`care-circle-sharing-mobile-dark.zh-CN.svg`](care-circle-sharing-mobile-dark.zh-CN.svg)<br>[`care-circle-sharing-mobile-dark.ja.svg`](care-circle-sharing-mobile-dark.ja.svg) | 同一张关爱圈示意图的竖版。 | 同上，页面宽度小于 640 px 时显示 | English · 中文 · 日本語 |
| [`care-circle-sharing.png`](care-circle-sharing.png)<br>[`care-circle-sharing.zh-CN.png`](care-circle-sharing.zh-CN.png)<br>深色：[`care-circle-sharing-dark.png`](care-circle-sharing-dark.png)<br>[`care-circle-sharing-dark.zh-CN.png`](care-circle-sharing-dark.zh-CN.png) | 横版关爱圈示意图的 PNG。 | 本仓库没有页面嵌入它 | English · 中文 |

## 标志和社交卡片

| 文件 | 画面内容 | 出现在哪里 | 语言 |
| --- | --- | --- | --- |
| [`mirobody-icon.svg`](mirobody-icon.svg)<br>深色：[`mirobody-icon-dark.svg`](mirobody-icon-dark.svg) | Mirobody 的标志，浅色页面和深色页面各一个。 | 每一版 README 的顶部 | 不含文字 |
| [`social-preview.png`](social-preview.png) | 仓库链接被分享时展开的卡片（2560 × 1280）：一句话介绍、两种运行方式、「询问」页的一帧画面，以及血红蛋白的四种写法解析到 718-7。卡片上还印着一个基准得分，那是渲染卡片当天 README 里的数字。 | 仓库的社交预览图 | English |
| [`social-preview.html`](social-preview.html)<br>[`social-preview-frame.png`](social-preview-frame.png) | 渲染这张卡片的页面，以及它嵌入的那一帧 `ask-own-demo.gif` 画面。 | 仅作为源文件 | English |
