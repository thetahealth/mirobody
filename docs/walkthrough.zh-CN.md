# 四分钟走完整条引擎

**[English](walkthrough.md)** · **中文**

本页用示例数据走完五个场景：进入系统、上传文件、查看编码后的读数、跨报告提问，以及写一句健康日记。
第 2 到第 5 段对着一套运行中的 `./deploy.sh`、在 `SEED_DEMO_DATA` 打开时录制；
第 1 段是示意图，图示背后的授权规则列在图下方。

`SEED_DEMO_DATA` 默认打开，所以 `./deploy.sh` 一结束，① → ② → ③ 这条链就能走通。
登录和浏览种子记录不需要模型；读取文件和回答问题，使用的是设置页里选定的模型：一把 Key，或[本地模型](local-models.zh-CN.md)。

**1 · 进门。** 你用 `you@mirobody.ai` 登录，看到的是两份记录，不是一份。
你自己的：一年的自测体征，加去年 11 月的一份化验单。
`mom@mirobody.ai` 的结构相同，但那是另一个人，以只读方式把记录共享给你，而她的体重在上升、睡眠偏短、HbA1c 已经超出范围。
同一个问题，两个答案，而两份记录里只有一份是你的。隔离是看得见的，不只是写在文档里。

<p align="center">
  <picture>
    <source media="(max-width: 640px) and (prefers-color-scheme: dark)" srcset="images/care-circle-sharing-mobile-dark.zh-CN.svg">
    <source media="(max-width: 640px)" srcset="images/care-circle-sharing-mobile.zh-CN.svg">
    <source media="(prefers-color-scheme: dark)" srcset="images/care-circle-sharing-dark.zh-CN.svg">
    <source media="(prefers-color-scheme: light)" srcset="images/care-circle-sharing.zh-CN.svg">
    <img src="images/care-circle-sharing.zh-CN.svg" alt="邀请家人，由本人决定是否允许查看或编辑自己的档案，再查询获准的档案；不自行登录的家人可先由你代管，接管后由本人重新决定你的权限" width="920">
  </picture>
</p>

<a id="the-four-promises"></a>

| 关爱圈的四条承诺 | 由什么保证 |
| --- | --- |
| 必须本人接受才算加入 | `status`，待处理不等于已接受 |
| 健康数据默认不开，除非你允许 | `health_access`，`NOT NULL DEFAULT 0` |
| 这是**你的**开关，在**你自己的**那一行上 | 它管的是你的记录，不是对方的 |
| 每个成员各管各的 | 任何别人的操作都无法提高你的权限 |

这四条每一条都由测试作为安全属性固定下来。

这个开关是一个字段，不是一句承诺：`care_circle_members.health_access`，`NOT NULL DEFAULT 0`，在**你自己的**那一行上。
被邀请进一个关爱圈本身不共享任何内容，由成员自己决定，别人的任何操作都无法提高它。
读取它的检查在拒绝时抛出异常，而不是返回一个假值，所以一条忘了检查的路由返回 403，而不是把记录交出去。
[`examples/06_care_circle_rules.py`](../examples/06_care_circle_rules.py) 能离线打印整张决策表。

只有一种成员例外：在「关爱圈」页面里添加的家人，比如不打算自己登录的父母。
TA 的邮箱是一个谁也无法登录的占位地址，所以这个开关由你代管，可读可写。
等 TA 想要自己的账号时，在 TA 的卡片上把邀请链接发给 TA。
TA 证明真实邮箱属于自己（向这个邮箱发送的验证码；部署不发邮件时则设置密码）后，这个账户和其中的全部数据就归 TA 所有，并由 TA 决定你还保留哪种权限：查看和编辑、仅查看，或者不再查看。
如果这个邮箱已经有账户，记录会合并进去。

**2 · ① 收集。** [`demo/upload/`](../demo/) 里放着四份种子数据有意没有写入的文件，所以上传一份不是空操作。
它们是四种不同的格式，因为健康档案本来就以化验所、诊所和家里各自产出的样子到来：

| 文件 | 格式 | 是什么 | 指标数 |
| --- | --- | --- | --- |
| `you_annual_checkup_2026-05.pdf` | PDF | 今年的体检套餐，诊所打印的 | 9 |
| `mom_physical_2026-06.jpg` | JPG | 一张打印单子的手机照片 | 9 |
| `you_lipid_panel_2026-08.csv` | CSV | 另一家化验所的导出，用它自己的写法 | 5 |
| `mom_clinic_visit_2026-07.xlsx` | XLSX | 诊所录入表格的内容 | 4 |

把一份文件拖到「数据」页，文件先被原样保存，可以追溯。
① 收集只做这一件事，而这个切分很重要：化验所写了什么是一个事实，它代表什么是另一个。

<p align="center">
  <img src="images/upload-demo.zh-CN.gif"
       alt="把化验单 PDF 拖到「数据」页；分析物被抽取出来，各自链回来源文件" width="880">
</p>

**3 · ② 转译。** 模型读完这一页后（用云端模型时是几秒，只用 CPU 时要几分钟），里面的分析物变成一条条指标，每一条都链回它被读出来的那份文件，每一条都带着编码：

```
Glycated Hemoglobin-HbA1c   5.2 %        loinc 4548-4
Fasting Blood Glucose-FBG   4.9 mmol/L   loinc 14771-0
Total Cholesterol-TC        4.45 mmol/L  loinc 14647-2
Low-Density Lipoprotein-LDL 2.48 mmol/L  loinc 22748-8
```

模型负责读这一页，但编码不由它决定。
解析是对着随包发布的词表查找，离线、结果确定，查不到就弃答，不猜测。

正是这个编码，让不同的文件能放在一起读。
那份 CSV 来自另一家化验所，分析物的写法也不一样：套餐上印的是 `Total Cholesterol-TC`，它写的是 `Cholesterol, Total`，而两者都是 14647-2，所以它们是一条序列，不是两条。
单位是这个身份的一部分，而不是被抹平的细节：胆固醇在 mmol/L 下是 14647-2，在 mg/dL 下是 2093-3。
把这一点说清楚，才能避免一条由两种单位混合而成的趋势悄悄出错。
把这两个编码归并成一条可比较的线，是 1.5.0 的 comparability key；
设备一侧的换算已经在做，[`examples/02_standardize_a_reading.py`](../examples/02_standardize_a_reading.py) 能离线把 `154.5 lb` 换算成 `70.08 kg`。

**4 · ③ 智能体。** 问一句胆固醇有什么变化。
回答过程中，每一步都会实时显示在思考旁边（「正在查询健康数据」「正在阅读文件」），答案开始后再收起来。
agent 会把带着这一项的文件全部找出来，包括 CSV 里不同的写法，然后根据它读到的内容作答：

```
Total Cholesterol   4.60 mmol/L   2025-11-12   you_lab_2025-11.md
                    4.45 mmol/L   2026-05-06   2026-05-06_Annual_Physical_Exam_Report.pdf
                    4.38 mmol/L   2026-08-04   you_lipid_panel_2026-08.csv
```

三份文件，三套写法，一条线。
每个数字都带一个编号的来源，点开就能看到这条读数、日期、参考范围和它出自哪份文件。
同一个问题问到共享的那份记录上，答案就来自另一个人。

<p align="center">
  <img src="images/ask-own-demo.zh-CN.gif"
       alt="问胆固醇怎么变化：agent 工作时实时显示每一步，三份报告画成一条趋势，点开数字上的编号来源，就能看到这条读数和它出自的文件" width="880">
</p>

<p align="center">
  <img src="images/ask-circle-demo.zh-CN.gif"
       alt="同一个问题问到共享记录上；agent 从一份你只能查看的记录里作答" width="880">
</p>

**5 · 写下感受。** 在「数据 › 记录」里写一句：`昨晚开始头疼，血压150/95，没发烧，每天早晚吃二甲双胍500mg`。
模型负责拆句并给每一部分分类；编码来自词表。
这句话变成一条编码为 NS01 的头痛、编码为 8480-6 和 8462-4 的血压，以及用药清单里的二甲双胍。
「没发烧」不会被当成发烧记下来。

<p align="center">
  <img src="images/journal-demo.zh-CN.gif"
       alt="写一句话，头疼编码为 NS01，血压编码为 8480-6 和 8462-4，二甲双胍进入用药清单；「没发烧」不记录" width="880">
</p>

这就是一次完整走通的整条链：一份文件进去，一条带编码的指标出来，一个 agent 在它上面作答。
**C · T · A**，每一阶段都能单独看到，而不只是一句声明。

所有数值都是生成的，这里没有一份文件描述真实的人，种子数据也不需要联网、不需要 Key。
要让一套部署保存真实数据，请把 `SEED_DEMO_DATA` 设为 `false`。
[`demo/README.md`](../demo/README.md) 说明每份文件是什么、如何重建；
抽取这一步对这些指标**尚未**做到的事，写在 [docs/roadmap.md](roadmap.md) 里，而不是在这里一笔带过。
