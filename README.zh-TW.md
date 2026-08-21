<div align="center">

# 🚀 Mirobody

**AI 原生的健康資料引擎——收集、標準化，並針對檢驗報告、穿戴裝置與基因體資料進行推理。**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/healthmemoryarena)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)

**[📚 文件](https://docs.mirobody.ai/)** · **[💬 雲端聊天服務——chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 API 平台——platform.mirobody.ai](https://platform.mirobody.ai/)**

**[English](README.md)** · **[简体中文](README.zh-CN.md)** · **繁體中文** · **[日本語](README.ja.md)**

*血液檢驗、穿戴裝置、基因體、影像——全部都是碎片化的，彼此互不相容。
在 AI 能真正理解你的健康之前，必須先有人把這些訊號統一成一個 AI
真正讀得懂的標準。這正是這個引擎在做的事。*

<img src="docs/images/where-your-data-comes-from.zh-TW.svg" alt="從穿戴裝置到餐點照片——一種標準格式，AI 可直接讀取。" width="920">

</div>

這個引擎做三件事，而整個程式碼庫（包括「貢獻方式」一節）也完全依照這三個階段來組織——正是[文件](https://docs.mirobody.ai/en/api-reference/)裡用的同一套 **C · S · A**：

| 階段 | 意思 | 位置 |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| **① 收集** | 把訊號拉進來：3 家裝置提供者 + 一個 SQL 來源 · 7 種檔案格式 · Apple Health | [`pulse/`](mirobody/pulse/) |
| **② 標準化** | 一套標準：把任何一筆讀值解析成標準代碼（LOINC · SNOMED CT · RxNorm），統一單位，並落地到 FHIR 認可的碼制 | [`indicator/`](mirobody/indicator/) |
| **③ 解答** | 推理：agent 透過虛擬檔案系統讀取*原始文件*，並用圖表與引用來源作答 | [`agent/`](mirobody/agent/) |

---

## ⚡ 60 秒試一下

指标解析是這個引擎的正門，不需要 key、不需要配置、不需要連網：

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン" "空腹血糖(GLU)"
```

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   任何語言，同一個碼
resolve("total cholesterol").loinc                     # '2093-3'  [質量/體積]
resolve_reading("total cholesterol", "5.0", "mmol/L")   # '14647-2' [摩爾/體積]
resolve_reading("total cholesterol", "193", "mg/dL")    # '2093-3'  單位決定了碼
resolve("血脂").resolved                                 # False    這是類別，不是一次觀測
```

**手上有數值和單位就一起傳進去。** LOINC 把單位**和**結果型別編進了身分，所以同一個
名稱會解析到不同的碼——把一筆 mmol/L 的結果歸到 mg/dL 的碼下，就是一個序列悄悄混進
兩種單位的原因。`resolve` 寧可棄權也不猜：`""` 是值得再看一眼的空缺，`"refused"`
才是答案。
→ [引擎參考](https://docs.mirobody.ai/en/engine/) ·
[指標](https://docs.mirobody.ai/en/concepts/indicators/)

---

## 標準化這一層到底是什麼

不是查表——這是相鄰的開源專案都沒有的那部分：

- **概念图谱**：440,961 個節點 · 22,044,110 條跨詞表邊 · **595,746 個來源 id**
  蒸餾成標準概念（LOINC · SNOMED CT · RxNorm 橋接）。
- **49,253 個多語言別名**（中文 22,578 · 日本語 16,809 · 另 5 種：de·es·fr·ko·ru）。
  `hemoglobin`、`血红蛋白`、`血紅素`、`ヘモグロビン` 全部落到 LOINC 718-7。
- **繁體中文是兩個問題，分兩套處理。** 字形折疊是機械的（隨包 3,336 字的
  zh-Hant → zh-Hans 表）；詞彙不是——台灣臨床用詞不同，把 `血紅素` 折疊會得到
  HbA1c 的碼。這類詞按繁體拼寫單獨收錄，收錄行永遠壓過折疊。
- **單位**歸一到約 310 個 UCUM 家族，含因次分析、依 LOINC 碼索引的摩爾質量橋接，
  以及對 `%` 與 `10*9/L` 的明確拒絕。300 個標準 pulse 指標。
- **這個說法我們是量出來的，不是斷言的。**
  [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 用健檢實際會開的
  套組給離線解析器打分，按報告實際列印的寫法，涵蓋英文、简体中文、繁體中文、日本語，
  外加平台 API 教的穿戴式詞彙。**今天 197/197；寫出來那天是 32/94。** 它評的是
  **臨床**正確性：把 `血红蛋白` 答成 HbA1c 的碼算失敗，而 `血脂` 必須解析為空。

```bash
pytest mirobody/test_engine_coverage.py -s   # 離線，約一秒
```

→ [標準化](https://docs.mirobody.ai/en/api-reference/standardization/) ·
[架構](https://docs.mirobody.ai/en/concepts/architecture/) ·
[資料流](https://docs.mirobody.ai/en/concepts/data-flow/)

---

## 📊 基準 —— 我們不說「相信我們」，我們把評測開源出來

我們的健康 AI 基準是 Hugging Face 上同類裡**下載量最高的**（各 4,000+）：

| 基準 | 測什麼 | 下載量 |
| --- | --- | --- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) | 事件驅動的縱向健康 agent——100 個合成使用者、10,000 個問題、程式化標準答案（[arXiv:2604.02834](https://arxiv.org/abs/2604.02834)） | 4,800+ |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | 醫療幻覺 | 4,500+ |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | 有害醫療建議 | 4,300+ |

用 **[mirobody-eval](https://github.com/thetahealth/mirobody-eval)** 一行指令複現
任何一個；它同時也能給部署灌入合成（不含 PHI）軌跡資料。

---

## 🚀 把整套跑起來

```bash
git clone https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs pull          # 引擎的資料包，`resolve` 需要它
./deploy.sh           # Postgres + pgvector、Redis、服務、worker
```

然後開啟 **http://localhost:18080**。服務啟動時會列印它接受的帳號——隨包那個是
`caregiver@mirobody.ai`，驗證碼 `111111`，名字就是角色：你以照護者身分登入，
讀的是別人的紀錄。

沒有郵件服務？不需要。登入頁預設落在**密碼**，把 Email 驗證碼留作第三個分頁：

```bash
curl -X POST localhost:18080/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

要對話需要一個 LLM key；embedding key 是可選的。
→ [Docker 部署](https://docs.mirobody.ai/en/deployment/docker/) ·
[設定](https://docs.mirobody.ai/en/configuration/) ·
[本地 Python 環境](https://docs.mirobody.ai/en/development/setup/)

### 👨‍👩‍👧 一個會回答、然後向你要檔案的 demo

`SEED_DEMO_DATA` 預設開啟，所以你一進來關愛圈裡就已經有一位合成使用者：
**Demo (synthetic)**，兩年跨度 244 個指標，五份 agent 可以 `read_file` 的文件。
你自己什麼都沒有；那份紀錄是她的。

<div align="center">
<img src="docs/images/your-care-circle.zh-TW.svg" alt="你自己沒有資料；你讀的是她的紀錄。" width="820">
</div>

**從提問開始，而不是從資料表開始。** 在 Ask 頁問：

> *「她最近一次的 LDL 是多少？和一年前比怎麼樣？」*

```
2024-04-16   3.4 mmol/L
2024-10-15   3.2
2025-04-15   3.1
```

……然後它會主動指出最近一次面板已經一年多了、值得再查一次。這就是後半段的引子。

**接著給它一個檔案。** `mirobody/demo/lab_report_2025-10-15.pdf` 是她**下一次**的
面板，刻意從灌入資料裡留出——所以上傳它不是空操作。拖到 Data 頁，看著 ① 收集 和
② 標準化 幹活：十二個分析物帶著單位出來、每一個解析到碼、LDL 序列多出第四個點。
再問一遍，答案就變了。

所有數值都是合成的——由 [mirobody-eval](https://github.com/thetahealth/mirobody-eval)
为 ESL-Bench 生成后固化在仓里，所以灌数据不需要連網、不需要 key。要让部署承载真实
数据，把 `SEED_DEMO_DATA` 设为 false。

---

## 🧩 擴充它

五個目錄鍵指向外掛根目錄；丟一個檔案進去，重啟即可。工具會同時成為 agent 工具和
MCP 工具，不需要額外接線。

| 你想要 | 丟進 | 文件 |
| --- | --- | --- |
| 一個新工具 | `mirobody/agent/tools/` | [新增工具](https://docs.mirobody.ai/en/tools/adding-tools/) |
| 一个 Agent Skill（SKILL.md） | `mirobody/agent/skills/` | [Skills](https://docs.mirobody.ai/en/tools/skills/) |
| 一整個 agent | `mirobody/agent/` | [Agents](https://docs.mirobody.ai/en/tools/agents/) |
| 一個裝置 provider | `mirobody/pulse/providers/` | [Provider 接入](https://docs.mirobody.ai/en/development/provider-integration/) |
| 別人的 MCP 服務 | Settings → MCP | [MCP 整合](https://docs.mirobody.ai/en/tools/mcp-integration/) |

agent 擁有的每個工具同時透過 `/mcp` 對外提供，依使用者門控。
→ [內建工具](https://docs.mirobody.ai/en/tools/built-in/) ·
[MCP 服務](https://docs.mirobody.ai/en/api-reference/mcp-servers/)

---

## 🔌 從你自己的程式碼裡用

| 介面 | 適合 | 文件 |
| --- | --- | --- |
| `pip install mirobody` | 解析與檔案解析，不需要服務 | [引擎](https://docs.mirobody.ai/en/engine/) |
| HTTP API | 你的應用對接一個部署 | [API 总览](https://docs.mirobody.ai/en/api-reference/overview/) · [資料](https://docs.mirobody.ai/en/api-reference/data/) |
| MCP | Claude、Cursor 或任何 MCP 用戶端讀取使用者紀錄 | [MCP 服務](https://docs.mirobody.ai/en/api-reference/mcp-servers/) |
| Backbone 模式 | 你自己的 agent，我們的資料層 | [Backbone](https://docs.mirobody.ai/en/api-reference/backbone-mode/) |

不確定選哪個？→ [如何選 API](https://docs.mirobody.ai/en/api-reference/choose-your-api/)

---

## 🏗️ repo 怎麼擺的

```
mirobody/
├── pulse/       ① 收集     —— provider、檔案解析、彙總
├── indicator/   ② 標準化   —— 解析器、單位、分類（無 DB、無網路）
├── agent/       ③ 回答     —— DeepAgent、工具、skills、chat
├── mcp/         MCP 服務
├── schema/      DDL，開發環境啟動時重放
└── demo/        關愛圈 fixture
```

**一條機器強制的規則**：`indicator/` 永不匯入 agent 層，所以
`pip install mirobody` 保持是一個 233 MB 的引擎，而不是拖來一整套框架。兩條
import-linter 契約守著這條線——`lint-imports` 會讓建置失敗。

→ [架構](https://docs.mirobody.ai/en/concepts/architecture/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

## 📚 文件

更深的內容都在 **[docs.mirobody.ai](https://docs.mirobody.ai/)** —— 50 個頁面，
英文與简体中文。

| | |
| --- | --- |
| [快速開始](https://docs.mirobody.ai/en/quickstart/) · [安裝](https://docs.mirobody.ai/en/installation/) · [自架](https://docs.mirobody.ai/en/self-host/) | 先跑起來 |
| [指標](https://docs.mirobody.ai/en/concepts/indicators/) · [Provider](https://docs.mirobody.ai/en/concepts/providers/) · [檔案處理](https://docs.mirobody.ai/en/concepts/file-processing/) | 三個階段怎麼工作 |
| [API 參考](https://docs.mirobody.ai/en/api-reference/) · [串流](https://docs.mirobody.ai/en/api-reference/streaming/) · [函式呼叫](https://docs.mirobody.ai/en/api-reference/function-calling/) | 基於它開發 |
| [貢獻](https://docs.mirobody.ai/en/development/contributing/) · [環境建置](https://docs.mirobody.ai/en/development/setup/) | 參與開發 |

repo 內、面向貢獻者：[CONTRIBUTING.md](CONTRIBUTING.md) ·
[docs/roadmap.md](docs/roadmap.md) · [SECURITY.md](SECURITY.md)

---

## 🤝 貢獻

槓桿最高的貢獻是一個解析器答錯的詞。跑 `mirobody resolve "<词>"`，如果答案錯了
或是空的，就往 [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv)
加一行，再往 [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 加一個
用例——覆蓋率分數就是評審。

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [貢獻指南](https://docs.mirobody.ai/en/development/contributing/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

<div align="center">

**[📚 文件](https://docs.mirobody.ai/)** · **[💬 Chat](https://chat.mirobody.ai/)** · **[🔌 平台](https://platform.mirobody.ai/)** · **[🧪 Eval](https://github.com/thetahealth/mirobody-eval)**

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>
