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

## ⚡ 60 秒內親自試試

不需要伺服器、不需要金鑰、不需要網路——這個術語引擎只是一次 pip install：

```bash
pip install mirobody
mirobody resolve "hemoglobin" "血红蛋白" "血紅素" "ヘモグロビン"
# all four -> LOINC 718-7
```

```python
from mirobody.engine import resolve
resolve("血红蛋白").loinc   # -> '718-7'   offline: no key, no config, no network
```

第三個才是真正有意思的地方。`血紅素` 和 `血红蛋白` 不是同一個詞換了個字形而已——台灣與中國大陸對這個概念用的是**不同的詞**，如果只做字元轉換，把 `血紅素` 轉成簡體字會得到 `血红素`，而一個原始索引會用**HbA1c**（糖化血色素）的代碼回答它：那是完全不同的檢驗項目。單純的繁簡轉換在這裡註定出錯，詞彙必須靠人工校訂才行。標準化這一層大部分的工作都是在處理這類狀況，而不是那些一眼就能對上的簡單案例。

### 你實際上會用到的兩個函式

`resolve()` 回答的是一個**名稱**。`resolve_reading()` 回答的是一次**量測結果**——兩者給出的代碼不一樣，因為 LOINC 把單位和結果型態都編進了識別身分裡：

```python
from mirobody.engine import resolve, resolve_reading

resolve("total cholesterol").loinc                       # '2093-3'   [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")    # '14647-2'  [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")     # '2093-3'   unchanged

resolve("尿糖").loinc                                     # '2350-7'   [Mass/volume]
resolve_reading("尿糖", "阴性")                            # '2349-9'   [Presence]
resolve_reading("尿糖", "5.6", "mmol/L")                   # '15076-3'  [Moles/volume]
```

**如果你手上有數值和單位，就把它們一起傳進去。**同一個指標會因為讀值不同而導向完全相反的代碼，把一筆 mmol/L 的結果歸到 mg/dL 的代碼下——或是把一筆「阴性」的結果歸到質量濃度的代碼下——就是一個系列悄悄混進兩種單位、卻沒人發現的原因。這兩個函式都是離線執行、結果穩定不變，而且能安全地在迴圈裡呼叫（第一次之後每次約 25 微秒）。

每一個答案都會說明自己是怎麼得到的，而其中一種值跟其他不一樣：

```python
r = resolve("血红蛋白")
r.loinc, r.canonical, r.method     # '718-7', 'Hemoglobin [Mass/volume] in Blood', 'lexical'

resolve("绝对不存在的指标名xyzzy").method   # ''         never seen it
resolve("血糖(HbA1c)").method              # 'refused'  two different tests in one string
```

`""` 是一個空缺，值得再找人確認；`"refused"` 才是真正的答案——`血糖(HbA1c)` 這個詞，括號外是血糖、括號內是 HbA1c，`血脂` 則是四個分析物的合稱，兩者都沒有哪一個單一代碼是對的。但一個確實**有**套組代碼的套組詞彙不算拒答：`blood pressure`（血壓）→ `85354-9`，這是 FHIR 生命徵象規範強制要求的代碼，用來告訴呼叫端要預期會有多個子項目。
**只有 `method == "lexical"` 才能當作身分識別**（也就是分組鍵、「這些是同一個系列」的判斷依據、FHIR 鏡像）。另一種情況請見本文件後面的「語義召回」一節。

### 單位只會被比較，從不被假設

```python
from mirobody.indicator.fhir.units import convert_value, convertible

convert_value(5.6, "mmol/L", "mg/dL", loinc_code="1558-6")   # 100.9  (molar-mass bridge)
convert_value(42.0, "U/L", "[IU]/L")                         # 42.0   (1:1, different families)
convert_value(24.0, "kg/m2", "mg/dL")                        # None   (BMI is not a concentration)
convertible("%", "10*9/L")                                   # False  (a fraction is not a count)
```

`None` 是一個答案，不是失敗：該把兩筆讀值分開回報，而不是硬把其中一個換算成另一個的樣子。**不要**用 `unit_family()` 來判斷是否可轉換——它是一個 LOINC PROPERTY 分類器，用在這個問題上兩個方向都會出錯（`kg/m2` 和 `mg/dL` 屬於同一個家族，卻無法互相轉換；`U/L` 和 `[IU]/L` 屬於不同家族，卻是同一個單位）。

接下來，把整套系統自行架設起來（見本文件後面的「快速上手」一節），登入，然後產生你自己的**個人 MCP 網址**（網頁客戶端 → Settings → MCP Url）。把任何 MCP 客戶端（Claude Desktop、Cursor、Cherry Studio）指向這個網址，就能跟你自己的健康資料引擎對話：

```json
{ "mcpServers": { "mirobody": { "url": "http://localhost:18080/mcp/<your-personal-secret>" } } }
```

MCP 對外的介面刻意做得很精簡：

| 工具 | 功能 | 需要什麼 |
| --- | --- | --- |
| `resolve_indicator` | 任何語言的指標名稱 → 標準 LOINC 代碼 | 不需要——離線執行，不涉及使用者資料 |
| `normalize_unit` | 自由格式的單位文字 → 標準 UCUM 及可比較的單位家族 | 不需要——離線執行，不涉及使用者資料 |
| `query_health_indicators` | 你自己的紀錄——搜尋、讀取、彙總，**一次呼叫**就完成；每筆結果都帶有 LOINC 身分 | 需要你的帳號 |
| `get_genetic_data` | 依 rsid 查詢你的基因變異 | 需要你的帳號 |

`tools/list` 會依帳號誠實回報：那兩個綁定帳號的工具，只有在你的帳號真的擁有那類資料時才會列出來。伺服器使用的是 [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/) 規範——目前這個無狀態版本（逐請求的 `_meta`、`server/discover`、固定的工具排序邏輯）——並會針對較舊的客戶端，向下相容協商到 `2024-11-05`。

可直接執行的教學：[`examples/`](examples/README.md)——五個腳本，從離線解析到完整伺服器的預檢流程都有，每一個都驗證過確實能執行。

---

## ① 收集——所有訊號，一個入口

- **正式支援的裝置提供者，全部走同一套外掛規格**——**Garmin、Oura、Whoop** 都有經過實戰考驗的提供者，另外透過 pulse 平台還支援 [300 種以上的裝置](mirobody/pulse/providers/README.md)。一個提供者就是一個目錄：把它放進去，探索、OAuth、拉取排程就自動幫你接好了。

  > **一個自架部署，要讓這些真正打開，需要什麼。** 每個提供者都是那家廠商自己的
  > 一個 OAuth 客戶端，所以在你提供**自己**從廠商開發者計畫拿到的憑證之前，它
  > 會保持休眠——在 `config.{env}.yaml` 裡填入 `GARMIN_CLIENT_ID`/`SECRET`、
  > `OURA_CLIENT_ID`/`SECRET`、`WHOOP_CLIENT_ID`/`SECRET`（以及各自的重新導向
  > 網址）。沒有這些設定，模組還是會載入，只會記錄一行
  > `declined to start (not configured)`——這是老實的狀態，不是故障。
  > [`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) 是你可以立刻
  > 試用的一個：把 `ENABLE_PGSQL_DEVICE` 設成 `1`，下次啟動平台就會記錄
  > `loaded 1 providers`。
  > **逐步說明：[docs/provider-setup.md](docs/provider-setup.md)** ——裡面有確切
  > 的回呼網址、設定鍵，以及如何在啟動日誌裡分辨「沒有設定」和「壞掉了」的差別。

- **Apple Health 只能推送資料進來，需要你自己寫一個 iOS App**——對應的端點都在這裡（[`/apple/health`、`/apple/statistics`、`/apple/cda`](mirobody/server/routers/apple_router.py)，加上[CDA 處理](mirobody/pulse/apple/README.md)），但它們只會*接收*資料；這個 repo 裡沒有任何東西能主動向 HealthKit 拉資料。HealthKit 只能從一個已簽署的 iOS App、在裝置本機端、且使用者針對每種資料類型都個別授權之後才能讀取——沒有網頁版的 OAuth 流程，也沒有伺服器對伺服器的 API。所以一個自架的網頁部署不會顯示「連接 Apple Health」的按鈕，這樣才正確：缺的那一塊，是一個具備 HealthKit entitlement 的 iOS 客戶端，而上面那些 API，就是這種客戶端會 POST 過去的對象。
- **用 AI 解析 7 種檔案格式**——PDF 檢驗報告、Excel、CSV、圖片、音訊、純文字，還有**基因檢測匯出檔（WeGene）**；由 LLM 進行指標擷取（[`pulse/file_parser/`](mirobody/pulse/file_parser/)，1.3 萬行程式碼）。
- 匯入流程：分階段收件 → 驗證 → 正規化 → 每日彙總 → 回饋進紀錄裡的 [AI 洞察](mirobody/pulse/insight/)——形成一個閉環。

## ② 標準化——一個 AI 真正讀得懂的標準

這是同類開源專案都沒有的部分——一個真正的**語義標準化層**，不是一個對照表而已：

- **概念圖**：440,961 個節點 · 22,044,110 條跨詞彙表的邊 · **595,746 個來源 ID**，萃取成標準概念（LOINC · SNOMED CT · RxNorm 之間互相橋接），透過 Git LFS 隨套件釋出（[`indicator/`](mirobody/indicator/README.md)）。
- **以 embedding 為基礎的解析**：自由格式的指標名稱 → 標準代碼，內建 **49,253 個多語言別名**（中文 22,578 · 日本語 16,809 · 另外 5 種：de·es·fr·ko·ru）——`hemoglobin`、`血红蛋白`、`血紅素`、`ヘモグロビン` 全部都對應到 LOINC 718-7。
- **繁體中文其實是兩個問題，我們也分成兩個問題來處理。** 字形轉換是機械式的：查詢會透過內建的 3,336 字對照表（[`zh_fold.py`](mirobody/indicator/zh_fold.py)）從 zh-Hant 折疊成 zh-Hans，做法呼應詞庫建置時對語料庫做的處理。但詞彙不是機械式的：台灣臨床慣用的詞不一樣，把 `血紅素` 折疊之後會變成 `血红素` → 對應到 HbA1c 的代碼。這些詞彙會直接以繁體寫法收錄進校訂表，而人工校訂過的資料列，永遠贏過機械式折疊的結果。
- **這個說法我們不是憑空主張，而是實測出來的。** [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 拿醫師實際會開的檢驗套組來為離線解析器評分——血脂肪、全血球計數（CBC）、代謝、肝功能、甲狀腺、荷爾蒙、腫瘤標記、尿液檢查、生命徵象——寫法就是報告單真正印出來的樣子，涵蓋英文、简体中文、繁體中文與日本語——再加上平台 API 會用到的裝置／穿戴裝置詞彙（`steps`、`resting_heart_rate`、`sleep_duration`）。**現在是 197/197；剛寫出來那天只有 32/94。**它評的是*臨床*上對不對，不是解析成功率：把 `血红蛋白` 解析成 HbA1c 的代碼算是失敗，而 `血脂`（一個分類，不是單一檢驗項目）則要求解析結果必須是*什麼都沒有*，因為一個信心十足卻錯誤的代碼，比一個老實的「不知道」更糟。
- **表面字元代數，讓寫法本身不會決定答案**（[`indicator/lexical.py`](mirobody/indicator/lexical.py)）：NFKC-lite 折疊（全形字元、上標、六種破折號變體），加上一個看得懂 CJK 的斷詞器，以及對檢驗報告常印出來的 `名称(缩写)` 這種形狀做的保守拆解。`ＦＢＧ`、`LDL–C`、`fasting_glucose`、`空腹血糖(GLU)`、`Cholesterol, total`，全部都會對到跟一般寫法一樣的代碼。當 `名称(缩写)` 前後兩半意見不合時——例如 `血糖(HbA1c)`——這個詞就會維持**未解析**狀態，而不是隨便選一個。
- **是讀值在決定代碼，不只是名稱**（[`engine.resolve_reading`](mirobody/engine.py)）。LOINC 把單位*和*結果型態都編進了識別身分裡，所以單位決定 `PROPERTY`，數值的性質決定 `SCALE_TYP`。`5.0 mmol/L` → 14647-2，`193 mg/dL` → 2093-3，`阴性` → `[Presence]` 這個變體。內建語料庫裡有一半是非 `Qn`（79,368 列裡有 38,687 列），所以一個只針對數字設計限制條件的解析器，對其中一半的資料等於是視而不見。
- **單位標準化**成 UCUM 家族（約 310 個），再加上[單位轉換](mirobody/indicator/fhir/units/convert.py)——因次分析、依 LOINC 代碼查表的摩爾質量橋接，以及對 `%` 相對 `10*9/L` 這種情況明確拒絕轉換。316 個標準 pulse 指標。
- 25 種臨床類別的分類法（生命徵象、檢驗與臨床、身體量測……等）。

### 🧪 語義召回：選擇加入，為什麼選擇加入

以上所有東西都是詞彙式的——內建詞庫加上查表。遇到不認得的詞，它會選擇棄答，這既是它的優點，也是它的天花板。還有第二層（[`indicator/semantic.py`](mirobody/indicator/semantic.py)）：對 LOINC 語料庫的 embedding 做餘弦召回，由 [`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py) 建置。

```python
from mirobody.engine import resolve_with_semantic_fallback

out = await resolve_with_semantic_fallback(["空腹血糖", "some unheard-of assay"])
out[0].method    # 'lexical'  — the lexical tier answered; the fallback never saw it
out[1].method    # ''         — no matrix installed, so nothing to fall back TO
                 # 'semantic' once one is, and that means "a suggestion", not an identity
```

**沒有隨套件附上任何矩陣**，所以在一般的 `pip install` 之下這一層什麼都不做，回傳的結果跟 `resolve()` 完全一樣。把 `MIROBODY_SEMANTIC_INDEX` 指向一個矩陣就能啟用它；一個矩陣大約 198 MB，還需要一支 embedding 金鑰，而且語料庫和查詢**必須**來自同一個模型——如果兩者不匹配，它不會直接失敗，而是會很有信心地回傳一堆無意義的結果。

即使裝上去了，它依然維持選擇加入的狀態，原因是這一層本身的特性，不是一個可以靠調參解決的問題：**餘弦召回沒有辦法棄答。**被問到一個它從沒見過的詞，它會用回答正確答案時同樣的信心，回傳離它最近的鄰居，而 LOINC 自己的問卷語料庫，又剛好提供了大量看似合理的鄰居可以讓它抓。沒有任何一個分數門檻，能把這兩種情況分開。

即便如此，它還是有用，因為當一筆讀值走到這一層時，已經不是一個光溜溜的字串。在它之前的擷取階段，遇到非健康內容會直接回傳空，交過來的是 `{indicator, value, unit}`，所以這一層可以依讀值所暗示的線索來篩選候選項目——用數值的性質推 `SCALE_TYP`，用單位的量綱推 `PROPERTY`——並跳過 `loinc_skip.txt` 裡早就列出來的非臨床資料列。

所以：把它當成**建議**一個代碼的工具，讓人或模型事後去確認。真正可以拿來當作身分依據的，是詞彙層。

## ③ 解答——讀取原始文件的 agent

使用這一層有**兩種方式**，各自對應一種 agent——差別在於*工具迴圈由誰執行*：

| | **DeepAgent**——引擎由你來執行 | **BaseAgent**——你的模型來消費我們的引擎 |
| --- | --- | --- |
| 工具迴圈執行位置 | 在這裡，在你自己的部署環境裡 | 在 **LLM 供應商**那邊，透過 HTTP 呼叫 `/mcp` |
| 適用情境 | 整套系統自架 | Claude Desktop · Cursor · ChatGPT Apps · 任何 MCP 客戶端 |
| 額外功能 | 虛擬檔案系統、QuickJS、Agent Skills、圖表 | MCP 工具介面公開了什麼，就只有那些——沒有藏東西 |

- **DeepAgent**——主要的 agent，建立在 [deepagents](https://github.com/langchain-ai/deepagents) 0.7 / LangChain 1.3 之上。支援多家供應商（OpenAI、Gemini、Anthropic、OpenRouter，以及任何相容 OpenAI 介面的端點）；一個**以 PostgreSQL 為後端的虛擬檔案系統**（`/uploads`、`/library`、`/memories`、`/skills`）讓模型可以直接 `read_file` 你*原始*的 PDF——以多模態方式——而不是透過有損的擷取結果；內嵌執行的 JS 直譯器（QuickJS）可以做真正的運算；每輪對話有模型呼叫額度上限，額度用完就優雅停止。
- **BaseAgent**——刻意不用 LangChain。它把 MCP 伺服器直接交給供應商去用（OpenAI Responses 的 `mcp_server`、Gemini Interactions），再把結果串流回來。這讓它變成我們自己對第三方使用體驗的一次預演：**任何 BaseAgent 自己做不到的事，外部的 MCP 客戶端一樣做不到。**它不會畫圖表——視覺化要由使用它的客戶端自己準備。
- **內建 MCP 伺服器**（[`mcp/`](mirobody/mcp/)）——每一個工具同時也是一個透過 HTTP 提供的 MCP 工具；同時能當 MCP 客戶端，*也*能當支援 OAuth 的 MCP 伺服器。**Agent Skills**（SKILL.md）透過 deepagents 原生的 SkillsMiddleware，從 [`mirobody/agent/skills/`](mirobody/agent/skills/) 提供。
- 照護圈分享，並逐人取得同意：

<div align="center"><img src="docs/images/your-care-circle.zh-TW.svg" alt="你的照護圈——用電子郵件邀請你信任的人；主控權一直在你手上：隨時可以移除成員或取消分享，而且健康資料在你允許之前，預設不會分享。" width="920"></div>

---

## 🏗️ 架構

這個引擎就是三個階段——**① 收集 → ② 標準化 → ③ 解答**——而套件的目錄結構，講的也是同一件事。

```
mirobody/
│
│  ── the ENGINE (pip install mirobody · no agent framework, machine-enforced) ──
│
├── engine.py            ②  The front door: resolve() offline, parse_file() one-LLM-call
├── cli.py                   mirobody parse | resolve | serve | worker
├── pulse/               ①  COLLECT — every signal, one intake
│   ├── providers/           production device providers (Garmin/Oura/Whoop, 300+ devices)
│   ├── apple/               Apple Health import (zip + CDA)
│   ├── file_parser/         7 file formats → indicators via LLM extraction (needs DB)
│   ├── ingest/              StandardPulseData: the universal exchange format that
│   │                        every source above converges on (was `data_upload/`)
│   └── core/                domain models, daily rollups, insights (needs DB)
├── indicator/           ②  STANDARDIZE — one standard AI can actually read
│   └── fhir/                concept graph · embedding resolution · units → UCUM · taxonomy
├── res/                     the shipped data: LOINC/SNOMED bundles (Git LFS, see
│                            LICENSE-3RD-PARTY + *.NOTICE) · resolver_overrides.tsv · sql/
│
│  ── shared infrastructure: not a fourth stage, used BY the three ─────────
│
├── mcp/                     MCP server: every tool doubles as an MCP tool over HTTP
├── task/                    background workers (indicator sync, profile refresh)
│                              ← used by pulse, server
├── user/                    accounts, auth, care-circle consent
│                              ← used by agent, mcp, pulse, server
├── utils/                   config (encrypted YAML), direct LLM SDK access, db,
│                            locales ← used by EVERY other package. Keep it a
│                            leaf: it must import nothing above itself
│
│  ── the AGENT LAYER (pip install 'mirobody[agents]' · LangChain lives ONLY here) ──
│
├── agent/               ③  ANSWERS — one roof for everything conversational
│   ├── deep_agent.py        DeepAgent — model 1: YOU run the engine. deepagents/
│   │                        LangChain, PG virtual fs, QuickJS, Agent Skills
│   ├── base_agent.py        BaseAgent — model 2: someone else's model consumes us
│   │                        over MCP. Hands /mcp to the provider, which drives
│   │                        the tool loop. No LangChain, on purpose.
│   ├── base/ · deep/        the two agents' internals (backends, middleware)
│   ├── chat/                sessions · messages · history replay · sharing · profile
│   ├── tools/               the MCP tool surface (MCP_TOOL_DIRS): terminology
│   │                        (② Standardize, offline), health records, genetics
│   ├── skills/              Agent Skills (SKILL.md) — deepagents SkillsMiddleware
│   ├── prompts/             Jinja system prompts
│   └── resources/           MCP UI widgets for ChatGPT Apps (see its README)
└── server/                  HTTP lifecycle + the FastAPI routers (server/routers/)

frontend/                    the bundled web client, shipped as a FIXED build —
                             outside the package on purpose: wheels ship the
                             engine, not 8MB of JS. Served when `frontend/`
                             exists next to the process (Docker/source). The
                             API + MCP surface is the real contract: build your
                             own frontend against it.
```

### 每種安裝規模能用到什麼

| 安裝方式 | 能做什麼 | 佔用空間 |
| --- | --- | --- |
| *只有 wheel + numpy* | `from mirobody.engine import resolve`——離線解析器 | mirobody 本身 **33 MB**（加上 numpy 共 76 MB） |
| `pip install mirobody` | + `mirobody parse`（一支 LLM 金鑰） · 檔案解析（PDF/Excel/audio） · 標準碼輸出 | 233 MB，90 個套件 |
| `pip install 'mirobody[server]'` | + HTTP API 與 MCP 端點 | 需要 Postgres + Redis |
| `pip install 'mirobody[agents]'` | + DeepAgent/BaseAgent 與 `mirobody serve`（包含 `[server]`） | + 整套 LangChain |
| `pip install 'mirobody[indicator-build]'` | 可以重新建置術語資料包本身 | 需要 LOINC/UMLS 原始資料 |

這些數字是在乾淨的 venv 裡實測出來的，不是估計值。**mirobody 那 33 MB 裡，有 24 MB 是隨附的 LOINC 資料**——那就是解析器本身，不是額外負擔，而且正是這些資料讓標準化在拔掉網路的情況下依然能運作。

#### 最小可用範圍，以及刻意不放進去的東西

`pip install mirobody` 裡裝的，剛好就是 `resolve()` 會讀到的東西，不多也不少：

| 隨附的檔案 | 誰會讀它 |
| --- | --- |
| `res/fhir_loinc_bundle.tar.gz` | 92.1 萬鍵的別名索引、LOINC 軸表、常見度先驗值 |
| `res/fhir_meta.csv.gz` | 別名索引所指向的 67.7 萬個名稱語料庫 |
| `res/aliases_src/*.tsv` | 49,253 筆多語言別名資料（中文 22,578 · 日本語 16,809 · 另外 5 種：de·es·fr·ko·ru） |
| `res/resolver_overrides.tsv` | 人工撰寫的校正，以及刻意設計的「不給答案」 |

曾經隨套件附上、現在已經不附的檔案有四個，加起來 **28 MB**。執行期完全沒有任何東西會讀到它們：對 `server/`、`agent/`、`pulse/`、`mcp/`、`task/` 這幾個目錄搜尋 `concept_graph` 或 `taxonomy`，結果都是空的。其中三個——`fhir_concept_graph.bin`、`fhir_taxonomy.bin`、`fhir_snomed_ct_bundle.tar.gz`——之所以還留在 repo 裡，是因為 [`indicator/`](mirobody/indicator/) 的資料包建置工具需要它們，而那套工具是從 git checkout 直接運作的，另外 v2 語義管線也需要它們，而且還額外需要一個完全沒有隨套件釋出的 embedding 矩陣。第四個，`fhir_id_map.npy`，則是**已經整個從 repo 裡移除**：它把標準 id 對應到 `fhir_indicators.id`，也就是某一個資料庫自己的主鍵，對其他人來說從來就沒有意義——需要的話用 `indicator id-map` 自己重新產生一份。拿掉 SNOMED 資料包，也順便讓每一個 pip 使用者不用再背負它的 Affiliate-Licence 義務。`scripts/check_wheel_data.py` 現在會雙向檢查——上面五個必須存在且真實，下面這四個必須不存在。

**這個最小範圍做不到的事**：解析出詞彙層漏掉的詞。這裡的解析，做的是精確鍵值比對和別名表查詢，查的是隨附的詞庫，再加上 [`indicator/lexical.py`](mirobody/indicator/lexical.py) 裡的表面字元代數。這裡沒有 embedding 召回——那是 [`fhir/resolve/pipeline.py`](mirobody/indicator/fhir/resolve/) 實作的，需要一個約 200 MB、由 [`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py) 建置的 LOINC embedding 矩陣，還要一支 embedding API 金鑰。這裡的漏解是老實的漏解，修法是在 `resolver_overrides.tsv` 裡加一列——見「貢獻方式」一節。

資料庫驅動程式、HTTP 伺服器、S3 和 email 客戶端，以前是預設安裝就有的；現在都移到 `[server]` 裡了，而 `[agents]` 會連帶把它拉進來。如果你只想把這個引擎當一個函式庫用，就不用再為一個 Postgres 驅動程式付出安裝成本。

### 唯一一條規則，由機器強制把關

**這個引擎必須在完全沒裝任何 agent 框架的情況下，也能被 import。** `langchain*`、`deepagents`、`langgraph` 只允許出現在 `agent/` 和 `server/` 底下——這跟 langchain 自己對 `langchain-core` 採用的分層方式一樣。`pyproject.toml` 裡有兩個 `[tool.importlinter.contracts]`，一旦違反就會讓建置失敗，連函式內部的 import 也不例外：

```bash
pip install -e '.[test]' && lint-imports
```

這就是為什麼 `mirobody.engine` 可以在唯一存在的第三方套件只有 numpy 的情況下，解析一個指標。`utils/` 刻意設計成一個葉節點——`utils/db.py` 裡曾經有一行頂層的 `from sqlalchemy import text`，結果讓整套資料庫堆疊變成一個從來不會真正開啟連線的函式的硬性相依。`utils/`、`user/`、`task/` 不是三個階段，它們是三個階段站在上面的基礎設施。現在還有一處刻意保留的邊界穿越，連同它的退場計畫都記錄在 `pyproject.toml` 的 `ignore_imports` 裡，以及 [docs/roadmap.md](docs/roadmap.md) 裡。

### 端到端的資料流

```
vendor APIs / files / Apple Health          ① pulse
        └─> StandardPulseData ─> validate ─> normalize ─> daily rollups
                 └─> indicator names ─> ② indicator: canonical codes (LOINC·SNOMED·RxNorm)
                          └─> coded rows in Postgres
                                   └─> ③ agent: read ORIGINAL documents through the
                                       virtual fs, compute, chart, answer — and insights
                                       feed back into the record, closing the loop
```

---

## 📊 基準測試——我們不說「相信我們就好」，我們直接把 eval 交出來

我們的健康 AI 基準測試，是**在 Hugging Face 同類別裡下載次數最高的**（每個都超過 4,000 次）：

| 基準測試 | 測的是什麼 | 下載次數 |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) | 事件驅動的長期健康 agent——100 個合成使用者、10,000 筆查詢、以程式產生的標準答案（[arXiv:2604.02834](https://arxiv.org/abs/2604.02834)） | 4,800+ |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | 醫療幻覺 | 4,500+ |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | 有害的醫療建議 | 4,300+ |

透過我們自己開發的開放評測框架 **[mirobody-eval](https://github.com/thetahealth/mirobody-eval)**，一行指令就能重現上面任何一個結果。它的產生器也會產出用來填滿新部署空資料庫的合成軌跡資料（不含 PHI，亦即不含可識別個人身分的健康資訊）——見[灌入資料](#-灌入資料)。

我們對引擎本身，也用同一套標準要求。**解析器覆蓋率**——② 標準化這一層，能不能叫得出一份真實檢驗報告上那些日常檢驗項目的名字？——這個測試就在這個 repo 裡跑，離線執行，不到一秒：

```bash
pytest mirobody/test_engine_coverage.py -s
#   offline resolver coverage: 197/197 = 100%
```

一開始的成績是 **32/94**——這個基準測試後來擴充到了 197 個案例。問題不在概念圖；而是這個索引是拿 LOINC 的長名稱建出來的，所以它認得 `LDL-C` 卻不認得 `LDL cholesterol`，認得葡萄糖卻不認得 `血糖`，還把 `血红蛋白` 解析成了 HbA1c 的代碼。這兩類錯誤都只要各補一列 TSV 就能修好——見「貢獻方式」一節。

---

## ⚡ 快速上手

### 📋 事前準備

- **Docker 與 Docker Compose**：請確認已安裝並且正在執行。
- **Git**：用來把 repository 複製下來。
- **Git LFS**：用來拉取二進位資料檔（例如 `fhir_concept_graph.bin`）。可透過 `apt install git-lfs`（Linux）或 `brew install git-lfs`（macOS）安裝；Windows 版 Git 預設就內建。安裝完之後執行一次 `git lfs install`。

### 透過 Docker 部署

```bash
git clone https://github.com/thetahealth/mirobody.git
cd mirobody
./deploy.sh
```

這個腳本會：

- 產生一個安全的 `.env` 檔案。
- 建立一個預設的設定檔（`config.localdb.yaml`）。
- 建置 Docker 映像檔。
- 啟動服務（Postgres、Redis、Mirobody）。

然後在瀏覽器打開 `http://localhost:18080`。

在動手之前，有三支金鑰和一個容易踩到的坑，值得先知道：

> - **LLM 金鑰**：`OPENROUTER_API_KEY` 用來驅動 Deep agent。
> - **Embedding 金鑰**——這是*另外一支*金鑰，而且真的是第二支：worker 在同步
>   指標時，會把名稱嵌入成 `th_series_dim` 這個向量欄位，而能做到這件事的供
>   應商只有兩家。`EMBEDDING_PROVIDER` 預設是 `gemini`（用
>   `GOOGLE_API_KEY`）；另一個選項是 `qwen`（用 `DASHSCOPE_API_KEY`）。如果
>   只設定了 OpenRouter 金鑰，聊天功能可以動，但**健康指標會一直停在
>   0**——worker 的記錄裡會看到 embedding 失敗。把 `EMBEDDING_PROVIDER` 設成
>   `openrouter` 並不會解決這個問題，而且也不會裝作解決了：它會直接丟出例
>   外，點名是哪個欄位缺失，因為一次悄悄沒寫入任何東西的同步，跟一次本來就
>   沒事可做的同步，看起來會一模一樣。那個供應商是為 `text_embedding` 的呼叫
>   端，以及前文「語義召回」一節提到的、以檔案為基礎的語義層所準備的，兩者
>   都不會碰到資料庫欄位。
> - 金鑰放在 `config.{env}.yaml` 裡；Mirobody 會在第一次載入時，用自動產生
>   的 `CONFIG_ENCRYPTION_KEY` 把它們加密。
> - 第一次啟動大約需要 1 分鐘（建立資料庫結構）——等到看到
>   `SQL files initialization completed` 就好了。
>
> 完整的設定指南：[CONFIG](mirobody/utils/config/README.md) ·
> [DATABASE](mirobody/schema/README.md) · [docs.mirobody.ai](https://docs.mirobody.ai/)

### 🐍 本機 Python 開發

在主機上直接跑程式碼，pg/redis 則跑在 Docker 裡——這是平常除錯時的標準流程：

```bash
docker compose up -d pg redis
pip install -e '.[agents]'        # Python ≥3.12; engine-only is `pip install -e .`
echo "ENV=localdb" > .env
# create config.localdb.yaml overriding PG_HOST/PG_PORT/REDIS_* to the
# containers' published ports, add your LLM keys, then:
mirobody serve
```

逐步的完整說明（連接埠、加密金鑰、`[cn]` 這個 extra）請見
[docs.mirobody.ai](https://docs.mirobody.ai/) 以及
[CONFIG](mirobody/utils/config/README.md)。

**CLI 一覽表**

| 指令 | 功能 |
| --- | --- |
| `mirobody parse <file>` | 丟一份檢驗報告進去，吐出一張標準化的 LOINC 表格——一支 LLM 金鑰，零基礎設施 |
| `mirobody resolve <terms…>` | 離線的指標名稱解析——不需要金鑰、不需要設定、不需要網路 |
| `mirobody serve` | 執行 HTTP 伺服器（chat、MCP、API）——需要 `[agents]` |
| `mirobody worker` | 執行背景工作程序（指標同步、個人檔案更新） |

### 👤 第一次登入

用一個預先建立好的 demo 帳號登入——伺服器啟動時會把它們印出來：

- **Email**：`caregiver@mirobody.ai`——名字就是角色：你以照護者身分登入，
  讀的是別人的紀錄
- **驗證碼**：`111111`

這些帳號來自 `config.yaml` 裡的 `EMAIL_PREDEFINE_CODES`：沒有設定 SMTP 的話，只有預先設定好的地址才能登入。你可以把自己的地址加進去，或者設定 `EMAIL_SMTP_*` 來寄送真正的驗證碼。

這三個是白名單，不是註冊的上限：任何通過驗證的地址都會被當場建立
（`add_or_get_user`）。白名單管的是**驗證**這一步——沒有設定 SMTP 時，只有預置
的驗證碼能通過，所以「發送驗證碼」會回傳 `No SMTP server configured.`，你直接
填你已經知道的那個碼。

**或者乾脆不用驗證碼。** 驗證碼那條路需要 Mandrill 或 SMTP，而你 clone 下來試用
的部署兩者都沒有——所以登入頁預設落在**登入 / 註冊**，把**Email 驗證碼**留作第三個
分頁。底層 API 如果你想直接 curl：

```bash
curl -X POST localhost:18080/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

它會回傳 token 並建立帳號；用同樣的 body 打 `POST /password/login` 就能再次登入。
`username` 可以代替 `email`。雜湊是 `pgcrypto` 在 Postgres 內部算的 bcrypt
（`crypt()` / `gen_salt('bf', 12)`），所以密碼從不在 Python 裡被雜湊、比較或寫進
日誌，本 repo 也不預置任何密碼。對已經設過密碼的帳號，`register` 會拒絕而不是覆蓋
——這個端點不驗證所有權，允許它改密碼等於送出一個帳號接管原語。密碼錯誤和帳號不存在
會回傳完全相同的訊息，這是刻意的：區分開來就等於允許枚舉帳號。

要為 Docker 部署加上自己的地址、又不改動被 git 追蹤的檔案，就把整張表用環境
變數傳進去——設定優先序是 `環境變數 > config.{env}.yaml > config.yaml`，而
JSON 字串會被解析：

```bash
docker compose run -e EMAIL_PREDEFINE_CODES='{"you@example.com":"424242","caregiver@mirobody.ai":"111111"}' mirobody
```

它是**取代**而不是追加，所以還想保留的 `exp*` 帳號要一起列上。若要對任意地址寄
出真實驗證碼，改為設定 `EMAIL_SMTP_*`，白名單就不再有作用。

### 👨‍👩‍👧 關愛圈 demo —— 先問，再上傳

`compose.yaml` 裡設了 `SEED_DEMO_DATA=true`，所以 Docker 這條路啟動完，你的關愛圈
裡已經有一位合成使用者：**Demo (synthetic)**——兩年跨度 244 个指标，另有五份 markdown
文件可供 agent `read_file`。用 `caregiver@mirobody.ai`（验证码 `111111`）登入，你
自己什麼都沒有；你讀的是別人的紀錄。

**從提問開始，而不是從資料表開始。** 在 Ask 頁問：

> *「她最近一次的 LDL 是多少？和一年前比怎麼樣？」*

在剛灌好的部署上，答案來自她真實的歷史：

```
| 日期       | LDL (mmol/L) |
| 2024-04-16 | 3.4          |
| 2024-10-15 | 3.2          |
| 2025-04-15 | 3.1          |
```

……然後它會主動指出最近一次面板已經一年多了、值得再查一次。這正是 demo 後半段的
引子。

**接著給它一個檔案。** `mirobody/demo/lab_report_2025-10-15.pdf` 是她**下一次**的
面板，刻意從灌入資料裡留出——所以上傳它不是空操作，而是資料庫裡確實沒有的資料。把它
拖到 Data 頁（或 Ask 頁的 ＋），看著 ① 收集 和 ② 標準化 各就各位：PDF 被讀取、
十二個分析物帶著單位出來、每一個解析到標準碼、LDL 序列多出第四個點。再問同一個
問題，答案就變了。

其他能落在灌入資料上的問題：*「她哪些結果超出參考區間？」*、*「她的睡眠和去年冬天
比有變化嗎？」*、*「幫我總結她最近一次化驗」*。

所有數值都是合成的。這條軌跡由
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) 为
[ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) 產生，
以一個 200 KB 檔案加一份 5 KB PDF 固化在本 repo，所以灌資料不需要連網、不需要從
HuggingFace 下載、也不需要任何 API key——PDF 抬頭就印著 "SYNTHETIC SAMPLE"。灌入
是 upsert，重啟不會灌重。若要讓部署承載真實資料，把 `SEED_DEMO_DATA` 设为 false。

回答問題需要 LLM key；瀏覽紀錄和上傳檔案不需要。指标名沿用数据源本身的拼写
（`AlanineAminotransferase-ALT`），而不是你自己上傳時得到的顯示名稱——那層打磨來自
dim/embedding 環節：配上 embedding key，`IndicatorSyncTask` 會把它們整理好。

### 🌱 灌入資料

裝好之後登入進去是一個空的資料庫——第一印象不好，而且對 agent 做的任何改動都
無從評估。兄弟專案 [mirobody-eval](https://github.com/thetahealth/mirobody-eval)
會用一位合成使用者五年的軌跡把它填滿，然後替你的部署打分：

```bash
uv run python -m generator.eslbench.prepare_data                   # 從 HuggingFace 抓約 20 MB
uv run python -m generator.eslbench.seed_mirobody --users user5086@demo
uv run python -m benchmark.basic_runner eslbench sample200-20260430 \
    --target-type mirobody --limit 20
```

灌資料需要 `pip install mirobody`，且設定要指向你要填的那個部署，另外還需要一組
embedding key；打分則需要該部署的 HTTP 服務處於執行中（`MIROBODY_BASE_URL`，
預設 `http://localhost:18080`）。加上 `--hold-out-exams 1` 可以把最近一次化驗
面板留在資料庫外，再用 `generator.eslbench.labreport` 產生成 PDF——這樣檔案上傳
這條路徑就有了資料庫裡確實沒有的資料。所有數值都是合成的，PDF 首頁也會註明。

### 擴充它——工具與技能

Mirobody 採用**「工具優先（Tools-First）」**的設計哲學：一個工具就是一個單純的 Python 函式，一個技能就是一個單純的 Markdown 檔案。不需要註冊，不需要綁定邏輯。

#### 🐍 Python 工具

工具模組會從 `MCP_TOOL_DIRS` 裡列出的目錄自動被發現（預設是 [`mirobody/agent/tools/`](mirobody/agent/tools/)——可以在 `config.{env}.yaml` 裡加上你自己的目錄）。每一個函式同時是一個 REST 工具，**也**是一個 MCP 工具，不管是本機還是遠端 HTTP 都一樣。**👉 開發者指南請見 [TOOLS](mirobody/agent/tools/README.md)。**

```python
# your_tools_dir/my_tools.py
def analyze_data(input_data: str) -> dict:
    """
    Description of this tool.

    Args:
        input_data: Description of this argument.

    Returns:
        Description of the return value.
    """
    return {"result": "analysis"}
```

> **🔐 JWT 認證**：如果你的工具需要知道是誰在呼叫，加一個
> **`user_info: Dict[str, Any]`** 參數，但不要把它寫進 docstring 的
> `Args:` 裡。伺服器會用驗證過的 JWT 自動把這個參數填進去，並且把它從工具
> 的 schema 裡隱藏起來，所以模型永遠看不到、也不會自己提供這個值：
>
> ```python
> async def my_tool(self, query: str, user_info: Dict[str, Any]) -> dict:
>     user_id = user_info.get("user_id")     # verified, not model-supplied
> ```
>
> **不要用 `user_id` 這個參數。** 這裡以前就是這樣寫的，而那樣寫出來的工
> 具，又壞又不安全：`user_id` 不是那個會被自動注入的參數名稱，所以伺服器
> 不會幫你填它，它會留在工具的 JSON Schema 裡讓外界看得到——也就是說，是
> 模型自己提供這個值，任何 MCP 客戶端都可以換一個值，就拿到別人的資料。工
> 具載入現在只要看到這種寫法，就會大聲發出警告。

#### 📖 Agent Skills

Mirobody 透過 [deepagents](https://docs.langchain.com/oss/python/deepagents/overview) 原生的 `SkillsMiddleware` 支援 **[Agent Skills](https://agentskills.io/)**——用的就是 LangChain 自己的 deep agent 所使用的那套機制，不是自己另外做的一套載入器：

- 一個技能就是一個含有 `SKILL.md` 的目錄（YAML frontmatter：`name` + `description`；內文：實際的指示）。除此之外不需要任何其他東西。
- 技能目錄來自設定裡的 `SKILL_DIRS`；預設隨套件附上的 [`mirobody/agent/skills/`](mirobody/agent/skills/) 會跟著 wheel 一起發布，並以唯讀方式掛載在 agent 虛擬檔案系統裡的 `/skills/`。
- **漸進式揭露**：agent 在啟動時就會看到每個技能的 frontmatter，只有在任務真的需要時，才會透過 `/skills/` 這個掛載點去讀取完整內文——功能豐富，卻幾乎不佔用常駐的上下文。

隨附的 [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md) 這個技能，就是參考範例：它把「讀原始文件 / 對照報告上印出來的參考值來標示異常 / 不做診斷」這套流程寫成規則，你要做自己的技能，照這個形狀抄就對了。

```
mirobody/agent/skills/
└── lab-report-walkthrough/
    └── SKILL.md          # frontmatter (name, description) + instructions
```

#### 🌐 HTTP 遠端 MCP 伺服器

Mirobody 的 MCP 伺服器支援**透過 HTTP/HTTPS 遠端存取**，這讓你可以：

- **雲端部署**：把你的 MCP 伺服器部署到任何雲端平台上
- **ChatGPT Apps**：透過 HTTPS 跟 OpenAI 的 ChatGPT Apps 整合
- **跨網路存取**：不只 localhost，從任何地方都能存取這些工具
- **OAuth 安全機制**：用 OAuth 認證來保護遠端存取的安全

要啟用遠端 HTTP 存取，在你的 `config.{env}.yaml` 裡設定 `MCP_PUBLIC_URL`：

```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

這樣一來，你的 MCP 伺服器就能透過設定好的 HTTPS 端點被存取，可以直接用在遠端整合上。

#### 🔑 你的個人 MCP 網址

每一個登入的使用者都可以產生一個**個人 MCP 網址**：打開網頁客戶端 →
**Settings → MCP Url → Copy**，再貼到任何 MCP 客戶端裡（Claude
Desktop、Cursor、Cherry Studio……）。這個網址裡嵌了一個只屬於你帳號的私密
憑證——不需要跑一遍 OAuth 流程，客戶端從第一次呼叫開始，讀到的就是*你自
己*的指標。請把它當成密碼一樣對待。

---

## 🔌 HTTP API

刻意分成兩個介面。

**紀錄 API——外形跟 [Mirobody Cloud](https://docs.mirobody.ai/en/api-reference/) 一樣。**
如果你讀過平台的文件，這些東西你已經都認識了：一樣的請求內容、一樣的回應
格式、一樣的欄位名稱，所以針對自架部署寫的程式碼，看起來就跟針對雲端版寫
的程式碼一樣。

| 端點 | 功能 |
| --- | --- |
| `POST /api/standardize` | 丟報告文字進去，吐出標準化的讀值——`{object: "extraction", data: [...]}`。除非帶 `store=true`，否則只是試跑不會真的寫入。 |
| `POST /api/data` | 寫入結構化紀錄（`records[]`，每次呼叫最多 500 筆）。每一筆寫入的資料，進來的時候都會先標準化。 |
| `GET /api/data` | 把資料讀回來，最新的排最前面——`{object: "list", data: [...], has_more}`，每筆讀值各自帶著自己的 `loinc_code`。 |
| `DELETE /api/data` | 依 `id`、依 `indicator`，或用 `all=true` 來刪除。 |

```bash
curl localhost:18080/api/data -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"indicator":"fasting_glucose","value":5.6,"unit":"mmol/L","time":"2026-08-19T07:30:00Z"}]}'
# {"status":"ok","ingested":1,"standardized":1}
```

**跟雲端版契約刻意不同的三個地方**，方向都一致——因為這是你自己的機器，
不是一個多租戶平台：

- **沒有 `/v1` 前綴。** 這些路徑不是雲端版的契約，不應該裝作跟它共用同一
  套版本編號。
- **沒有 `user` / `retention` / `session_id` / `mb_live_*` 這些欄位。** 主
  體識別、到期排程、計費，這些都是給多租戶平台用的維運機制；在這裡，你的
  JWT 就說明了你是誰，一筆資料會一直留著，直到有人把它刪掉為止。
  `retention` 和 `session_id` 會被*接受但忽略*，而不是直接拒絕——畢竟一個
  平台文件教你要送的欄位，卻換來一個 400，對誰都沒有幫助。
- **`DELETE /api/data` 需要明確指定範圍。** 雲端版的端點把「沒有篩選條件」
  當成「全部」，這在一個維運者刻意核發的金鑰背後是合理的。但在這裡，一次
  打錯字的 curl，可能只差一個按鍵就刪掉一個人的整份紀錄，所以最大的範圍就
  只到 `all=true`。

**網頁客戶端自己用的 API**（`/api/v1/health-indicators`、`/api/chat`、
`/api/v1/pulse/*`、`/files/*`、`/invitation/*`）維持自家慣用的
`{code, msg, data}` 格式。這是內建的前端實際在講的那套 API；如果你要換掉
前端，就針對這一套來寫；如果只是要把資料寫進去或讀出來，就用上面的紀錄
API。

## 🔐 你會在哪裡用到它

| 介面 | 網址 | 是什麼 |
| --- | --- | --- |
| **你的網頁客戶端** | `http://localhost:18080` | 你的部署環境所提供的內建應用程式——下面全部功能都在這裡。 |
| **你的 MCP 端點** | `http://localhost:18080/mcp` | 給 Claude Desktop / Cursor 用；在 Settings 裡產生一個個人網址。要用 HTTPS/遠端（ChatGPT Apps、OAuth）就設定 `MCP_PUBLIC_URL`。 |
| **雲端聊天服務** | [chat.mirobody.ai](https://chat.mirobody.ai/) | 如果你不想自己架，這是雲端版的客戶端。 |
| **API 平台** | [platform.mirobody.ai](https://platform.mirobody.ai/) | 金鑰、用量，以及可以在上面繼續開發的健康資料 API。 |

內建的網頁客戶端是一個完整的終端使用者應用程式，不是一個 demo 空殼：

- **資料**（`/data`）——把檢驗報告 PDF、報告照片（含 HEIC）、Excel/CSV、
  音訊、文字/Markdown，以及原始基因型檔案，拖拉進來就能上傳。擷取過程會
  把它們變成標準化的指標；每一筆讀值都連結回它的**來源檔案**，而且可以直
  接**就地修正或刪除**（只限自己的紀錄）。
- **詢問**（`/ask`）——針對自己的紀錄對話：DeepAgent 會自己找到資料、畫
  成圖表，並且針對每個答案回報**token 用量**（是 token 數，不是憑空捏造
  的美金金額）。也可以選第二個模型，把答案並排比較。
- **照護圈**——代替和你分享資料的人上傳與提問，並以逐人同意為前提。

登入方式：email 驗證碼（SMTP）、Google/Apple OAuth，或是上面預先建立好的
demo 帳號——全部都在 `config.{env}.yaml` 裡設定。

---

## 🧪 測試

```bash
pip install -e '.[test]'
pytest        # 495 tests, ~9s — no database, no network, no API key
```

測試檔案就放在它們測的程式碼旁邊，所以單純執行 `pytest` 就是完整的測試套件。其中兩個測試，扛著這個專案對外公開的說法：`test_engine_coverage.py` 就是前面提到的 197/197 解析器成績，而 `pulse/gate_tests/` 則是把每一家廠商的原始資料，跟它標準化後的結果做快照比對。

**👉 [docs/testing.md](docs/testing.md)** ——目錄結構、markers、快照重新產生的方式，以及發布前的檢查關卡（`lint-imports`、`check_wheel_data.py`）。

---

## 📚 文件

**[docs.mirobody.ai](https://docs.mirobody.ai/)** 是統一的文件平台——部署方式、API 平台，還有這個開源引擎，三者會隨著彼此演進持續保持同步。

repo 裡的文件只遵守一條規則：**每個套件都帶一個簡短的 `README.md` 說明自己是什麼；長篇的說明文件則放在 [`docs/`](docs/) 裡**，這樣 `pip install` 就不會把貢獻者才需要看的文件，一起拖進 `site-packages`。

| | 主題 | 位置 |
| --- | --- | --- |
| | **可執行的範例** | [`examples/`](examples/README.md) |
| ① | 收集——pulse 引擎 | [`mirobody/pulse/`](mirobody/pulse/README.md) |
| ① | **連接 Garmin / Oura / Whoop** | [docs/provider-setup.md](docs/provider-setup.md) |
| ① | 撰寫一個資料提供者 | [docs/provider-guide.md](docs/provider-guide.md) |
| ① | 提供者目錄結構 | [`mirobody/pulse/providers/`](mirobody/pulse/providers/README.md) |
| ① | 檔案處理管線 | [docs/file-processing.md](docs/file-processing.md) |
| ① | Apple Health / CDA 匯入 | [docs/apple-health.md](docs/apple-health.md) |
| ② | 指標搜尋與解析 | [`mirobody/indicator/`](mirobody/indicator/README.md) |
| ② | 健康指標與單位 | [`mirobody/pulse/standardize/`](mirobody/pulse/standardize/README.md) |
| ③ | Agent 開發 | [`mirobody/agent/`](mirobody/agent/README.md) |
| ③ | 工具開發 | [`mirobody/agent/tools/`](mirobody/agent/tools/README.md) |
| ③ | ChatGPT Apps 小工具 | [`mirobody/agent/resources/`](mirobody/agent/resources/README.md) |
| | 設定指南 | [`mirobody/utils/config/`](mirobody/utils/config/README.md) |
| | 測試 | [docs/testing.md](docs/testing.md) |
| | 已知缺口與延後處理的工作 | [docs/roadmap.md](docs/roadmap.md) |
| | 變更紀錄 · 安全性 | [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md) |

---

## 🤝 貢獻方式

貢獻的方式，依照引擎的三個階段來分——挑一條你想走的路：

| 路線 | 可以貢獻什麼 | 通常的規模 |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------ |
| **① 收集** | 一個新的裝置提供者——在一個 `mirobody_<slug>/` 目錄裡實作 [`BasePullProvider`](mirobody/pulse/providers/platform/base.py)，平台啟動時就會自動發現它；[`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) 是最小的參考範例，[`mirobody_whoop/`](mirobody/pulse/providers/mirobody_whoop/) 則是 OAuth2 的範例。或者是幫解析器新增一種檔案格式 | 中 |
| **② 標準化** | **讓一個詞可以被解析。** 找一個結果錯誤或空白的詞——`mirobody resolve "<term>"`——然後在 [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) 加一列，再到 [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) 加一個測試案例。任何語言都可以。這是這個 repo 裡門檻最低、卻真正有用的 PR，而且會直接改動我們公開對外的數字。另外也歡迎：單位對應、分類法修正 | 小 |
| **③ 解答** | 一個 Agent Skill（[`mirobody/agent/skills/`](mirobody/agent/skills/) 底下的 `SKILL.md` 套件——可以照抄 [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md)），一個 MCP 工具，一個圖表 schema | 中 |

找到一份解析錯誤的檢驗報告，或是一個解析不出來的指標名稱？**這就是一個很棒的 issue**——附上（去識別化過的）樣本。PR 的實際流程請見[貢獻指南](CONTRIBUTING.md)。

---

<div align="center">

**[📚 docs.mirobody.ai](https://docs.mirobody.ai/)** · **[💬 chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 platform.mirobody.ai](https://platform.mirobody.ai/)**

Apache-2.0 · 你的資料留在你自己的機器上

</div>
