<div align="center">

# 🚀 Mirobody

**AIネイティブな健康データエンジン ―― 検査値・ウェアラブル・ゲノムを収集し、標準化し、そこから推論する。**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/healthmemoryarena)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)

**[📚 ドキュメント](https://docs.mirobody.ai/)** · **[💬 ホスト型チャット — chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 APIプラットフォーム — platform.mirobody.ai](https://platform.mirobody.ai/)**

**[English](README.md)** · **[简体中文](README.zh-CN.md)** · **[繁體中文](README.zh-TW.md)** · **日本語**

*血液検査、ウェアラブル、ゲノム、画像診断 ―― どれも断片化していて、互換性がない。
AIが健康データを理解する前に、まずこれらの信号を統合し、AIが実際に読める単一の標準に
落とし込む必要がある。それがこのエンジンの仕事である。*

<img src="docs/images/where-your-data-comes-from.ja.svg" alt="ウェアラブルから食事の写真まで —— ひとつの標準形式に、AI が読める形で。" width="920">

</div>

本エンジンが行うことは3つ。コードベース(および後述の「コントリビューション」)もこの3段階 ―― [ドキュメント](https://docs.mirobody.ai/en/api-reference/)が使う**C・S・A**と同じ ―― に沿って構成されている。

| 段階                | 意味                                                                                                     | 場所                                                   |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| **① Collect(収集)** | 信号を取り込む:デバイスプロバイダ3種 + SQLソース・ファイル形式7種・Apple Health                                             | [`pulse/`](mirobody/pulse/) |
| **② Standardize(標準化)**    | 単一標準への統一:任意の測定値を正規コード(LOINC・SNOMED CT・RxNorm)に解決し、単位を正規化し、FHIRとして格納 | [`indicator/`](mirobody/indicator/)                    |
| **③ Answers(応答)**  | 推論:エージェントが仮想ファイルシステム経由で*元の文書*を読み、グラフと引用付きで回答     | [`agent/`](mirobody/agent/)                  |

---

## ⚡ 60秒で試す

サーバー不要、キー不要、ネットワーク不要 ―― 用語解決エンジンは pip install するだけ:

```bash
pip install mirobody
mirobody resolve "hemoglobin" "血红蛋白" "血紅素" "ヘモグロビン"
# all four -> LOINC 718-7
```

```python
from mirobody.engine import resolve
resolve("血红蛋白").loinc   # -> '718-7'   offline: no key, no config, no network
```

面白いのは3つ目の例だ。`血紅素` は `血红蛋白` の単なる字体違いではない ―― 台湾と中国本土では
ヘモグロビンを指す語そのものが**異なる**。`血紅素` をそのまま簡体字にフォールディングすると
`血红素` になるが、素朴な索引はこれに**HbA1c**(別の検査)のコードを返してしまう。
スクリプトのフォールディングだけではこの違いを捉えられず、語彙そのものをキュレーションす
るしかない。標準化レイヤーの仕事の大半はここにある ―― 簡単な行の解決ではなく。

### 実際に呼ぶ2つの関数

`resolve()` は**名前**に答える。`resolve_reading()` は**測定値**に答える ―― この2つが返す
コードが違うのは、LOINCが単位と結果の型そのものをコードの識別性に組み込んでいるからだ:

```python
from mirobody.engine import resolve, resolve_reading

resolve("total cholesterol").loinc                       # '2093-3'   [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")    # '14647-2'  [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")     # '2093-3'   unchanged

resolve("尿糖").loinc                                     # '2350-7'   [Mass/volume]
resolve_reading("尿糖", "阴性")                            # '2349-9'   [Presence]
resolve_reading("尿糖", "5.6", "mmol/L")                   # '15076-3'  [Moles/volume]
```

**値と単位が分かっているなら、それを渡すこと。** 同じ指標でも測定値次第でコードは正反対に
振れる。mmol/Lの結果をmg/dLのコードで記録したり、陰性(阴性)の結果を質量濃度のコードで記録
したりすれば、ひとつの系列に単位が2種類混在し、それに誰も気づかない。
どちらの関数もオフラインかつ決定的で、ループの中で呼んでも安全だ(初回以降は1回あたり約
25 µs)。

どの回答も「どうやってそこに辿り着いたか」を示す。ただし1つだけ、他とは扱いが違う値がある:

```python
r = resolve("血红蛋白")
r.loinc, r.canonical, r.method     # '718-7', 'Hemoglobin [Mass/volume] in Blood', 'lexical'

resolve("绝对不存在的指标名xyzzy").method   # ''         never seen it
resolve("血糖(HbA1c)").method              # 'refused'  two different tests in one string
```

`""` は欠落であり、二次確認の価値がある。`"refused"` はそれ自体が答えだ ―― `血糖(HbA1c)`
は括弧の外が血糖、括弧の内がHbA1cという、ひとつの文字列に2つの検査が入っており、`血脂` は
4つの分析対象を束ねたものであって、どちらも単一のコードでは正しく表せない。ただし、パネル
(項目群)としてのコードを*持つ*パネル用語は拒否にはならない:`blood pressure`(血圧) →
`85354-9`、FHIRのバイタルサインプロファイルが定めるコードで、これは呼び出し側にコンポーネ
ントの存在を期待させる。
**`method == "lexical"` のときだけを識別子として扱うこと**(グルーピングキー、「これらは
同一系列」という判定、FHIRへの写像に使ってよいのはこの場合だけ)。もう一方の種類について
は後述の「Semantic recall」を参照。

### 単位は比較する、仮定はしない

```python
from mirobody.indicator.fhir.units import convert_value, convertible

convert_value(5.6, "mmol/L", "mg/dL", loinc_code="1558-6")   # 100.9  (molar-mass bridge)
convert_value(42.0, "U/L", "[IU]/L")                         # 42.0   (1:1, different families)
convert_value(24.0, "kg/m2", "mg/dL")                        # None   (BMI is not a concentration)
convertible("%", "10*9/L")                                   # False  (a fraction is not a count)
```

`None` は失敗ではなく回答だ:片方をもう片方に見せかけて換算するのではなく、両方の測定値を
別々に報告すること。変換可能性の判定に `unit_family()` を使っては**いけない** ―― これは
LOINCのPROPERTY分類子であり、この問いに対しては両方向で間違っている(`kg/m2` と `mg/dL`
は同じファミリーに属するが変換できない。`U/L` と `[IU]/L` は異なるファミリーだが同一の単位
である)。

ここまではエンジン単体の話。フルスタックを自前でホストし(手順は後述の「Quick Start」を
参照)、サインインして**個人用MCP URL**を発行する(Webクライアント → Settings → MCP Url)。
任意のMCPクライアント(Claude Desktop、Cursor、Cherry Studio)をこのURLに向ければ、自分自身
の健康データエンジンと対話できる:

```json
{ "mcpServers": { "mirobody": { "url": "http://localhost:18080/mcp/<your-personal-secret>" } } }
```

MCPサーフェスは意図的に小さい:

| ツール | 内容 | 必要なもの |
| --- | --- | --- |
| `resolve_indicator` | 任意言語の指標名 → 正規LOINC | 不要 ―― オフライン、ユーザーデータ不要 |
| `normalize_unit` | 自由記述の単位 → 正規UCUM + 比較可能性ファミリー | 不要 ―― オフライン、ユーザーデータ不要 |
| `query_health_indicators` | 自分の記録 ―― 検索・読み取り・集計を**1回の呼び出し**で実行。各結果にLOINC識別子を付与 | アカウント |
| `get_genetic_data` | rsidによる自分の変異情報 | アカウント |

`tools/list` はアカウントごとに正直だ:アカウント紐付けの2つのツールは、そのアカウントが
実際に該当データを保持している場合にのみ列挙される。サーバーが話すのは
[MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/) ―― 現行の
ステートレス版(リクエストごとの `_meta`、`server/discover`、決定的なツール順序)―― で、
古いクライアントに対しては `2024-11-05` までネゴシエーションを下げる。

実行可能なウォークスルー: [`examples/`](examples/README.md) ―― オフライン解決からフルサー
バーのプリフライトまで5本のスクリプト、いずれも実行確認済み。

---

## ① Collect(収集) ―― あらゆる信号を、単一の取り込み口へ

- **単一のプラグイン契約の裏にある本番デバイスプロバイダ群** ―― **Garmin、Oura、Whoop** 向
  けの実戦投入済みプロバイダに加え、pulseプラットフォーム経由で[300以上のデバイス](mirobody/pulse/providers/README.md)。
  プロバイダは1つのディレクトリとして実装する:配置するだけで検出・OAuth・取得スケジュー
  リングが自動的に組み込まれる。

  > **セルフホスト環境で実際にこれらを有効化するために必要なもの。** 各プロバイダはその
  > ベンダーのOAuthクライアントであり、ベンダーの開発者プログラムから**自分で**取得した
  > 認証情報 ―― `config.{env}.yaml` 内の `GARMIN_CLIENT_ID`/`SECRET`、
  > `OURA_CLIENT_ID`/`SECRET`、`WHOOP_CLIENT_ID`/`SECRET`(および各自のリダイレクトURL)
  > ―― を与えるまで休止状態のままになる。認証情報がなくてもモジュールは読み込まれ、
  > `declined to start (not configured)` とログに出る ―― これは正直な状態であり、障害では
  > ない。[`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) はすぐに試せる
  > プロバイダだ:`ENABLE_PGSQL_DEVICE: 1` を設定すれば、次回起動時に
  > `loaded 1 providers` とログに出る。
  > **手順の詳細: [docs/provider-setup.md](docs/provider-setup.md)** ―― 正確なコール
  > バックURL、設定キー、そしてブートログで "not configured"(未設定)と "broken"(故障)
  > をどう見分けるか。

- **Apple Healthはプッシュ専用であり、自作のiOSアプリが必要** ―― エンドポイント自体はここ
  にある([`/apple/health`、`/apple/statistics`、`/apple/cda`](mirobody/server/routers/apple_router.py)、
  [CDA処理](mirobody/pulse/apple/README.md)込み)が、これらは*受け取る*だけで、この
  リポジトリのどこにもHealthKitから取りに行くコードはない。HealthKitは署名済みのiOSアプリ
  からオンデバイスで、かつユーザーがデータ種別ごとに許可した場合にのみ読み取れる ―― Web
  OAuthフローもサーバー間APIも存在しない。したがってセルフホストのWebデプロイに「Apple
  Healthを接続」ボタンが出ないのは正しい挙動だ:欠けているのはHealthKitエンタイトルメント
  を持つiOSクライアントであり、上記のAPIはそのクライアントがPOSTする先である。
- **AIで解析するファイル形式7種** ―― PDF検査報告書、Excel、CSV、画像、音声、
  プレーンテキスト、**遺伝子エクスポート(WeGene)**。LLMによる指標抽出
  ([`pulse/file_parser/`](mirobody/pulse/file_parser/)、13,000行)。
- 取り込みパイプライン:ステージング取り込み → 検証 → 正規化 → 日次集計 →
  [AIインサイト](mirobody/pulse/insight/)として記録にフィードバック ―― ここでループが閉じる。

## ② Standardize(標準化) ―― AIが実際に読める、単一の標準

隣接するオープンソースプロジェクトのどれにもない部分 ―― 単なる参照テーブルではない、
**意味的な標準化レイヤー**:

- **コンセプトグラフ**:ノード440,961・語彙間エッジ22,044,110・**ソースID 595,746件**を
  正規コンセプト(LOINC・SNOMED CT・RxNormの橋渡し)に凝縮し、Git LFS経由で配布
  ([`indicator/`](mirobody/indicator/README.md))。
- **エンベディングによる解決**:自由記述の指標名 → 正規コード。**多言語エイリアス49,253件**
  (中文 22,578・日本語 16,809・他5言語:de・es・fr・ko・ru) ―― `hemoglobin`、`血红蛋白`、`血紅素`、
  `ヘモグロビン` はいずれもLOINC 718-7に帰着する。
- **繁體中文は2つの問題であり、2つとして扱う。** 字体は機械的な問題:クエリは同梱の3,336
  文字テーブル([`zh_fold.py`](mirobody/indicator/zh_fold.py))によってzh-Hant → zh-Hansに
  フォールディングされる ―― 辞書ビルドがコーパスに対して既にやっていることと同じ処理だ。
  語彙はそうではない:台湾の臨床現場では別の語を使うため、`血紅素` をフォールディングすると
  `血红素` になり、HbA1cのコードに帰着してしまう。こうした語は繁体字表記のままキュレーショ
  ンされており、キュレーション済みの行は常にフォールディングに優先する。
- **その主張を言い切るのではなく、測定する。**
  [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) は、オフラインリゾルバを
  実際の健康診断で発注されるパネル ―― 脂質、CBC(血球計数)、代謝、肝機能、甲状腺、ホルモ
  ン、腫瘍マーカー、尿検査、バイタルサイン ―― に対して採点する。報告書に印字されるとおり
  の表記で、英語・简体中文・繁體中文・日本語について用意されており、さらにプラットフォーム
  APIが扱うデバイス/ウェアラブル語彙(`steps`、`resting_heart_rate`、`sleep_duration`)も
  含む。**現在197/197。書かれた当日は32/94だった。** これが採点するのは*臨床的な*正しさで
  あり、解決率そのものではない:`血红蛋白` にHbA1cのコードを返せば失敗として採点され、
  `血脂`(観測値ではなくカテゴリ)は*何も解決しない*ことが要求される ―― 自信満々な誤答は、
  正直な「わからない」より悪いからだ。
- **表記が答えを左右しないための表層代数**([`indicator/lexical.py`](mirobody/indicator/lexical.py)):
  NFKC-liteフォールディング(全角、上付き文字、6種類のダッシュ異体字)に加え、CJK対応トー
  クナイザ、そして検査報告書が印字する `名称(略称)` という形の、条件付きの取り除き処理。
  `ＦＢＧ`、`LDL–C`、`fasting_glucose`、`空腹血糖(GLU)`、`Cholesterol, total` はいずれも
  素の表記と同じコードに到達する。`名称(略称)` の両半分が食い違う場合 ―― `血糖(HbA1c)`
  ―― は、どちらかを選ぶのではなく**未解決のまま**にする。
- **コードを決めるのは測定値であり、名前だけではない**
  ([`engine.resolve_reading`](mirobody/engine.py))。LOINCは単位*と*結果の型の両方をコード
  の識別性に組み込んでいるため、単位が `PROPERTY` を、値の種類が `SCALE_TYP` を決める。
  `5.0 mmol/L` → 14647-2、`193 mg/dL` → 2093-3、`阴性` → `[Presence]` バリアント。同梱コ
  ーパスの半分は非`Qn`(79,368行中38,687行)であり、数値だけで制約するリゾルバはその半分に
  対して盲目になる。
- UCUMファミリー(約310)への**単位正規化**、および
  [変換処理](mirobody/indicator/fhir/units/convert.py) ―― 次元解析、LOINCコードをキーとす
  るモル質量ブリッジ、`%` と `10*9/L` に対する明示的な変換拒否。標準pulse指標316種、
  FHIR R4形式で出力。
- 臨床カテゴリ25種のタキソノミー(バイタルサイン、臨床検査、身体測定、……)。

### 🧪 Semantic recall ―― オプトインであることの理由

ここまでの話はすべて字句的(lexical)なもの ―― 同梱の語彙とテーブル引きだ。知らない語には
答えを保留する。これは長所であると同時に限界でもある。第二の層
([`indicator/semantic.py`](mirobody/indicator/semantic.py))が存在する:
[`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py) が構築するLOINC
コーパスのエンベディングに対する、コサイン類似度によるリコールだ。

```python
from mirobody.engine import resolve_with_semantic_fallback

out = await resolve_with_semantic_fallback(["空腹血糖", "some unheard-of assay"])
out[0].method    # 'lexical'  — the lexical tier answered; the fallback never saw it
out[1].method    # ''         — no matrix installed, so nothing to fall back TO
                 # 'semantic' once one is, and that means "a suggestion", not an identity
```

**行列は同梱されていない**ため、素の `pip install` ではこの層は何もせず、`resolve()` と
全く同じ結果を返す。有効化するには `MIROBODY_SEMANTIC_INDEX` にそれを指すパスを設定する。
行列は約198 MBあり、エンベディング用のキーが必要で、コーパスとクエリは**必ず同じモデル**
から生成されたものでなければならない ―― 組み合わせが食い違っていてもエラーにはならず、
自信満々のナンセンスを返すだけだ。

導入した後もオプトインのままである理由は、チューニングの問題ではなくこの層固有の性質にあ
る:**コサインリコールは「わからない」と言えない。** 見たことのない語を尋ねられても、正解
を返すときと同じ確信度で最近傍を返してしまう。しかもLOINC自身の質問票コーパスには、いかに
もそれらしい近傍がいくらでも用意されている。この2つを分けるスコアの閾値は存在しない。

それでもこの層が役に立つのは、ここに届く時点で測定値がすでに裸の文字列ではないからだ。手
前の抽出パスは健康と無関係な内容には何も返さず、`{indicator, value, unit}` を渡してくる。
そのため本層は測定値が示唆する情報 ―― 値の種類から導く `SCALE_TYP`、単位の次元から導く
`PROPERTY` ―― で候補を絞り込み、`loinc_skip.txt` が既に列挙している非臨床的な行を除外でき
る。

つまり:この層は人間やモデルが後で確認する前提でコードを**提案する**ために使うものだ。識
別子の基盤として使うのはあくまで字句層である。

## ③ Answers(応答) ―― 原本を読むエージェント

この層を利用する方法は**2通り**あり、それぞれに専用のエージェントが対応する ―― 違いは*ツ
ールループを誰が回すか*にある:

| | **DeepAgent** ―― 自分でエンジンを動かす | **BaseAgent** ―― 自分のモデルがこちらを利用する |
| --- | --- | --- |
| ツールループの実行場所 | ここ、つまり自分のデプロイの中 | **LLMプロバイダ**側、HTTP経由の `/mcp` に対して |
| 対象 | 全体をセルフホストする場合 | Claude Desktop・Cursor・ChatGPT Apps・任意のMCPクライアント |
| 追加機能 | 仮想ファイルシステム、QuickJS、Agent Skills、グラフ描画 | MCPツールサーフェスが公開するものすべて ―― 隠されているものはない |

- **DeepAgent** ―― メインのエージェント。[deepagents](https://github.com/langchain-ai/deepagents)
  0.7 / LangChain 1.3上に構築。マルチプロバイダ対応(OpenAI、Gemini、Anthropic、
  OpenRouter、任意のOpenAI互換エンドポイント)。**PostgreSQLを裏付けとする仮想ファイルシス
  テム**(`/uploads`、`/library`、`/memories`、`/skills`)により、モデルは劣化を伴う抽出結
  果ではなく、*元の*PDFをマルチモーダルに `read_file` できる。実計算のためのインプロセス
  JSインタプリタ(QuickJS)。ターン単位のモデル呼び出し予算と、それを超えた際の穏やかな停
  止。
- **BaseAgent** ―― 意図的にLangChainを使わない。MCPサーバーをプロバイダ側に渡し(OpenAI
  Responsesの `mcp_server`、GeminiのInteractions)、結果をストリーミングする。これにより、
  サードパーティ体験を自ら予行演習している状態になる:**BaseAgentが単独でできないことは、
  外部のMCPクライアントにもできない。** グラフは描かない ―― 可視化は利用側のクライアント
  が持ち込むものだからだ。
- **MCPサーバーを内蔵**([`mcp/`](mirobody/mcp/)) ―― すべてのツールはHTTP経由のMCPツール
  としても機能する。MCPクライアント*かつ*OAuth対応のMCPサーバーとして動く。**Agent
  Skills**(SKILL.md)はdeepagents純正のSkillsMiddlewareを通じて
  [`mirobody/agent/skills/`](mirobody/agent/skills/)から提供される。
- ケアサークル共有 ―― 相手ごとの同意に基づく:

<div align="center"><img src="docs/images/your-care-circle.ja.svg" alt="ケアサークル —— 信頼できる人をメールで招待する。決定権は本人にある:メンバーの削除やスレッドの共有解除はいつでも可能で、健康データの共有は許可するまでオフ。" width="920"></div>

---

## 🏗️ アーキテクチャ

エンジンは3段階 ―― **① Collect(収集) → ② Standardize(標準化) → ③ Answers(応答)** ――
であり、パッケージ構成もそれをそのまま表している。

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

### インストール規模ごとに得られるもの

| インストール | できること | フットプリント |
| --- | --- | --- |
| *wheel + numpyのみ* | `from mirobody.engine import resolve` ―― オフラインリゾルバ | mirobody部分で**33 MB**(numpy込みで76 MB) |
| `pip install mirobody` | + `mirobody parse`(LLMキー1つ)・ファイル解析(PDF/Excel/音声)・FHIR出力 | 233 MB、90パッケージ |
| `pip install 'mirobody[server]'` | + HTTP APIとMCPエンドポイント | Postgres + Redisが必要 |
| `pip install 'mirobody[agents]'` | + DeepAgent/BaseAgentと `mirobody serve`(`[server]` を含む) | + LangChainスタック |
| `pip install 'mirobody[indicator-build]'` | 用語バンドル自体の再構築 | LOINC/UMLSソースが必要 |

サイズはクリーンなvenvで実測したものであり、推定値ではない。**mirobodyの33 MBのうち
24 MBは同梱のLOINCデータ**である ―― これはオーバーヘッドではなくリゾルバそのものであり、
ネットワークを切った状態でも標準化が機能する理由がここにある。

#### 最小構成の利用範囲 ―― そして意図的に含めていないもの

`pip install mirobody` が運ぶのは `resolve()` が読むものそのものだけであり、それ以外は何
もない:

| 同梱物 | 読み込む側 |
| --- | --- |
| `res/fhir_loinc_bundle.tar.gz` | 92.1万キーのエイリアス索引、LOINC軸テーブル、頻度事前分布 |
| `res/fhir_meta.csv.gz` | エイリアス索引が指す67.7万名のコーパス |
| `res/aliases_src/*.tsv` | 多言語エイリアス行 49,253件(中文 22,578・日本語 16,809・他5言語:de・es・fr・ko・ru) |
| `res/resolver_overrides.tsv` | 手作業による修正、および意図的な非回答 |

以前は同梱されていたが今は同梱されていないアーティファクトが4つあり、合計**28 MB**。実行
時にこれらを読むコードはどこにもない:`server/`、`agent/`、`pulse/`、`mcp/`、`task/` を
`concept_graph` や `taxonomy` でgrepしても何も出てこない。うち3つ ――
`fhir_concept_graph.bin`、`fhir_taxonomy.bin`、`fhir_snomed_ct_bundle.tar.gz` ――
はgitチェックアウトから動く[`indicator/`](mirobody/indicator/)のバンドルビルドツール、お
よびv2セマンティックパイプライン(こちらはさらに、まったく配布されていないエンベディング
行列も必要とする)のために、リポジトリには残っている。4つ目の `fhir_id_map.npy` は**リポ
ジトリから完全に削除済み**だ:これは正規IDを、あるデータベースの主キーである
`fhir_indicators.id` に対応付けるものであり、他の誰にとっても意味を持たなかった ―― 必要
なら `indicator id-map` で自分用に再生成すること。SNOMEDバンドルを外したことで、pipを使う
全ユーザーからAffiliate Licenceの義務も取り除かれている。`scripts/check_wheel_data.py` は
今、両方向を検査する ―― 上記5つが実在すること、この4つが存在しないこと。

**最小構成でできないこと**:字句層が取り逃した語の解決。解決処理は同梱語彙に対する完全一
致キーおよびエイリアステーブルの参照と、[`indicator/lexical.py`](mirobody/indicator/lexical.py)
の表層代数だけで行われる。エンベディングによるリコールはここにはない ――
これを実装するのは[`fhir/resolve/pipeline.py`](mirobody/indicator/fhir/resolve/)であり、
[`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py) が構築する約
200 MBのLOINCエンベディング行列と、エンベディング用のAPIキーを必要とする。ここでの取り逃
しは正直な取り逃しであり、直し方は `resolver_overrides.tsv` に1行加えるだけだ ―― 詳しくは
後述の「コントリビューション」を参照。

データベースドライバ、HTTPサーバー、S3・メールクライアントは以前デフォルトのインストール
に含まれていたが、`[server]` に移動した(`[agents]` はこれを引き込む)。エンジンをライブ
ラリとして使うだけなら、もはやPostgresドライバのコストを払う必要はない。

### たった1つのルール、機械的に強制

**エンジンはエージェントフレームワークが1つも入っていない環境でもimportできなければなら
ない。** `langchain*`、`deepagents`、`langgraph` は `agent/` と `server/` の下でのみ許可さ
れる ―― langchain自身が `langchain-core` に対して使っているのと同じ層分けだ。
`pyproject.toml` の2つの `[tool.importlinter.contracts]` が違反時にビルドを失敗させる
(関数ローカルのimportも対象):

```bash
pip install -e '.[test]' && lint-imports
```

これにより、`mirobody.engine` はサードパーティパッケージがnumpyしか入っていない環境でも指
標を解決できる。`utils/` は意図的にリーフ(末端)にしてある ―― かつて `utils/db.py` のト
ップレベルにあった `from sqlalchemy import text` が、接続を一度も開かない関数にまでデータ
ベーススタック全体を必須要件として課してしまったことがある。`utils/`、`user/`、`task/` は
3段階そのものではなく、3段階が立つ基盤インフラだ。現時点で意図的に境界を越えている縫い目
が1箇所あり、`pyproject.toml` の `ignore_imports` と [docs/roadmap.md](docs/roadmap.md) に
その解消計画とともに記録されている。

### データフロー、端から端まで

```
vendor APIs / files / Apple Health          ① pulse
        └─> StandardPulseData ─> validate ─> normalize ─> daily rollups
                 └─> indicator names ─> ② indicator: canonical codes (LOINC·SNOMED·RxNorm)
                          └─> FHIR R4 rows in Postgres
                                   └─> ③ agent: read ORIGINAL documents through the
                                       virtual fs, compute, chart, answer — and insights
                                       feed back into the record, closing the loop
```

---

## 📊 Benchmarks(ベンチマーク) ―― 「信じてほしい」ではなく、評価一式を出す

私たちのヘルスAIベンチマークは**Hugging Face上でカテゴリ最多ダウンロード**を記録している
(いずれも4,000件超):

| ベンチマーク                                                                       | 測定対象                                                                                                                                                | ダウンロード数 |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench)         | イベント駆動型の長期健康エージェント ―― 合成ユーザー100人、クエリ1万件、プログラムによる正解データ([arXiv:2604.02834](https://arxiv.org/abs/2604.02834)) | 4,800+    |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | 医療ハルシネーション                                                                                                                                          | 4,500+    |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | 有害な医療アドバイス                                                                                                                                            | 4,300+    |

いずれも**[mirobody-eval](https://github.com/thetahealth/mirobody-eval)**(私たちのオープ
ンな評価フレームワーク)を使えば1コマンドで再現できる。そのジェネレータは、デモやテストで
使う合成(PHIを含まない)健康データも生成する。

エンジン自体にも同じ基準を課している。**リゾルバカバレッジ** ―― ② Standardize(標準化)は
実際の検査報告書にある日常的な検査項目に正しく名前を付けられるか ―― は本リポジトリの中
で、オフラインかつ1秒未満で実行できる:

```bash
pytest mirobody/test_engine_coverage.py -s
#   offline resolver coverage: 197/197 = 100%
```

始まりは**32/94**だった ―― ベンチマークはその後197件まで増えた。ギャップの原因はコンセプ
トグラフではなく、索引がLOINCの長い名称から構築されているために、`LDL-C` は知っていても
`LDL cholesterol` は知らず、葡萄糖は知っていても `血糖` は知らず、`血红蛋白` にHbA1cのコー
ドを返していたことだった。どちらの失敗パターンも、直すのに必要なのはTSVの1行だけだ ――
詳しくは後述の「コントリビューション」を参照。

---

## ⚡ Quick Start(クイックスタート)

### 📋 前提条件

- **Docker & Docker Compose**:インストール済みで、起動していることを確認する。
- **Git**:リポジトリをクローンするために使う。
- **Git LFS**:バイナリデータファイル(例:`fhir_concept_graph.bin`)を取得するために必要。
  Linuxでは `apt install git-lfs`、macOSでは `brew install git-lfs` で導入する。Git for
  Windowsには標準で含まれている。導入後に一度だけ `git lfs install` を実行する。

### Dockerでデプロイ

```bash
git clone https://github.com/thetahealth/mirobody.git
cd mirobody
./deploy.sh
```

このスクリプトが行うこと:

- 安全な `.env` ファイルを生成する。
- デフォルトの設定ファイル(`config.localdb.yaml`)を作成する。
- Dockerイメージをビルドする。
- サービス(Postgres、Redis、Mirobody)を起動する。

その後、Webブラウザで `http://localhost:18080` を開く。

他の何より先に知っておくべき、3つのキーと1つの落とし穴:

> - **LLMキー**:`OPENROUTER_API_KEY` がDeep agentを動かす。
> - **エンベディングキー** ―― これは*別の*キーであり、あくまで2本目として必要になる:
>   ワーカーの指標同期処理は名前を `th_series_dim` ベクター列にエンベディングするが、これ
>   に対応するプロバイダは2つしかない。`EMBEDDING_PROVIDER` はデフォルトで `gemini`
>   (`GOOGLE_API_KEY`)、もう一方は `qwen`(`DASHSCOPE_API_KEY`)。OpenRouterのキーしか
>   ない場合、チャットは動くが**Health indicatorsは0のまま**になる ―― ワーカーのログにエ
>   ンベディングの失敗が出る。`EMBEDDING_PROVIDER: openrouter` を設定してもこれは解決せ
>   ず、解決したふりもしない:欠けている列名を挙げて例外を発生させる。これは、静かに何も
>   書かなかった同期処理と、そもそも書くことが何もなかった同期処理とが見分けられなくなる
>   のを避けるためだ。このプロバイダは `text_embedding` の呼び出し元と、ファイルベースの
>   Semantic recall層(前述)のために存在しており、どちらもデータベースの列には触れない。
> - キーは `config.{env}.yaml` に置く。Mirobodyは初回ロード時に、生成された
>   `CONFIG_ENCRYPTION_KEY` でこれを暗号化する。
> - 初回起動には(スキーマ作成のため)約1分かかる ―― `SQL files initialization completed`
>   が出るまで待つこと。
>
> 設定の完全ガイド: [CONFIG](mirobody/utils/config/README.md) ·
> [DATABASE](mirobody/schema/README.md) · [docs.mirobody.ai](https://docs.mirobody.ai/)

### 🐍 ローカルでのPython開発

pg/redisはDockerで動かしつつ、コード自体はホスト上で実行する ―― 通常のデバッグループ:

```bash
docker compose up -d pg redis
pip install -e '.[agents]'        # Python ≥3.12; engine-only is `pip install -e .`
echo "ENV=localdb" > .env
# create config.localdb.yaml overriding PG_HOST/PG_PORT/REDIS_* to the
# containers' published ports, add your LLM keys, then:
mirobody serve
```

手順ごとのウォークスルー(ポート、暗号化キー、`[cn]` エクストラ)は
[docs.mirobody.ai](https://docs.mirobody.ai/) と
[CONFIG](mirobody/utils/config/README.md) にある。

**CLI一覧**

| コマンド | 内容 |
| --- | --- |
| `mirobody parse <file>` | 検査報告書を入力し、標準化されたLOINCテーブルを出力 ―― LLMキー1つ、インフラ不要 |
| `mirobody resolve <terms…>` | オフラインでの指標名解決 ―― キー不要、設定不要、ネットワーク不要 |
| `mirobody serve` | HTTPサーバー(チャット、MCP、API)を実行 ―― `[agents]` が必要 |
| `mirobody worker` | バックグラウンドタスクワーカー(指標同期、プロファイル更新)を実行 |

### 👤 初回ログイン

事前に用意されたデモアカウントでサインインする ―― サーバーは起動時にこれらを出力する:

- **メールアドレス**:`exp1@mirobody.ai`(`exp2@` / `exp3@` も利用可)
- **確認コード**:`111111`

これらは `config.yaml` の `EMAIL_PREDEFINE_CODES` から来ている:SMTPが未設定の場合、サイ
ンインできるのは事前定義済みのアドレスのみだ。自分のアドレスをそこに追加するか、
`EMAIL_SMTP_*` を設定して実際のコードを送信するようにする。

### 拡張する ―― ToolsとSkills

Mirobodyは**「Tools-First」**の思想を採用している:toolはただのPython関数、skillはただの
Markdownファイル。登録処理もバインディングロジックも不要。

#### 🐍 Python Tools(Pythonツール)

ツールモジュールは `MCP_TOOL_DIRS` に列挙されたディレクトリから自動検出される(デフォル
ト: [`mirobody/agent/tools/`](mirobody/agent/tools/) ―― 自分のディレクトリを
`config.{env}.yaml` に追加できる)。すべての関数はRESTツール**かつ**MCPツールとして、ロー
カルでもリモートHTTPでも機能する。**👉 開発者ガイドは [TOOLS](mirobody/agent/tools/README.md)
を参照。**

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

> **🔐 JWT認証**:ツールが呼び出し元のユーザーを必要とする場合は、
> **`user_info: Dict[str, Any]`** パラメータを受け取り、docstringの `Args:` からは除外す
> る。サーバーは検証済みのJWTからこれを埋め込み、ツールスキーマからは隠すため、モデルはこ
> れを目にすることも、値を渡すこともない:
>
> ```python
> async def my_tool(self, query: str, user_info: Dict[str, Any]) -> dict:
>     user_id = user_info.get("user_id")     # verified, not model-supplied
> ```
>
> **`user_id` パラメータを受け取ってはならない。** このノートはかつて実際にそう書いてい
> たが、それは壊れていて安全でもないツールを生む:`user_id` は注入対象のフックではないた
> めサーバーはこれを埋めず、ツールのJSON Schema上で可視のままになる ―― つまりモデルがこ
> れを供給することになり、どのMCPクライアントも別の値を渡すことで他人のデータを要求でき
> てしまう。ツールローディングは今、この形を検出すると強く警告する。

#### 📖 Agent Skills

Mirobodyは[deepagents](https://docs.langchain.com/oss/python/deepagents/overview)純正の
`SkillsMiddleware` を通じて**[Agent Skills](https://agentskills.io/)**をサポートする ――
LangChain自身のdeep agentが使っているのと同じ仕組みであり、独自のローダーではない:

- skillとは `SKILL.md` を持つディレクトリのこと(YAMLフロントマター:`name` +
  `description`;本文:指示内容)。それ以外は何も要らない。
- skillディレクトリは設定の `SKILL_DIRS` から来る。パッケージ同梱のデフォルト
  [`mirobody/agent/skills/`](mirobody/agent/skills/) はwheelに同梱され、エージェントの仮
  想ファイルシステム内の `/skills/` に読み取り専用でマウントされる。
- **段階的開示(progressive disclosure)**:エージェントは起動時にすべてのskillのフロント
  マターを目にし、本文全体は `/skills/` マウント経由で、タスクが必要とするときだけ読む
  ―― 常駐コンテキストは最小限に、能力は豊富に。

同梱の[`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md)
skillが参照実装であり、「原本を読む/印字された基準範囲と照合してフラグを立てる/診断はし
ない」というワークフローを体現している。自分のskillを作る際にコピーすべき形そのものだ。

```
mirobody/agent/skills/
└── lab-report-walkthrough/
    └── SKILL.md          # frontmatter (name, description) + instructions
```

#### 🌐 HTTPリモートMCPサーバー

MirobodyのMCPサーバーは**HTTP/HTTPS経由のリモートアクセス**をサポートし、次を可能にする:

- **クラウドデプロイ**:任意のクラウドプラットフォーム上にMCPサーバーをデプロイ
- **ChatGPT Apps**:HTTPS経由でOpenAIのChatGPT Appsと連携
- **ネットワーク越しのアクセス**:localhostに限らず、どこからでもツールにアクセス
- **OAuthによる保護**:OAuth認証でリモートアクセスを保護

リモートHTTPアクセスを有効にするには、`config.{env}.yaml` に `MCP_PUBLIC_URL` を設定す
る:

```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

これでMCPサーバーは設定済みのHTTPSエンドポイントでアクセス可能になり、リモート連携の準備
が整う。

#### 🔑 個人用MCP URL

サインイン済みのユーザーは誰でも**個人用MCP URL**を発行できる:Webクライアントを開き、
**Settings → MCP Url → Copy** の順に進み、任意のMCPクライアント(Claude Desktop、Cursor、
Cherry Studio……)に貼り付ける。このURLにはアカウントに紐づく非公開の認証情報が埋め込ま
れており、OAuthの往復は不要で、クライアントは最初の呼び出しから*自分の*指標を読める。パス
ワードと同様に扱うこと。

---

## 🔌 HTTP API

意図的に、2つのサーフェスを用意している。

**records API ―― [Mirobody Cloud](https://docs.mirobody.ai/en/api-reference/)と同じ形を
している。** プラットフォームのドキュメントを読んでいれば、これらは既に見慣れたものだ:リ
クエストボディ、レスポンスのエンベロープ、フィールド名がすべて同じであり、セルフホスト環
境向けに書いたコードは、ホスト版向けに書いたコードと同じ見た目になる。

| エンドポイント | 内容 |
| --- | --- |
| `POST /api/standardize` | 報告書テキストを入力し、標準化済みの測定値を出力 ―― `{object: "extraction", data: [...]}`。`store=true` を指定しない限りドライラン。 |
| `POST /api/data` | 構造化レコードを書き込む(`records[]`、1回の呼び出しにつき500件まで)。書き込みは取り込み時にすべて標準化される。 |
| `GET /api/data` | 新しい順に読み出す ―― `{object: "list", data: [...], has_more}`。各測定値は `loinc_code` 付きの1オブジェクトとして返る。 |
| `DELETE /api/data` | `id`、`indicator`、または `all=true` のいずれかで削除する。 |

```bash
curl localhost:18080/api/data -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"indicator":"fasting_glucose","value":5.6,"unit":"mmol/L","time":"2026-08-19T07:30:00Z"}]}'
# {"status":"ok","ingested":1,"standardized":1}
```

**ホスト版の契約からの意図的な差異は3つ**、すべて同じ方向を向いている ―― ここはマルチテ
ナントのプラットフォームではなく、あなた自身のマシンだからだ:

- **`/v1` プレフィックスがない。** これらのパスはホスト版の契約ではなく、それと並んでバ
  ージョン管理されているかのように見せるべきでもない。
- **`user` / `retention` / `session_id` / `mb_live_*` キーがない。** サブジェクト、有効期
  限のスケジューリング、課金は、多数のテナントを抱えるプレーンのための運用者側の機構だ。
  ここではJWTがあなた自身を示し、行は何かが削除するまで存在し続ける。`retention` と
  `session_id` は拒否されるのではなく*受け取って無視される* ―― プラットフォームのドキュ
  メントが送るよう指示したフィールドに対して400を返しても、誰の得にもならない。
- **`DELETE /api/data` は明示的なスコープを要求する。** ホスト版のエンドポイントは「フィ
  ルタなし」を「全件」と解釈するが、これは運用者が意図して発行したキーの裏にあるなら問題
  ない。ここでは打ち間違えたcurl一発が、その人の記録全体を消しかねない。だからこそ、最も
  広いスコープは `all=true` と明示する形になっている。

**Webクライアント側のAPI**(`/api/v1/health-indicators`、`/api/chat`、`/api/v1/pulse/*`、
`/files/*`、`/invitation/*`)は、ハウスルールである `{code, msg, data}` エンベロープを維持
している。これは同梱フロントエンドが話す相手であり、フロントエンドを置き換えるならこちら
を対象に組む。データを入出力したいだけなら、前述のrecords APIを対象にする。

## 🔐 使う場所

| サーフェス | URL | 内容 |
| --- | --- | --- |
| **Webクライアント** | `http://localhost:18080` | 自分のデプロイが提供する同梱アプリ ―― 以下すべてを含む。 |
| **MCPエンドポイント** | `http://localhost:18080/mcp` | Claude Desktop / Cursor向け。Settingsで個人用URLを発行する。HTTPS/リモート(ChatGPT Apps、OAuth)には `MCP_PUBLIC_URL` を設定する。 |
| **ホスト型チャット** | [chat.mirobody.ai](https://chat.mirobody.ai/) | 自分で運用したくない場合のホスト版クライアント。 |
| **APIプラットフォーム** | [platform.mirobody.ai](https://platform.mirobody.ai/) | キー、使用量、そしてその上に構築するための健康データAPI。 |

同梱のWebクライアントはデモ用の外殻ではなく、フル機能のコンシューマーアプリだ:

- **Data**(`/data`) ―― 検査PDF、報告書の写真(HEIC含む)、Excel/CSV、音声、テキスト/
  Markdown、遺伝子型の生ファイルをドラッグ&ドロップする。抽出処理がこれらを標準化された
  指標に変換し、各測定値は**元ファイル**にリンクして戻れるほか、その場で**修正・削除**も
  できる(自分自身の記録に限る)。
- **Ask**(`/ask`) ―― 自分の記録についてチャットする:DeepAgentがデータを見つけ、グラフ
  化し、回答ごとの**トークン使用量**を報告する(架空のドル換算額ではなく、トークンそのも
  の)。2番目のモデルを選んで回答を並べて比較することもできる。
- **ケアサークル** ―― 共有してくれている相手に代わってアップロードや質問ができる。相手ご
  との同意によって制御される。

ログイン方法:メール確認コード(SMTP)、Google/Apple OAuth、または前述の事前登録済みデモ
アカウント ―― いずれも `config.{env}.yaml` で設定する。

---

## 🧪 テスト

```bash
pip install -e '.[test]'
pytest        # 495 tests, ~9s — no database, no network, no API key
```

テストはそれが対象とするコードの隣に置かれているため、素の `pytest` を打つだけで全体のス
イートが走る。このうち2つが、プロジェクトが公に主張している数値の根拠になっている:
`test_engine_coverage.py` は前述のリゾルバ197/197という数値そのものであり、
`pulse/gate_tests/` はすべてのベンダーペイロードをその標準化後の形と照合するスナップショッ
トだ。

**👉 [docs/testing.md](docs/testing.md)** ―― レイアウト、マーカー、スナップショットの再
生成、リリースゲート(`lint-imports`、`check_wheel_data.py`)。

---

## 📚 ドキュメント

**[docs.mirobody.ai](https://docs.mirobody.ai/)** がドキュメントプラットフォームであり、
デプロイ方法、APIプラットフォーム、そしてこのオープンソースエンジンを扱う。両者が進化して
も同期が保たれている。

リポジトリ内ドキュメントは1つの規則に従う:**各パッケージは「それが何か」を説明する短い
`README.md` を持ち、長文のガイドは[`docs/`](docs/)に置く** ―― `pip install` がコントリビ
ューター向けドキュメントまで `site-packages` に引き込まないようにするためだ。

| | トピック | 場所 |
| --- | --- | --- |
| | **実行可能なサンプル** | [`examples/`](examples/README.md) |
| ① | Collect(収集) ―― pulseエンジン | [`mirobody/pulse/`](mirobody/pulse/README.md) |
| ① | **Garmin / Oura / Whoopとの接続** | [docs/provider-setup.md](docs/provider-setup.md) |
| ① | データプロバイダの書き方 | [docs/provider-guide.md](docs/provider-guide.md) |
| ① | プロバイダのディレクトリ構成 | [`mirobody/pulse/providers/`](mirobody/pulse/providers/README.md) |
| ① | ファイル処理パイプライン | [docs/file-processing.md](docs/file-processing.md) |
| ① | Apple Health / CDAインポート | [docs/apple-health.md](docs/apple-health.md) |
| ② | 指標の検索と解決 | [`mirobody/indicator/`](mirobody/indicator/README.md) |
| ② | 健康指標と単位 | [`mirobody/pulse/standardize/`](mirobody/pulse/standardize/README.md) |
| ③ | エージェント開発 | [`mirobody/agent/`](mirobody/agent/README.md) |
| ③ | ツール開発 | [`mirobody/agent/tools/`](mirobody/agent/tools/README.md) |
| ③ | ChatGPT Appsウィジェット | [`mirobody/agent/resources/`](mirobody/agent/resources/README.md) |
| | 設定ガイド | [`mirobody/utils/config/`](mirobody/utils/config/README.md) |
| | テスト | [docs/testing.md](docs/testing.md) |
| | 既知の課題と保留中の作業 | [docs/roadmap.md](docs/roadmap.md) |
| | 変更履歴・セキュリティ | [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md) |

---

## 🤝 コントリビューション

コントリビューションはエンジンの3段階に沿って整理されている ―― 自分のレーンを選ぶこと:

| レーン                 | 貢献の内容                                                                                                                                                                                                                                                                                                               | 規模の目安 |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| **① Collect(収集)** | 新しいデバイスプロバイダ ―― [`BasePullProvider`](mirobody/pulse/providers/platform/base.py) を1つの `mirobody_<slug>/` ディレクトリに実装すれば、プラットフォームが起動時に検出する。[`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) が最小の参照実装、[`mirobody_whoop/`](mirobody/pulse/providers/mirobody_whoop/) がOAuth2版。あるいはパーサー向けの新しいファイル形式 | 中       |
| **② Standardize(標準化)**    | **語を解決できるようにする。** 間違った答えや空の答えが返ってくる語を見つけ(`mirobody resolve "<term>"`)、[`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) に1行、[`test_engine_coverage.py`](mirobody/test_engine_coverage.py) に1ケース追加する。言語は問わない。これは本リポジトリの中で最も障壁が低く、かつ有用なPRであり、私たちが公開している数値を実際に動かす。単位のマッピングやタキソノミーの修正も同様 | 極小         |
| **③ Answers(応答)**  | Agent Skill([`mirobody/agent/skills/`](mirobody/agent/skills/) 配下の `SKILL.md` パッケージ ―― [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md) をコピーして作る)、MCPツール、チャートスキーマ                                                                                                                                                                                                                             | 中       |

解析結果が間違っている検査報告書や、解決できない指標名を見つけたら?**それは素晴らしい
Issueになる** ―― (匿名化した)サンプルを添付すること。PRの実務については
[Contributing Guide](CONTRIBUTING.md) を参照。

---

<div align="center">

**[📚 docs.mirobody.ai](https://docs.mirobody.ai/)** · **[💬 chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 platform.mirobody.ai](https://platform.mirobody.ai/)**

Apache-2.0 · データは自分のマシンに留まる

</div>
