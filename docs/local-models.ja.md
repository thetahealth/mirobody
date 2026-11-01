<a id="running-on-your-own-machine-with-no-api-key"></a>

# モデルをローカルで実行する

**[English](local-models.md)** · **[中文](local-models.zh-CN.md)** · **日本語**

Mirobody のモデルを自分のマシンで動かせば、モデル API キーなしで文書処理と質問をローカルにとどめられます。初回起動時にモデルの重みをダウンロードし、以降はキャッシュを再利用します。

Mirobody は [llama.cpp](https://github.com/ggml-org/llama.cpp) の OpenAI 互換 API である `llama-server` に接続します。1つのモデルが質問に答えて要約を書き、GLM-OCR-0.9B が報告書の画像とスキャンページを読み取ります。

<a id="quick-start"></a>

## 環境を選ぶ

| プラットフォーム | モデルサービス | 必要なもの |
| --- | --- | --- |
| GPU なしの Windows、Linux、macOS | `local-cpu` を使う Docker Compose | メモリ 16 GB、うち 8 GB 以上を Docker に割り当て |
| Apple シリコン搭載の Mac | ホストで直接動かす `llama-server` | 既定モデルにメモリ 16 GB |
| NVIDIA GPU を搭載した Linux または Windows | `local` を使う Docker Compose | 対応する GPU。Linux では NVIDIA Container Toolkit が必要 |
| Intel、AMD、NVIDIA いずれかの GPU を搭載した Windows | Vulkan 版のネイティブ `llama-server` | Windows 上のチェックアウトと対応する llama.cpp ビルド |

CPU での回答には数分かかることがあります。Mac では llama.cpp をホストで直接動かすと GPU
を使えますが、Mac の Docker コンテナはその GPU を使えません。

<a id="choose-a-size"></a>

## モデルを選ぶ

| セットアップの選択肢 | 回答モデル | モデルのダウンロード量（OCR を含む） | ハードウェア |
| --- | --- | --- | --- |
| **Small**（既定） | MiniCPM5-2B、Q8_0 | 約 4.1 GB | メモリ 16 GB、GPU 不要 |
| **Large** | Qwen3.8-27B、IQ3_S | 約 14.5 GB | メモリ 32 GB の Mac、または VRAM 24 GB の NVIDIA GPU |

どちらの選択肢も文書には GLM-OCR を使います。MiniCPM5-2B はチャットの写真の OCR 文字を受け取り、Qwen3.8-27B は画像そのものも確認できます。モデルの比較は[モデルの選び方](model-choice.ja.md)、測定結果は[ベンチマーク記録](../benchmarks/local_models/README.md)を参照してください。

<a id="start-the-models"></a>

## モデルサービスを起動する

これらのコマンドは `deploy.sh` があるリポジトリのルートで実行してください。プリセットは `docker/minicpm5-2b.jinja` を含め、相対パスを使います。`--models-max 2` は回答モデルと OCR モデルを読み込んだままにします。回答モデルを切り替えると、それまでのモデルは解放されます。

### CPU 上の Docker

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

このスクリプトはアプリとモデルを起動し、`COMPOSE_PROFILES` を `.env` に保存して、
`LOCAL_BASE_URL` と `LOCAL_OCR_BASE_URL` を `http://llama:8080/v1` に設定します。初回のダウンロード状況は `docker compose logs -f llama_cpu` で確認できます。

既存のデプロイでは `docker compose --profile local-cpu up -d` を実行し、`.env` に `COMPOSE_PROFILES=local-cpu` を追加してください。これにより `docker compose down` のあとで `up` してもモデルサービスが戻ってきます。

### Apple シリコン搭載の Mac

ホストで llama.cpp を起動します。

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

### Docker と NVIDIA GPU

Linux では先に NVIDIA Container Toolkit をインストールします。Windows では WSL 2
バックエンドを使う Docker Desktop を使います。そのうえで次を実行します。

```bash
COMPOSE_PROFILES=local ./deploy.sh
```

### Windows ネイティブのモデルサービス

公式の winget パッケージは Vulkan を使い、x64 と arm64 上の Intel、AMD、NVIDIA の GPU に対応します。PowerShell で次を実行します。

```powershell
winget install --id ggml.llamacpp
git clone https://github.com/thetahealth/mirobody.git C:\src\mirobody
cd C:\src\mirobody
llama-server --models-preset docker\local-models.ini --port 8080 --models-max 2
```

作業ディレクトリには `\\wsl.localhost\...` の UNC パスではなく、Windows のドライブを使ってください。このチェックアウトは、アプリの WSL 側のチェックアウトと同じリリースに保ちます。CPU で実行する場合は、代わりに `docker\local-models-cpu.ini` を使います。

CUDA ビルドを使う場合は、自分の GPU に対応する llama.cpp のリリースを選び、対応するランタイムライブラリをインストールしてください。

Mirobody 自体は Docker Desktop の中で動きます。**Settings → Resources → WSL
integration** を有効にし、WSL のターミナルでリポジトリをクローンして `./deploy.sh` を実行してください。Docker が `permission denied` と表示する場合は `sudo usermod -aG docker $USER` を実行し、続けて Windows 側で `wsl --shutdown` を実行します。

その他の Linux 向け GPU や、Docker を使わないモデルサービスには、対応する [llama.cpp のリリース](https://github.com/ggml-org/llama.cpp/releases)を使い、同じ `llama-server` コマンドを実行します。

<a id="point-mirobody-at-them"></a>

## Mirobody を接続する

`./deploy.sh` が表示するセットアップリンクを開くか、**設定 → モデル**から、
**100% このマシンで実行**を選びます。Mirobody は `host.docker.internal:8080`、
`llama:8080`、`127.0.0.1:8080` の順に確認します。モデルを選ぶと、ページはサーバーがそのモデルを提供しているかを検証し、読み込んで進捗を表示します。選択内容は暗号化されてデータベースに保存されます。

または、`.env` にアドレスを直接設定することもできます。

```bash
LOCAL_BASE_URL=http://host.docker.internal:8080/v1
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
```

`local` または `local-cpu` の Compose プロファイルを使う場合は、両方のアドレスに `http://llama:8080/v1` を使います。アプリとモデルの両方がホストで直接動く場合は `http://127.0.0.1:8080/v1` を使います。

`.env` 内のクラウド提供元のキーは優先され、それがある間はセットアップページでローカルを選べません。キーを削除するか、`DEFAULT_MODEL=local`、`UTILS_VISION_MODEL=local-utils`、
`UTILS_TEXT_MODEL=local-utils` を明示的に設定してください。ローカル用のデプロイスクリプトは、シェルの提供元キーを `.env` にコピーすることはなく、ファイルにすでにあるキーがあれば知らせます。

`.env` を編集したら `docker compose up -d` を実行してください。再起動だけでは読み込み直されません。

<a id="check-it"></a>

## 接続を確認する

```bash
docker compose exec mirobody mirobody doctor --probe
```

これは、ツール呼び出し・構造化抽出・画像・OCR を含め、実際のリクエストで選択中のモデルを確認します。

<a id="when-it-does-not-work"></a>
<a id="without-a-gpu"></a>

## よくある問題

| 症状 | 対処 |
| --- | --- |
| モデルサーバーが見つからない | ポート 8080 で起動してください。Linux では下記のホストネットワークの注意点を確認します。 |
| 最初の質問で待たされる | モデルがまだダウンロード中の可能性があります。`docker compose logs -f llama_cpu`（GPU サービスでは `llama`）を確認してください。 |
| Docker のメモリ不足 | Docker Desktop で 8 GB 以上を割り当てるか、`colima start --memory 8` を使います。CPU デプロイは `docker/local-models-cpu.ini`、GPU デプロイは `docker/local-models.ini` を使います。 |
| ログがモデルプリセットについて `Is a directory` と表示する | Docker がチェックアウトにアクセスできるか確認してください。Colima は既定でホームディレクトリを共有します。チェックアウトをその中に置くか、パスを `mounts` に追加してください。 |
| ダウンロードが Hugging Face に届かない | Compose では `.env` に、直接起動する場合は `llama-server` を起動するシェルに `HF_ENDPOINT=https://<mirror>` を設定してください。 |
| システムのプロキシ経由でローカルのリクエストが失敗する | リクエスト元のシェルで `export NO_PROXY=localhost,127.0.0.1` を設定してください。WSL が Windows のプロキシを引き継ぐ場合も同様です。 |

モデルのキャッシュは、Compose プロジェクトの `mirobody_models` ボリュームです。別のチェックアウトや `COMPOSE_PROJECT_NAME` を使うと、別のキャッシュが作られて再ダウンロードされることがあります。イメージは llama.cpp b11429 に固定されています。別のビルドやレジストリミラーが必要な場合は、`.env` で `LLAMA_CPU_IMAGE` または `LLAMA_IMAGE` を上書きしたうえで `docker compose up -d` を実行してください。

**Linux のホストネットワーク：** `host.docker.internal` は Docker のブリッジを指します。
ホスト側のモデルサーバーはそのブリッジアドレス（例えば `--host 172.17.0.1`）で待ち受ける必要があり、アドレスは `ip -4 addr show docker0` で確認できます。`0.0.0.0` で待ち受けると、
認証なしのモデルサーバーをネットワークに公開することになります。Mac の Docker Desktop と Colima は、ホストの既定の `127.0.0.1` 待ち受けに到達できます。Compose のモデルサービスはホストポートを必要とせず、内部ネットワークで通信します。アプリ自体も既定で `127.0.0.1` にバインドされます。`MIROBODY_BIND=0.0.0.0` に変更する前に、
[SECURITY.md](../SECURITY.md)（英語）、特に公開デモのコード `111111` の部分を確認してください。

<a id="other-models"></a>
<a id="other-servers"></a>

## 別のモデルやサーバーを使う

llama.cpp のプリセットにモデルを追加し、セットアップページで選ぶか、次を設定します。

```bash
LOCAL_MODEL=qwen3.8-27b
LOCAL_OCR_MODEL=glm-ocr
```

サーバーは選んだモデルを提供している必要があります。他の OpenAI 互換サーバーも同じアドレスとモデル名の設定で使え、モデル一覧は `curl <address>/v1/models` で確認できます。
`local` 項目は `temperature: 0` を使い、モデル固有の追加設定はプリセット側に書きます。
クラウド項目では、これに相当する上書きは `config.llm.yaml` の各項目が持つ `model_env`
変数で行います。例えば `OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5.5` のようにします。
セットアップページは、保存する前に変更後のモデルを実際のリクエストで検証します。

Ollama を使う場合は、Mirobody のプロンプトで `exceed_context_size_error` にならないよう、
`ollama serve` の前に `OLLAMA_CONTEXT_LENGTH=65536` を設定してください。Ollama の `glm-ocr` には 0.34.1 以降で既知の終了処理の問題があり（[issue](https://github.com/ollama/ollama/issues/18609)）、この問題が残っている間は文書の OCR を llama.cpp 側にとどめてください。

<a id="the-document-reader"></a>
<a id="what-each-model-can-read-in-a-photo"></a>
<a id="things-that-behave-differently-from-a-hosted-model"></a>
<a id="how-a-document-is-read-whichever-model-reads-the-rest"></a>

## 文書・写真・計算

GLM-OCR は文字と表を読み取りますが、食事や発疹のような場面は認識できません。既定の回答モデルでは、チャットは画像そのものではなく OCR の文字を受け取り、読み取れる文字がない場合は説明を求めます。Qwen3.8-27B のような画像対応モデルは写真を直接確認できますが、
食事の推定はあくまで概算です。

ローカルでもクラウドでも、同じ文書処理パイプラインを使います。長い報告書はページ単位で処理され、認識できた表はルールで抽出され、残りの内容はテキストモデルに渡されます。処理の流れは[パイプライン](pipeline.md)（英語）、OCR の代替案とその設定は[モデルの選び方](model-choice.ja.md#the-document-reader-glm-ocr-09b)を参照してください。

ローカルモデルは、照会した記録に対して `code_exec` で計算します。アプリが Docker
デーモンに接続できるときは Python で、そうでなければアプリ内の JavaScript で実行します。
詳しくは[チャットでの計算](python-analysis.ja.md)を参照してください。
