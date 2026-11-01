# チャットでの計算

**[English](python-analysis.md)** · **[中文](python-analysis.zh-CN.md)** · **日本語**

`code_exec` を使うと、チャットモデルは問い合わせ済みの健康記録を計算できます。指標・服薬・遺伝子・薬物遺伝学をまたいだ比較も含みます。もとのデータは自動的に渡されるため、モデルが表示された表から数値を書き写す必要はありません。モデルがどの言語でコードを書くかはモデルの実行場所によって決まり、ツールの名前は変わりません。

| チャットモデルの実行場所 | コードの実行方式 | 必要な設定 |
|---|---|---|
| クラウドサービス | アプリ内のエンジンで JavaScript として実行 | 不要 |
| このマシンまたは自分のネットワーク上 | ローカルの Docker コンテナで Python として実行 | アプリが到達できる Docker デーモン。なければ JavaScript |

[計算のベンチマーク](../benchmarks/local_agent/eval_language_results.md)（英語）の比較では、クラウドモデルはどちらの言語でも同じ正答率でした。JavaScript はコンテナを必要としないため、メインの Compose ファイルだけの構成でも、クラウドモデルには必ず計算ツールが使えます。

## 入力と出力

コードが使えるのは次のものです。

- `data`：直近に成功し、空でなかった記録クエリのもとのデータ行。
- `results`：今回のターン内で成功したクエリ結果。メタデータ、前提、出典、ツール呼び出し ID を含みます。
- Python のみ、`/workspace/data.json`：計算コンテナ内にある、同じ `data` と `results`。文書のアップロードや読み取り用のパスではありません。

今回のターンはユーザーの直近の質問から始まり、回答を修復する自動リクエストもそのターンに含まれます。直近のクエリが失敗したか0件だった場合、計算は拒否されます。古いデータを使ったり、0で代用したりすることはありません。すでに提供されている集計には、クエリの `stats` と時間バケットのビューを使ってください。

コードは結果ともとになった `rid` を出力してください。Python では `print(...)`、JavaScript では `console.log(...)` を使います。JavaScript では最後の式の値も返されます。ツールは終了コード、もとになった呼び出しの ID、入力または出力が不完全かどうかを報告します。出力の上限は 40,000 文字で、それを超えると不完全と明示されます。

## JavaScript

実行エンジンは、WebAssembly にコンパイルされた QuickJS で、アプリのプロセス内で wasmtime により動きます。これは Deep Agents の[インタープリタ](https://docs.langchain.com/oss/python/deepagents/interpreters)が使うものと同じエンジンですが、そのツールブリッジは使わないため、コードは注入された行だけを読み取ります。呼び出しごとに新しいエンジンが起動し、メモリは 128 MB、実行期限は 1〜30 秒で、ホスト側が最大 10 秒追加で待ちます。標準の組み込みオブジェクト（`Math`、`JSON`、`Date`）は使えますが、ネットワーク、ファイル、プロセス、`require`、`import` は使えません。設定は何も要りません。

## Python

### ローカルのセットアップ

ソースからインストールした場合は、アプリの依存関係をインストールし、計算用イメージを取得します。

```bash
pip install -e '.[app,test]'
docker pull python:3.12-slim
```

対応するランタイムは、macOS では Docker Desktop または Colima、Linux では Docker Engine です。実行器は `PYTHON_SANDBOX_DOCKER_HOST`、`DOCKER_HOST`、`/var/run/docker.sock`、現在の Docker CLI コンテキストの順に確認します。受け付けるのはローカルの Unix ソケットのみで、TCP と SSH のエンドポイントは拒否されます。

既定のイメージには Python 標準ライブラリが入っています。分析用パッケージを追加するには、独自のローカルイメージを用意し、`PYTHON_SANDBOX_IMAGE` を設定してください。計算用コンテナはネットワークに接続できないため、依存関係はあらかじめインストールしておきます。

### アプリ自体も Docker で動いている場合

計算用コンテナを起動するには、アプリがホストの Docker デーモンにアクセスできる必要があります。オプションの [compose-python.yaml](../compose-python.yaml) は管理用ソケットをアプリにマウントし、コンテナを管理する権限を与えます。これはオプトインで、メインの Compose ファイルはこのソケットをマウントしません。計算用コンテナがこのソケットを受け取ることもありません。

`PYTHON_DOCKER_SOCKET` に Docker ホスト上のソケットパスを、`PYTHON_DOCKER_GID` にデーモンから見たその所有グループ ID を設定します（Colima では 991）。次のコマンドで確認できます。

```bash
docker run --rm -v /var/run/docker.sock:/s python:3.12-slim stat -c %g /s
```

その後、次を実行します。

```bash
docker compose -f compose.yaml -f compose-python.yaml up -d
```

### デーモンが応答しない場合

アプリはデーモンに起動時に一度だけ問い合わせます。問い合わせるのは、チャットモデルがこのマシンか自分のネットワーク上で動いている場合だけです。応答がない場合（メインの Compose ファイルだけの構成、アプリが所属していないグループのソケット、Docker が動いていないなど）、ローカルモデルも JavaScript で計算し、ログに一度だけその旨が記録されます。

```text
code_exec runs JavaScript for local models too: no local Docker daemon answered: error_type=FileNotFoundError (docs/python-analysis.md)
```

設定を直したら、アプリを再起動してください。アプリの実行中にデーモンが止まった場合、ツールは設定の問題として報告し、すでに取得済みの測定値は有効なままです。

## 無効にする

`DISALLOWED_TOOLS: [code_exec]` を設定します。このツールの以前の名称 `python_exec` と `eval` は `code_exec` として読み込まれ、ログに警告が出ます。

<a id="runtime-and-checks"></a>

## Python コンテナの制限

計算用コンテナには、アプリのマウント、データベースの認証情報、ネットワークがありません。UID/GID 65534 で動き、イメージは読み取り専用で、書き込み領域は一時的なものだけです。CPU 1個、メモリ 512 MB、プロセス数 64 個までに制限されています。コマンドの実行期限は 1〜30 秒で、ホスト側が最大 10 秒追加で待ちます。タイムアウトまたはキャンセル時はストリームを閉じてコンテナを削除します。有限のリース期間と自動削除により、放置された実行も片付けられます。モデルに公開されるのはコード実行だけで、汎用のシェルツールではありません。

実装と検証の詳細は、[計算のベンチマーク](../benchmarks/local_agent/README.md)（英語）と [Deep Agents サンドボックスインターフェース](https://docs.langchain.com/oss/python/deepagents/sandboxes#the-execute-method)を参照してください。
