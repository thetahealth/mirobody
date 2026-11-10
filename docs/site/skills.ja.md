# エージェントスキル

スキルをインストールすると、コーディング用アシスタントに Mirobody の操作手順とコマンドを教えられます。
これらのスキルはアシスタント側で動き、アプリ内蔵のチャットエージェントとは別物です。

| スキル | 用途 |
| --- | --- |
| `standardize-health-data` | Python ライブラリを使って測定値を標準化し、健康文書を読み取ります。 |
| `mirobody` | アプリケーションを起動し、アシスタントを MCP で接続します。 |

## インストールする

```bash
npx skills add thetahealth/mirobody --skill standardize-health-data
npx skills add thetahealth/mirobody --skill mirobody
```

ライブラリの操作には Python 3.12 以上と `mirobody` が必要です。文書の読み取りには、さらに解析用の依存関係とモデルキーが要ります。アプリケーションの操作には Docker と、設定済みのクラウドまたはローカルモデルが必要です。

すでに稼働中のデプロイがあれば、[MCP](https://docs.mirobody.ai/ja/tools/mcp-integration) で直接接続できます。
ほかのインストール方法は[スキルのリファレンス](../../skills/README.md)（英語）にあります。
