# デバイスサービスに接続する

Mirobody には Garmin・Oura・WHOOP との連携が含まれます。デプロイごとに、ベンダーの開発者プログラムから自分の認証情報を取得する必要があります。
Apple Health は別の[インポートとクライアントからの送信の仕組み](apple-health.ja.md)を使います。

## 接続を準備する

1. ベンダーにアプリケーションを登録します。
2. デプロイに公開到達可能な HTTPS コールバックを設定します。
3. 認証情報は、同梱の既定値ではなく、有効な設定の上書きファイルに書き込みます。
4. 設定を反映し、Web クライアントでアカウントを認可します。

コールバックの形式:

```text
https://your-host/api/v1/pulse/theta/<provider_slug>/callback
```

| ベンダー | Provider slug | 認証情報のキー |
| --- | --- | --- |
| Oura | `theta_oura` | `OURA_CLIENT_ID`、`OURA_CLIENT_SECRET`、`OURA_REDIRECT_URL` |
| WHOOP | `theta_whoop` | `WHOOP_CLIENT_ID`、`WHOOP_CLIENT_SECRET`、`WHOOP_REDIRECT_URL` |
| Garmin | `theta_garmin` | `GARMIN_CLIENT_ID`、`GARMIN_CLIENT_SECRET`、`GARMIN_REDIRECT_URL` |

ベンダー側に登録するリダイレクト URL は、設定した値と完全に一致させてください。
Oura と WHOOP は OAuth 2.0、同梱の Garmin 連携は OAuth 1.0a を使います。

## 設定例

```yaml
OURA_CLIENT_ID: your-client-id
OURA_CLIENT_SECRET: your-client-secret
OURA_REDIRECT_URL: https://your-host/api/v1/pulse/theta/theta_oura/callback
```

Docker では、`ENV` と `MIROBODY_CONFIG_FILE` が選ぶマウント済みの上書きファイルにこれを保存します。チェックアウトの `config.devices.yaml` を編集しても、既存イメージ内のコピーは変わりません。[設定](https://docs.mirobody.ai/ja/configuration)を参照してください。

<a id="troubleshooting"></a>

## 同期を確認する

`docker compose up -d` で変更を反映し、アプリとワーカーのログを確認します。

```bash
docker compose logs --tail 80 mirobody mirobody_worker
```

認証情報がないデータソースはスキップされます。認可に失敗する場合は、コールバック URL と認証情報を確認してください。連携したデータが届かない場合は、ワーカーの状態とデータベース接続を確認してください。
開発者ポータル、任意のエンドポイント、ベンダーごとの詳細は[設定の完全なリファレンス](../provider-setup.md)（英語）に記載されています。
