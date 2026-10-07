# 接入一个设备 provider（Garmin · Oura · Whoop）

**[English](provider-setup.md)** · **中文**

随包发布的三个 pull provider，本质上是各家厂商的 OAuth 客户端。在**你自己**拿到那家
厂商开发者计划发的凭证之前，这个仓库里没有任何东西能和 Garmin、Oura、Whoop 说上话：
凭证是按应用逐个签发的，没法跟着一个开源版本一起分发。

这一页讲的是从「装好了」到「开始拉数据」这段路。如果你只想在和任何厂商打交道之前，
先确认 provider 这套机制是通的，直接跳到[验证](#验证)：一个因为缺凭证而拒绝启动的
provider，照样会在启动日志里说出来，那就说明机制是通的。

这些配置住在 [`config.devices.yaml`](../config.devices.yaml) 里（由 `config.yaml`
顶部的 `INCLUDE` 列表引进来），不在 `config.yaml` 本身：一套从不接穿戴设备的部署，
根本不会看到它们。那个文件发布时凭证是空的，各家的 endpoint 默认值已经填好；把你的
凭证写进去，或者写进你的 `config.{env}.yaml` 覆盖层，后者优先级更高（下面的 YAML 块
放哪个里都行）。

**用 Docker 部署时**，镜像里自带这几个文件，改 checkout 里的 `config.devices.yaml` 不会
生效。把 YAML 块写进 `compose.yaml` 旁边的 `config.localdb.yaml`，再跑一次 `./deploy.sh`：
它会把这个文件挂进去（在 `.env` 里设置 `MIROBODY_CONFIG_FILE`）。checkout 要放在你的
home 目录下：Docker 不共享的路径（比如用 colima 时放在 `/tmp` 下的 checkout）会被挂成一个
空目录，服务就读不到你的文件。

> **Apple Health 不在这个列表里，也不可能在。** HealthKit 只能由签名过的 iOS App 在
> 设备本机、并且用户按类型逐项授权之后读取，没有网页 OAuth 流程，也没有服务器到服务
> 器的 API。这个服务端是**接收**方（`/apple/health`、`/apple/statistics`、
> `/apple/cda`），数据由一个已经拿到它的客户端交过来。见
> [`collect/providers/apple/`](../mirobody/collect/providers/apple/README.md)。

---

## 开始之前

**一个公网可达的 HTTPS 地址。** 用户点了同意之后，每一家厂商都会把浏览器重定向回你
的服务器，而且没有一家接受 `localhost` 或者裸 HTTP 作为注册的回调地址。本地开发就开
个隧道：

```bash
ngrok http 18060     # → https://abc123.ngrok-free.app
```

下面所有地方都用这个主机名，并且在 `config.{env}.yaml` 里把它设成 `MCP_PUBLIC_URL`，
好让服务端其余部分对「自己的地址是什么」有一致的认识。

**回调路由本来就有。** 它挂在：

```
{你的-https-主机}/api/v1/pulse/{platform}/{provider_slug}/callback
```

这三家的 `platform` 永远是 `theta`。slug 是 `theta_garmin`、`theta_oura`、
`theta_whoop`，是完整的 slug，不是光秃秃的厂商名；处理器就是拿这个字符串精确查
provider 的（`ProviderPlatform.get_provider`）。

所以你在厂商那边注册的重定向地址，和你写进配置的那个，是同一个字符串，长这样：

```
https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_oura/callback
```

---

## Oura

**OAuth 2.0。** 到 [cloud.ouraring.com/oauth/applications](https://cloud.ouraring.com/oauth/applications)
注册。重定向地址填上面那条、slug 用 `theta_oura`。

```yaml
# config.{env}.yaml
OURA_CLIENT_ID:     'your-client-id'
OURA_CLIENT_SECRET: 'your-client-secret'
OURA_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_oura/callback'
```

## Whoop

**OAuth 2.0。** 到 [developer.whoop.com](https://developer.whoop.com/) 注册，重定向
地址的 slug 用 `theta_whoop`。

```yaml
WHOOP_CLIENT_ID:     'your-client-id'
WHOOP_CLIENT_SECRET: 'your-client-secret'
WHOOP_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_whoop/callback'
```

下面这些都是可选的，默认值就能用，只有要钉到另一套环境时才需要设：
`WHOOP_AUTH_URL`、`WHOOP_TOKEN_URL`、`WHOOP_API_BASE_URL`、`WHOOP_SCOPES`、
`WHOOP_REQUEST_TIMEOUT`。

## Garmin

**OAuth 1.0a**，不是 2.0。流程是 request-token → 用户授权 → `oauth_verifier` →
access-token，回调处理器按这个分支走（`LinkType.OAUTH1`）。通过
[Garmin Connect 开发者计划](https://developer.garmin.com/gc-developer-program/)申请；
审批是人工的，通常也是最花时间的一环。

```yaml
GARMIN_CLIENT_ID:     'your-consumer-key'
GARMIN_CLIENT_SECRET: 'your-consumer-secret'
GARMIN_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_garmin/callback'
```

可选、有默认值的：`GARMIN_AUTH_URL`、`GARMIN_TOKEN_URL`、
`GARMIN_ACCESS_TOKEN_URL`、`GARMIN_API_BASE_URL`、`OAUTH_TEMP_TTL_SECONDS`。

> 密钥会自动静态加密：任何名字里含 `_SECRET`、`_KEY`、`_TOKEN`、`_PASSWORD` 等等的
> 键，服务端第一次读它的时候就会用 `.env` 里的 `CONFIG_ENCRYPTION_KEY` 加密。明文只
> 用粘贴一次，文件会自己改写自己。

---

## 验证

**1. provider 起来了。** 再跑一次 `./deploy.sh`，或者 `docker compose up -d`
（`docker compose restart` 不会重新挂载文件，也不会重读环境变量），然后用
`docker compose logs mirobody` 看启动日志：

```
Loaded provider from /app/mirobody/collect/providers/mirobody_oura/provider_oura.py
  - provider platform loaded 1 providers
```

下面这一行的意思是**凭证没配**，不是**代码坏了**：

```
Provider OuraProvider declined to start (not configured)
```

这两条都是 INFO。WARNING 级别的 `Failed to load provider …` 是另一回事：那是 import
错误，有回归测试守着。

**2. 它被提供给用户了。**

```bash
curl -s http://localhost:18060/api/v1/pulse/providers | jq '.data.providers[].slug'
```

**3. 绑一个账号。** `POST /api/v1/pulse/user/providers/link`（需要鉴权）会返回厂商的
授权地址；打开、同意，厂商就会把浏览器送到你的回调上。成功之后，用户已绑定的
provider 会出现在：

```bash
curl -s http://localhost:18060/api/v1/pulse/user/providers -H "Authorization: Bearer $TOKEN"
```

解绑用 `POST /api/v1/pulse/user/providers/unlink`。

**4. 数据到了。** 要么按 provider 启动时注册的拉取计划来，要么走 webhook：
`POST /api/v1/pulse/{platform}/{provider}/webhook`，这就是你给厂商填的那个推送地址。
没设 `COLLECT_WEBHOOK_SECRET` 时 webhook 是关着的（404）；设了之后，给厂商填的地址要带
`?secret=<值>`，或者在 `X-Webhook-Secret` 头里带上这个值。推送用厂商自己的用户 id
指明是谁的数据，再通过已关联的账户对上本地用户。

---

## 排查

**只有 `loaded 0 providers`，再没别的行。** 所有 provider 都拒绝了。对着这一页核一遍
键名：`OURA_CLIENT_ID` 打错一个字母，和根本没配长得一模一样，因为 `create_provider`
两种情况都返回 `None`。

**厂商说重定向地址不对。** 它必须和你注册的那条**逐字节**一致，包括协议、主机、路径，
以及结尾有没有斜杠。最常见的漏是 slug：是 `theta_oura`，不是 `oura`。

**回调返回 "provider not available"。** provider 在启动时就拒绝了，所以平台那边根本
没有东西注册在这个 slug 下。先修凭证，回调在注册的下游。

**一开始好好的，一小时后不动了。** access token 会过期，靠 `refresh_access_token` 刷
新；如果当初根本没存下 refresh token，那是厂商那边的授权缺了 offline/refresh 权限。
配好正确的 scope 之后重新绑一次。

---

## 自己写一个

provider 的契约就是一个目录：`mirobody_<slug>/provider_<slug>.py`，导出一个
`BasePullProvider` 的子类，并且 `create_provider(config)` 在没配置时返回 `None`。
[`mirobody_whoop/`](../mirobody/collect/providers/mirobody_whoop/) 是 OAuth2 的参考实
现；[`mirobody_oura/`](../mirobody/collect/providers/mirobody_oura/) 是同样的形状换了
一家厂商。完整指南：[provider-guide.md](provider-guide.md)。

包外面的 provider 放进 `PROVIDER_DIRS`，它们是按文件位置加载的，所以里面要用绝对
import。
