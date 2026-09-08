# Connecting a device provider (Garmin · Oura · Whoop)

The three shipped pull providers are OAuth clients of their vendor. Nothing in
this repo can talk to Garmin, Oura or Whoop until **you** hold credentials from
that vendor's developer programme — they are issued per application, and cannot
be bundled with an open-source release.

This page is the path from "installed" to "pulling data". If you only want to
prove the provider mechanism works before dealing with any vendor, jump to
[the zero-credential check](#the-zero-credential-check).

> **Apple Health is not in this list, and cannot be.** HealthKit is readable
> only from a signed iOS app, on-device, after per-type user consent — there is
> no web OAuth flow and no server-to-server API. This server *receives* Apple
> data (`/apple/health`, `/apple/statistics`, `/apple/cda`) from a client that
> already has it. See [`pulse/apple/`](../mirobody/pulse/apple/README.md).

---

## Before you start

**A publicly reachable HTTPS URL.** Every vendor redirects the user's browser
back to your server after they approve, and none of them accept `localhost` or
plain HTTP for a registered redirect. For local development, tunnel:

```bash
ngrok http 18060     # → https://abc123.ngrok-free.app
```

Use that hostname everywhere below, and set it as `MCP_PUBLIC_URL` in
`config.{env}.yaml` so the rest of the server agrees about its own address.

**The callback route already exists.** It is served at:

```
{your-https-host}/api/v1/pulse/{platform}/{provider_slug}/callback
```

`platform` is always `theta` for these three. The slugs are `theta_garmin`,
`theta_oura`, `theta_whoop` — the full slug, not the bare vendor name; the
handler looks the provider up by exactly that string
(`ProviderPlatform.get_provider`).

So the redirect URL you register with the vendor, and the one you put in
config, are the same string, and it looks like:

```
https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_oura/callback
```

---

## Oura

**OAuth 2.0.** Register at [cloud.ouraring.com/oauth/applications](https://cloud.ouraring.com/oauth/applications).
Set the redirect URI to the callback above with `theta_oura`.

```yaml
# config.{env}.yaml
OURA_CLIENT_ID:     'your-client-id'
OURA_CLIENT_SECRET: 'your-client-secret'
OURA_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_oura/callback'
```

## Whoop

**OAuth 2.0.** Register at [developer.whoop.com](https://developer.whoop.com/)
and set the redirect URI with `theta_whoop`.

```yaml
WHOOP_CLIENT_ID:     'your-client-id'
WHOOP_CLIENT_SECRET: 'your-client-secret'
WHOOP_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_whoop/callback'
```

Optional, all with working defaults — set only to pin a different environment:
`WHOOP_AUTH_URL`, `WHOOP_TOKEN_URL`, `WHOOP_API_BASE_URL`, `WHOOP_SCOPES`,
`WHOOP_REQUEST_TIMEOUT`, `WHOOP_CONCURRENT_REQUESTS`, `WHOOP_MAX_DETAIL_RECORDS`.

## Garmin

**OAuth 1.0a**, not 2.0 — the flow is request-token → user authorises →
`oauth_verifier` → access-token, and the callback handler branches on it
(`LinkType.OAUTH1`). Apply through the
[Garmin Connect Developer Program](https://developer.garmin.com/gc-developer-program/);
approval is a manual process and is usually the long pole.

```yaml
GARMIN_CLIENT_ID:     'your-consumer-key'
GARMIN_CLIENT_SECRET: 'your-consumer-secret'
GARMIN_REDIRECT_URL:  'https://abc123.ngrok-free.app/api/v1/pulse/theta/theta_garmin/callback'
```

Optional, with defaults: `GARMIN_AUTH_URL`, `GARMIN_TOKEN_URL`,
`GARMIN_ACCESS_TOKEN_URL`, `GARMIN_API_BASE_URL`, `OAUTH_TEMP_TTL_SECONDS`.

> Secrets are encrypted at rest automatically: any key whose name contains
> `_SECRET`, `_KEY`, `_TOKEN`, `_PASSWORD` … is encrypted with
> `CONFIG_ENCRYPTION_KEY` from `.env` the first time the server reads it. Paste
> the plaintext once; the file rewrites itself.

---

## Verify

**1. The provider starts.** Restart and read the boot log:

```
Loaded provider from /app/mirobody/pulse/providers/mirobody_oura/provider_oura.py
  - provider platform loaded 1 providers
```

The line that means *credentials are missing*, not *code is broken*:

```
Provider OuraProvider declined to start (not configured)
```

Both are INFO. A `Failed to load provider …` at WARNING is a different problem —
that is an import error, and there is a regression test for it
(`pulse/providers/test_provider_loading.py`).

**2. It is offered to users.**

```bash
curl -s http://localhost:18060/api/v1/pulse/providers | jq '.data[].slug'
```

**3. Link an account.** `POST /api/v1/pulse/user/providers/link` (authenticated)
returns the vendor authorisation URL; open it, approve, and the vendor sends the
browser to your callback. On success the user's linked providers appear in:

```bash
curl -s http://localhost:18060/api/v1/pulse/user/providers -H "Authorization: Bearer $TOKEN"
```

Unlink with `POST /api/v1/pulse/user/providers/unlink`.

**4. Data arrives** either on the pull schedule the provider registers at
startup, or via webhook — `POST /api/v1/pulse/{platform}/{provider}/webhook`,
which is the endpoint you give the vendor for push notifications.

---

## Troubleshooting

## Troubleshooting

**`loaded 0 providers` and no other line.** Every provider declined. Check the
key names against this page — a typo in `OURA_CLIENT_ID` looks identical to not
configuring it, because `create_provider` returns `None` either way.

**Vendor rejects the redirect URI.** It must match what you registered
*byte for byte*, including scheme, host, path and the absence of a trailing
slash. The most common miss is the slug: `theta_oura`, not `oura`.

**Callback returns "provider not available".** The provider declined at startup,
so the platform has nothing registered under that slug. Fix the credentials
first; the callback is downstream of registration.

**It worked, then stopped after an hour.** Access tokens expire and are
refreshed through `refresh_access_token`; if a refresh token was never stored,
the vendor consent needs the offline/refresh scope. Re-link once with the right
scope configured.

---

## Writing your own

The provider contract is one directory:
`mirobody_<slug>/provider_<slug>.py`, exporting a `BasePullProvider` subclass
with `create_provider(config)` returning `None` when unconfigured.
[`mirobody_whoop/`](../mirobody/pulse/providers/mirobody_whoop/) is the OAuth2
reference; [`mirobody_oura/`](../mirobody/pulse/providers/mirobody_oura/) is the
same shape with a different vendor. Full guide: [provider-guide.md](provider-guide.md).

Providers outside the package go in `PROVIDER_DIRS`; those are loaded by file
location, so use absolute imports in them.
