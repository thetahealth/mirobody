"""Which decoded tokens count as a credential, and where.

A valid signature says this server issued the token. Whether it still speaks
for someone is decided here, for every route that takes a bearer token:

- a refresh token is never a bearer credential;
- a token the OAuth endpoint issued to a registered client (an MCP client) is
  accepted only at the MCP endpoint it names as its audience (RFC 8707), so a
  connector granted `mcp:read` cannot call `DELETE /api/data`;
- the account must exist, and must not have revoked the token since it was
  minted (`tokens_valid_after`, see `user.prove_address`).
"""

from urllib.parse import urlsplit

from .jwt import REFRESH_TOKEN_TYPE, minted_at

#: `client_id` prefix of clients registered at /oauth/register. The web
#: client's own tokens carry the configured JWT_CLIENT_ID instead.
MCP_CLIENT_PREFIX = "mcp_client_"


def issued_to_client(payload: dict) -> bool:
    return str(payload.get("client_id") or "").startswith(MCP_CLIENT_PREFIX)


def mcp_resource(origin: str, uri_prefix: str = "") -> str:
    """The canonical URI of this server's MCP endpoint: the audience of the
    tokens issued to MCP clients, and `resource` in its protected-resource
    metadata."""
    return f"{origin.rstrip('/')}{uri_prefix}/mcp"


def _resource_key(url: object) -> tuple[str, str] | None:
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return None
    if not parts.netloc:
        return None
    return parts.netloc.lower(), parts.path.rstrip("/")


def audience_matches(aud: object, resource: str) -> bool:
    """Whether `aud` (a string or a list) names `resource`: same host and path.
    The scheme is not compared: both sides are computed behind the same proxy."""
    want = _resource_key(resource)
    if want is None:
        return False
    names = aud if isinstance(aud, list) else [aud]
    return any(_resource_key(a) == want for a in names if isinstance(a, str))


async def bearer_subject(payload: object, *, mcp_resource: str = "", decode=None) -> int:
    """The account a decoded token speaks for here, or 0.

    `mcp_resource` is the MCP endpoint this request reached, and empty for
    every other route. `decode` maps `sub` to an id when the deployment
    obfuscates it (Server's `jwt_sub_decode_func`).
    """
    from mirobody.user.user import is_active_account

    if not isinstance(payload, dict) or payload.get("token_type") == REFRESH_TOKEN_TYPE:
        return 0
    if issued_to_client(payload) and not (mcp_resource and audience_matches(payload.get("aud"), mcp_resource)):
        return 0
    sub = payload.get("sub")
    try:
        user_id = int(decode(sub) if decode else sub)
    except (TypeError, ValueError):
        return 0
    if user_id <= 0 or not await is_active_account(user_id, minted_at(payload)):
        return 0
    return user_id
