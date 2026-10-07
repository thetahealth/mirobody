"""`BasePullProvider`: the contract a pulled provider implements, and its pull loop."""

import json
import logging
import time
import uuid
from abc import abstractmethod
from typing import Any

from mirobody.collect import LinkRequest
from mirobody.collect.base import Provider
from mirobody.collect.core import LinkType
from mirobody.collect.core.push_service import push_service
from mirobody.collect.ingest import FormatDataContext, StandardPulseData, StandardPulseMetaInfo
from mirobody.collect.providers._platform.database_service import ProviderDatabaseService
from mirobody.kernel import connect
from mirobody.kernel.ops import is_driver_exception
from mirobody.user.platform import PlatformUserService
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class BasePullProvider(Provider):
    """A provider Mirobody pulls from the vendor, on a schedule or once after a link.

    A subclass implements `create_provider`, `info`, `format_data` (abstract,
    from `Provider`, so a class without it fails to load) and
    `pull_from_vendor_api`. It sets `raw_table` to keep payloads as received,
    or overrides `save_raw_data_to_db`. The loop is here: `pull_and_push`
    runs every linked account every `pull_interval_hours`, and an OAuth
    callback runs `_pull_and_push_for_user` once with `backfill_days`.
    """

    #: Hours between two scheduled pulls.
    pull_interval_hours: float = 1.0
    #: Days a scheduled pull asks for. Two, so a day the vendor finished
    #: scoring after the last pull is asked for again.
    pull_days: int = 2
    #: Days the pull right after a link asks for.
    backfill_days: int = 2
    #: The `health_data_<vendor>` table `save_raw_data_to_db` keeps payloads
    #: in; empty keeps none.
    raw_table: str = ""

    #: When to stop trying a credential the vendor keeps refusing, and for how
    #: long. Three because a transient outage is one or two refusals and a
    #: changed password is every one; thirty minutes because the person who
    #: fixes it does so by relinking, which resets the state anyway.
    DEBOUNCE = connect.DebouncePolicy(threshold=3, cooldown_ms=30 * 60 * 1000)

    def __init__(self) -> None:
        self.db_service = ProviderDatabaseService()
        self.user_service = PlatformUserService()
        self._credential_states: dict[str, connect.Credential] = {}

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "BasePullProvider | None":
        """An instance, or None when this deployment cannot run the provider.

        Override to return None when a credential the provider needs is not
        configured; that is the usual self-hosted state, not a fault.
        """
        try:
            return cls()
        except Exception as e:
            logger.warning("provider not created: provider_class=%s error_type=%s", cls.__name__,
                           type(e).__name__, exc_info=not is_driver_exception(e))
            return None

    def register_pull_task(self) -> bool:
        """Whether the scheduler pulls this provider every `pull_interval_hours`."""
        return True

    # ---- linking ---------------------------------------------------------------------

    async def link(self, request: LinkRequest) -> dict[str, Any]:
        """Store a PASSWORD or CUSTOMIZED credential after `_validate_credentials`
        accepts it. OAuth providers override this with their authorization step."""
        user_id = request.user_id
        auth_type = self.info.auth_type
        if auth_type in (LinkType.OAUTH1, LinkType.OAUTH2, LinkType.OAUTH):
            raise ValueError(f"{auth_type.value} links through the OAuth callback")

        await self._validate_credentials(request.credentials)
        connect_info = request.credentials.get("connect_info")
        if auth_type == LinkType.CUSTOMIZED:
            if not connect_info:
                raise ValueError("connect_info is required for a customized link")
            username = password = ""
        elif auth_type == LinkType.PASSWORD:
            username = request.credentials.get("username", "")
            password = request.credentials.get("password", "")
            if not username or not password:
                raise ValueError("username and password are required for a password link")
        else:
            raise ValueError(f"unsupported auth type {auth_type.value}")

        creds = self.db_service.Credentials(username=username, password=password, connect_info=connect_info)
        await self.db_service.save_user_theta_provider(user_id, self.info.slug, auth_type, creds)
        self._relinked(user_id)
        logger.info("provider linked: provider=%s user_id=%s", self.info.slug, user_id)
        result = {"provider_slug": self.info.slug, "msg": "ok", "connected": True}
        if username:
            result["username"] = username
        return result

    async def unlink(self, user_id: str) -> dict[str, Any]:
        await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
        logger.info("provider unlinked: provider=%s user_id=%s", self.info.slug, user_id)
        return {"provider_slug": self.info.slug}

    async def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        """Reject credentials that cannot work, by raising. Default: accept.

        `credentials` is the LinkRequest's dict: `username`/`password` for
        `LinkType.PASSWORD`, `connect_info` for `LinkType.CUSTOMIZED`. Override
        to probe the vendor.
        """

    def _relinked(self, user_id: str) -> None:
        """A relink is a new credential: the old one's refusals say nothing about it."""
        self._credential_states.pop(str(user_id), None)

    # ---- formatting ------------------------------------------------------------------

    async def _get_user_timezone(self, user_id: str) -> str:
        try:
            user_info = await self.user_service.get_user_by_id(user_id)
        except Exception as e:
            logger.warning("timezone lookup failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                           exc_info=not is_driver_exception(e))
            return "UTC"
        tz = str((user_info or {}).get("tz") or "").strip()
        return tz or "UTC"

    def _extract_theta_user_id(self, saved_data: dict[str, Any]) -> str:
        """The account a saved payload belongs to. Only the pull loop sets
        `theta_user_id`; the webhook route strips it from a push."""
        return str(saved_data.get("theta_user_id") or "")

    def _extract_external_user_id(self, saved_data: dict[str, Any]) -> str:
        """The vendor's id for the person: top-level `user_id` by default.
        Override where the vendor keeps it deeper in the payload."""
        return str(saved_data.get("user_id") or "")

    async def build_format_context(self, saved_data: dict[str, Any]) -> FormatDataContext:
        """What `format_data` needs resolved first (account, vendor id,
        timezone, msg_id), so that it needs no database."""
        theta_user_id = self._extract_theta_user_id(saved_data)
        return FormatDataContext(
            theta_user_id=theta_user_id,
            external_user_id=self._extract_external_user_id(saved_data),
            user_timezone=await self._get_user_timezone(theta_user_id),
            msg_id=saved_data.get("msg_id", ""),
        )

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())

    def _create_empty_response(self, request_id: str, user_id: str) -> StandardPulseData:
        meta_info = StandardPulseMetaInfo(userId=user_id or "", requestId=request_id, source="theta", timezone="UTC")
        return StandardPulseData(metaInfo=meta_info, healthData=[])

    # ---- pulling ---------------------------------------------------------------------

    @abstractmethod
    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        """The last `days` of one linked account's data, as packages for `push_data`.

        `credentials` is the account as `get_all_user_credentials` returns it
        for this provider's auth type: always `user_id`, then `username` and
        `password` (PASSWORD), `access_token`, `refresh_token` and `expires_at`
        (OAUTH2), `access_token` and `access_token_secret` (OAUTH1) or
        `connect_info` (CUSTOMIZED). Raise `PermissionError` when the vendor
        refuses the credential, which is what counts toward expiring it; any
        other exception is a failed run and the next one tries again.
        """

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Keep a pulled package in `raw_table` and return it for `format_data`.

        A payload with no `theta_user_id` came from outside the pull loop and
        names no account this provider can verify, so it is not kept.
        """
        theta_user_id = self._extract_theta_user_id(raw_data)
        if not theta_user_id:
            logger.warning("payload without an account dropped: provider=%s", self.info.slug)
            return []
        raw_data["msg_id"] = str(raw_data.get("msg_id") or uuid.uuid4())
        if self.raw_table:
            try:
                await execute_query(
                    query=(
                        f"INSERT INTO {self.raw_table} "
                        "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
                        "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, FALSE, :msg_id, :raw_data, "
                        ":theta_user_id, :external_user_id)"
                    ),
                    params={
                        "msg_id": raw_data["msg_id"],
                        "raw_data": json.dumps(raw_data, ensure_ascii=False),
                        "theta_user_id": theta_user_id,
                        "external_user_id": self._extract_external_user_id(raw_data),
                    },
                )
            except Exception as e:
                logger.error("raw save failed: provider=%s user_id=%s error_type=%s", self.info.slug,
                             theta_user_id, type(e).__name__, exc_info=not is_driver_exception(e))
                return []
        return [raw_data]

    async def get_all_user_credentials(self) -> list[dict[str, Any]]:
        return await self.db_service.get_all_user_credentials_for_provider(self.info.slug, self.info.auth_type)

    async def pull_and_push(self) -> bool:
        """One scheduled run: every linked account, `pull_days` back."""
        results = [
            await self._pull_and_push_for_user(credentials, days=self.pull_days)
            for credentials in await self.get_all_user_credentials()
        ]
        logger.info("pull run done: provider=%s account_count=%d failed_count=%d",
                    self.info.slug, len(results), results.count(False))
        return all(results)

    def _now_ms(self) -> int:
        """The clock, as one overridable call. A back-off is a decision about
        elapsed time, and a test that cannot move time can only assert that
        nothing happens yet."""
        return int(time.time() * 1000)

    async def _pull_and_push_for_user(self, credentials: dict[str, Any], *, days: int) -> bool:
        """Pull one account's last `days` and hand each package to the platform.

        A credential the vendor refused `DEBOUNCE.threshold` times in a row
        is left alone until the person relinks: a changed password otherwise
        fails on every run, and some vendors lock the account the person
        still uses. Only a refusal (`PermissionError`) counts; a timeout or a
        vendor outage is not the credential's fault. The state is held per
        worker, the right scope for "do not hammer this account right now".
        """
        user_id = str(credentials["user_id"])
        slug = self.info.slug
        state = self._credential_states.get(user_id) or connect.Credential(provider=slug, subject_id=user_id)
        now_ms = self._now_ms()
        if not connect.may_attempt(state, now_ms=now_ms, policy=self.DEBOUNCE):
            logger.info("pull skipped: provider=%s user_id=%s state=%s failure_count=%d",
                        slug, user_id, state.state, state.failures)
            return True
        try:
            packages = await self.pull_from_vendor_api(credentials, days)
        except PermissionError as e:
            state = connect.record_failure(state, now_ms=now_ms, policy=self.DEBOUNCE)
            self._credential_states[user_id] = state
            logger.warning("pull refused: provider=%s user_id=%s error_type=%s failure_count=%d",
                           slug, user_id, type(e).__name__, state.failures)
            return False
        except Exception as e:
            logger.error("pull failed: provider=%s user_id=%s error_type=%s", slug, user_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False
        self._credential_states[user_id] = connect.record_success(state)

        pushed = 0
        for package in packages:
            package["theta_user_id"] = user_id
            if await push_service.push_data(platform="theta", provider_slug=slug, data=package):
                pushed += 1
        logger.info("pull done: provider=%s user_id=%s package_count=%d pushed_count=%d",
                    slug, user_id, len(packages), pushed)
        return pushed == len(packages)
