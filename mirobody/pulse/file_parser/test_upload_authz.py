"""A proxy upload must prove write access to the target's record.

The WebSocket upload path takes `query_user_id` — whose record the file is
filed under — straight from the client message. Without an authorization
check, any authenticated user could write a file into ANY user's record
(even a non-existent id) by naming it: the file lands under the victim's
query_user_id, the victim's agent VFS reads its content, and it never appears
in the attacker's own file list — the prompt-injection delivery surface
SECURITY.md lists. `handle_upload_start` is the choke point; it must call
`resolve_subject(..., require_write=True)` before a session is created, and
refuse when the grant is missing.
"""

from __future__ import annotations

import pytest

from mirobody.pulse.file_parser.file_upload_manager import WebSocketFileUploadManager


@pytest.fixture
def manager(monkeypatch):
    mgr = WebSocketFileUploadManager()
    sent: list[dict] = []

    async def fake_send(connection_id, message):
        sent.append(message)

    monkeypatch.setattr(mgr, "send_message", fake_send)
    mgr._sent = sent
    return mgr


def _start_msg(query_user_id=None):
    msg = {
        "messageId": "m1",
        "sessionId": "s1",
        "files": [{"filename": "report.pdf"}],
        "_real_user_id": "2",
    }
    if query_user_id is not None:
        msg["query_user_id"] = query_user_id
    return msg


async def test_a_proxy_upload_without_write_access_is_refused(manager, monkeypatch):
    import mirobody.user.care_circle as cc

    async def deny(operator_id, subject_id, *, require_write=False):
        raise cc.CareCircleDenied("no write grant")
    monkeypatch.setattr(cc, "resolve_subject", deny)

    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id="1"))

    assert ok is False, "an unauthorized proxy upload must not proceed"
    assert "m1" not in manager.upload_sessions, "no session may be created for a refused upload"
    assert manager._sent and manager._sent[-1]["type"] == "error", "the client must get an error event"


async def test_a_proxy_upload_to_a_nonexistent_user_is_refused(manager, monkeypatch):
    """user 3 with no membership (and not even a real account) must be denied —
    the real resolve_subject raises CareCircleDenied for a missing membership."""
    import mirobody.user.care_circle as cc

    async def deny(operator_id, subject_id, *, require_write=False):
        raise cc.CareCircleDenied("no membership")
    monkeypatch.setattr(cc, "resolve_subject", deny)

    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id="3"))
    assert ok is False and "m1" not in manager.upload_sessions


async def test_a_malformed_target_id_is_refused_not_crashed(manager, monkeypatch):
    import mirobody.user.care_circle as cc

    async def real_shaped(operator_id, subject_id, *, require_write=False):
        int(subject_id)  # what resolve_subject does; raises ValueError on "abc"
    monkeypatch.setattr(cc, "resolve_subject", real_shaped)

    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id="abc"))
    assert ok is False and "m1" not in manager.upload_sessions


async def test_a_proxy_upload_with_write_access_proceeds(manager, monkeypatch):
    import mirobody.user.care_circle as cc
    calls = []

    async def allow(operator_id, subject_id, *, require_write=False):
        calls.append((str(operator_id), str(subject_id), require_write))
    monkeypatch.setattr(cc, "resolve_subject", allow)

    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id="1"))

    assert ok is True, "an authorized proxy upload must proceed"
    assert manager.upload_sessions["m1"]["query_user_id"] == "1"
    assert calls == [("2", "1", True)], "must check the target for WRITE access"


async def test_a_self_upload_needs_no_care_circle_check(manager, monkeypatch):
    """Uploading to your own record is not proxy access — the gate must not
    even consult resolve_subject (which would be a needless round-trip and,
    with a strict fake, a failure)."""
    import mirobody.user.care_circle as cc

    async def boom(*a, **k):
        raise AssertionError("resolve_subject called for a self upload")
    monkeypatch.setattr(cc, "resolve_subject", boom)

    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id=None))
    assert ok is True and manager.upload_sessions["m1"]["user_id"] == "2"

    # query_user_id equal to the uploader is also self, not proxy.
    manager.upload_sessions.clear()
    ok = await manager.handle_upload_start("2_t", _start_msg(query_user_id="2"))
    assert ok is True
