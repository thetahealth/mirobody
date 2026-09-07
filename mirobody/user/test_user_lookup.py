"""One `health_app_user` lookup, and the reasons the exceptions are exceptions.

A hand-rolled `SELECT ... FROM health_app_user` at every call site is a
per-site chance to drop the `is_del = false` predicate — and a lookup without
it answers for deleted accounts, name, language, timezone and all.

`user.get_user` is the lookup. The static check below allows exactly the reads
that cannot go through it, and each allowance names why.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

from mirobody.user.user import get_user

ROOT = pathlib.Path(__file__).resolve().parents[2] / "mirobody"

# The reads that legitimately do not go through get_user. Each entry is
# (file suffix, a substring of the query itself) and each has a reason — an entry
# whose reason is "not converted yet" is a suppression, not an exception.
ALLOWED = [
    # The accessor's own query.
    ("user/user.py", "_USER_COLUMNS"),
    # Read-then-write inside ONE transaction. `get_user` runs its own
    # `engine.begin()`, so calling it here would move the SELECT to a different
    # connection and reopen the race the transaction exists to close.
    ("user/user.py", "WHERE email=%s AND is_del=FALSE"),
    ("user/care_circle.py", "SELECT id FROM ins"),
    # The demo seeder CREATES the sign-in accounts it then shares data between,
    # by email, upserting them; `get_user` reads an id it does not have yet.
    # Synthetic rows only, and only when SEED_DEMO_DATA is on.
    ("server/demo.py", "SELECT id FROM health_app_user WHERE email"),
    # The password check IS the query: `password_hash = crypt(:password,
    # password_hash)` compares inside Postgres so the hash never crosses into
    # Python, and the call runs with log_sql=False.
    ("user/user_service.py", "crypt("),
    # This one must SEE deleted rows — it selects `is_del` instead of filtering
    # on it, because signing up with the email of a soft-deleted account
    # revives that account rather than creating a second one.
    ("pulse/core/user.py", "ORDER BY create_at DESC"),
    # The fallback half of a find-or-create: `ON CONFLICT DO UPDATE ...
    # RETURNING` can come back empty under a concurrent insert, and this reads
    # back the row that insert wrote. Demo seeding, no user-facing path.
    ("demo/__init__.py", "LIMIT 1"),
]


def _sql_reads(path: pathlib.Path) -> list[str]:
    """Every SELECT ... FROM health_app_user in the file, with enough context to
    tell which one it is.

    The module docstring is dropped first: prose that quotes the pattern (this
    file, and `user/user.py`) is not a query, and matching it would force a
    suppression entry for a sentence.
    """
    text = path.read_text(encoding="utf-8")
    try:
        doc = ast.get_docstring(ast.parse(text))
    except SyntaxError:
        doc = None
    if doc:
        text = text.replace(doc, "", 1)

    out = []
    for m in re.finditer(r"FROM\s+health_app_user", text, re.I):
        start = text.rfind("SELECT", max(0, m.start() - 500), m.start())
        if start == -1:
            continue
        out.append(" ".join(text[max(0, start - 300):m.end() + 400].split()))
    return out


def _unexplained_reads() -> list[str]:
    found = []
    for path in sorted(ROOT.rglob("*.py")):
        if "__pycache__" in str(path) or path.name.startswith("test_"):
            continue
        rel = str(path.relative_to(ROOT))
        for context in _sql_reads(path):
            if any(rel.endswith(f) and marker in context for f, marker in ALLOWED):
                continue
            i = context.upper().find("SELECT")
            found.append(f"{rel}: {context[i:i + 110]}")
    return found


def test_no_new_hand_rolled_user_lookup():
    leftovers = _unexplained_reads()
    assert not leftovers, (
        "these read health_app_user without going through user.get_user, and "
        "without an entry in ALLOWED saying why:\n  " + "\n  ".join(leftovers)
    )


def test_the_scan_can_see_a_query_at_all():
    """Positive control: the regex must find the accessor's own SELECT."""
    hits = _sql_reads(ROOT / "user" / "user.py")
    assert hits, "the scan found nothing in user.py — the regex is broken"
    assert any("_USER_COLUMNS" in h for h in hits)
    assert not any("per-call-site chance" in h for h in hits), \
        "the module docstring is being scanned as if it were SQL"


@pytest.mark.parametrize("kwargs", [
    {},
    {"user_id": 1, "email": "a@b.c"},
    {"user_id": 1, "apple_sub": "x"},
    {"email": "a@b.c", "apple_sub": "x"},
])
async def test_exactly_one_selector(kwargs):
    """Two selectors is an ambiguous question, not a narrower one — an AND of
    id and email would silently return nothing for a mismatched pair."""
    with pytest.raises(ValueError, match="exactly one"):
        await get_user(**kwargs)


async def test_the_deleted_filter_is_not_optional(monkeypatch):
    """`is_del` is not a parameter, so no caller can widen the lookup."""
    seen = {}

    async def spy(query, params=None, **kw):
        seen["sql"] = " ".join(query.split())
        seen["params"] = params
        return []

    monkeypatch.setattr("mirobody.user.user.execute_query", spy)
    assert await get_user(user_id=7) is None
    assert "is_del = false" in seen["sql"]
    assert "WHERE id = :value" in seen["sql"]
    assert seen["params"] == {"value": 7}


async def test_email_is_matched_the_way_it_is_stored(monkeypatch):
    """Normalization belongs in the lookup, not in twenty callers: one call
    site matching without `.strip().lower()` is a login that fails for a
    correct password."""
    seen = {}

    async def spy(query, params=None, **kw):
        seen.update(params or {})
        return []

    monkeypatch.setattr("mirobody.user.user.execute_query", spy)
    await get_user(email="  Mixed.Case@Example.COM  ")
    assert seen["value"] == "mixed.case@example.com"


async def test_a_string_id_is_accepted(monkeypatch):
    """Care-circle ids travel as strings (`th_share_relationship` stores them as
    VARCHAR); the lookup coerces, so callers carry no `int(...)` at the
    boundary."""
    seen = {}

    async def spy(query, params=None, **kw):
        seen.update(params or {})
        return [{"id": 42}]

    monkeypatch.setattr("mirobody.user.user.execute_query", spy)
    row = await get_user(user_id="42")
    assert seen["value"] == 42 and row == {"id": 42}
