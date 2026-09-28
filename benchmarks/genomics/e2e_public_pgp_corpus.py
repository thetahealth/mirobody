"""Upload original open PGP genotype exports from an external corpus.

The corpus is never packaged. Use --ids to resume a long run without
re-uploading cases already measured. Only counts and public file IDs print.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
from pathlib import Path

import httpx
import jwt

from e2e_public_truth import mcp, upload
from prepare_public_candidate import digest


async def run(base: str, directory: Path, ids: set[str] | None = None) -> None:
    entries = json.loads((directory / "MANIFEST.json").read_text(encoding="utf-8"))
    selected = [entry for entry in entries if entry.get("status") == "ok"
                and not entry["file"].lower().endswith(".html")
                and (ids is None or entry["id"] in ids)]
    assert selected and (ids is None or {entry["id"] for entry in selected} == ids)
    async with httpx.AsyncClient(timeout=30) as client:
        email = f"genomics-pgp-{secrets.token_hex(4)}@example.invalid"
        password = secrets.token_urlsafe(24)
        response = await client.post(f"{base}/password/register", json={"email": email, "password": password})
        response.raise_for_status()
        registration = response.json()
        assert registration["code"] == 0, registration.get("msg")
        token = registration["data"]["access_token"]
        subject = jwt.decode(token, options={"verify_signature": False})["sub"]
        headers = {"Authorization": f"Bearer {token}"}
        previous_set = None
        for entry in selected:
            source = directory / entry["file"]
            assert digest(source) == entry["sha256"], f"public PGP hash mismatch id={entry['id']}"
            await upload(base, token, subject, source.name, source.read_bytes(), timeout=900)
            response = await client.get(f"{base}/api/v1/genomics/active-set", headers=headers)
            response.raise_for_status()
            active = response.json()["data"]["active_set"]
            assert active["status"] == "active" and active["id"] != previous_set, active
            assert active["n_rows"] > 0 and 0 <= active["n_called"] <= active["n_rows"], active
            previous_set = active["id"]
            profile = await mcp(client, base, headers, {})
            assert "dbsnp-b155-common-pgx-candidate" in profile["result"], profile
            print(f"public_pgp_id={entry['id']} kind={entry['kind']} "
                  f"rows={active['n_rows']} called={active['n_called']} active_set={active['id']}",
                  flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18096")
    parser.add_argument("--pgp-dir", type=Path, required=True)
    parser.add_argument("--ids", help="Comma-separated public PGP IDs; default is all 17 genotype files")
    args = parser.parse_args()
    asyncio.run(run(args.base, args.pgp_dir, set(args.ids.split(",")) if args.ids else None))
