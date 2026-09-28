"""Upload two open PGP exports against the bounded packaged site candidate.

Raw participant exports must be held outside the source tree. The script
reports only counts and identifiers, never the participant calls.
"""

from __future__ import annotations

import argparse
import asyncio
import secrets
from pathlib import Path

import httpx
import jwt

from e2e_public_truth import mcp, upload
from prepare_public_candidate import PGP_SOURCES, digest


async def run(base: str, pgp_dir: Path) -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        email = f"genomics-candidate-{secrets.token_hex(4)}@example.invalid"
        password = secrets.token_urlsafe(24)
        response = await client.post(f"{base}/password/register", json={"email": email, "password": password})
        response.raise_for_status()
        registration = response.json()
        assert registration["code"] == 0, registration.get("msg")
        token = registration["data"]["access_token"]
        subject = jwt.decode(token, options={"verify_signature": False})["sub"]
        headers = {"Authorization": f"Bearer {token}"}
        previous_set = None
        for name, (filename, _, expected_sha) in PGP_SOURCES.items():
            path = pgp_dir / filename
            assert digest(path) == expected_sha, f"public source hash mismatch: {name}"
            await upload(base, token, subject, filename, path.read_bytes(), timeout=360)
            response = await client.get(f"{base}/api/v1/genomics/active-set", headers=headers)
            response.raise_for_status()
            active = response.json()["data"]["active_set"]
            assert active["status"] == "active" and active["id"] != previous_set, active
            assert active["n_rows"] > 600_000 and 0 < active["n_called"] < 1_000, active
            previous_set = active["id"]
            profile = await mcp(client, base, headers, {})
            assert "dbsnp-b155-common-pgx-candidate" in profile["result"], profile
            fact = await mcp(client, base, headers, {"rsids": ["rs4244285"]})
            assert "rs4244285" in fact["result"], fact
            print(f"{name}: rows={active['n_rows']} called={active['n_called']} active_set={active['id']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18095")
    parser.add_argument("--pgp-dir", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.base, args.pgp_dir))
