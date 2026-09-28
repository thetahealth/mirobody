"""Upload two open PGP exports against the bounded packaged site candidate.

Raw participant exports must be held outside the source tree. The script
reports only counts and identifiers, never the participant calls.
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
from prepare_public_candidate import PGP_SOURCES, digest


async def run(base: str, pgp_dir: Path, *, include_bigy: bool = False) -> None:
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
            if name == "pgp_ancestry_v2":
                par = await mcp(client, base, headers, {
                    "chromosome": "PAR", "start": 170700, "end": 170800, "build": "raw",
                })
                assert "rs28736870" in par["result"] and "raw" in par["result"], par
            print(f"{name}: rows={active['n_rows']} called={active['n_called']} active_set={active['id']}")
        if include_bigy:
            manifest = json.loads((pgp_dir / "MANIFEST.json").read_text(encoding="utf-8"))
            entry = next(item for item in manifest if item.get("id") == "3779")
            path = pgp_dir / entry["file"]
            assert digest(path) == entry["sha256"], "public Big-Y source hash mismatch"
            await upload(base, token, subject, path.name, path.read_bytes(), timeout=360)
            response = await client.get(f"{base}/api/v1/genomics/active-set", headers=headers)
            response.raise_for_status()
            active = response.json()["data"]["active_set"]
            assert active["status"] == "active" and active["id"] != previous_set, active
            assert (active["n_rows"], active["n_called"]) == (444_297, 444_297), active
            profile = await mcp(client, base, headers, {})
            assert "dbsnp-b155-common-pgx-candidate" in profile["result"], profile
            print(f"pgp_bigy_vcf_zip: rows={active['n_rows']} called={active['n_called']} active_set={active['id']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18095")
    parser.add_argument("--pgp-dir", type=Path, required=True)
    parser.add_argument("--include-bigy", action="store_true",
                        help="Also upload the public PGP 3779 multi-member Big-Y archive")
    args = parser.parse_args()
    asyncio.run(run(args.base, args.pgp_dir, include_bigy=args.include_bigy))
