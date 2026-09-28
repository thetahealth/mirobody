"""Upload packaged public examples and query the same canonical calls over MCP.

Run against a disposable database-backed server. Each upload replaces the
previous active set; the original participant exports are not required.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
from importlib.resources import files

import httpx
import jwt

from e2e_public_truth import _table_positions, mcp, upload

EXAMPLES = files("mirobody.testing").joinpath("genomics")
FORMATS = (
    "hg00096-wegene.txt", "hg00096-23andme.txt", "hg00096-ancestry.txt",
    "hg00096-myheritage.csv", "hg00096-ftdna.csv", "hg00096-grch37.vcf",
    "hg00096-grch38.vcf", "hg00096-grch37.vcf.gz",
    "hg00096-grch37.vcf.bgz", "hg00096-grch37-vcf-sidecars.zip",
)


async def run(base: str) -> None:
    truth = {row["rsid"]: row for row in json.loads(
        EXAMPLES.joinpath("canonical.json").read_text(encoding="utf-8"))["hg00096"]}
    async with httpx.AsyncClient(timeout=30) as client:
        email = f"genomics-packaged-{secrets.token_hex(4)}@example.invalid"
        password = secrets.token_urlsafe(24)
        response = await client.post(f"{base}/password/register", json={"email": email, "password": password})
        response.raise_for_status()
        registration = response.json()
        assert registration["code"] == 0, registration.get("msg")
        token = registration["data"]["access_token"]
        subject = jwt.decode(token, options={"verify_signature": False})["sub"]
        headers = {"Authorization": f"Bearer {token}"}
        previous_set = None
        for name in FORMATS:
            await upload(base, token, subject, name, EXAMPLES.joinpath(name).read_bytes())
            response = await client.get(f"{base}/api/v1/genomics/active-set", headers=headers)
            response.raise_for_status()
            active = response.json()["data"]["active_set"]
            assert active["status"] == "active" and active["id"] != previous_set, active
            assert active["n_rows"] == (16 if name.endswith((".vcf", ".vcf.gz", ".vcf.bgz", ".zip"))
                                        and name != "hg00096-grch38.vcf" else 13), active
            previous_set = active["id"]
            result = await mcp(client, base, headers, {"rsids": sorted(truth)})
            rendered = result["result"]
            build = "pos38" if name == "hg00096-grch38.vcf" else "pos37"
            assert _table_positions(rendered) == {
                rsid: row[build] for rsid, row in truth.items()
            }, (name, rendered)
            assert "called" in rendered, (name, rendered)
            gene = await mcp(client, base, headers, {"gene": "CYP2C19"})
            assert all(rsid in gene["result"] for rsid in ("rs4986893", "rs4244285")), name
            print(f"{name}: active rows={active['n_rows']}, canonical calls={len(truth)}")
        await upload(base, token, subject, "pgp4220-nocall-23andme.txt",
                     EXAMPLES.joinpath("pgp4220-nocall-23andme.txt").read_bytes())
        no_call = await mcp(client, base, headers, {"rsids": ["rs3745274"]})
        assert "no_call" in no_call["result"], no_call
        pgx = await mcp(client, base, headers, {"drugs": ["clopidogrel"]},
                        name="query_pharmacogenomics")
        assert "not_determined" in pgx["result"], pgx
        print("public no-call and conservative CPIC result passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18092")
    args = parser.parse_args()
    asyncio.run(run(args.base))
