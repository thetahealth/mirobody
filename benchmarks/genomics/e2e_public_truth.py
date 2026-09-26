"""Exercise public 1000G calls through the real upload, MCP and Agent stack.

Generate the input with generate_public_formats.py and start a disposable
database-backed server with GENOTYPE_SITE_CATALOG pointed at its test catalog.
The truth manifest is derived from pinned 1000G and PharmCAT files.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import secrets
import uuid
from pathlib import Path
from urllib.parse import quote

import httpx
import jwt
import websockets


async def upload(base: str, token: str, subject: str, filename: str, payload: bytes) -> None:
    message_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    uri = base.replace("http://", "ws://") + f"/ws/upload-health-report?token={quote(token)}"
    async with websockets.connect(uri, max_size=8 * 1024 * 1024) as socket:
        await socket.send(json.dumps({
            "type": "upload_start", "messageId": message_id, "sessionId": session_id,
            "query_user_id": subject, "isFirstMessage": False,
            "files": [{"filename": filename, "contentType": "text/plain", "size": len(payload)}],
        }))
        async with asyncio.timeout(120):
            while True:
                event = json.loads(await socket.recv())
                if event.get("type") == "upload_start_confirmed":
                    break
                if event.get("type") in {"error", "upload_error"}:
                    raise AssertionError(f"upload rejected: {event.get('type')}")
        await socket.send(json.dumps({
            "type": "upload_chunk", "messageId": message_id, "filename": filename,
            "contentType": "text/plain", "chunk": base64.b64encode(payload).decode(),
            "chunkIndex": 0, "totalChunks": 1, "fileSize": len(payload),
        }))
        await socket.send(json.dumps({"type": "upload_end", "messageId": message_id, "sessionId": session_id}))
        async with asyncio.timeout(120):
            while True:
                event = json.loads(await socket.recv())
                if event.get("messageId") != message_id:
                    continue
                if event.get("type") in {"error", "upload_error"} or event.get("status") == "failed":
                    raise AssertionError(f"upload failed: {event.get('type')}")
                if event.get("genetic_processing_final"):
                    return


async def mcp(client: httpx.AsyncClient, base: str, headers: dict, args: dict,
              name: str = "query_genetic_data") -> dict:
    response = await client.post(f"{base}/mcp", headers=headers, json={
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": args},
    })
    response.raise_for_status()
    body = response.json()
    assert "error" not in body, body.get("error")
    result = json.loads(body["result"]["content"][0]["text"])
    assert result["status"] == "ok", result
    return result


async def run(base: str, corpus: Path, *, agent: bool = False,
              export_path: Path | None = None) -> None:
    manifest = json.loads((corpus / "MANIFEST.json").read_text())
    truth = {call["rsid"]: call for call in manifest["calls"]}
    async with httpx.AsyncClient(timeout=30) as client:
        email = f"genomics-public-{secrets.token_hex(4)}@example.invalid"
        password = secrets.token_urlsafe(24)
        response = await client.post(f"{base}/password/register", json={"email": email, "password": password})
        response.raise_for_status()
        data = response.json()
        assert data["code"] == 0, data.get("msg")
        token = data["data"]["access_token"]
        subject = jwt.decode(token, options={"verify_signature": False})["sub"]
        headers = {"Authorization": f"Bearer {token}"}
        previous_set = None
        cases = ("public_23andme.txt", "public_ancestry.txt", "public_myheritage.csv", "public.vcf")
        for filename in cases:
            await upload(base, token, subject, filename, (corpus / filename).read_bytes())
            response = await client.get(f"{base}/api/v1/genomics/active-set", headers=headers)
            response.raise_for_status()
            active = response.json()["data"]["active_set"]
            assert active["status"] == "active" and active["n_rows"] == len(truth), active
            assert active["id"] != previous_set, active
            previous_set = active["id"]
            for args in ({"rsids": sorted(truth)}, {"gene": "CYP2C19"},
                         {"chromosome": "10", "start": 94780653, "end": 94781859, "build": "GRCh38"}):
                result = await mcp(client, base, headers, args)
                rendered = result["result"]
                for rsid, call in truth.items():
                    assert rsid in rendered, (filename, args, rendered)
                    assert str(call["pos38"]) in rendered, (filename, args, rendered)
                    if filename == "public.vcf":
                        assert call["gt"].replace("|", "\\|") in rendered, (filename, args, rendered)
                    else:
                        assert call["genotype"] in rendered, (filename, args, rendered)
                        assert call["gt"].replace("|", "/") in rendered or (
                            call["gt"] == "1|0" and "0/1" in rendered
                        ), (filename, args, rendered)
                assert "called" in rendered and "GRCh37" in rendered, rendered
            profile = await mcp(client, base, headers, {})
            assert "public-1000g-pharmcat-e2e" in profile["result"], profile
            pgx = await mcp(client, base, headers, {"drugs": ["clopidogrel"]},
                            name="query_pharmacogenomics")
            assert all(term in pgx["result"] for term in (
                "CYP2C19", "not_determined", "v1.60.0", "43", "41",
            )), pgx
            export = await client.get(f"{base}/api/v1/genomics/export.vcf", headers=headers,
                                      params={"build": "GRCh38"})
            export.raise_for_status()
            if filename == "public.vcf" and export_path is not None:
                export_path.parent.mkdir(parents=True, exist_ok=True)
                export_path.write_text(export.text)
            assert "##reference=GRCh38" in export.text
            exported = {line.split("\t")[2]: line.split("\t") for line in export.text.splitlines()
                        if line and not line.startswith("#")}
            assert set(exported) == set(truth), (filename, exported)
            for rsid, call in truth.items():
                fields = exported[rsid]
                assert (fields[0], int(fields[1]), fields[3], fields[4]) == (
                    "chr10", call["pos38"], "G", "A",
                ), (filename, fields)
            print(f"{filename}: {len(truth)} public calls, active set {active['id']}, rsID/gene/GRCh38 queries passed")

        if agent:
            questions = (
                ("What does my uploaded rs4244285 say? Include its genotype, source and genome build; do not diagnose.",
                 "query_genetic_data", ("rs4244285", "GRCh37")),
                ("Does my uploaded genotype establish how I respond to clopidogrel? Name the CPIC version and any missing sites.",
                 "query_pharmacogenomics", ("CYP2C19", "v1.60.0")),
            )
            for question, expected_tool, required in questions:
                events = []
                async with client.stream(
                    "POST", f"{base}/api/chat", headers=headers,
                    json={"question": question, "query_user_id": subject,
                          "language": "en", "timezone": "UTC"},
                    timeout=120,
                ) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            events.append(json.loads(line[6:]))
                calls = [event.get("name") for event in events if event.get("type") == "tool_call"]
                answer = "".join(str(event.get("content") or event.get("text") or "")
                                 for event in events if event.get("type") == "text")
                assert expected_tool in calls, (expected_tool, calls, [event.get("type") for event in events], answer[:1000])
                assert all(term in answer for term in required), answer[:1000]
                if expected_tool == "query_genetic_data":
                    assert any(value in answer for value in ("AG", "0/1", "1|0", "1\\|0")), answer[:1000]
                    assert "unphased" not in answer.lower(), answer[:1000]
                else:
                    assert "not determined" in answer.lower() or "cannot" in answer.lower(), answer[:1000]
                print(f"Agent: tool={expected_tool}, calls={calls}, answer_chars={len(answer)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:18092")
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--agent", action="store_true", help="Also require a live model answer")
    parser.add_argument("--export-path", type=Path, help="Save the public GRCh38 VCF for an external validator")
    options = parser.parse_args()
    asyncio.run(run(options.base, options.corpus, agent=options.agent,
                    export_path=options.export_path))
