"""Ensure a public genotype upload cannot bypass bounded tools via read_file.

Point MIROBODY_GENOMICS_TEST_DSN at the isolated public-truth E2E database,
after e2e_public_truth.py has uploaded at least one genotype file.
"""

from __future__ import annotations

import asyncio
import argparse
import gzip
import io
import os
import re
import uuid
import zipfile
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from mirobody.agent.filesystem.files_backend import ThFilesBackend
from mirobody.agent.chat.file import _detect_file_scene
from mirobody.collect.files.services.file_db_service import FileDbService
from mirobody.utils.db import execute_query, use_engines


async def run(schema: str) -> None:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
        raise ValueError("schema must be a simple PostgreSQL identifier")
    engine = create_async_engine(
        os.environ["MIROBODY_GENOMICS_TEST_DSN"],
        connect_args={"options": f"-c search_path={schema},public -c app.encryption_key=public-genomics-fixture-key"},
    )
    use_engines(lambda _name: engine)
    try:
        rows = await execute_query(
            """SELECT user_id, file_key, file_name, original_text, text_length
                FROM th_files WHERE scene = 'genetic'
                AND is_del = false ORDER BY id DESC LIMIT 1""", log_sql=False,
        )
        assert rows, "run the public upload E2E first"
        row = rows[0]
        user_id, file_key = str(row["user_id"]), str(row["file_key"])
        # A processed file can have extracted text in th_files. Force that
        # state so the library guard is tested even when the E2E upload has
        # no extraction text yet. This database contains public truth only.
        await execute_query(
            """UPDATE th_files SET file_name = encrypt_content(:name),
                original_text = encrypt_content(:text), text_length = :size
                WHERE user_id = :user_id AND file_key = :file_key""",
            {"name": "public.vcf", "text": "public synthetic genotype placeholder", "size": 37,
             "user_id": user_id, "file_key": file_key}, log_sql=False,
        )
        try:
            uploads = ThFilesBackend(user_id=user_id, scope="uploads", file_keys=[file_key])
            library = ThFilesBackend(user_id=user_id, scope="library")
            await execute_query(
                "UPDATE th_files SET scene = 'report' WHERE user_id = :user_id AND file_key = :file_key",
                {"user_id": user_id, "file_key": file_key}, log_sql=False,
            )
            assert (await uploads.als("/")).entries, "upload projection did not read the control file"
            assert (await library.als("/")).entries, "library projection did not read the control file"
            await execute_query(
                "UPDATE th_files SET scene = 'genetic' WHERE user_id = :user_id AND file_key = :file_key",
                {"user_id": user_id, "file_key": file_key}, log_sql=False,
            )
            assert not (await uploads.als("/")).entries, "genotype escaped into /uploads/"
            assert not (await library.als("/")).entries, "genotype escaped into /library/"
        finally:
            await execute_query(
                """UPDATE th_files SET scene = 'genetic', file_name = :name,
                    original_text = :text, text_length = :size
                    WHERE user_id = :user_id AND file_key = :file_key""",
                {"name": row["file_name"], "text": row["original_text"],
                 "size": row["text_length"],
                 "user_id": user_id, "file_key": file_key}, log_sql=False,
            )
        public = (Path(__file__).parent / "fixtures/public-hg00096.vcf").read_bytes()
        padded = public + b"##source=1000Genomes HG00096 public truth\n" * 500
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as output:
            output.writestr("public.vcf", padded)
        for name, mime, content in (
            ("public.vcf", "text/vcf", padded),
            ("public.vcf.gz", "application/gzip", gzip.compress(padded, compresslevel=0)),
            ("public.zip", "application/zip", archive.getvalue()),
        ):
            key = f"public-privacy-{uuid.uuid4()}"
            info = {"file_key": key, "file_name": name, "file_type": mime,
                    "content_type": mime, "content_bytes": content,
                    "original_text": public.decode(), "text_length": len(public)}
            scene = _detect_file_scene(info)
            assert scene == "genetic", (name, scene)
            try:
                inserted = await FileDbService.insert_files_batch(
                    user_id=user_id, files_info=[info], scene="report",
                    scenes_by_key={key: scene}, created_source="web_chat",
                    query_user_id=user_id,
                )
                assert len(inserted) == 1, (name, inserted)
                saved = await execute_query("SELECT scene FROM th_files WHERE file_key = :key",
                                            {"key": key}, log_sql=False)
                assert saved[0]["scene"] == "genetic", (name, saved)
                assert not (await ThFilesBackend(user_id=user_id, scope="uploads", file_keys=[key]).als("/")).entries, name
                library_rows = await ThFilesBackend(user_id=user_id, scope="library")._files()
                assert all(item["file_key"] != key for item in library_rows), name
            finally:
                await execute_query("DELETE FROM th_files WHERE file_key = :key", {"key": key}, log_sql=False)
        genetic_key, report_key = (f"public-privacy-{uuid.uuid4()}" for _ in range(2))
        mixed = [
            {"file_key": genetic_key, "file_name": "public.vcf", "file_type": "text/vcf",
             "content_type": "text/vcf", "content_bytes": public,
             "original_text": public.decode(), "text_length": len(public)},
            {"file_key": report_key, "file_name": "notes.txt", "file_type": "text/plain",
             "content_type": "text/plain", "content_bytes": b"public test control",
             "original_text": "public test control", "text_length": 19},
        ]
        try:
            inserted = await FileDbService.insert_files_batch(
                user_id=user_id, files_info=mixed, scene="report",
                scenes_by_key={item["file_key"]: _detect_file_scene(item) for item in mixed},
                created_source="web_chat", query_user_id=user_id,
            )
            assert len(inserted) == 2, inserted
            scenes = await execute_query("SELECT file_key, scene FROM th_files WHERE file_key IN (:g, :r)",
                                         {"g": genetic_key, "r": report_key}, log_sql=False)
            assert {item["file_key"]: item["scene"] for item in scenes} == {
                genetic_key: "genetic", report_key: "report",
            }
            uploads = ThFilesBackend(user_id=user_id, scope="uploads", file_keys=[genetic_key, report_key])
            assert len((await uploads.als("/")).entries) == 1
            library_rows = await ThFilesBackend(user_id=user_id, scope="library")._files()
            assert report_key in {item["file_key"] for item in library_rows}
            assert genetic_key not in {item["file_key"] for item in library_rows}
        finally:
            await execute_query("DELETE FROM th_files WHERE file_key IN (:g, :r)",
                                {"g": genetic_key, "r": report_key}, log_sql=False)
        print("genotype filesystem privacy passed: no raw upload/library entries")
    finally:
        await engine.dispose()
        use_engines(None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=os.environ.get("PG_SCHEMA", "theta_ai"),
                        help="schema holding th_files; use public for the isolated fixture database")
    asyncio.run(run(parser.parse_args().schema))
