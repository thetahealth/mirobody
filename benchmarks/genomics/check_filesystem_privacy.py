"""Ensure a public genotype upload cannot bypass bounded tools via read_file.

Point MIROBODY_GENOMICS_TEST_DSN at the isolated public-truth E2E database,
after e2e_public_truth.py has uploaded at least one genotype file.
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy.ext.asyncio import create_async_engine

from mirobody.agent.filesystem.files_backend import ThFilesBackend
from mirobody.utils.db import execute_query, use_engines


async def run() -> None:
    engine = create_async_engine(
        os.environ["MIROBODY_GENOMICS_TEST_DSN"],
        connect_args={"options": "-c search_path=public -c app.encryption_key=public-genomics-fixture-key"},
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
        print("genotype filesystem privacy passed: no raw upload/library entries")
    finally:
        await engine.dispose()
        use_engines(None)


if __name__ == "__main__":
    asyncio.run(run())
