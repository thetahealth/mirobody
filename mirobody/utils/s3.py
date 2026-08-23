"""Presigned S3 read URLs.

What is left after removing seven upload/download/thumbnail helpers that no
caller had used in a long time — along with their `PIL`/`aiofiles` imports,
which were the only thing pulling Pillow into this import path.

The two survivors are both reached from `pulse/file_parser` and
`user/sharing`; `get_s3_config`/`get_s3_client` stay because `aget_s3_url`
calls them, not because anything outside this module does.
"""

import logging
from contextlib import asynccontextmanager

from mirobody.utils.config import safe_read_cfg
from .file_types import guess_mime

# `aioboto3` is imported inside `get_s3_client`, not here: it ships with the
# `[server]` extra only, while this module is imported (via `pulse/file_parser`)
# from the bare engine install, whose header contract says `pip install
# mirobody` must be able to import it. A module-scope import made the whole
# file_parser chain crash on ImportError in the bare install — and broke test
# collection under `[test]`-only environments. Same pattern as
# `utils/config/storage/aws.py`.

# S3 configuration - lazy initialization
def get_s3_config():
    """Get S3 configuration safely"""
    try:
        return {
            "key": safe_read_cfg("s3_key"),
            "token": safe_read_cfg("s3_token"),
            "region": safe_read_cfg("s3_region"),
            "bucket": safe_read_cfg("s3_bucket"),
            "prefix": safe_read_cfg("s3_prefix"),
            "cdn": safe_read_cfg("s3_cdn"),
        }
    except Exception:
        return {
            "key": None,
            "token": None,
            "region": None,
            "bucket": None,
            "prefix": None,
            "cdn": None,
        }


@asynccontextmanager
async def get_s3_client():
    """
    Create and provide S3 client async context manager
    """
    import aioboto3

    config = get_s3_config()
    session = aioboto3.Session()
    async with session.client(
        "s3",
        region_name=config["region"],
        aws_secret_access_key=config["token"],
        aws_access_key_id=config["key"],
    ) as client:
        yield client  # Provide client to caller


def get_content_type(file_type: str) -> str:
    """MIME type for a bare extension, for the `Content-Type` on a presigned URL.

    Delegates to `utils.file_types.guess_mime`, which is the one table. This was
    an eight-branch ladder building the type by string interpolation, and it was
    wrong for 12 of the 18 extensions this project accepts — every Office format
    got an invented type (`application/xlsx`, `application/doc`) and everything
    outside the eight fell to octet-stream, i.e. a forced download for exactly
    the file kinds the README advertises accepting.
    """
    return guess_mime(file_type)


async def aget_s3_url(key, file_name, content_type=None, expires_in=3600, bucket_name=None):
    url = ""
    try:
        if bucket_name is None:
            config = get_s3_config()
            bucket_name = config["bucket"]
        
        if content_type is None:
            content_type = get_content_type(file_name.split(".")[-1])

        async with get_s3_client() as client:
            url = await client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket_name,
                    "Key": key,
                    "ResponseContentDisposition": "inline",
                    "ResponseContentType": content_type,
                },
                ExpiresIn=expires_in,
            )
        return url
    except Exception:
        # Said "file upload to S3" — a leftover from before the upload helpers
        # were removed from this module. This call only ever signs a GET.
        logging.warning("Failed to generate a presigned S3 URL", stack_info=True)
        return ""


