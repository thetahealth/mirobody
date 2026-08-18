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

import aioboto3

from mirobody.utils.config import safe_read_cfg

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
    config = get_s3_config()
    session = aioboto3.Session()
    async with session.client(
        "s3",
        region_name=config["region"],
        aws_secret_access_key=config["token"],
        aws_access_key_id=config["key"],
    ) as client:
        yield client  # Provide client to caller


def get_content_type(file_type):
    contentType = None
    if file_type in ["png", "jpeg", "jpg", "gif"]:
        contentType = f"image/{file_type}"
    elif file_type in ["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx"]:
        contentType = f"application/{file_type}"
    elif file_type == "json":
        contentType = "application/json"
    else:
        contentType = "application/octet-stream"
    return contentType


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


