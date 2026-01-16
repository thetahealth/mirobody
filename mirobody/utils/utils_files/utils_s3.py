import logging
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Tuple, Optional

# from config.apollo_config import S3_KEY, S3_TOKEN, S3_PRIVATE_BUCKET, S3_REGION_NAME
import aioboto3
import aiofiles
from PIL import Image

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
        logging.warning("An error occurred during file upload to S3", stack_info=True)
        # raise Exception(f"An error occurred during file upload to S3: {e}")
        return ""
    return url


async def aupload_file(file_name, key, content_type=None, bucket_name=None):
    url = ""
    try:
        if bucket_name is None:
            config = get_s3_config()
            bucket_name = config["bucket"]
        async with get_s3_client() as client:
            async with aiofiles.open(file_name, "rb") as data:
                file_data = await data.read()
                await client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=file_data,
                    ContentType=content_type,
                )
            url = await aget_s3_url(key, file_name, content_type, bucket_name=bucket_name)
        return url
    except Exception as e:
        logging.warning("An error occurred during file upload to S3", stack_info=True)
        raise Exception(f"An error occurred during file upload to S3: {e}")
    return url


async def aupload_object(key, data, content_type=None, expires_in=3600):
    url = ""
    try:
        config = get_s3_config()
        async with get_s3_client() as client:
            await client.put_object(Bucket=config["bucket"], Key=key, Body=data, ContentType=content_type)
        # We need to extract filename from key for URL generation
        file_name = key.split("/")[-1] if "/" in key else key
        url = await aget_s3_url(key, file_name, content_type, expires_in)
        return url
    except Exception as e:
        logging.warning("An error occurred during object upload to S3", stack_info=True)
        raise Exception(f"An error occurred during object upload to S3: {e}")


async def adownload_file(key, file_name):
    try:
        config = get_s3_config()
        async with get_s3_client() as client:
            response = await client.get_object(Bucket=config["bucket"], Key=key)
            data = await response["Body"].read()
            async with aiofiles.open(file_name, "wb") as f:
                await f.write(data)
    except Exception as e:
        logging.warning("An error occurred during file download from S3", stack_info=True)
        raise Exception(f"An error occurred during file download from S3: {e}")


async def a_get_object(key, bucket_name=None):
    try:
        if bucket_name is None:
            config = get_s3_config()
            bucket_name = config["bucket"]
        async with get_s3_client() as client:
            response = await client.get_object(Bucket=bucket_name, Key=key)
            data = await response["Body"].read()
            return data
    except Exception as e:
        logging.warning("An error occurred during get object from S3", stack_info=True)
        raise Exception(f"An error occurred during get object from S3: {e}")


async def adelete_object(key, bucket_name=None):
    """
    Delete object from S3
    :param key: Object key to delete
    :param bucket_name: Optional bucket name, defaults to configured bucket
    :return: Dict with success status and details
    """
    try:
        if bucket_name is None:
            config = get_s3_config()
            bucket_name = config["bucket"]
        
        if not bucket_name:
            logging.warning("S3 bucket not configured, deletion skipped")
            return {"success": False, "error": "S3 bucket not configured"}
        
        async with get_s3_client() as client:
            response = await client.delete_object(Bucket=bucket_name, Key=key)
            logging.info(f"Successfully deleted S3 object: {key}")
            return {
                "success": True,
                "key": key,
                "request_id": response.get("ResponseMetadata", {}).get("RequestId")
            }
    except Exception as e:
        logging.error(f"Error deleting S3 object: {e}", stack_info=True)
        return {"success": False, "error": str(e), "key": key}


def create_thumbnail(
    image_data: bytes,
    max_size: Tuple[int, int] = (200, 200),
    quality: int = 85,
    format: str = "PNG"
) -> bytes:
    """
    Create a thumbnail from image data
    
    :param image_data: Original image bytes
    :param max_size: Maximum size as (width, height) tuple, maintains aspect ratio
    :param quality: Image quality (1-100), only for JPEG
    :param format: Output format (PNG, JPEG, etc.)
    :return: Thumbnail image bytes
    """
    try:
        # Open image from bytes
        img = Image.open(BytesIO(image_data))
        
        # Convert RGBA to RGB if saving as JPEG
        if format.upper() == "JPEG" and img.mode == "RGBA":
            # Create a white background
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3] if len(img.split()) == 4 else None)
            img = background
        
        # Create thumbnail maintaining aspect ratio
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # Save to bytes
        output = BytesIO()
        save_kwargs = {"format": format}
        if format.upper() == "JPEG":
            save_kwargs["quality"] = quality
        
        img.save(output, **save_kwargs)
        return output.getvalue()
    
    except Exception as e:
        logging.error(f"Error creating thumbnail: {e}", stack_info=True)
        raise Exception(f"Failed to create thumbnail: {e}")


async def aupload_image_with_thumbnail(
    image_data: bytes,
    original_key: str,
    thumbnail_prefix: str = "thumb_",
    thumbnail_size: Tuple[int, int] = (200, 200),
    thumbnail_quality: int = 85,
    content_type: str = "image/png",
    expires_in: int = 3600,
    bucket_name: Optional[str] = None
) -> dict:
    """
    Upload an image and its thumbnail to S3
    
    :param image_data: Original image bytes
    :param original_key: S3 key for original image (e.g., "charts/image123.png")
    :param thumbnail_prefix: Prefix for thumbnail key (default: "thumb_")
    :param thumbnail_size: Maximum thumbnail size (width, height)
    :param thumbnail_quality: Thumbnail quality (1-100)
    :param content_type: Content type for both images
    :param expires_in: URL expiration time in seconds
    :param bucket_name: Optional bucket name
    :return: Dict with original and thumbnail URLs and keys
    
    Example:
        >>> result = await aupload_image_with_thumbnail(
        ...     image_data=image_bytes,
        ...     original_key="charts/myimage.png"
        ... )
        >>> print(result["original_url"])
        >>> print(result["thumbnail_url"])
        >>> # To get thumbnail later: use result["thumbnail_key"]
    """
    try:
        thumbnail_key = f"{thumbnail_prefix}{original_key}"
        
        # Determine image format from content type or file extension
        img_format = "PNG"
        if "jpeg" in content_type or "jpg" in content_type:
            img_format = "JPEG"
        elif original_key.lower().endswith((".jpg", ".jpeg")):
            img_format = "JPEG"
        
        # Create thumbnail
        thumbnail_data = create_thumbnail(
            image_data,
            max_size=thumbnail_size,
            quality=thumbnail_quality,
            format=img_format
        )
        
        # Upload original image
        original_url = await aupload_object(
            key=original_key,
            data=image_data,
            content_type=content_type,
            expires_in=expires_in
        )
        
        # Upload thumbnail
        thumbnail_url = await aupload_object(
            key=thumbnail_key,
            data=thumbnail_data,
            content_type=content_type,
            expires_in=expires_in
        )
        
        logging.info(f"Uploaded image and thumbnail: {original_key} -> {thumbnail_key}")
        
        return original_url
    
    except Exception as e:
        logging.error(f"Error uploading image with thumbnail: {e}", stack_info=True)
        raise Exception(f"Failed to upload image with thumbnail: {e}")


