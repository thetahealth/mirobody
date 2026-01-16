import aiohttp, hashlib

#-----------------------------------------------------------------------------

async def gemini_upload(data: bytes, mime_type: str, api_key: str) -> tuple[str | None, str | None]:
    if not data:
        return None, "Empty data."

    if not api_key:
        return None, "Empty API key."

    #-----------------------------------------------------

    n = len(data)
    filename = hashlib.md5(data).hexdigest()

    init_url = "https://generativelanguage.googleapis.com/upload/v1beta/files"
    init_headers = {
        "x-goog-api-key": api_key,
        "X-Goog-Upload-Protocol": "resumable",
        "X-Goog-Upload-Command": "start",
        "X-Goog-Upload-Header-Content-Length": f"{n}",
        "X-Goog-Upload-Header-Content-Type": f"{mime_type}",
        "Content-Type": "application/json"
    }
    init_json = {
        "file": {
            "display_name": filename
        }
    }

    upload_url = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=init_url, headers=init_headers, json=init_json) as resp:
                json_resp = await resp.json()
                print(f"resp: {json_resp}")

    except Exception as e:
        return None, str(e)

    #-----------------------------------------------------

    upload_headers = {
        "Content-Length": f"{n}",
        "X-Goog-Upload-Offset": "0",
        "X-Goog-Upload-Command": "upload, finalize"
    }

    file_url = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=upload_url, headers=upload_headers, data=data) as resp:
                json_resp = await resp.json()
                print(f"resp: {json_resp}")

    except Exception as e:
        return None, str(e)

    #-----------------------------------------------------

    return file_url, None

#-----------------------------------------------------------------------------

async def gemini_transcript(
    file_url: str,
    api_key: str,
    model: str = "gemini-2.5-flash",
    mime_type: str = "audio/pcm"
) -> tuple[dict | None, str | None]:
    if not file_url:
        return None, "Empty file URL."

    if not api_key:
        return None, "Empty API key."

    #-----------------------------------------------------

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json"
    }
    data = {
        "contents": [{
            "parts":[
                {"text": "Describe this audio clip"},
                {"file_data":{"mime_type": "${mime_type}", "file_uri": '$file_uri'}}
            ]
        }]
    }

    text = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, json=data) as resp:
                json_resp = await resp.json()
                print(f"resp: {json_resp}")

    except Exception as e:
        return None, str(e)

    return text, None

#-----------------------------------------------------------------------------
