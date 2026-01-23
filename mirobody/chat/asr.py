import aiohttp, hashlib, json, struct

#-----------------------------------------------------------------------------

def get_mime_type(data: bytes, default: str = "audio/pcm") -> str:
    if not data or len(data) < 12:
        return default
    
    result = None
    result = {
        b"\xff\xfb": "audio/mpeg",
        b"\xff\xf9": "audio/aac",
        b"\xff\xf3": "audio/mpeg",
        b"\xff\xf2": "audio/mpeg",
        b"\xff\xf1": "audio/aac",
    }.get(data[:2])
    if result:
        return result

    return {
        b"RIFF": "audio/wav",
        b"flac": "audio/flac",
        b"fLaC": "audio/flac",
        b"ID3\x03": "audio/mpeg",  # MP3 (ID3v2.3)
        b"ID3\x04": "audio/mpeg",  # MP3 (ID3v2.4)
        b"OggS": "audio/ogg",
    }.get(data[:4], default)

#-----------------------------------------------------------------------------

def pcm_to_wav(pcm_data: bytes, sample_rate: int = 16000, channels: int = 1, bit_depth: int = 16) -> bytes:
    data_size = len(pcm_data)
    byte_rate = sample_rate * channels * bit_depth // 8
    block_align = channels * bit_depth // 8

    # 4s (RIFF) I (Size) 4s (WAVE) 4s (fmt ) I (16)
    # H (Format) H (Channels) I (SampleRate) I (ByteRate) H (BlockAlign) H (BitDepth)
    # 4s (data) I (DataSize)
    header = struct.pack(
        '<4sI4s4sIHHIIHH4sI',
        b'RIFF', data_size + 36, b'WAVE',
        b'fmt ', 16, 1, channels, sample_rate, byte_rate, block_align, bit_depth,
        b'data', data_size
    )

    return header + pcm_data

#-----------------------------------------------------------------------------

async def gemini_upload_file(data: bytes, api_key: str, mime_type: str = "audio/wav") -> tuple[str | None, str | None, str | None]:
    if not data:
        return None, None, "Empty data."

    if not api_key:
        return None, None, "Empty API key."

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
                if not resp.ok:
                    return None, None, f"{init_url}: {resp.status}"

                upload_url = resp.headers.get("X-Goog-Upload-Control-URL")
                if not upload_url or not isinstance(upload_url, str):
                    return None, None, f"Failed to get X-Goog-Upload-Control-URL: {resp.headers}"

    except Exception as e:
        return None, None, str(e)

    #-----------------------------------------------------

    upload_headers = {
        "Content-Length": f"{n}",
        "X-Goog-Upload-Offset": "0",
        "X-Goog-Upload-Command": "upload, finalize"
    }

    file_name = ""
    file_url = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=upload_url, headers=upload_headers, data=data) as resp:
                resp_text = await resp.text()
                if not resp.ok:
                    return None, None, f"{upload_url}: {resp.status} {resp_text}"

                try:
                    resp_json = json.loads(resp_text)
                    file_name = resp_json["file"]["name"]
                    file_url = resp_json["file"]["uri"]
                except Exception as e:
                    return None, None, f"{upload_url}: {resp_text}"
                
                if not file_url or not isinstance(file_url, str):
                    return None, None, f"{upload_url}: {resp_text}"

                return file_name, file_url, None

    except Exception as e:
        return None, None, str(e)

#-----------------------------------------------------------------------------

async def gemini_transcript(
    file_url: str,
    api_key: str,
    model: str = "gemini-2.5-flash",
    mime_type: str = "audio/wav"
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
                {"text": "Describe this audio clip, the output should be a string and contain the transcription only."},
                {"file_data":{"mime_type": mime_type, "file_uri": file_url}}
            ]
        }]
    }

    text = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, json=data) as resp:
                resp_text = await resp.text()
                if not resp.ok:
                    return None, f"{resp.status} {resp_text}"

                try:
                    resp_json = json.loads(resp_text)
                    text = resp_json["candidates"][0]["content"]["parts"][0]["text"]
                except Exception as e:
                    return None, resp_text
                
                if not text or not isinstance(text, str):
                    return None, resp_text

    except Exception as e:
        return None, str(e)

    return text, None

#-----------------------------------------------------------------------------

async def gemini_list_files(api_key: str) -> tuple[list | None, str | None]:
    url = "https://generativelanguage.googleapis.com/v1beta/files"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers) as resp:
                resp_text = await resp.text()
                if not resp.ok:
                    return None, f"{resp.status} {resp_text}"

                try:
                    resp_json = json.loads(resp_text)
                    files = resp_json["files"]
                except Exception as e:
                    return None, resp_text
                
                if not isinstance(files, list):
                    return None, resp_text

                return files, None

    except Exception as e:
        return None, str(e)

#-----------------------------------------------------------------------------

async def gemini_delete_file(name: str, api_key: str) -> str | None:
    if not name or not isinstance(name, str):
        return "Invalid filename."

    if not api_key or not isinstance(api_key, str):
        return "Invalid API key."

    url = f"https://generativelanguage.googleapis.com/v1beta/{name}"
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(url=url, headers=headers) as resp:
                resp_text = await resp.text()
                if not resp.ok:
                    return f"{url} {resp.status} {resp_text}"

    except Exception as e:
        return str(e)
    
    print(url)
    return None

#-----------------------------------------------------------------------------
