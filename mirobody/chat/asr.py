import aiohttp, base64, hashlib, json, struct

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

def _parse_mp4_box(data: bytes, offset: int) -> tuple[bytes, bytes, int, int]:
    """Parse a single MP4 box. Returns (box_type, box_data, header_size, box_size)"""
    if offset + 8 > len(data):
        return b'', b'', 0, 0

    box_size = struct.unpack('>I', data[offset:offset+4])[0]
    box_type = data[offset+4:offset+8]
    header_size = 8

    # Handle extended size
    if box_size == 1 and offset + 16 <= len(data):
        box_size = struct.unpack('>Q', data[offset+8:offset+16])[0]
        header_size = 16

    # Box size of 0 means rest of file
    if box_size == 0:
        box_size = len(data) - offset

    # Validate
    if box_size < header_size or offset + box_size > len(data):
        return b'', b'', 0, 0

    box_data = data[offset+header_size:offset+box_size]
    return box_type, box_data, header_size, box_size


def _find_box(data: bytes, box_type: bytes, offset: int = 0) -> bytes:
    """Find and return the contents of a box by type"""
    while offset < len(data) - 8:
        btype, bdata, _, bsize = _parse_mp4_box(data, offset)
        if not btype:
            break
        if btype == box_type:
            return bdata
        offset += bsize
    return b''


def _find_box_recursive(data: bytes, path: list[bytes]) -> bytes:
    """Find a box by following a path of box types"""
    current_data = data
    for box_type in path:
        current_data = _find_box(current_data, box_type)
        if not current_data:
            return b''
    return current_data


def _parse_esds(esds_data: bytes) -> tuple[int, int, int]:
    """Parse esds box to extract audio config. Returns (profile, sample_rate_index, channels)"""
    # esds has version/flags (4 bytes), then ES_Descriptor tag structure
    # This is a simplified parser that looks for AudioSpecificConfig
    if len(esds_data) < 20:
        return 2, 8, 2  # Defaults: AAC-LC, 16kHz, stereo

    # Scan for decoder config descriptor (tag 0x04) and then decoder specific info (tag 0x05)
    offset = 4  # Skip version/flags

    while offset < len(esds_data) - 5:
        tag = esds_data[offset]
        offset += 1

        # Parse descriptor size (variable length)
        size = 0
        for _ in range(4):
            if offset >= len(esds_data):
                break
            byte = esds_data[offset]
            offset += 1
            size = (size << 7) | (byte & 0x7F)
            if not (byte & 0x80):
                break

        # Tag 0x05 is DecoderSpecificInfo containing AudioSpecificConfig
        if tag == 0x05 and offset + 2 <= len(esds_data):
            # AudioSpecificConfig: first 5 bits = audioObjectType, next 4 bits = samplingFrequencyIndex
            # next 4 bits = channelConfiguration
            config_bytes = esds_data[offset:offset+2]
            if len(config_bytes) >= 2:
                # Parse bit fields
                byte1 = config_bytes[0]
                byte2 = config_bytes[1]

                audio_object_type = (byte1 >> 3) & 0x1F
                sample_rate_index = ((byte1 & 0x07) << 1) | ((byte2 >> 7) & 0x01)
                channel_config = (byte2 >> 3) & 0x0F

                return audio_object_type, sample_rate_index, channel_config

        offset += size

    # Fallback defaults
    return 2, 8, 2  # AAC-LC, 16kHz, stereo


def _parse_stsz(stsz_data: bytes) -> list[int]:
    """Parse stsz (Sample Size) box to get list of frame sizes"""
    if len(stsz_data) < 12:
        return []

    # stsz: version/flags (4), sample_size (4), sample_count (4), then size table
    sample_size = struct.unpack('>I', stsz_data[4:8])[0]
    sample_count = struct.unpack('>I', stsz_data[8:12])[0]

    # If sample_size != 0, all samples have the same size
    if sample_size != 0:
        return [sample_size] * sample_count

    # Otherwise, read size table
    sizes = []
    offset = 12
    for _ in range(sample_count):
        if offset + 4 > len(stsz_data):
            break
        size = struct.unpack('>I', stsz_data[offset:offset+4])[0]
        sizes.append(size)
        offset += 4

    return sizes


def _create_adts_header(profile: int, sample_rate_index: int, channels: int, frame_length: int) -> bytes:
    """Create 7-byte ADTS header"""
    # ADTS header structure (7 bytes, no CRC)
    # Syncword: 0xFFF (12 bits)
    # ID: 0 (MPEG-4) (1 bit)
    # Layer: 0 (2 bits)
    # Protection absent: 1 (1 bit)
    # Profile: profile - 1 (2 bits)
    # Sample rate index: (4 bits)
    # Private: 0 (1 bit)
    # Channel config: (3 bits)
    # Original: 0 (1 bit)
    # Home: 0 (1 bit)
    # Copyright ID: 0 (1 bit)
    # Copyright start: 0 (1 bit)
    # Frame length: including header (13 bits)
    # Buffer fullness: 0x7FF (11 bits)
    # Number of frames: 0 (2 bits)

    # Adjust profile (AAC profile - 1 for ADTS)
    adts_profile = (profile - 1) & 0x03

    # Build header bytes
    byte0 = 0xFF  # Syncword part 1
    byte1 = 0xF0 | (0 << 3) | (0 << 2) | (1 << 0)  # Syncword part 2, ID=0, Layer=0, Protection=1
    byte2 = (adts_profile << 6) | (sample_rate_index << 2) | (0 << 1) | ((channels >> 2) & 0x01)
    byte3 = ((channels & 0x03) << 6) | ((frame_length >> 11) & 0x03)
    byte4 = (frame_length >> 3) & 0xFF
    byte5 = ((frame_length & 0x07) << 5) | 0x1F  # frame_length low bits + buffer fullness high bits
    byte6 = 0xFC  # buffer fullness low bits + number of frames (0)

    return struct.pack('7B', byte0, byte1, byte2, byte3, byte4, byte5, byte6)


def m4a_to_aac(m4a_data: bytes, sample_rate: int = 16000, channels: int = 1, bit_depth: int = 16) -> bytes:
    """
    Extract AAC audio stream from M4A container and add ADTS headers.
    """
    if not m4a_data or len(m4a_data) < 8:
        return b''

    # Sample rate to index mapping
    sample_rate_map = {
        96000: 0, 88200: 1, 64000: 2, 48000: 3,
        44100: 4, 32000: 5, 24000: 6, 22050: 7,
        16000: 8, 12000: 9, 11025: 10, 8000: 11
    }

    # Parse MP4 structure
    # Get audio config from esds
    esds_data = _find_box_recursive(m4a_data, [b'moov', b'trak', b'mdia', b'minf', b'stbl', b'stsd', b'mp4a', b'esds'])

    if esds_data:
        profile, sr_index, ch_config = _parse_esds(esds_data)
    else:
        # Fallback to function parameters
        profile = 2  # AAC-LC
        sr_index = sample_rate_map.get(sample_rate, 8)  # Default to 16kHz
        ch_config = channels

    # Get frame sizes from stsz
    stsz_data = _find_box_recursive(m4a_data, [b'moov', b'trak', b'mdia', b'minf', b'stbl', b'stsz'])
    frame_sizes = []
    if stsz_data:
        frame_sizes = _parse_stsz(stsz_data)

    # Get raw AAC data from mdat
    mdat_data = _find_box(m4a_data, b'mdat')
    if not mdat_data:
        return b''

    # If we don't have frame sizes, treat entire mdat as one frame
    if not frame_sizes:
        frame_length = 7 + len(mdat_data)
        header = _create_adts_header(profile, sr_index, ch_config, frame_length)
        return header + mdat_data

    # Split mdat by frame sizes and add ADTS headers
    result = b''
    offset = 0

    for frame_size in frame_sizes:
        if offset + frame_size > len(mdat_data):
            break

        frame_data = mdat_data[offset:offset+frame_size]
        frame_length = 7 + frame_size
        header = _create_adts_header(profile, sr_index, ch_config, frame_length)
        result += header + frame_data
        offset += frame_size

    return result

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
                {"text": "Describe this audio clip, the output should be a string and contain the transcription only, and never use traditional Chinese."},
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

async def google_cloud_transcript(
    data: bytes,
    google_cloud_api_key: str,
    model: str = "default",
) -> tuple[str | None, str | None]:
    """
    Transcribe audio using Google Cloud Speech-to-Text API.
    Returns (transcript_text, error_message).
    """

    if not data:
        return None, "Empty audio data."

    if not google_cloud_api_key:
        return None, "Empty API key."
    
    mime_type = get_mime_type(data=data, default="audio/pcm")

    # Map mime_type to Google Cloud encoding
    encoding = {
        "audio/wav": "LINEAR16",
        "audio/pcm": "LINEAR16",
        "audio/flac": "FLAC",
        "audio/ogg": "OGG_OPUS",
        "audio/mpeg": "MP3",
    }.get(mime_type)
    if not encoding:
        return None, "Unsupported mime type."

    # Encode audio content as base64
    audio_content_b64 = base64.b64encode(data).decode('utf-8')

    # Prepare request
    url = f"https://speech.googleapis.com/v1/speech:recognize?key={google_cloud_api_key}"
    headers = {
        "Content-Type": "application/json"
    }

    # Build request body
    request_data = {
        "config": {
            "encoding": encoding,
            "languageCode": "zh-CN",  # Chinese, can be changed to "en-US" or auto-detect
            "alternativeLanguageCodes": ["en-US"],  # Fallback to English
            "enableAutomaticPunctuation": True,
            "model": model if model != "default" else "default"
        },
        "audio": {
            "content": audio_content_b64
        }
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, json=request_data) as resp:
                resp_text = await resp.text()

                if not resp.ok:
                    return None, f"{resp.status} {resp_text}"

                try:
                    resp_json = json.loads(resp_text)

                    # Extract transcript from response
                    if "results" not in resp_json or not resp_json["results"]:
                        return None, "No transcription results"

                    # Concatenate all transcript alternatives
                    transcript = ""
                    for result in resp_json["results"]:
                        if "alternatives" in result and result["alternatives"]:
                            transcript += result["alternatives"][0]["transcript"]

                    if not transcript:
                        return None, "Empty transcript"

                    return transcript, None

                except Exception as e:
                    return None, f"Failed to parse response: {resp_text}"

    except Exception as e:
        return None, str(e)

#-----------------------------------------------------------------------------
