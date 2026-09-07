"""Audio duration, read from the container header without decoding.

Two readers in order — tinytag, then a hand-rolled MP4 atom walk for the
Android M4A/3GP containers tinytag misses. A third library (mutagen) used to
sit behind them as "fallback for other formats"; tinytag already reads every
format the upload surface accepts, so the second library only added an install.

Was `utils_audio.py` (`mirobody.utils.utils_audio`).
"""

import logging
import struct
from io import BytesIO

logger = logging.getLogger(__name__)


def _get_duration_with_tinytag(file_content: bytes, content_type: str) -> int | None:
    """
    Calculate audio duration using tinytag library (pure Python, no external dependencies)
    
    Args:
        file_content: Audio file content as bytes
        content_type: MIME type of the audio file
        
    Returns:
        int: Audio duration in milliseconds, or None if failed
    """
    try:
        from tinytag import TinyTag
        
        audio_buffer = BytesIO(file_content)
        tag = TinyTag.get(file_obj=audio_buffer)
        
        if tag.duration is not None and tag.duration > 0:
            duration_ms = int(tag.duration * 1000)
            if duration_ms < 200:
                logger.info(f"Audio duration too short ({duration_ms}ms), using minimum 200ms for type {content_type}")
                return 200
            logger.info(f"TinyTag calculated audio duration: {duration_ms}ms ({tag.duration:.2f}s) for type {content_type}")
            return duration_ms
        logger.warning(f"TinyTag: duration is zero or invalid for content_type: {content_type}")
        return None
            
    except ImportError:
        logger.warning("tinytag library is not installed. Trying fallback method.")
        return None
    except Exception as e:
        logger.warning(f"TinyTag error: {str(e)}")
        return None


def _parse_mp4_duration(file_content: bytes, content_type: str) -> int | None:
    """
    Parse MP4/M4A/3GP file header to extract duration (zero dependencies)
    
    This method parses the moov/mvhd atom to get timescale and duration.
    Works with Android M4A, 3GP, and standard MP4 files.
    
    Args:
        file_content: Audio file content as bytes
        content_type: MIME type of the audio file
        
    Returns:
        int: Audio duration in milliseconds, or None if failed
    """
    try:
        data = file_content
        data_len = len(data)
        
        def read_atom(start: int) -> tuple:
            """Read atom size and type at position"""
            if start + 8 > data_len:
                return 0, b'', start
            size = struct.unpack('>I', data[start:start+4])[0]
            atom_type = data[start+4:start+8]
            # Handle extended size (size=1 means 64-bit size follows)
            if size == 1 and start + 16 <= data_len:
                size = struct.unpack('>Q', data[start+8:start+16])[0]
                return size, atom_type, start + 16
            return size, atom_type, start + 8
        
        def find_atom(start: int, end: int, target: bytes) -> int | None:
            """Find atom within range, return content start position"""
            pos = start
            while pos < end:
                size, atom_type, content_start = read_atom(pos)
                if size == 0:
                    break
                if atom_type == target:
                    return content_start
                pos += size
            return None
        
        # Find moov atom at top level
        moov_start = find_atom(0, data_len, b'moov')
        if moov_start is None:
            # moov might be at the end, search backwards or just fail
            logger.warning(f"MP4 parser: moov atom not found for {content_type}")
            return None
        
        # Get moov atom size to limit search
        moov_pos = moov_start - 8
        moov_size, _, _ = read_atom(moov_pos)
        moov_end = moov_pos + moov_size
        
        # Find mvhd atom inside moov
        mvhd_start = find_atom(moov_start, moov_end, b'mvhd')
        if mvhd_start is None:
            logger.warning(f"MP4 parser: mvhd atom not found for {content_type}")
            return None
        
        # Parse mvhd atom
        # Version (1 byte) + Flags (3 bytes)
        if mvhd_start + 4 > data_len:
            return None
            
        version = data[mvhd_start]
        
        if version == 0:
            # 32-bit timestamps
            # Skip: version(1) + flags(3) + creation_time(4) + modification_time(4)
            offset = mvhd_start + 12
            if offset + 8 > data_len:
                return None
            timescale = struct.unpack('>I', data[offset:offset+4])[0]
            duration = struct.unpack('>I', data[offset+4:offset+8])[0]
        elif version == 1:
            # 64-bit timestamps
            # Skip: version(1) + flags(3) + creation_time(8) + modification_time(8)
            offset = mvhd_start + 20
            if offset + 12 > data_len:
                return None
            timescale = struct.unpack('>I', data[offset:offset+4])[0]
            duration = struct.unpack('>Q', data[offset+4:offset+12])[0]
        else:
            logger.warning(f"MP4 parser: unknown mvhd version {version} for {content_type}")
            return None
        
        if timescale == 0:
            logger.warning(f"MP4 parser: timescale is 0 for {content_type}")
            return None
        
        duration_seconds = duration / timescale
        duration_ms = int(duration_seconds * 1000)
        
        if duration_ms < 200:
            logger.info(f"Audio duration too short ({duration_ms}ms), using minimum 200ms for type {content_type}")
            return 200
            
        logger.info(f"MP4 parser calculated audio duration: {duration_ms}ms ({duration_seconds:.2f}s) for type {content_type}")
        return duration_ms
        
    except Exception as e:
        logger.warning(f"MP4 parser error: {str(e)}")
        return None


def get_audio_duration_from_bytes(file_content: bytes, content_type: str) -> int | None:
    """
    Calculate audio duration from file bytes
    
    Uses two methods in order:
    1. TinyTag (pure Python, supports common formats)
    2. MP4 header parser (zero dependencies, for Android M4A/3GP)
    
    Args:
        file_content: Audio file content as bytes
        content_type: MIME type of the audio file (e.g., 'audio/mpeg', 'audio/wav')
        
    Returns:
        int: Audio duration in milliseconds, or None if duration cannot be determined
        
    Supported formats:
        - MP3 (audio/mpeg)
        - WAV (audio/wav, audio/x-wav)
        - M4A/AAC (audio/mp4, audio/x-m4a)
        - 3GP (audio/3gpp, video/3gpp)
        - OGG (audio/ogg)
        - FLAC (audio/flac)
        - WMA (audio/x-ms-wma)
        - Other formats supported by tinytag
        
    Note:
        - Returns None for corrupted files or unsupported formats
        - Returns None for empty files
        - Logs warnings for processing errors
    """
    # Validate input
    if not file_content or len(file_content) == 0:
        logger.warning("Cannot calculate duration: file content is empty")
        return None
        
    # Skip duration calculation for very large files (> 500MB)
    file_size_mb = len(file_content) / (1024 * 1024)
    if file_size_mb > 500:
        logger.info(f"Skipping duration calculation for large file: {file_size_mb:.2f}MB")
        return None
    
    # Try tinytag first (pure Python, common formats)
    duration = _get_duration_with_tinytag(file_content, content_type)
    if duration is not None:
        return duration
    
    # Try MP4 header parser for Android M4A/3GP/MP4 files
    mp4_types = {
        "audio/mp4", "audio/x-m4a", "audio/m4a", "audio/aac",
        "audio/3gpp", "video/3gpp", "audio/3gp",
        "video/mp4", "application/mp4"
    }
    if content_type in mp4_types or file_content[:8].find(b'ftyp') != -1:
        logger.info("TinyTag failed, trying MP4 header parser")
        duration = _parse_mp4_duration(file_content, content_type)
        if duration is not None:
            return duration
    
    # Both methods failed
    logger.warning(f"All methods failed to calculate duration for content_type: {content_type}")
    return None
