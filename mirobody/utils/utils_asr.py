# Audio file related processing
import json
import logging
import os
import time
from http import HTTPStatus
from urllib import request

import dashscope

from mirobody.utils.config import safe_read_cfg

# Flag for initialization status
_asr_initialized = False


def _init_dashscope():
    """Initialize DashScope API"""
    global _asr_initialized
    if not _asr_initialized:
        dashscope_api_key = safe_read_cfg("DASHSCOPE_API_KEY")
        if not dashscope_api_key:
            logging.error("Missing DASHSCOPE_API_KEY configuration")
            raise ValueError("DASHSCOPE_API_KEY configuration missing")
        else:
            os.environ["DASHSCOPE_API_KEY"] = dashscope_api_key
            dashscope.api_key = dashscope_api_key
            logging.info("DashScope API key set successfully")
            _asr_initialized = True


def asr_paraformer_with_urls(audio_url_list: list[str]) -> dict[str, str]:
    """
    Perform speech recognition on a list of audio URLs

    Args:
        audio_url_list: List of audio URLs

    Returns:
        dict: Dictionary containing recognition results, key is URL, value is recognized text
    """
    # Initialize only when used
    _init_dashscope()

    start_time = time.time()
    audio_url_list_str = ",".join(audio_url_list)
    logging.info(f"Starting speech recognition, using paraformer model, audio: {audio_url_list_str}")

    try:
        texts = dict()
        task_response = dashscope.audio.asr.Transcription.async_call(
            model="paraformer-v2",
            file_urls=audio_url_list,
            language_hints=["zh"] * len(audio_url_list),
        )

        transcription_response = dashscope.audio.asr.Transcription.wait(task=task_response.output.task_id)

        if transcription_response.status_code == HTTPStatus.OK:
            for transcription in transcription_response.output[
                "results"
            ]:  # results is a list, each element is a dict with key transcription_url
                try:
                    url = transcription["transcription_url"]
                    result = json.loads(request.urlopen(url).read().decode("utf8"))
                    text = result["transcripts"][0]["text"]
                    file_url = result["file_url"]
                    texts[file_url] = text
                except Exception as e:
                    logging.error(f"Speech recognition error: {str(e)}", stack_info=True)
            end_time = time.time()
            logging.info(f"paraformer transcription done! Duration: {end_time - start_time} seconds, Audio count: {len(audio_url_list)}")
        else:
            logging.error(f"Error: {transcription_response.output.message}")

        return texts
    except Exception as e:
        logging.error(f"Speech recognition error: {str(e)}", stack_info=True)
        return {}
