"""A profile rewrite is bounded, and what it writes is stored only when it is a profile.

On 2026-10-30 MiniCPM5-2B wrote 32,000 tokens per rewrite (19 minutes on a
GPU) and opened two profiles with the prompt's own instructions. The rewrite
and its LLM client are the application layer; the library-only test
installation skips this module.
"""

import asyncio
import itertools
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("dotenv")
pytest.importorskip("ruamel.yaml")
pytest.importorskip("cryptography")
pytest.importorskip("openai")

from mirobody.task.profile_refresh import ProfileRefreshTask
from mirobody.user import profile
from mirobody.utils.config.llm import RouteSpec
from mirobody.utils.llm import CutAtMaxTokens, async_get_text_completion, clients
from mirobody.utils.llm import utils as llm_utils

SPEC = RouteSpec(alias="local-utils", model="minicpm5-2b", api_key_env="", base_url="http://127.0.0.1:8080/v1",
                 temperature=0, extra_body={"chat_template_kwargs": {"enable_thinking": False}})


class EndlessModel:
    """An OpenAI-compatible server whose model never writes a stop token: it
    writes until `max_tokens`, the one limit a server enforces whether or not
    the caller is still listening, and without one it would write forever."""

    def __init__(self):
        self.requests: list[dict] = []
        self.written: list[int] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    async def create(self, *, model, messages, **params):
        self.requests.append(params)
        limit = params.get("max_tokens") or params.get("max_completion_tokens")
        assert limit, "a request without max_tokens to this model never ends"
        tokens = list(itertools.islice(itertools.cycle(["- Glucose", ": 5.1", " mmol/L", " (2026-10-01)\n"]), limit))
        self.written.append(len(tokens))
        return SimpleNamespace(
            choices=[SimpleNamespace(finish_reason="length", message=SimpleNamespace(content="".join(tokens)))],
            usage=SimpleNamespace(completion_tokens=len(tokens)),
        )


class ReplyModel:
    """A model that answers `text` and stops."""

    def __init__(self, text: str):
        self.text = text
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    async def create(self, *, model, messages, **params):
        return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop", message=SimpleNamespace(content=self.text))],
                               usage=SimpleNamespace(completion_tokens=len(self.text) // 3))


def _serve(monkeypatch, model) -> None:
    monkeypatch.setattr(llm_utils, "_route", lambda provider, model_name, surface: SPEC)
    monkeypatch.setattr(clients.client_manager, "for_spec", lambda spec: model)


@pytest.fixture
def store(monkeypatch):
    """The refresh's reads answer one new reading; its one write is recorded."""
    monkeypatch.setattr(profile.UserProfileService, "_get_version_info",
                        AsyncMock(return_value={"version": 3, "last_execute_doc_id": 100}))
    monkeypatch.setattr(profile.BasicInfoService, "get_user_basic_info",
                        AsyncMock(return_value={"gender": "Male", "age": 45, "language": "English"}))
    monkeypatch.setattr(profile.UserProfileService, "_get_incremental_data", AsyncMock(return_value=[
        {"id": 101, "original_indicator": "Glucose", "start_time": "2026-10-01", "value": "5.1", "unit": "mmol/L"},
    ]))
    monkeypatch.setattr(profile.DeviceDataService, "get_device_data", AsyncMock(return_value="No device data available"))
    monkeypatch.setattr(profile.UserProfileGenerator, "_get_existing_profile", AsyncMock(return_value=(None, None)))
    save = AsyncMock(return_value=7)
    monkeypatch.setattr(profile.UserProfileService, "_save_profile", save)
    return save


async def test_a_model_that_never_stops_is_cut_at_the_cap_and_not_stored(monkeypatch, store):
    model = EndlessModel()
    _serve(monkeypatch, model)

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert profile.MAX_OUTPUT_TOKENS == 6144
    assert [r["max_tokens"] for r in model.requests] == [profile.MAX_OUTPUT_TOKENS]
    assert model.written == [profile.MAX_OUTPUT_TOKENS]
    assert result["status"] == "rejected"
    assert result["last_execute_doc_id"] == 100  # the reading stays for the next refresh
    store.assert_not_called()


async def test_a_chunk_cut_at_the_cap_rejects_the_rewrite_without_a_merge(monkeypatch, store):
    """Two chunks of readings: the cut one must not be merged into a profile."""
    model = EndlessModel()
    _serve(monkeypatch, model)
    monkeypatch.setattr(profile, "MAX_TOKENS", 5)  # one reading per chunk

    readings = [{"id": 100 + i, "original_indicator": "Glucose", "start_time": f"2026-10-0{i}", "value": "5.1",
                 "unit": "mmol/L"} for i in (1, 2)]
    monkeypatch.setattr(profile.UserProfileService, "_get_incremental_data", AsyncMock(return_value=readings))

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert len(model.requests) == 2  # both chunks, no merge
    assert result["status"] == "rejected"
    store.assert_not_called()


def _echo() -> str:
    """The Core Summary as MiniCPM5-2B wrote it: the prompt's instruction
    paragraph under the prompt's heading, then real sections."""
    prompt = profile.GENERATE_USER_PROFILE_PROMPT
    paragraph = prompt[prompt.index("**ALWAYS output"):prompt.index("### 1.")].strip()
    return (f"# 核心摘要 / Core Summary\n{paragraph}\n\n# 用户基础信息 / Basic Information\n"
            "- **性别**: Male\n- **年龄**: 45\n\n## 检验指标 / Lab Test Results\n- **Glucose**: 5.1 mmol/L (2026-10-01)\n")


async def test_an_echoed_prompt_is_not_stored(monkeypatch, store):
    _serve(monkeypatch, ReplyModel(_echo()))

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert result["status"] == "rejected"
    store.assert_not_called()


ENGLISH = """### 0. Core Summary
45-year-old male, English-speaking. No known chronic conditions, medications or allergies.
Fasting glucose 5.1 mmol/L (2026-10-01), within range.

### 1. Basic Information
- Gender: Male
- Age: 45
- Language: English

### 3. Health Status
- Lab Test Results:
  - Glucose: 5.1 mmol/L (2026-10-01)

### 4. Recent Device Data (Past 7 Days)
- Heart rate: 62 bpm (as of 2026-10-30)
- Steps: 8,412 (as of 2026-10-30)
"""

CHINESE = """### 0. 核心摘要 / Core Summary
- **基本信息**：男性，45岁，中文
- **当前重要检查结果**：空腹血糖 5.1 mmol/L（2026-10-01）

### 1. 用户基础信息 / Basic Information
- 性别：男
- 年龄：45岁

### 3. 健康情况 / Health Status
- 检验指标 / Lab Test Results:
  - 空腹血糖: 5.1 mmol/L (2026-10-01)

### 4. 近一周设备数据 / Recent Device Data (Past 7 Days)
- 心率: 62 bpm

## 场景

糖尿病早期预防与筛查
"""


@pytest.mark.parametrize("text", [ENGLISH, CHINESE], ids=["english", "chinese"])
async def test_a_profile_in_the_prompts_own_headings_is_stored(monkeypatch, store, text):
    _serve(monkeypatch, ReplyModel(text))

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert result["status"] == "success"
    store.assert_awaited_once()
    assert store.await_args.kwargs["last_execute_doc_id"] == 101


def test_every_instruction_line_long_enough_to_echo_is_caught():
    """Each prompt line of ECHO_WORDS words or more, copied alone, is an echo:
    the check follows the prompt when it is edited."""
    lines = [line for prompt in (profile.GENERATE_USER_PROFILE_PROMPT, profile.MERGE_USER_PROFILE_PROMPT)
             for line in prompt.splitlines() if len(profile._words(line)) >= profile.ECHO_WORDS]
    assert len(lines) > 20
    for line in lines:
        assert profile._echoes_instructions(f"### 0. Core Summary\n{re.sub(r'[*`#]', '', line)}\n"), line


async def test_a_rewrite_past_the_timeout_is_reported_as_timed_out(monkeypatch):
    async def forever(user_id):
        await asyncio.sleep(3600)

    monkeypatch.setattr(profile, "PROFILE_LOCK_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(profile.UserProfileService, "_create_user_profile_locked", forever)
    monkeypatch.setattr(profile.user_profile_lock_manager, "acquire_lock_with_wait", AsyncMock(return_value="lock"))
    release = AsyncMock(return_value=True)
    monkeypatch.setattr(profile.user_profile_lock_manager, "release_lock", release)

    result = await profile.UserProfileService.create_user_profile("5")

    assert result["status"] == "timed_out"
    release.assert_awaited_once_with("5", "lock")


@pytest.mark.parametrize("status", ["rejected", "timed_out"])
async def test_a_rewrite_not_stored_is_not_retried(monkeypatch, status):
    """The queue retries a batch that raises; these would be written again
    from the same readings, beside a server still writing the first."""
    monkeypatch.setattr(profile.UserProfileService, "create_user_profile", AsyncMock(return_value={"status": status}))

    await ProfileRefreshTask(pg_config=None).consume(["5"])


async def test_a_failed_rewrite_is_still_retried(monkeypatch):
    monkeypatch.setattr(profile.UserProfileService, "create_user_profile", AsyncMock(return_value={"status": "error"}))

    with pytest.raises(RuntimeError):
        await ProfileRefreshTask(pg_config=None).consume(["5"])


async def test_a_title_cut_at_max_tokens_is_still_returned(monkeypatch, caplog):
    """Only a caller that asks for the whole answer is refused a cut one."""
    _serve(monkeypatch, EndlessModel())

    with caplog.at_level("WARNING", logger="mirobody.utils.llm.utils"):
        title = await async_get_text_completion([{"role": "user", "content": "title?"}], max_tokens=4)
    assert title == "- Glucose: 5.1 mmol/L (2026-10-01)\n"
    assert "hit max_tokens" in caplog.text

    with pytest.raises(CutAtMaxTokens):
        await async_get_text_completion([{"role": "user", "content": "title?"}], max_tokens=4, whole=True)


async def test_the_anthropic_backend_reports_a_cut_the_same_way(monkeypatch):
    pytest.importorskip("anthropic")
    from mirobody.utils.llm import backends_anthropic

    cut = SimpleNamespace(stop_reason="max_tokens", usage=SimpleNamespace(output_tokens=4),
                          content=[SimpleNamespace(type="text", text="- Glucose: 5.1")])
    monkeypatch.setattr(backends_anthropic, "_create", AsyncMock(return_value=cut))
    spec = RouteSpec(alias="anthropic-utils", model="claude-haiku-4-5-20251001", api_key_env="ANTHROPIC_API_KEY",
                     base_url="", llm_type="anthropic")

    assert await backends_anthropic.text_completion(spec, [{"role": "user", "content": "title?"}],
                                                    max_tokens=4) == "- Glucose: 5.1"
    with pytest.raises(CutAtMaxTokens):
        await backends_anthropic.text_completion(spec, [{"role": "user", "content": "title?"}],
                                                 max_tokens=4, whole=True)
