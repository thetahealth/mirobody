"""The health profile is the record as code writes it, plus a scenario the model picks.

On 2026-10-30 MiniCPM5-2B, asked to rewrite the whole profile, wrote 32,000
tokens per refresh (19 minutes on a GPU), filed readings under headings it
made up and opened two profiles with the prompt's own instructions. The
refresh and its LLM client are the application layer; the library-only test
installation skips this module.
"""

import asyncio
import itertools
from datetime import date, timedelta
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
from mirobody.utils.llm import async_get_text_completion, clients
from mirobody.utils.llm import utils as llm_utils

SPEC = RouteSpec(alias="local-utils", model="minicpm5-2b", api_key_env="", base_url="http://127.0.0.1:8080/v1",
                 temperature=0, extra_body={"chat_template_kwargs": {"enable_thinking": False}})

TODAY = date.today().isoformat()


def _row(name, value="", unit="", *, kind="measurement", ref="", flag="", day=TODAY, value_num=None, series=None):
    return {"kind": kind, "name": name, "series": series or f"local:{name}", "value": value, "unit": unit,
            "ref": ref, "flag": flag, "date": day, "value_num": value_num, "text": "" if kind == "measurement" else value,
            "modality": "lab-report"}


#: One check-up, newest first as `PostgresHealthQuery.records` returns it. The
#: urine glucose is the reading MiniCPM5-2B filed under 血糖 (blood glucose).
RECORDS = [
    _row("ALT", "74", "U/L", ref="9-50", value_num=74),
    _row("FPG", "5.06", "mmol/L", ref="3.90-6.10", value_num=5.06),
    _row("GLU", "Negative", ref="阴性"),
    _row("HDL-C", "1.00", "mmol/L", ref=">1.04", flag="low", value_num=1.0),
    _row("Mild fatty liver", "Mild fatty liver", kind="finding", flag="high"),
    _row("Liver", "Not palpable", kind="finding"),
    _row("tired", kind="symptom"),
    _row("tired", kind="symptom", day="2026-01-02"),
    _row("toothache", kind="symptom"),
]


class EndlessModel:
    """An OpenAI-compatible server whose model never writes a stop token: it
    writes until `max_tokens`, the one limit a server enforces whether or not
    the caller is still listening, and without one it would write forever."""

    def __init__(self):
        self.requests: list[dict] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    async def create(self, *, model, messages, **params):
        self.requests.append({"messages": messages, **params})
        limit = params.get("max_tokens") or params.get("max_completion_tokens")
        assert limit, "a request without max_tokens to this model never ends"
        tokens = list(itertools.islice(itertools.cycle(["- 尿酸", ": 无记录", "\n"]), limit))
        return SimpleNamespace(
            choices=[SimpleNamespace(finish_reason="length", message=SimpleNamespace(content="".join(tokens)))],
            usage=SimpleNamespace(completion_tokens=len(tokens)),
        )


class ReplyModel:
    """A model that answers `text` and stops."""

    def __init__(self, text: str):
        self.text = text
        self.requests: list[dict] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    async def create(self, *, model, messages, **params):
        self.requests.append({"messages": messages, **params})
        return SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop", message=SimpleNamespace(content=self.text))],
                               usage=SimpleNamespace(completion_tokens=len(self.text) // 3))


def _serve(monkeypatch, model) -> None:
    monkeypatch.setattr(llm_utils, "_route", lambda provider, model_name, surface: SPEC)
    monkeypatch.setattr(clients.client_manager, "for_spec", lambda spec: model)


@pytest.fixture
def store(monkeypatch):
    """One new reading since version 3, whose scenario was 糖尿病早期预防与筛查.
    Every statement is recorded; the one write is the returned mock."""
    statements: list[str] = []

    async def execute_query(query, params=None, **_):
        statements.append(query)
        if "last_execute_doc_id" in query and "select version" in query:
            return [{"version": 3, "last_execute_doc_id": 100}]
        if "scenario_zh" in query:
            return [{"scenario_zh": "糖尿病早期预防与筛查"}]
        if "v_observation" in query:
            return [{"id": 101}]
        return []

    monkeypatch.setattr(profile, "execute_query", execute_query)
    monkeypatch.setattr(profile.BasicInfoService, "get_user_basic_info",
                        AsyncMock(return_value={"gender": "Male", "age": 45, "language": "en"}))
    monkeypatch.setattr(profile.DeviceDataService, "get_device_data", AsyncMock(return_value=profile.NO_DEVICE_DATA))
    monkeypatch.setattr(profile.UserProfileService, "_get_records", AsyncMock(return_value=RECORDS))
    save = AsyncMock(return_value=7)
    monkeypatch.setattr(profile.UserProfileService, "_save_profile", save)
    save.statements = statements
    return save


def _stored(save) -> dict:
    save.assert_awaited_once()
    return save.await_args.kwargs


async def test_the_record_is_written_by_code_and_the_model_answers_one_line(monkeypatch, store):
    model = ReplyModel("场景：高血脂与冠心病高危人群管理")
    _serve(monkeypatch, model)

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert result["status"] == "success"
    stored = _stored(store)
    document = stored["profile_markdown"]
    assert "- GLU (ref 阴性): Negative (" in document  # under its own name, not 血糖
    assert "- FPG (ref 3.90-6.10): 5.06 mmol/L (" in document
    assert "- HDL-C (ref >1.04): 1.00 mmol/L (low) (" in document
    assert "- tired: " + TODAY + ", 2026-01-02" in document
    assert stored["scenario_zh"] == "高血脂与冠心病高危人群管理"
    assert stored["last_execute_doc_id"] == 101
    assert [r["max_tokens"] for r in model.requests] == [profile.SCENARIO_MAX_TOKENS]


async def test_a_model_that_never_stops_writes_one_capped_answer_and_the_profile_is_stored(monkeypatch, store):
    model = EndlessModel()
    _serve(monkeypatch, model)

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert len(model.requests) == 1
    assert model.requests[0]["max_tokens"] == profile.SCENARIO_MAX_TOKENS
    assert result["status"] == "success"
    stored = _stored(store)
    assert "无记录" not in stored["profile_markdown"]
    assert stored["scenario_zh"] == "糖尿病早期预防与筛查"  # the previous one stays


async def test_a_model_that_repeats_its_prompt_puts_none_of_it_in_the_profile(monkeypatch, store):
    _serve(monkeypatch, ReplyModel(profile.SCENARIO_PROMPT))

    await profile.UserProfileService._create_user_profile_locked("5")

    stored = _stored(store)
    for line in profile.SCENARIO_PROMPT.splitlines():
        if len(line) > 30:
            assert line not in stored["profile_markdown"]
    assert stored["scenario_zh"] == "糖尿病早期预防与筛查"


async def test_a_scenario_not_in_the_table_is_not_stored(monkeypatch, store):
    _serve(monkeypatch, ReplyModel("场景：糖尿病"))

    await profile.UserProfileService._create_user_profile_locked("5")

    assert _stored(store)["scenario_zh"] == profile.DEFAULT_SCENARIO_ZH


async def test_no_model_at_all_still_stores_the_profile(monkeypatch, store):
    monkeypatch.setattr(profile, "async_get_text_completion", AsyncMock(return_value=None))

    result = await profile.UserProfileService._create_user_profile_locked("5")

    assert result["status"] == "success"
    assert _stored(store)["scenario_zh"] == "糖尿病早期预防与筛查"


async def test_the_previous_document_is_never_read(monkeypatch, store):
    """`kernel.memory`: a rewrite that reads its own output keeps its own errors."""
    model = ReplyModel("场景：无")
    _serve(monkeypatch, model)

    await profile.UserProfileService._create_user_profile_locked("5")

    assert store.statements and not any("common_part" in sql for sql in store.statements)
    assert _stored(store)["scenario_zh"] == profile.DEFAULT_SCENARIO_ZH


def test_the_core_summary_names_what_is_abnormal_or_recurring_and_nothing_else():
    core = profile._core_summary({"gender": "Male", "age": 45, "language": "en"}, RECORDS, "en")

    assert core.splitlines() == [
        "- Male, 45 years",
        f"- Conditions and abnormal findings: Mild fatty liver ({TODAY})",
        f"- Outside reference range: ALT 74 U/L high ({TODAY}); HDL-C 1.00 mmol/L low ({TODAY})",
        f"- Recurring symptoms: tired (2 times, last {TODAY})",
    ]
    assert "FPG" not in core and "Liver" not in core.replace("fatty liver", "") and "toothache" not in core


def test_the_core_summary_is_in_chinese_for_a_chinese_speaker_and_counts_what_it_leaves_out():
    many = [_row(f"指标{i}", "9", "mmol/L", ref="1-5", value_num=9) for i in range(profile.CORE_ITEMS + 2)]

    core = profile._core_summary({"gender": "男", "age": 41, "language": "zh-CN"}, many, "zh-CN")

    assert core.splitlines()[0] == "- 男，41岁"
    assert core.splitlines()[1].startswith("- 超出参考范围: 指标0 9 mmol/L 偏高 (")
    assert core.endswith("另有 2 项见下文")


async def test_a_diagnosis_is_kept_however_old_and_a_reading_for_the_window(monkeypatch):
    old = (date.today() - timedelta(days=profile.HISTORY_DAYS + 30)).isoformat()
    rows = [_row("Hypertension", kind="condition", day=old), _row("ALT", "40", "U/L", day=old), _row("ALT", "41", "U/L")]
    records = AsyncMock(return_value={"rows": rows, "total": 3, "has_more": False})
    monkeypatch.setattr(profile.PostgresHealthQuery, "records", records)

    kept = await profile.UserProfileService._get_records("5")

    assert [(r["name"], r["date"]) for r in kept] == [("Hypertension", old), ("ALT", TODAY)]
    assert records.await_args.kwargs["kind"] == "all" and records.await_args.kwargs["limit"] is None
    assert "device-sensed" not in records.await_args.kwargs["modalities"]


@pytest.mark.parametrize("answer, scenario", [
    ("场景：糖尿病早期预防与筛查", "糖尿病早期预防与筛查"),
    ("场景：无", ""),
    ("## 场景\n\n", ""),  # MiniCPM5-2B's answer to the old format
    ("- 尿酸: 无记录\n- 尿酸: 无记录", None),
    ("", None),
])
def test_the_scenario_line(answer, scenario):
    assert profile._scenario_in(answer) == scenario


def test_the_digest_keeps_every_abnormal_reading_and_counts_the_normal_ones_it_leaves_out(monkeypatch):
    monkeypatch.setattr(profile, "DIGEST_TOKENS", 200)
    normal = [_row(f"normal {i}", "5", "mmol/L", ref="1-9", value_num=5) for i in range(50)]

    digest, left_out = profile._digest({"gender": "Male"}, RECORDS + normal, profile.NO_DEVICE_DATA, None)

    assert "- ALT: 74 U/L (" in digest and "- HDL-C: 1.00 mmol/L (" in digest
    assert left_out > 0 and f"- and {left_out} more" in digest


async def test_a_refresh_past_the_timeout_is_reported_as_timed_out(monkeypatch):
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


async def test_a_timed_out_refresh_is_not_retried(monkeypatch):
    """The queue retries a batch that raises; this one would run beside a
    model server still writing the first."""
    monkeypatch.setattr(profile.UserProfileService, "create_user_profile",
                        AsyncMock(return_value={"status": "timed_out"}))

    await ProfileRefreshTask(pg_config=None).consume(["5"])


async def test_a_failed_refresh_is_still_retried(monkeypatch):
    monkeypatch.setattr(profile.UserProfileService, "create_user_profile", AsyncMock(return_value={"status": "error"}))

    with pytest.raises(RuntimeError):
        await ProfileRefreshTask(pg_config=None).consume(["5"])


async def test_a_reply_cut_at_max_tokens_is_returned_and_logged(monkeypatch, caplog):
    _serve(monkeypatch, EndlessModel())

    with caplog.at_level("WARNING", logger="mirobody.utils.llm.utils"):
        title = await async_get_text_completion([{"role": "user", "content": "title?"}], max_tokens=4)

    assert title == "- 尿酸: 无记录\n- 尿酸"
    assert "hit max_tokens" in caplog.text


async def test_the_anthropic_backend_logs_a_cut_the_same_way(monkeypatch, caplog):
    pytest.importorskip("anthropic")
    from mirobody.utils.llm import backends_anthropic

    cut = SimpleNamespace(stop_reason="max_tokens", usage=SimpleNamespace(output_tokens=4),
                          content=[SimpleNamespace(type="text", text="- Glucose: 5.1")])
    monkeypatch.setattr(backends_anthropic, "_create", AsyncMock(return_value=cut))
    spec = RouteSpec(alias="anthropic-utils", model="claude-haiku-4-5-20251001", api_key_env="ANTHROPIC_API_KEY",
                     base_url="", llm_type="anthropic")

    with caplog.at_level("WARNING", logger="mirobody.utils.llm.utils"):
        text = await backends_anthropic.text_completion(spec, [{"role": "user", "content": "title?"}], max_tokens=4)

    assert text == "- Glucose: 5.1"
    assert "hit max_tokens" in caplog.text
