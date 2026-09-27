"""Regressions from the 1.5.2 public-data review."""

from __future__ import annotations

import asyncio
import gzip
import io
import csv
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from deepagents.backends import StateBackend
from deepagents.middleware.summarization import SummarizationMiddleware as DeepSummarizationMiddleware
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from mirobody.agent.chat.file import _detect_file_scene
from mirobody.agent import harness
from mirobody.agent.filesystem.backend import PgFilesystemBackend
from mirobody.agent.filesystem.files_backend import ThFilesBackend
from mirobody.agent.middleware.genotype_row_guard import GenotypeRowGuardMiddleware, redact_genotype_history
from mirobody.agent.middleware.genotype_summarization import GenotypeSafeSummarizationMiddleware
from mirobody.agent.tools.genetic_service import TOOL_NAME
from mirobody.kernel import tools
from mirobody.translate.genotype import normalize, pseudoautosomal_status


PUBLIC_VCF = Path(__file__).parent / "fixtures/public-hg00096.vcf"
PUBLIC_X = Path(__file__).parent / "fixtures/public-1000g-x.tsv"


class ChatClassificationTests(unittest.TestCase):
    def test_public_vcf_plain_gzip_zip_are_genetic(self) -> None:
        # Padding contains no extra calls; the only genotype rows are the
        # published 1000 Genomes HG00096 CYP2C19 calls in the fixture.
        public = PUBLIC_VCF.read_bytes() + b"##source=1000Genomes HG00096 public truth\n" * 500
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as output:
            output.writestr("public.vcf", public)
        cases = (
            ("public.vcf", "text/vcf", public),
            ("public.vcf.gz", "application/gzip", gzip.compress(public, compresslevel=0)),
            ("public.zip", "application/zip", archive.getvalue()),
        )
        for name, mime, data in cases:
            with self.subTest(name=name):
                self.assertGreater(len(data), 16 * 1024)
                self.assertEqual(_detect_file_scene({
                    "file_name": name, "content_type": mime, "content_bytes": data,
                }), "genetic")

    def test_classification_is_per_file(self) -> None:
        files = (
            {"file_name": "public.vcf", "content_type": "text/vcf", "content_bytes": PUBLIC_VCF.read_bytes()},
            {"file_name": "notes.txt", "content_type": "text/plain", "content_bytes": b"non-genetic notes"},
        )
        self.assertEqual([_detect_file_scene(file) for file in files], ["genetic", "report"])


class ParCoordinateTests(unittest.TestCase):
    def test_public_grc_and_ensembl_par_boundaries(self) -> None:
        self.assertTrue(pseudoautosomal_status("X", 60001, None))
        self.assertTrue(pseudoautosomal_status("Y", 2649520, None))
        self.assertTrue(pseudoautosomal_status("X", None, 2781479))
        self.assertTrue(pseudoautosomal_status("Y", None, 56887903))
        self.assertFalse(pseudoautosomal_status("X", 2699521, None))
        self.assertFalse(pseudoautosomal_status("Y", None, 2781480))
        self.assertIsNone(pseudoautosomal_status("X", None, None))

    def test_public_1000g_x_calls_respect_par_and_inferred_sex(self) -> None:
        with PUBLIC_X.open() as source:
            rows = list(csv.DictReader((line for line in source if not line.startswith("#")), delimiter="\t"))
        calls = {}
        for row in rows:
            pos = int(row["pos37"])
            site = {"rsid": f"X:{pos}:{row['ref']}:{row['alt']}", "chrom": "X",
                    "pos37": pos, "ref": row["ref"], "alt": row["alt"]}
            calls[pos] = normalize(
                rsid=site["rsid"], chrom="X", position=pos, genotype="", vcf_gt=row["gt"],
                vcf_ref=row["ref"], vcf_alt=row["alt"], site=site, sex="male",
            )
        self.assertEqual((calls[60052].call_status, calls[60052].gt), ("called", "0|1"))
        self.assertEqual((calls[60026].call_status, calls[60026].gt), ("called", "0|0"))
        self.assertEqual((calls[3000679].call_status, calls[3000679].strand_check),
                         ("unresolved", "haploid_conflict"))
        self.assertEqual((calls[3000166].call_status, calls[3000166].zygosity),
                         ("called", "hemizygous"))


class GenotypeRowGuardTests(unittest.TestCase):
    def test_private_summarizer_replaces_the_deepagents_core_slot(self) -> None:
        model = FakeListChatModel(responses=["ok"])
        backend = StateBackend()
        guard = GenotypeRowGuardMiddleware()
        private_summary = GenotypeSafeSummarizationMiddleware(model, backend, guard)

        class StubAgent:
            def with_config(self, _config):
                return self

        with patch("deepagents.graph.create_agent", return_value=StubAgent()) as create_agent:
            harness.assemble(model=model, tools=[], system_prompt="", backend=backend,
                             middleware=[private_summary, guard])
        stack = create_agent.call_args.kwargs["middleware"]
        summarizers = [item for item in stack if item.name == "SummarizationMiddleware"]
        self.assertEqual(summarizers, [private_summary])
        self.assertFalse(any(item.name == "SubAgentMiddleware" for item in stack))

    def test_checkpoint_replay_requires_a_new_bounded_tool_result(self) -> None:
        # rs4244285=AG comes from the pinned public HG00096 VCF above.
        envelope = tools.Envelope(tools.STATUS_OK,
                                  data=[{"rsid": "rs4244285", "genotype": "AG"}],
                                  meta=tools.Meta(row_count=1))
        previous = ToolMessage(name=TOOL_NAME, tool_call_id="previous",
                               content="rs4244285 | AG", artifact=envelope)
        current = ToolMessage(name=TOOL_NAME, tool_call_id="current",
                              content="rs4244285 | AG", artifact=envelope)
        guard = GenotypeRowGuardMiddleware()
        messages = [HumanMessage(content="My public call?"), previous,
                    AIMessage(content="Your rs4244285 genotype is AG."),
                    HumanMessage(content="What is it now?"), current, current]
        guard._record(current)
        guarded, visible, returned, redacted, answers = guard._guard_messages(messages)
        self.assertEqual((visible, returned, redacted, answers), (1, 1, 2, 1))
        self.assertNotIn("AG", guarded[1].content + guarded[2].content + guarded[5].content)
        self.assertIsNone(guarded[1].artifact)
        self.assertEqual(guarded[4].content, current.content)

    def test_memory_redacts_current_result_and_dependent_answer(self) -> None:
        envelope = tools.Envelope(tools.STATUS_OK,
                                  data=[{"rsid": "rs4244285", "genotype": "AG"}],
                                  meta=tools.Meta(row_count=1))
        genotype = ToolMessage(name=TOOL_NAME, tool_call_id="public",
                               content="rs4244285 | AG", artifact=envelope)
        copied = AIMessage(content="The public HG00096 genotype is AG.", tool_calls=[{
            "name": "write_file", "args": {"file_path": "/memo", "content": "rs4244285 AG"},
            "id": "copied", "type": "tool_call",
        }])
        messages = [HumanMessage(content="What is my public call?"), genotype,
                    copied, ToolMessage(name="write_file", tool_call_id="copied", content="Saved AG"),
                    HumanMessage(content="Next question")]
        safe = redact_genotype_history(messages)
        self.assertNotIn("AG", safe[1].content + safe[2].content + safe[3].content)
        self.assertIsNone(safe[1].artifact)
        self.assertFalse(safe[2].tool_calls)
        self.assertEqual(safe[4].content, messages[4].content)

    def test_tool_result_without_name_follows_call_id(self) -> None:
        call = AIMessage(content="", tool_calls=[{
            "name": TOOL_NAME, "args": {"rsids": ["rs4244285"]},
            "id": "genetic-call", "type": "tool_call",
        }])
        result = ToolMessage(tool_call_id="genetic-call", content="rs4244285 | AG")
        messages = [HumanMessage(content="Public call?"), call, result,
                    AIMessage(content="The public genotype is AG."),
                    HumanMessage(content="Next question")]
        guarded, visible, returned, redacted, answers = GenotypeRowGuardMiddleware()._guard_messages(messages)
        self.assertEqual((visible, returned, redacted, answers), (0, 0, 1, 1))
        self.assertNotIn("AG", guarded[2].content + guarded[3].content)
        offloaded = redact_genotype_history(messages)
        self.assertNotIn("AG", offloaded[2].content + offloaded[3].content)

    def test_checkpoint_redacts_copied_tool_arguments(self) -> None:
        guard = GenotypeRowGuardMiddleware()
        messages = [
            HumanMessage(content="What is my public call?"),
            ToolMessage(name=TOOL_NAME, tool_call_id="previous", content="rs4244285 | AG"),
            AIMessage(content="Stored the AG call", tool_calls=[{
                "name": "write_file", "args": {"file_path": "/memo", "content": "rs4244285 AG"},
                "id": "copied", "type": "tool_call",
            }]),
            ToolMessage(name="write_file", tool_call_id="copied", content="Saved AG"),
            HumanMessage(content="What is it now?"),
        ]
        safe, visible, returned, redacted, answers = guard._guard_messages(messages)
        self.assertEqual((visible, returned, redacted, answers), (0, 0, 2, 1))
        self.assertNotIn("AG", " ".join(str(message.content) for message in safe))
        self.assertFalse(safe[2].tool_calls)

    def test_genetic_session_cannot_write_or_reopen_scratch_files(self) -> None:
        guard = GenotypeRowGuardMiddleware()
        previous = ToolMessage(name=TOOL_NAME, tool_call_id="previous", content="rs4244285 | AG")
        history = [HumanMessage(content="Public call?"), previous,
                   HumanMessage(content="Read it later")]
        model = FakeListChatModel(responses=["ok"])
        guard._guard_request(ModelRequest(model=model, messages=history, state={"messages": history}))
        calls: list[str] = []

        def handler(request):
            calls.append(request.tool_call["name"])
            return ToolMessage(name=request.tool_call["name"],
                               tool_call_id=request.tool_call["id"], content="safe")

        for name, args in (
            ("write_file", {"file_path": "/memo", "content": "rs4244285 AG"}),
            ("edit_file", {"file_path": "/memo", "old_string": "A", "new_string": "AG"}),
            ("read_file", {"file_path": "/memo"}),
            ("read_file", {"file_path": "/uploads/../memo"}),
            ("read_file", {"file_path": "/uploads_evil/raw.vcf"}),
            ("grep", {"path": "/conversation_history", "pattern": "AG"}),
            ("ls", {"path": "/"}),
        ):
            request = SimpleNamespace(tool_call={"name": name, "args": args, "id": name})
            refused = guard.wrap_tool_call(request, handler)
            self.assertEqual(refused.status, "error", name)
            self.assertNotIn("AG", refused.content)
        for name, args in (("read_file", {"file_path": "/library/report.pdf"}),
                           ("ls", {"path": "/uploads/"})):
            allowed = SimpleNamespace(tool_call={"name": name, "args": args, "id": "document"})
            self.assertEqual(guard.wrap_tool_call(allowed, handler).content, "safe")
        self.assertEqual(calls, ["read_file", "ls"])

        async def async_handler(_request):
            self.fail("blocked scratch tool reached its handler")

        request = SimpleNamespace(tool_call={"name": "read_file",
                                             "args": {"file_path": "/conversation_history/raw.md"},
                                             "id": "old_history"})
        self.assertEqual(asyncio.run(guard.awrap_tool_call(request, async_handler)).status, "error")

    def test_deepagents_summary_and_history_only_receive_redacted_rows(self) -> None:
        model = FakeListChatModel(responses=["summary"])
        guard = GenotypeRowGuardMiddleware()
        middleware = GenotypeSafeSummarizationMiddleware(model, StateBackend(), guard)
        envelope = tools.Envelope(tools.STATUS_OK,
                                  data=[{"rsid": "rs4244285", "genotype": "AG"}],
                                  meta=tools.Meta(row_count=1))
        previous = ToolMessage(name=TOOL_NAME, tool_call_id="previous",
                               content="rs4244285 | AG", artifact=envelope)
        current = ToolMessage(name=TOOL_NAME, tool_call_id="current",
                              content="rs4244285 | AG", artifact=envelope)
        guard._record(current)
        messages = [HumanMessage(content="First question"), previous,
                    AIMessage(content="The public genotype was AG."),
                    HumanMessage(content="Query again"), current]
        request = ModelRequest(model=model, messages=messages, state={"messages": messages})
        seen: dict[str, list] = {}

        def offload(_instance, _backend, history, _session):
            seen["history"] = history
            return "/conversation_history/public.md"

        def summarize(_instance, history):
            seen["summary"] = history
            return "Previous query requires a fresh lookup."

        def handler(model_request):
            seen["model"] = model_request.messages
            return ModelResponse(result=[AIMessage(content="done")])

        with (patch.object(middleware, "_should_summarize", return_value=True),
              patch.object(middleware, "_determine_cutoff_index", return_value=3),
              patch.object(DeepSummarizationMiddleware, "_offload_to_backend", offload),
              patch.object(DeepSummarizationMiddleware, "_create_summary", summarize)):
            result = middleware.wrap_model_call(request, handler)
        for key in ("history", "summary"):
            self.assertNotIn("AG", " ".join(str(message.content) for message in seen[key]))
            self.assertTrue(all(getattr(message, "artifact", None) is None for message in seen[key]))
        self.assertEqual(seen["model"][-1].content, current.content)
        summary_message = result.command.update["_summarization_event"]["summary_message"]
        self.assertTrue(summary_message.additional_kwargs["genotype_rows_redacted"])

    def test_real_history_serializer_persists_only_redacted_public_rows(self) -> None:
        model = FakeListChatModel(responses=["summary"])
        backend = StateBackend()
        guard = GenotypeRowGuardMiddleware()
        middleware = GenotypeSafeSummarizationMiddleware(model, backend, guard)
        messages = [
            HumanMessage(content="What is the public call?"),
            AIMessage(content="", tool_calls=[{"name": TOOL_NAME,
                                               "args": {"rsids": ["rs4244285"]},
                                               "id": "old-call", "type": "tool_call"}]),
            ToolMessage(tool_call_id="old-call", content="rs4244285 | AG"),
            AIMessage(content="The public call is AG."),
            HumanMessage(content="What now?"),
        ]
        saved: list[str] = []

        def write(_path, content):
            saved.append(content)
            return SimpleNamespace(error=None)

        request = ModelRequest(model=model, messages=messages, state={"messages": messages})
        with (patch.object(middleware, "_should_summarize", return_value=True),
              patch.object(middleware, "_determine_cutoff_index", return_value=4),
              patch.object(middleware, "_create_summary", return_value="Fresh lookup required."),
              patch.object(backend, "download_files", return_value=[]),
              patch.object(backend, "write", side_effect=write)):
            result = middleware.wrap_model_call(
                request, lambda _request: ModelResponse(result=[AIMessage(content="done")]))
        self.assertEqual(len(saved), 1)
        self.assertNotIn("AG", saved[0])
        self.assertNotIn("AG", result.command.update["_summarization_event"]["summary_message"].content)

    def test_old_summary_event_cannot_reopen_raw_history(self) -> None:
        model = FakeListChatModel(responses=["summary"])
        middleware = GenotypeSafeSummarizationMiddleware(model, StateBackend(), GenotypeRowGuardMiddleware())
        previous = ToolMessage(name=TOOL_NAME, tool_call_id="previous", content="rs4244285 | AG")
        messages = [HumanMessage(content="First question"), previous,
                    HumanMessage(content="What now?")]
        request = ModelRequest(model=model, messages=messages, state={
            "messages": messages,
            "_summarization_event": {"cutoff_index": 2,
                                     "summary_message": HumanMessage(content="The AG genotype was found."),
                                     "file_path": "/conversation_history/raw.md"},
        })
        safe = middleware._safe_request(request)
        event = safe.state["_summarization_event"]
        self.assertNotIn("AG", event["summary_message"].content)
        self.assertIsNone(event["file_path"])

    def test_pruned_legacy_summary_is_neutralized_without_tool_history(self) -> None:
        model = FakeListChatModel(responses=["summary"])
        guard = GenotypeRowGuardMiddleware()
        middleware = GenotypeSafeSummarizationMiddleware(model, StateBackend(), guard)
        prior = HumanMessage(content="The AG genotype was found.",
                             additional_kwargs={"lc_source": "summarization"})
        messages = [prior, HumanMessage(content="What now?")]
        request = ModelRequest(model=model, messages=messages, state={
            "messages": messages,
            "_summarization_event": {"cutoff_index": 1,
                                     "summary_message": prior,
                                     "file_path": "/conversation_history/raw.md"},
            "_summarization_session_id": "old-raw-history",
        })
        safe = middleware._safe_request(request)
        self.assertNotIn("AG", safe.messages[0].content)
        self.assertNotIn("AG", safe.state["_summarization_event"]["summary_message"].content)
        self.assertIsNone(safe.state["_summarization_event"]["file_path"])
        self.assertIsNone(safe.state["_summarization_session_id"])
        old_read = SimpleNamespace(tool_call={"name": "read_file", "id": "old-file",
                                             "args": {"file_path": "/conversation_history/raw.md"}})
        self.assertEqual(guard._scratch_refusal(old_read).status, "error")

    def test_async_summary_and_overflow_recovery_redact_genotypes(self) -> None:
        model = FakeListChatModel(responses=["summary"])
        guard = GenotypeRowGuardMiddleware()
        middleware = GenotypeSafeSummarizationMiddleware(model, StateBackend(), guard)
        envelope = tools.Envelope(tools.STATUS_OK,
                                  data=[{"rsid": "rs4244285", "genotype": "AG"}],
                                  meta=tools.Meta(row_count=1))
        result = ToolMessage(name=TOOL_NAME, tool_call_id="current",
                             content="rs4244285 | AG", artifact=envelope)
        guard._record(result)
        messages = [HumanMessage(content="First question"), result,
                    AIMessage(content="The genotype was AG."),
                    HumanMessage(content="Next question")]
        request = ModelRequest(model=model, messages=messages, state={"messages": messages})
        seen: dict[str, list] = {}

        async def offload(_instance, _backend, history, _session):
            seen["history"] = history
            return "/conversation_history/public.md"

        async def summarize(_instance, history):
            seen["summary"] = history
            return "A fresh query is needed."

        async def handler(model_request):
            seen["model"] = model_request.messages
            return ModelResponse(result=[AIMessage(content="done")])

        async def run():
            with (patch.object(middleware, "_should_summarize", return_value=True),
                  patch.object(middleware, "_determine_cutoff_index", return_value=3),
                  patch.object(DeepSummarizationMiddleware, "_aoffload_to_backend", offload),
                  patch.object(DeepSummarizationMiddleware, "_acreate_summary", summarize)):
                return await middleware.awrap_model_call(request, handler)

        asyncio.run(run())
        for key in ("history", "summary"):
            self.assertNotIn("AG", " ".join(str(message.content) for message in seen[key]))
        self.assertEqual(seen["model"][-1].content, messages[-1].content)

        async def capture_recovery(_instance, recovery_request, _handler, **_kwargs):
            seen["recovery"] = recovery_request.messages
            return ModelResponse(result=[AIMessage(content="done")]), []

        with patch.object(DeepSummarizationMiddleware, "_acall_with_budget", capture_recovery):
            asyncio.run(middleware._acall_with_budget(request, handler, error=RuntimeError("overflow")))
        self.assertNotIn("AG", " ".join(str(message.content) for message in seen["recovery"]))


class LegacyGenotypeReadTests(unittest.TestCase):
    def test_mislabelled_plain_upload_cannot_be_read_or_downloaded(self) -> None:
        backend = ThFilesBackend(user_id="public-test", scope="uploads", file_keys=["public-file"])
        row = {"path": "/notes.txt", "content": "", "encoding": "utf-8",
               "object_storage_key": "public-file", "created_at": None, "updated_at": None}

        async def run(raw: bytes):
            with (patch.object(backend, "_files", new=AsyncMock(return_value=[row])),
                  patch.object(PgFilesystemBackend, "_get_from_storage",
                               new=AsyncMock(return_value=raw))):
                return await backend.aread("/notes.txt"), await backend.adownload_files(["/notes.txt"])

        blocked_read, blocked_download = asyncio.run(run(PUBLIC_VCF.read_bytes()))
        self.assertEqual(blocked_read.error, "object_storage_read_failed")
        self.assertEqual(blocked_download[0].error, "object_storage_read_failed")
        allowed_read, allowed_download = asyncio.run(run(b"public test control"))
        self.assertIn("public test control", allowed_read.file_data["content"])
        self.assertEqual(allowed_download[0].content, b"public test control")
