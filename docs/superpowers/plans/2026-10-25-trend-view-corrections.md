# Trend view corrections implementation plan

**Goal:** Replace PR180 with a tested trend view that preserves numeric meaning, source coverage and evidence on chat, MCP and REST.

**Architecture:** Keep the existing HealthQuery daily read authority and its bounded 400-day input budget. Move pure trend arithmetic into the existing `kernel/query.py`; normalize multiplicative UCUM conversions with the existing units library, and abstain when conversion is not reliable. The service renders source coverage and unsupported data explicitly; citation definitions identify the actual daily source span.

**Constraints:** No new dependencies, library modules, second harness, stored derived values, keys or real health fixtures. Preserve day election and reported-data semantics. The new PR targets main and includes PR180 plus fixes; leave PR180 unchanged and comment with the replacement link after validation.

## 1. Pure arithmetic and minimal-install contract

- [x] Add failing tests in `mirobody/tests/test_trend_view.py` that import only `mirobody.kernel.query.trend_rows`: mixed kg/lb stays near70kg; incompatible and offset units abstain; Jan1/11/21 values1/2/3 have slope0.1/day; temperature deltas -1/0/1 rise; repeated peaks are swing; different series with identical labels stay distinct.
- [x] Implement `trend_rows(daily: Sequence[Mapping[str, object]]) -> list[dict[str, object]]` in the existing kernel query module. Rows retain series/system/code/provenance/day_known, OR unverified, source_days/total_days/input_truncated and missing_numeric_days. Unavailable results retain identity and a reason, with no fabricated numeric statistics.
- [x] Compute slope from real ISO local dates; relative delta uses mean absolute magnitude, undefined for an all-zero scale. Describe the2% threshold as a descriptive policy. Detect significant reversals before calling a shape a single hump/dip.
- [x] Remove the agent helper and call the pure implementation. Verify the existing minimal `[test]` environment runs these tests without importing agent/config dependencies.

## 2. Service and rendering contracts

- [x] Add integration rejection controls under `mirobody/tests/test_trend_service.py`, with collection-time gating in root `conftest.py` when the optional stack is unavailable.
- [x] Recognize direction-bearing trend rows before catalogue detection. Render shape, means, slope, units, reasons and coverage in compact/REST; retain distinct reported rendering.
- [x] A401-day source capped to400 must produce partial/truncated with the actual shown span; a complete three-day input must remain complete despite one output row. Unsupported reported/nonnumeric/mixed units retain a result and practical day/stats guidance rather than catalogue fallback.
- [x] Propagate source verification and citation identities. Update the shared prompt and tool documentation for observed-day thirds, incomplete inputs and numeric-only applicability.

## 3. Citation evidence

- [x] Add source tests for trend aggregate definitions and resolution: view=trend, daily authority basis, actual first/last observed input days; no collision with stats for the same series/window.
- [x] Reuse the existing day-authority SQL in citation membership resolution so unelected same-day rows are not presented as trend contributors. Preserve the live-read/deletion behavior and person/session isolation.
- [x] Exercise source membership with a disposable PostgreSQL fixture if SQL changes; use the existing regression fixture and leave product databases unchanged.

## 4. Verification and delivery

- [x] Update CHANGELOG and `docs/answers.md`; remove the duplicate Added heading and stale comments describing the old trend contract.
- [x] Run targeted red/green tests, full app suite, full minimal suite, ruff, compileall, PHI/comment checks, all six import contracts and bare library imports against source.
- [x] Independently verify the nine original findings, commit and push `codex/fix-trend-view`, open a new PR against main, attach it to this task, and leave an evidence-backed comment on PR180 linking the replacement. Verify CI on the final head. Do not merge either PR.

Verified locally: app 989 pass / 114 skip / 12 xfail; minimal 555 / 174 / 12; PostgreSQL 111 pass; 25 pure trend and 6 service controls; ruff, compileall, six contracts, bare imports, PHI baseline and comment style pass. Delivery and CI are tracked in the replacement PR.

Delivered as https://github.com/thetahealth/mirobody/pull/182, with the review response on PR180. Python 3.12/3.13, Windows benchmark and Linux/macOS/Windows import CI passed on implementation head e74b71a8. Final documentation-only head is rechecked by CI. The separate PR181 review and probe corrections do not modify this branch.
