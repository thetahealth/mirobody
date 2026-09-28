# 1.5.3 Web Backend Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Align `feat/1.5.3` with the current `mirobody-web` main build by delivering B3 raw records, B4 data delta, B5 medication CRUD/lifecycle, B6 capability flags, and a rebuilt bundled frontend.

**Architecture:** Extend `PostgresHealthQuery` with the shared visible-observation period rules, then let thin FastAPI routers validate/authenticate and serialize the results in `{code, msg, data}`. Add a medication HTTP adapter around the existing kernel and Postgres stores, with owner-only writes and care-circle reads. Update the web source in `../mirobody-web`, build its open-source bundle, and copy only `dist/` into this backend worktree.

**Tech Stack:** Python 3.12, FastAPI, PostgreSQL/SQLAlchemy, existing `mirobody.kernel.meds`, React 19, Vite, Zustand, Ant Design.

## Global Constraints

- Work only in `/Users/admin/Desktop/development/Mirobody/mirobody-1.5.3`; do not edit the `feat/1.5.2` worktree.
- Web endpoints use `{code, msg, data}` and preserve current `/api/data` and data-distribution contracts.
- All observation reads go through `PostgresHealthQuery`; do not add a second observation SQL reader in routers.
- Resolve `target_user_id` through `resolve_subject`; writes are owner-only.
- Derived medication state (`due`, `missed`, `upcoming`, adherence) is calculated by `mirobody.kernel.meds`, never persisted.
- Logs contain identifiers, counts, durations, status codes, and type names only; no medication names, indicator names, values, request bodies, or exception messages.
- A capability is true only when its route is mounted and its frontend consumer is bundled.

---

### Task 1: Shared observation records and delta query

**Files:**
- Modify: `mirobody/collect/query.py`
- Modify: `mirobody/server/routers/indicator_router.py`
- Modify: `mirobody/server/routers/__init__.py`
- Modify: `mirobody/schema/30_observations.sql`
- Test: `mirobody/tests/test_web_records_delta.py`

- [ ] Add `PostgresHealthQuery.records(...)` returning `{rows, total, has_more}` with stable `(observed_start DESC, id DESC)` order, `kind`, `source_kind`, `created_at`, `file_key`, and the existing reading fields.
- [ ] Add `/health-indicators/export?format=csv|json` that pages through the same query authority and exports the full visible standardized record set.
- [ ] Add `/api/user/data-export` with a self-describing `health_records` manifest, page metadata, and NDJSON header/row/footer streaming modeled on the a007-mirovital contract.
- [ ] Add `PostgresHealthQuery.delta(...)` using the same visible observation source and current-period start calculation across amendment/retraction chains; return `since`, `total_new`, and `by_source`.
- [ ] Add strict ISO-8601 timezone parsing and date validation in the router; apply the existing subject authorization before calling the query.
- [ ] Add indexes for `(user_id, observed_start DESC, id DESC)` and `source_kind`/`created_at` access without changing the view's visibility semantics.
- [ ] Add route tests covering pagination, amendment visibility, deletion, reassertion, source totals, invalid `since`, and care-circle authorization.

### Task 2: Medication HTTP adapter

**Files:**
- Create: `mirobody/server/routers/medication_router.py`
- Modify: `mirobody/server/routers/__init__.py`
- Modify: `mirobody/server/server.py`
- Modify: `mirobody/collect/meds/store.py`
- Test: `mirobody/tests/test_medication_router.py`

- [ ] Define request models for concept, dose, schedule, dates, and optional code fields with explicit bounds and date/frequency validation.
- [ ] Implement owner-only `GET /api/v1/medications`, `GET /{plan_id}`, `POST /`, `PATCH /{plan_id}`, `POST /{plan_id}/stop`, `POST /{plan_id}/resume`, and `DELETE /{plan_id}`.
- [ ] Implement care-circle read authorization for list/detail/courses and reject write attempts with a target member id.
- [ ] Add transactional store methods for update, void, and kernel `plan_status_transition` plus course close/open; keep deleted rows queryable only through explicit course history.
- [ ] Add route tests for create/edit/stop/resume/delete, invalid transitions, owner/member/denied access, and encrypted persistence.

### Task 3: Capability flags and current web source

**Files:**
- Modify: `mirobody/server/server.py`
- Modify: `../mirobody-web/src/store/system.js`
- Modify: `../mirobody-web/src/api/indicators.js`
- Create: `../mirobody-web/src/api/medications.js`
- Create/modify: `../mirobody-web/src/pages/Medications/*`
- Modify: `../mirobody-web/src/router/*`, `src/config/navConfig.js`, `src/i18n/*.json`
- Test: `mirobody/tests/test_capability_flags.py` and web Vitest tests

- [ ] Emit `__IS_INDICATOR_RECORDS_ON__`, `__IS_INDICATOR_EXPORT_ON__`, `__IS_DATA_DELTA_ON__`, and `__IS_MEDICATIONS_ON__` from actual route registration state.
- [ ] Make the web client treat explicit `false` as disabled and add API clients for records/delta/medications.
- [ ] Add records pagination and a data-delta panel/detail path without exposing an unmounted endpoint.
- [ ] Add medication list/detail/form/lifecycle UI with owner-only write controls and read-only care-circle view.
- [ ] Add English, Chinese, Japanese, and Traditional Chinese copy keys required by the new screens.
- [ ] Run `npm run lint`, `npm test -- --run`, and `npm run build:opensource`; copy the generated `dist/` into backend `frontend/`.

### Task 4: Integration gates and packaging

**Files:**
- Modify: `CHANGELOG.md`, `docs/roadmap.md`, `internal/plans/1.5.x/2026-09-28-web-backend-1.5.3-remaining.md`
- Modify: bundled `frontend/` assets

- [ ] Run backend gates, route contract tests, live Postgres CRUD/delta checks, frontend gates, and a bundled-image smoke test.
- [ ] Verify `/mirobody.json` flags match mounted routes and the copied bundle's API calls.
- [ ] Rebase on the latest committed `feat/1.5.2` before final verification; do not touch its uncommitted files.
- [ ] Commit backend/API and web bundle alignment in `feat/1.5.3`; do not push Docker Hub.
