"""Load one person of a mirobody-gen build into an account, through the
same HTTP API the web client and a phone client use.

    # the three accounts every size's questions read (once, before any run):
    python benchmarks/local_models/load.py --corpus <build> --qa
    # one person into an account of your choosing:
    python benchmarks/local_models/load.py --corpus <build> --person p004 --email me@example.org \\
        --documents files/p004/p004_2023-11-12_e01a.pdf --genomics

What goes in, and how:

* device batches (`devices/<person>/*.json`) with `POST /api/data`, as a
  phone client sends them;
* the genotype export with `POST /files/upload?file=true`, which detects it;
* documents with `POST /files/upload?file=true`, one at a time, each waited
  for until its `th_files` row says `processed` (`GET
  /api/v1/data/uploaded-files`). A document whose readings were filed under
  a date other than the one printed on it is re-dated with `POST
  /api/v1/health-indicators/file-date`, which is what a person does when the
  Data page asks "which date?"; load.json says when that happened;
* the journal's symptoms with `POST /api/v1/journal`, one entry per symptom on
  the day the build wrote it, the sentence kept as the entry's note. Not
  `/journal/sentence`, which asks the text model: the record the questions
  read must not depend on which model loaded it.

A rerun signs in to the same account and skips every step it finished
(`results/load.json`). `--verify` compares what the account holds with the
build's truth: each document's rows, scored as the extraction part scores
them, and each case fact that cites a printed row.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (
    APP, COMPOSE_DIR, RESULTS, ROOT, Account, ApiError, Corpus, git_dirty, git_head, guard, now_iso, pending_tasks,
    psql_exec, psql_json, qa_email, read_json, read_jsonl, router_status, sign_in, wait_tasks_drained, write_json,
)
from score import score_document

LOAD_STATE = RESULTS / "load.json"
UPLOAD_TIMEOUT = 1800


def settings_for(account: Account, person: dict) -> None:
    """Sex, birth year, language and zone, as the Settings page saves them."""
    account.put("/api/user/settings", {"settings": {
        "profile": {"gender": person["sex"], "birth": f"{person['birth_year']}-07-01"},
        "preferences": {"language": account.lang, "timezone": account.tz},
    }})


def load_devices(account: Account, corpus: Corpus, person: str, log=print) -> dict:
    out = {"batches": 0, "ingested": 0, "standardized": 0, "rejected": {}}
    for path in sorted((corpus.dir / "devices" / person).glob("*.json")):
        body = json.loads(path.read_text(encoding="utf-8"))
        r = account.post("/api/data", body, timeout=600)
        out["batches"] += 1
        out["ingested"] += int(r.get("ingested") or 0)
        out["standardized"] += int(r.get("standardized") or 0)
        for k, v in (r.get("rejected") or {}).items():
            out["rejected"][k] = out["rejected"].get(k, 0) + v
        log(f"  {path.name}: {len(body['records'])} records, {r.get('ingested')} ingested")
    return out


def upload_document(account: Account, path: Path, *, timeout: float = UPLOAD_TIMEOUT, genotype: bool = False,
                    log=print) -> dict:
    """Upload one file to be filed and extracted, and return once its
    readings are in, with how long that took.

    "In" is not what the files API says. `GET /api/v1/data/uploaded-files`
    marks a document `processed` once it is filed and titled, while the
    indicator extraction it started is still running (README.md, "Product
    issues"), so the end is read where the app states it: its log line
    "Async indicator extraction finished for <type>: <file_key>". A genotype
    file is in when `/api/v1/genomics/active-set` shows a set.
    """
    since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.monotonic()
    mime = {".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".csv": "text/csv", ".txt": "text/plain",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}.get(path.suffix.lower(),
                                                                                              "application/octet-stream")
    with open(path, "rb") as fh:
        r = account.session.post(APP + "/files/upload", params={"file": "true"}, headers=account.headers(),
                                 files={"files": (path.name, fh, mime)}, timeout=600)
    body = r.json()
    if r.status_code >= 400 or body.get("code") != 0 or not body.get("data"):
        raise ApiError(f"upload {path.name}: {body.get('msg')}")
    return wait_document(account, body["data"][0]["file_key"], path.name, since=since, t0=t0, timeout=timeout,
                         genotype=genotype, log=log)


def uploaded(account: Account, name: str) -> dict | None:
    """The account's file uploaded as `name`, if there is one: a rerun after
    an interruption waits for it instead of uploading it twice."""
    files = account.get("/api/v1/data/uploaded-files", limit=100)["data"]["files"]
    return next((f for f in files if f.get("original_name") == name), None)


def wait_document(account: Account, key: str, name: str, *, since: str, t0: float, timeout: float = UPLOAD_TIMEOUT,
                  genotype: bool = False, log=print) -> dict:
    row: dict[str, Any] = {}
    status, filed_s = "timeout", None
    while time.monotonic() - t0 < timeout:
        time.sleep(5)
        files = account.get("/api/v1/data/uploaded-files", limit=100)["data"]["files"]
        row = next((f for f in files if f.get("file_key") == key), {})
        if row.get("processed") and filed_s is None:
            filed_s = round(time.monotonic() - t0, 1)
        if row.get("upload_status") == "failed":
            status = "failed"
            break
        if genotype:
            if account.get("/api/v1/genomics/active-set")["data"].get("active_set"):
                status = "done"
                break
            continue
        ended = _extraction_end(key, since)
        if ended:
            status = "done" if ended == "finished" else "failed"
            time.sleep(2)
            files = account.get("/api/v1/data/uploaded-files", limit=100)["data"]["files"]
            row = next((f for f in files if f.get("file_key") == key), row)
            break
        if filed_s is not None and time.monotonic() - t0 > filed_s + 60 and not _extraction_started(key, since):
            status = "done"                     # filed with no text to extract from
            break
    seconds = round(time.monotonic() - t0, 1)
    log(f"  {name}: {status} in {seconds:.0f} s, {row.get('indicators_count', 0)} readings")
    return {"file_key": key, "status": status, "seconds": seconds, "filed_s": filed_s, "error": row.get("error") or "",
            "indicators_count": row.get("indicators_count", 0), "report_date": row.get("report_date") or "",
            "date_source": row.get("date_source") or "", "scene": row.get("scene") or "",
            "title": row.get("file_name") or ""}


def _app_log(since: str) -> str:
    out = subprocess.run(["docker", "compose", "logs", "--no-log-prefix", "--since", since, "mirobody"],
                         cwd=COMPOSE_DIR, capture_output=True, text=True, timeout=120)
    return out.stdout


#: The app's lines for an extraction, as 1.5.4 wrote them ("Async indicator
#: extraction finished for pdf: <key>") and as 1.5.5 does ("indicator
#: extraction finished: file_key=<key>"). Missing the second, every document
#: read as done 60 s after it was filed, before some of its readings landed.
_EXTRACTION = re.compile(r"indicator extraction (started|finished|failed)", re.I)


def _extraction_events(key: str, since: str) -> set[str]:
    return {m.group(1).lower() for line in _app_log(since).splitlines() if key in line
            for m in [_EXTRACTION.search(line)] if m}


def _extraction_end(key: str, since: str) -> str | None:
    events = _extraction_events(key, since)
    return "finished" if "finished" in events else "failed" if "failed" in events else None


def _extraction_started(key: str, since: str) -> bool:
    return "started" in _extraction_events(key, since)


def stored_readings(account: Account) -> list[dict]:
    """Every measurement the account holds (`GET /api/v1/health-indicators/
    records`), each with the extractor that wrote it (from the database)."""
    rows, offset = [], 0
    while True:
        page = account.get("/api/v1/health-indicators/records", limit=200, offset=offset)["data"]
        rows += page["rows"]
        if not page.get("has_more"):
            break
        offset += 200
    extractors = psql_json(
        "select coalesce(json_object_agg(o.id, e.extractor), '{}') from v_observation o "
        f"left join th_extraction e on e.id = o.extraction_id where o.user_id = '{int(account.user_id)}'") or {}
    for r in rows:
        r["extractor"] = extractors.get(str(r.get("row_id")), "")
    return rows


def journal_entries(account: Account, sentences: list[dict], log=print) -> dict:
    """One `POST /api/v1/journal` per symptom a sentence states."""
    from zoneinfo import ZoneInfo

    written = skipped = 0
    for s in sentences:
        at = datetime.fromisoformat(f"{s['date']}T20:00:00").replace(tzinfo=ZoneInfo(account.tz)).isoformat()
        for e in s["expected"]:
            if e["kind"] != "symptom":
                continue
            r = account.post("/api/v1/journal", {
                "text": e["name"], "kind": "symptom", "observed_at": at, "tz": account.tz, "note": s["text"]})
            if (r.get("data") or {}).get("written") == 0:
                skipped += 1
            else:
                written += 1
    log(f"  journal: {written} entries written, {skipped} already there")
    return {"written": written, "skipped": skipped}


def load_person(corpus: Corpus, person: str, email: str, *, tz: str, lang: str, documents: list[str],
                genomics: bool, devices: bool = True, journal: bool = True, replace: tuple[str, ...] = (),
                log=print) -> dict:
    state = read_json(LOAD_STATE, {})
    entry = state.setdefault(email, {"person": person})
    account = sign_in(email, tz=tz, lang=lang)
    entry.update({"user_id": account.user_id, "tz": tz, "lang": lang})
    entry.setdefault("mirobody_commit", git_head(ROOT))
    entry.setdefault("mirobody_dirty", git_dirty(ROOT))
    steps = entry.setdefault("steps", {})

    def save() -> None:
        write_json(LOAD_STATE, state)

    log(f"{person} -> {email} (user {account.user_id})")
    guard(f"loading {person}")
    if "settings" not in steps:
        settings_for(account, corpus.people[person])
        steps["settings"] = now_iso()
        save()
    if devices and "devices" not in steps and person in corpus.devices:
        steps["devices"] = load_devices(account, corpus, person, log=log)
        save()
    if genomics and "genomics" not in steps and person in corpus.genomics:
        g = upload_document(account, corpus.path(corpus.genomics[person]["file"]), genotype=True, log=log)
        active = account.get("/api/v1/genomics/active-set")["data"]["active_set"]
        steps["genomics"] = {**g, "active_set": active}
        save()
    for rel in documents:
        if rel in replace:
            replace_document(account, entry, rel, log=log)
            save()
    docs = steps.setdefault("documents", {})
    for rel in documents:
        if rel in docs and docs[rel].get("status") == "done":
            continue
        guard(f"uploading {Path(rel).name}")
        earlier = uploaded(account, Path(rel).name)
        if earlier:
            log(f"  {Path(rel).name}: uploaded before this run; waiting for it")
            since = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            docs[rel] = wait_document(account, earlier["file_key"], Path(rel).name, since=since, t0=time.monotonic(),
                                      log=log)
            docs[rel]["seconds"] = None
            docs[rel]["note"] = "the loader was restarted while this file was processed; its time is not measured"
        else:
            docs[rel] = upload_document(account, corpus.path(rel), log=log)
        docs[rel]["agent_model"] = _agent_model()
        save()
    for rel in documents:
        _confirm_date(account, corpus, rel, docs[rel], log=log)
        save()
    if journal and "journal" not in steps:
        steps["journal"] = journal_entries(account, corpus.journal_of(person), log=log)
        save()
    entry["loaded_at"] = now_iso()
    save()
    return entry


def replace_document(account: Account, entry: dict, rel: str, log=print) -> None:
    """Delete a document's upload through the app (`POST /api/v1/data/delete-files`,
    what the Data page does) and wait until its readings are gone, so load_person
    uploads it again: a fix to how a document is read reaches a loaded record
    without loading the rest again. load.json keeps the old upload under
    `replaced`."""
    docs = entry.setdefault("steps", {}).setdefault("documents", {})
    old = docs.get(rel) or {}
    row = uploaded(account, Path(rel).name)
    if not row:
        log(f"  {Path(rel).name}: no upload to replace")
        return
    key = row["file_key"]
    r = account.post("/api/v1/data/delete-files", {"message_id": row.get("created_source_id") or row.get("id"),
                                                    "file_keys": [key]})
    log(f"  {Path(rel).name}: deleted {key}: {r.get('msg')}")
    t0 = time.monotonic()
    while True:
        n = psql_json(f"select to_json(count(*)) from v_observation where user_id = '{int(account.user_id)}' "
                      f"and source_ref = 'th_files:{key}'")
        if not n:
            break
        if time.monotonic() - t0 > 300:
            raise SystemExit(f"{key}: {n} readings still stored 300 s after the delete")
        time.sleep(5)
    entry.setdefault("replaced", []).append({**old, "file": rel, "replaced_at": now_iso(),
                                             "readings_gone_s": round(time.monotonic() - t0, 1)})
    docs.pop(rel, None)


def _agent_model() -> str:
    """The answering model the router has loaded now, which also reads the
    text a document's tables leave."""
    loaded = [m for m, s in router_status().items() if s == "loaded" and m != "glm-ocr" and "/" not in m]
    return ",".join(sorted(loaded))


def _confirm_date(account: Account, corpus: Corpus, rel: str, doc: dict, log=print) -> None:
    truth = corpus.files[rel]["encounter_dates"]
    if doc.get("status") != "done" or len(truth) != 1 or doc.get("date_set"):
        return
    printed = truth[0]
    files = account.get("/api/v1/data/uploaded-files", limit=100)["data"]["files"]
    row = next((f for f in files if f.get("file_key") == doc["file_key"]), {})
    doc["report_date"], doc["date_source"] = row.get("report_date") or "", row.get("date_source") or ""
    if doc["report_date"][:10] == printed and doc["date_source"] == "extracted":
        return
    r = account.post("/api/v1/health-indicators/file-date", {"file_key": doc["file_key"], "report_date": printed})
    doc["date_set"] = {"from": doc.get("report_date") or "", "source": doc.get("date_source") or "", "to": printed,
                       "result": r.get("data")}
    log(f"  {Path(rel).name}: filed under {doc.get('report_date') or 'the upload time'}"
        f" ({doc.get('date_source') or '?'}); set to the printed {printed}")


#: How long the task queue must stay empty before profiles are turned off. A
#: document's readings, and the refresh they queue, can land a minute after
#: its file reads `processed`: a re-read with --replace found the queue empty
#: and its refresh ran later, under the next run (2026-10-10).
QUIET_S = 120


def disable_profiles(user_ids: list[str], log=print) -> None:
    """Mark the accounts' generated health profiles deleted, the state the
    product's own invalidation leaves (file_processing_service.py), so no
    size answers from a profile another size wrote. Waits for the refresh
    the uploads queued first, or it would write a new one afterwards."""
    t0 = time.monotonic()
    quiet = 0.0
    while quiet < QUIET_S:
        wait_tasks_drained(log=log)
        time.sleep(10)
        quiet = quiet + 10 if pending_tasks() == 0 else 0.0
    waited = time.monotonic() - t0
    ids = ",".join(f"'{int(u)}'" for u in user_ids)
    status = psql_exec(f"update health_user_profile_by_system set is_deleted = true, last_update_time = now() "
                       f"where user_id in ({ids}) and is_deleted = false")
    log(f"  profiles: waited {waited:.0f} s for the queue, then {status}")


def verify(corpus: Corpus, email: str, cases_path: Path, log=print) -> dict:
    entry = read_json(LOAD_STATE, {})[email]
    account = sign_in(email, tz=entry["tz"], lang=entry["lang"])
    stored = stored_readings(account)
    by_key: dict[str, list[dict]] = {}
    for r in stored:
        by_key.setdefault(r.get("file_key") or "", []).append(r)
    report: dict[str, Any] = {"email": email, "readings": len(stored), "documents": {}, "facts_missing": []}
    found_rows: dict[str, set[int]] = {}
    for rel, doc in (entry.get("steps", {}).get("documents") or {}).items():
        s = score_document(corpus.files[rel], by_key.get(doc["file_key"], []))
        found_rows[rel] = {d["row"] for d in s["detail"] if d.get("found")}
        report["documents"][rel] = {k: s[k] for k in ("rows", "found", "unit_ok", "range_ok", "extra", "dates_stored",
                                                      "by_extractor")}
        report["documents"][rel]["missing"] = [f"{d['name']} {d['value']}" for d in s["detail"]
                                               if not d.get("found") and d.get("readable", True)]
        log(f"  {Path(rel).name}: {s['found']}/{s['rows']} rows, unit {s['unit_ok']}, range {s['range_ok']}, "
            f"{s['extra']} extra, dates {s['dates_stored']}")
    for case in read_jsonl(cases_path):
        if case["person"] != entry["person"]:
            continue
        for fact in case["facts"]:
            src = fact.get("source")
            if src and src[1] not in found_rows.get(src[0], set()):
                report["facts_missing"].append({"case": case["id"], "fact": fact["label"], "file": src[0]})
    if report["facts_missing"]:
        log(f"  {len(report['facts_missing'])} case fact(s) cite a row this account lacks: {report['facts_missing']}")
    report["genotype"] = account.get("/api/v1/genomics/active-set")["data"]["active_set"]
    report["catalogue"] = len(account.get("/api/v1/health-indicators")["data"].get("indicators") or [])
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Load a mirobody-gen person into an account.")
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--qa", action="store_true", help="load the three Q&A accounts of plan.json")
    ap.add_argument("--person")
    ap.add_argument("--email")
    ap.add_argument("--tz", default="Asia/Shanghai")
    ap.add_argument("--lang", default="zh")
    ap.add_argument("--documents", nargs="*", default=[])
    ap.add_argument("--genomics", action="store_true")
    ap.add_argument("--no-devices", action="store_true")
    ap.add_argument("--no-journal", action="store_true")
    ap.add_argument("--keep-profile", action="store_true",
                    help="leave the generated health profile on (the Q&A accounts turn it off)")
    ap.add_argument("--verify", action="store_true", help="only compare what the accounts hold with the truth")
    ap.add_argument("--replace", nargs="*", default=[],
                    help="documents (corpus paths) to delete and upload again in the accounts that hold them")
    args = ap.parse_args()
    corpus = Corpus(args.corpus)
    here = Path(__file__).resolve().parent
    if args.qa:
        plan = json.loads((here / "plan.json").read_text(encoding="utf-8"))
        targets = [(p, qa_email(p), cfg) for p, cfg in plan["qa_people"].items()]
    else:
        if not (args.person and args.email):
            ap.error("--person and --email, or --qa")
        targets = [(args.person, args.email, {"tz": args.tz, "lang": args.lang, "documents": args.documents,
                                              "genomics": args.genomics})]
    if args.verify:
        reports = {email: verify(corpus, email, here / "cases.jsonl") for _, email, _ in targets}
        state = read_json(LOAD_STATE, {})
        for email, rep in reports.items():
            state[email]["verify"] = rep
        write_json(LOAD_STATE, state)
        return
    ids = []
    for person, email, cfg in targets:
        entry = load_person(corpus, person, email, tz=cfg["tz"], lang=cfg["lang"], documents=cfg["documents"],
                            genomics=cfg["genomics"], devices=not args.no_devices, journal=not args.no_journal,
                            replace=tuple(args.replace))
        ids.append(entry["user_id"])
    if args.qa and not args.keep_profile:
        disable_profiles(ids)
        state = read_json(LOAD_STATE, {})
        for _, email, _ in targets:
            state[email]["profile"] = "off"
        write_json(LOAD_STATE, state)
    print(f"done; {pending_tasks()} background task(s) pending")


if __name__ == "__main__":
    main()
