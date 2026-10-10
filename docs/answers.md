# The answers layer: one tool per data class, one envelope

Three surfaces show a person their own data — a chat turn, an MCP client, the
web client's Indicators tab — and all three read through one authority. This
page is what the model sees, what it may ask for, what comes back, and what
stops it looping.

---

## One tool per data class, every parameter applicable to every call

The model gets **`query_health_indicators`** for readings and
**`query_medications`** for medications. `query_genetic_data` reads genotype
facts; `query_pharmacogenomics` checks CPIC drug-gene links and coverage.
Each tool has a distinct query grammar.

Two rules decided that shape, and they pull in opposite directions:

* **Merge what is always called in sequence.** A search tool paired with a
  read tool (`search_health_indicators` + `fetch_health_data`, 1.2.0) cost a
  model call to discover names before any numbers could be asked for, and the
  two halves could disagree about what a window meant. So a readings call
  with no selector returns the catalogue, whatever `view` it names,
  `keywords` finds names, and exact `indicators` fetch values — one tool.
* **Split what has a different grammar.** A plan has a lifecycle, a dose has
  a day, a course has a reason it closed; none of that is a reading's `view`
  of a series. A draft of 1.4.0 put medications behind a `kind` switch inside
  the readings tool: seven of thirteen parameters were invalid for
  medications, one was invalid for readings, and every misuse cost a refusal
  round-trip. A parameter the model must decide on every call and which is
  wrong most of the time is a cost with no benefit. So medications got their
  own tool, and the readings tool went from thirteen parameters to eight. It
  is five now: `resolution` × `aggregate` became one `view`, and `limit`
  and `member` became the system's to decide (below).

What the person reported (a symptom felt, a diagnosis given) passes the first
rule, not the second: it is the same table and the same series as a reading,
coded on ICPC-3 instead of LOINC, and every parameter still applies.
`keywords=["headache"]` finds an entry written 头疼 through its code, and
`view="stats"` says how often. A 1.5.1 draft gave it a tool of its own,
whose parameters were five of the eight the readings tool then had. Those rows come back as a second
table, the catalogue lists them first, and the web client's Indicators tab
leaves them out, because the journal is its own tab there.

What left, and why nothing was lost: `panel` (a panel is its member names;
`keywords=["blood pressure"]` finds them), `exclude` (the catalogue is the
second round), `order` (a baseline is `view=stats`, whose `first` and
`first_date` say what the earliest reading was), `source` (rows carry their
source; day and coarser already publish one elected source), and `kind` (two
tools). "How did my blood pressure move after I switched drugs" is
`query_medications(view="history")` for the date, then
`query_health_indicators(start=…)` — two calls whose second input is a date
the person can read, not an opaque handle.

The MCP surface is exactly seven tools:

```
query_health_indicators  query_medications  query_genetic_data
query_pharmacogenomics
resolve_indicator  convert_unit  normalize_unit
```

A second, separate surface needs no server: `mirobody mcp` speaks MCP over
stdio on a bare `pip install mirobody`, with no database and no key. It serves
the vocabularies, not a record: `standardize_reading` (a reading as printed
to a FHIR Observation carrying its LOINC coding), `standardize_complaint`
(ICPC-3), `standardize_report` (needs `[parse]` and a model key), and the same
three terminology tools, whose bodies are shared (`translate.terminology`).

`tools/list` hides a data tool from a user who has none of that data
(`mcp/service.py::_DATA_GATED`): nothing measured or reported, no
`query_health_indicators`; no
plans, no `query_medications`; no active genotype set, no `query_genetic_data`
or `query_pharmacogenomics`. A chat turn gets the four record tools, not the
three terminology tools: the readings tool already resolves names and units, so
those serve only a client holding readings of its own
(`agent/tool_loader.py::_MCP_ONLY_TOOLS`). Beside them, the harness's own: `ls
read_file write_file edit_file glob grep`, the `eval` REPL, `ask_user`, and
`search_medical_reference` (`agent/medref.py`): an offline FTS lookup over the
bundled MedlinePlus summaries and FDA drug labels (`res/medref/`), grounding
general medical knowledge for small local models and returning citeable
`ref:` ids. Neither is ever an MCP tool — an MCP client has no widget to
answer `ask_user` with, and the reference lookup reads no record, so the
asserted seven-tool surface does not change for it.

The local suite asserts both lists exactly.

---

## The record query schemas

```
query_health_indicators(keywords | indicators, start, end, view)
query_medications      (view=plan|log|history, keywords, start, end)
query_genetic_data     (rsids | gene | chromosome+start+end+build)
query_pharmacogenomics (drugs | genes)
```

No parameter names a person. A call reads the account it is authenticated as:
the MCP token or URL, or the record a chat turn was opened on, which the chat
layer authorises before the model runs. One call, one person's data.

Genetic region queries accept `build=GRCh37|GRCh38|raw`. The explicit `raw`
choice reads the file's original coordinates without asserting an assembly;
Ancestry PAR rows use this path because the bundled site index has no
dual-build PAR mapping.

All are flat (models handle flat schemas better than `oneOf`), all are
closed (`additionalProperties: false`), and a parameter the schema does not
have — the former `kind`, say, from a client that learned the draft — comes
back as a structured, recoverable refusal naming it:

```
query_health_indicators(kind="medications", view="day")
→ error (invalid_arguments): kind: unknown parameter. Fix the arguments and try once more.
```

`query_medications` reads the window per view: `plan` and `history` mean "in
effect at some point in the window" (so "what was I taking in March" is a
window on the plan view), `log` means "recorded in the window" and defaults to
the last 30 days. Every `plan` answer carries the note that a plan is intent,
not an intake record; every `log` answer, that a missing dose is not evidence
it was not taken.

And `view` is a dispatch TABLE, not a chain of `if`s (`query.DISPATCH`):

| `view` | answers with |
|---|---|
| `raw` | readings, newest first, at most 50 per indicator (`query.ROW_CAP`) |
| `minute` / `hour` | one point per bucket, the newest 92 per indicator (`query.BUCKET_CAP`) |
| `day` / `week` / `month` | the same, from the day authority: each day counts once |
| `stats` | count/min/max/avg/first/last/change over the window |
| `latest` | the most recent value per indicator |
| any, with no `keywords` or `indicators` | the catalogue, and a note that the view waits for names |

A view with nothing selected used to be refused ("the catalogue has one
shape"). In the 2026-10-06 local-model evaluation MiniCPM5-2B opened with
`view="latest"` and no indicator 3 times and MiniCPM5-1B 7 times, then
repeated the refused call until the harness stopped it (`repeated_call`, 6
times), each attempt a model turn on an 8k-token prompt; DeepSeek V4.1 Flash
never sent it. Now the call is a catalogue call that teaches: the answer is
the catalogue, `next_steps` is `use_indicators`, and a note says
`view=latest was not applied … call again with indicators copied from it`.
The chat tool and the MCP tool are one `HealthIndicatorsService`, so both
answer it the same way.

The catalogue does not grow a latest-value column to answer that call in one
step, although `collect/query.py` already computes `latest_value`. A value
there would come without the range the report printed and without its file,
and those columns were added to readings because a model judged a value
against a range it remembered (1.5.4 local runs). It would also be a second
rule for "latest": the catalogue takes the newest row, `view="latest"` the
day's elected one, so on a day a watch and a phone both reported the two
would print different numbers. And it would cost a column on every catalogue,
up to 200 rows, to save one call in the case a small model gets wrong.

It replaced `resolution` × `aggregate`: eighteen cells, two refused, `limit`
valid in one, and two prompt paragraphs teaching which was which. The
choice that pair really carried was what a statistic counts, and that is not a
question the model can answer from the question. `stats` now counts, per
series and local day, the elected authority where the write side published
one, and every reading of a day where it did not
(`collect/query.py::_STATS_CTE`). Election runs after device aggregation, so a
day with forty intraday samples, or a watch and a phone reporting the same
Tuesday, counts once — as the dashboard shows it. A day nothing elected (a
file upload, a typed-in entry) counts every reading, so a morning and an
evening blood pressure both count. Over raw readings everywhere, the first
case would weigh days by sampling frequency; over the newest reading of each
day, the second would drop the morning.

`limit` went for the same reason: a budget, not a question. Raw rows are cut
at `query.ROW_CAP` per indicator and say so, and what a cut means is a
narrower window or `view=stats`. The browser's reading list is a separate
budget (`collect.REST_ROW_MAX`).

Bucket views have one too, `query.BUCKET_CAP`: the newest 92 points per
indicator, cut in SQL, with `truncated` set.

A cut answer says so first. `meta.cut` holds the notice in plain words, and
`render_compact` puts it before the table: the span shown, that the earlier
data exists and is not missing, and the calls that show it (the coarser
view, `view=stats` for raw rows, or `end=` the day before the first one
shown):

```
Only part of the data is shown: the latest 92 day points per indicator, 2026-06-01 to 2026-08-31.
Earlier data exists and is not shown here; it is not missing. For it, call again with view=week or with end=2026-05-31.
```

It used to be a note after the table, and was missed: MiniCPM5-2B, asked
for monthly resting heart rate from March to August and handed the newest
92 days (June to August) with the cut among the notes, answered that March
and April had no data (benchmarks/local_models small-v2, 2026-10-07). An MCP
client reads the same rendering; `render_rest` carries `cut` beside
`truncated`. Uncapped, MiniCPM5-2B asked
for `view="day"` with no dates and got the whole record, 13,930 and 33,657
characters; the second was evicted to a file it paged until the context
overflowed. 92 is the longest three calendar months, so the three-month daily
chart that evaluation asked for is never cut. Rendered through the tool from
the demo generator's year (`server/demo.py`), daily steps went from 12,346
characters to 3,622, and steps with resting heart rate from 40,192 (the
renderer's 40,000-character cut) to 14,150. The alternative, choosing a
coarser view when the answer is long, was not taken: the model asked for
days, and weekly rows that read like days are a worse answer than the newest
days with the cut said out loud.

A day, week or month point counts each day once: the day's elected value, or
its last reading where nothing was elected, the number a dashboard shows. So
a month's `avg` is a mean of days, not of readings, and differs from `stats`
over the same month whenever a day had several readings: a month of home
blood pressures read 106.0 as `view="month"` and 111.0 over its readings
(2026-10-06). Every such answer says so in `assumptions`.

---

## Citation ids

Every citable row carries a **`rid`** (r1, r2, …) as its first column, and a
note on the answer says to cite it. A model can only cite what it can name:
`row_id` and `file_key` are database handles — a model handed only those
once cited "web_uploads/17eaf4f6-…-edbee3267ea6.pdf" as a value's source —
and an aggregate row has no id at all. So:

* a readings or `latest` row's rid maps one-to-one to its `row_id`;
* a `stats` or bucket row's rid maps to exactly the rows the SQL counted
  (`ARRAY_AGG(o.id)` on the same statement — `collect/query.py`'s
  `_SRC_IDS`), never a list re-derived later;
* the catalogue carries none: its rows stand for series, not for readings.

The map lives in `kernel.citations`, keyed by the record being read — which
is the conversation, from the model's side — and rids mint in first-seen
order, so the same row keeps one rid across a conversation's calls. It is
in-process and never persisted: a verifier resolves a cited rid with
`citations.citation_support(scope, rid)`, which returns the supporting row
ids and `()` for an unknown one (evicted, another process, or made up — all
three are "unverifiable", not "wrong"). The back-handles stay out of the
model's view: `file_key`, `row_id` and the aggregates' `src_ids` ride the
envelope for the web client and the registry, and the rendered table shows
`rid` and the human `file` name.

---

## Windows

* Both `start` and `end` are local dates in the SUBJECT's zone, inclusive.
* Neither given means **the whole record**, and the answer says so
  (`window=all recorded data`). It does not silently apply a default window and
  then report that window as though the rows had come from it.
* A named window that leaves out readings of a series it answered says so,
  in one note naming the series and the date its readings start or end
  outside it: a trend question read from a window holding 2 of 3 cholesterol
  readings was answered from those two (newcomer review, 2026-10-07).
* Every answer states its **window semantics**: `tz_exact` when the rows carry
  a stored local day, `date_padded_naive` when some row predates that column and
  was found by padding its naive timestamp a day either side. That is the
  difference between "on the 3rd" and "around the 3rd", and it is reported
  rather than assumed.

---

## The envelope

The model reads a rendered string. Everything a *program* needs travels beside
it, in `tools.Envelope`:

```python
Envelope(
    status="ok" | "partial" | "error",
    data=[...],                    # the rows
    meta=Meta(window, tz, window_semantics, view, row_count, truncated,
              cut, catalog_total),
    error_class="recoverable" | "unrecoverable" | None,
    error_kind=...,                # one of a closed set
    provenance={indicator: "measured" | "computed" | "elected:<rule>"},
    assumptions=(...),             # methodology, never findings
    next_steps=(...),              # from a closed set
)
```

Two rules about it are load-bearing:

* **Governance reads the envelope, never the text.** A harness may truncate,
  summarise or evict a tool message's content; governance that parsed prose
  would silently stop governing exactly where a long turn needs it. The
  envelope rides on `ToolMessage.artifact`.
* **`next_steps` never suggests a call that cannot help.** A truncated
  catalogue is told to use `indicators` and narrow the window — not to ask
  for `view=stats`, which with no names answers with the same catalogue.

### Two renderings, one query

`render_compact` for the model: a pipe table with constant columns hoisted into
one `(constants: unit=mmol/L)` line, because a third-party MCP client pays per
token for the unit repeated on 200 rows. `render_rest` for the browser: arrays
of objects it can sort and paginate. Re-parsing a pipe table in JavaScript to
rebuild the dicts that existed two calls earlier is not a serialisation
strategy.

---

## Governance: what stops a loop

The harness's own middleware, outermost first (`harness.standard_middleware`;
deepagents runs its filesystem, summarisation and tool-call patching before
them, and the `ask_user` pause after). Two of those core pieces follow the
model's entry rather than a global default: summarisation compacts at 85% of
the entry's `profile.max_input_tokens` when declared (the `local` entry
declares 24,000; undeclared, the trigger is a fixed 170,000 tokens a 32k
window never reaches), and a large tool result offloads to the virtual
filesystem at the entry's `tool_result_offload_tokens` (or a quarter of
`max_input_tokens`, converted from real tokens — deepagents counts 4
characters per token, and the local models' Chinese text tokenizes at ~1.5;
without an entry value the 20,000 default stands).

1. **`ToolFaultMiddleware`** — a crashing tool becomes an error result instead
   of a dead turn. The text carries the tool name, the error kind and the
   exception TYPE; never its message, because driver messages quote SQL with
   bound parameters and models echo what they are given.
2. **`RetryGovernanceMiddleware`** — a call whose `(tool, normalised arguments)`
   already returned an **unrecoverable** result is refused before it runs, and
   one that has run its per-turn limit is refused with a different reason.
   Arguments are normalised, so `["a","b"]` and `["b","a"]` are one call and a
   reordering does not buy another attempt.
3. **`InvalidToolCallRepairMiddleware`** — arguments that never parsed as JSON
   reach no tool at all; this feeds the parse error back, and salvages the
   narrow deterministic cases (`none` for `null`, an unquoted date).
4. **`EmptyAnswerRepairMiddleware`** — a reply with neither text nor a tool
   call is asked for once more instead of ending the turn blank.
5. **`ModelCallBudgetMiddleware`** + **`ToolCallLimitMiddleware`** — the turn's
   budget of model calls, and a cap on how often the data tool may run (the
   reference search has its own, lower cap, for the same reason). The
   budget's last call is made with tool calls off (`tool_choice` none, the
   tools still declared) after one instruction to answer from what the model
   has; a model that calls a tool anyway is stopped there. The cap uses
   `exit_behavior="continue"`: hitting it removes the tool for the rest of
   the turn rather than ending the turn, so the model still writes its answer
   from what it has.
6. the `eval` interpreter, then the tail: a model that cannot see gets
   images as their OCR text, genotype rows are kept to the turn that read
   them, and prompt caching marks the request last.

The data tool itself **never raises**. The `eval` REPL can call it directly
(PTC), and a PTC call bypasses the tool middleware entirely — there is nothing
above it to contain a fault.

---

## Citations for values computed in the REPL

A rid answers "which row", but a value the model COMPUTED inside `eval` — a
window mean, a diff, a count — refers to rows that only existed in the REPL,
and the eval's result was whatever the JS returned, usually `null`: the
derived number cited nothing. The interpreter
(`agent/middleware/eval_refs.py`) now brackets each eval with a rid sink
(`agent/tools/_refs.py`): the readings tool reports every row it surfaced,
in first-surfaced order, and the eval result carries them:

* a returned **object** gets `refs: [...]` injected into it — unless it names
  one already: explicit refs win, narrowed to what the computation actually
  used, and it is the judge's business to grade the narrowing. No console in
  the envelope: the object carries the numbers, and the log stays in its
  `<stdout>` block;
* anything else becomes `{"result": <value-or-null>, "console": "<stdout
  tail>", "refs": [...]}` — teachers commonly `console.log` computed values
  and `return null`, and without the tail those numbers are invisible to the
  citation check. The console is the LAST 2,000 characters of what the REPL
  captured (`console_tail`; cut tails open with `…`; the REPL's own buffer
  may have dropped earlier output first), it REPLACES the `<stdout>` block
  rather than duplicating it, and the key is omitted when nothing was
  logged. The envelope stays strict JSON in these cases;
* an **errored** eval keeps the `<error>` shape — a crashed computation
  cites nothing.

Refs are exactly what THIS eval surfaced; they do not leak across evals or
borrow the conversation's accumulated set, because a value computed here
may only cite what this computation read. Both templates tell the model the
same rule, so the contract it is graded on is the contract it read.

---

## PHI: three layers, all executable

1. **Static** — `mirobody.testing.phi_lint` walks the AST of every log
   statement and reports any interpolated expression that is not an
   identifier, a count, a duration, a status code or a type name. It has a
   baseline that may only shrink.
2. **Runtime** — `ops.PHIPolicy().install()` adds a logging filter that
   redacts by field name, so a log line added at 2am is caught even if the
   lint was not run.
3. **End to end** — one demo reading's comment carries `demo.PHI_CANARY`, and
   the Docker check greps the running container's logs for it. A string rather
   than an odd number, because a leaked bare value is indistinguishable from
   any other number in a log.

The tool's own error paths obey the same rule: the model is told the fault
KIND and the exception TYPE, and the message goes to the log.

---

## What the model is told about absence

Every readings answer carries:

> no data for an indicator means it was never recorded, not that the condition
> is absent

and every medication `plan` answer carries:

> a plan is what the person intends to take; it is not a record of doses taken

Both are in `assumptions`, not in prose the renderer might drop. A model that
is not told the first draws the opposite conclusion, and the opposite
conclusion about health data is the one that matters.
