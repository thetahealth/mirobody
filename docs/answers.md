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
three terminology tools, whose bodies are shared (`standardize.terminology`).

`tools/list` hides a data tool from a user who has none of that data
(`mcp/service.py::_DATA_GATED`): nothing measured or reported, no
`query_health_indicators`; no
plans, no `query_medications`; no active genotype set, no `query_genetic_data`
or `query_pharmacogenomics`. A chat turn gets the four record tools, not the
three terminology tools: the readings tool already resolves names and units, so
those serve only a client holding readings of its own
(`agent/tool_loader.py::_MCP_ONLY_TOOLS`). Beside them: the harness's `ls`,
`read_file`, `glob`, `grep`, the local `python_exec` calculator, and `ask_user`.
`ask_user` is never an MCP tool — an MCP client has no widget to answer a
question with.

The calculator consumes original artifacts rather than display tables;
[Local Python analysis](python-analysis.md) describes data access and setup.

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
| `stats` | count/min/max/avg/sum/first/last/change over the window |
| `trend` | numeric direction and shape from the newest 400 elected daily values, with coverage |
| `latest` | the most recent value per indicator |
| any, with no `keywords` or `indicators` | the catalogue, and a note that the view waits for names |

`trend` compares the means of the first and last thirds of **observed numeric
days**, not equal calendar intervals. `delta_pct` divides their difference by
the mean absolute daily magnitude; it is undefined on an all-zero scale.
`slope_per_day` uses elapsed local calendar days, including gaps. Direction
uses a descriptive 2% threshold; shape distinguishes a single hump or dip
from repeated significant reversals (`swing`). Neither is a clinical test.
Compatible multiplicative units, including known LOINC molar conversions,
share a numeric basis; offset or incompatible conversions return
`incomparable` instead of combining unlike numbers.

Each trend row retains `series`, `system`, `code`, source verification and
day semantics. `n` counts numeric days, `source_days` all daily inputs,
`total_days` all matching source days and `missing_numeric_days` the inputs
without a finite number. `input_truncated` marks the 400-day input cap;
the envelope is partial and names its actual first/last source dates.
Missing numeric values are never zero-filled. Fewer than three numeric days
is `insufficient`; reported and nonnumeric series are `unsupported`, with
an explicit `reason` and day/stats guidance, rather than a catalogue fallback.
Incomplete numeric coverage also yields partial status. Compact output shows
the same statistics, shape, units and coverage as the REST rows.

Trend citation definitions store the series, actual input date span,
`view=trend` and `basis=day_authority`, without derived values. Resolving one
reuses the same elected-or-newest daily selection as the query, so other
intraday readings are not presented as contributors. Sources are read live
after amendments or erasure, within the same person and conversation.

Raw and latest rows, and the records list, show the reading's local time with
its stored microseconds when it has any (`2026-06-01 12:00:00.000001`), trailing
zeros trimmed; a whole second stays `2026-06-01 12:00:00`.

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

## Every number has a source

A chat answer cites what it rests on, in the LongCite form
(`mirobody/kernel/citations.py`):

```
<statement>LDL fell from 2.48 to 2.41 mmol/L<cite>[r3][r4]</cite></statement>
```

* **Row ids.** Every row a record tool shows in a conversation carries a short
  id (`r1`, `r2`, …), minted the first time that conversation sees the row and
  stored in `th_chat_citation`. The id stores which row it is, never its value:
  `GET /api/citations` reads the row live, so an id still names the same
  reading after a restart, and a reading the person has deleted resolves to
  "gone".
* **Three kinds of cite.** A row (`r3`), a reference passage
  (`ref:medlineplus_test:alt-blood-test`, from the medical-knowledge tools in
  `mirobody/agent/tools/README.md`), or lines of a document as `read_file`
  numbered them (`/library/report.pdf#L12-L14`).
* **The check.** `citations.check(answer, support)` returns every number that
  is not traced: written outside a statement, cited to nothing, or found
  neither in its cited rows nor as a difference, ratio, percentage change, sum
  or mean of their values. Chart data in a fenced block is not read as prose.
  It is stdlib only, so the agent, the benchmark and a training judge run the
  same copy.

Automatic row citations check the metric, stated unit and encounter date or
aggregate bounds before combining matching numeric fields by rid. A shared
aggregate rid does not endorse another metric or unit. Units, including
Chinese forms such as `公斤`, `毫克/分升` and `次/分`, use the shared unit
normalizer; an explicit unknown unit remains `auto_uncited`. A generic count
can be called `步` only when the source metric identifies steps. Missing units
do not assert a conversion. Citation markers follow the complete unit.

Change and endpoint phrases retain the same metric binding. Explicit signs
are preserved; only unsigned magnitudes infer direction from prose. Statistic
operators are recognized outside the matched metric name, so `Mean corpuscular
volume` is a metric and `Vitamin D` does not request a minimum. Trend threshold
comparisons use relative tolerance rather than an absolute decimal cutoff.

The web client draws each cite as a numbered chip; clicking it shows the
reading, its date and range, and the file it came from.

## What the person sees while it works

The web client shows the work behind an answer as it happens: the model's
reasoning streams into a panel, and each tool call is a step in the reader's
language ("Looking up your health data · LDL", "Searching medical
references"), with a timer. When the answer starts, the panel folds to one
line ("Thought for 12s · 4 steps") that opens again on a click. Which models
stream their reasoning is in [model-choice.md](model-choice.md).

The first visible reasoning token counts as time to first token. The local
evaluation also measures the first answer token and first tool activity
separately; a heartbeat or loading notice is not a generated token.

`wire.stream.TurnStream` keeps one presentation state per turn. Native
reasoning streams immediately. Ordinary text is held until its model message
completes, because some endpoints send prose before revealing a tool call.
Text from a tool step belongs to reasoning; only a completed non-tool reply
becomes `text`. This delays the start of the final answer until that model
step finishes, while preserving the early thinking feedback. It also prevents
lookup narration from entering saved answer text or a future speech consumer.
Tools keep their structured `tool_call` / `tool_result` blocks, including
multimodal content. The renderer never synthesizes a model's private reasoning
when an endpoint does not return it.

Calculation sources follow the person's turn, not the most recent message of
type `HumanMessage`: the named harness repair and last-call instructions do
not erase results already obtained by the record tools.

Automatic context summarization retains DeepAgents' model-derived trigger and
recent-history policy. The complete selected history is redacted and checked
against the summarizer's input budget, including its instructions and output
reserve. It is not first trimmed to a fixed 4,000-token tail. Unknown input
budgets, oversized prompts and empty/truncated summaries fail explicitly;
recursive chunk summarization is not implemented. Genotype protection applies
before both summarization and history offloading.

Each turn's `usage.context` receipt carries numeric summary-call counts,
committed advancing cutoffs and the declared input window. It never carries
summary text. The long-conversation evaluation requires both committed events
and completed summarizer calls, so a persisted event seen twice cannot satisfy
the two-summary gate. Token fields remain absent when the endpoint reports no
usage; context observability does not manufacture zero token counts.

Summary generation also emits a content-free activity block for the web work
panel: `{"type":"context","phase":"summarization","id":"…","status":"started"}`,
followed by the same operation ID with `completed` or `failed`. These events
come from the actual summarization middleware, via LangGraph's
[custom stream](https://docs.langchain.com/oss/python/langgraph/streaming#custom-data).
`completed` means generation and validation succeeded; it is not evidence of a
committed cutoff. The numeric `usage.context` receipt remains the evaluation
authority. Neither activity nor summary output counts as a visible model token.

The web client flushes its first visible reasoning/answer block immediately,
then batches subsequent tokens. Tool failures and partial results remain in the
work panel, and copy reads only final answer text. Stopping reception aborts the
browser request; the server's existing background-turn persistence still runs
after disconnect. An explicit server-side generation-cancellation API is separate
work, not provided by aborting an HTTP stream.

---

## Governance: what stops a loop

The harness's own middleware, outermost first (`harness.standard_middleware`;
deepagents runs its filesystem, summarisation and tool-call patching before
them, and the `ask_user` pause after):

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
   budget of model calls, and a cap on how often the data tool may run. The
   budget's last call is made with tool calls off (`tool_choice` none, the
   tools still declared) after one instruction to answer from what the model
   has; a model that calls a tool anyway is stopped there. The cap uses
   `exit_behavior="continue"`: hitting it removes the tool for the rest of
   the turn rather than ending the turn, so the model still writes its answer
   from what it has.
6. the tail: a model that cannot see gets
   images as their OCR text, genotype rows are kept to the turn that read
   them, and prompt caching marks the request last.

The data tool itself **never raises**. MCP and REST invoke the same service
outside chat's middleware, so the shared record boundary returns an envelope
on every path. Python is an ordinary bounded tool in that middleware stack.

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
