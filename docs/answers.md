# The answers layer: one tool per data class, one envelope

Three surfaces show a person their own data — a chat turn, an MCP client, the
web client's Indicators tab — and all three read through one authority. This
page is what the model sees, what it may ask for, what comes back, and what
stops it looping.

---

## One tool per data class, every parameter applicable to every call

The model gets **`query_health_indicators`** for readings and
**`query_medications`** for medications. Genetics is `query_genetic_data`. Three
classes of data, three grammars, three tools — and inside each, search and
read are ONE call.

Two rules decided that shape, and they pull in opposite directions:

* **Merge what is always called in sequence.** A search tool paired with a
  read tool (`search_health_indicators` + `fetch_health_data`, 1.2.0) cost a
  model call to discover names before any numbers could be asked for, and the
  two halves could disagree about what a window meant. So a readings call
  with no selector returns the catalogue, `keywords` finds names, and exact
  `indicators` fetch values — one tool.
* **Split what has a different grammar.** A plan has a lifecycle, a dose has
  a day, a course has a reason it closed; none of that is a `resolution` or an
  `aggregate`. A draft of 1.4.0 put medications behind a `kind` switch inside
  the readings tool: seven of thirteen parameters were invalid for
  medications, one was invalid for readings, and every misuse cost a refusal
  round-trip. A parameter the model must decide on every call and which is
  wrong most of the time is a cost with no benefit. So medications got their
  own tool, and the readings tool went from thirteen parameters to eight.

What left, and why nothing was lost: `panel` (a panel is its member names;
`keywords=["blood pressure"]` finds them), `exclude` (the catalogue is the
second round), `order` (a baseline is `aggregate=stats`, whose `first` and
`first_date` say what the earliest reading was), `source` (rows carry their
source; day and coarser already publish one elected source), and `kind`/`view`
(two tools). "How did my blood pressure move after I switched drugs" is
`query_medications(view="history")` for the date, then
`query_health_indicators(start=…)` — two calls whose second input is a date
the person can read, not an opaque handle.

The MCP surface is exactly six tools:

```
query_health_indicators  query_medications  query_genetic_data
resolve_indicator  convert_unit  normalize_unit
```

`tools/list` hides a data tool from a user who has none of that data
(`mcp/service.py::_DATA_GATED`): no readings, no `query_health_indicators`; no
plans, no `query_medications`; no genotype, no `query_genetic_data`. A chat turn
gets all six plus the harness's own: `ls read_file write_file edit_file glob
grep`, the `eval` REPL, and `ask_user`. `ask_user` is never an MCP tool — an
MCP client has no widget to answer a question with.

`tests/agent/test_tool_surface.py` asserts both lists exactly.

---

## The two schemas

```
query_health_indicators(keywords | indicators, start, end, resolution, aggregate, limit, member)
query_medications      (view=plan|log|history, keywords, start, end, member)
```

Both are flat (models handle flat schemas better than `oneOf`), both are
closed (`additionalProperties: false`), and a parameter the schema does not
have — the former `kind`, say, from a client that learned the draft — comes
back as a structured, recoverable refusal naming it:

```
query_health_indicators(kind="medications", resolution="day")
→ error (invalid_arguments): kind: unknown parameter. Fix the arguments and try once more.
```

`query_medications` reads the window per view: `plan` and `history` mean "in
effect at some point in the window" (so "what was I taking in March" is a
window on the plan view), `log` means "recorded in the window" and defaults to
the last 30 days. Every `plan` answer carries the note that a plan is intent,
not an intake record; every `log` answer, that a missing dose is not evidence
it was not taken.

And `(resolution, aggregate)` is a dispatch TABLE, not a chain of `if`s
(`query.DISPATCH`). A cell with no entry is a refused combination — the readings tool's only conditional rule:

| | `aggregate=none` | `stats` | `latest` |
|---|---|---|---|
| `raw` | readings | over readings | most recent reading |
| `minute` / `hour` | buckets | over buckets | — |
| `day` / `week` / `month` | buckets from the day authority | over DAYS | most recent day |

`aggregate=stats` at `resolution=day` averages **days**, not readings. Over raw
readings a day with forty intraday samples would weigh forty times a day with
one, and "my average resting heart rate this month" would be a number about
sampling frequency.

---

## Windows

* Both `start` and `end` are local dates in the SUBJECT's zone, inclusive.
* Neither given means **the whole record**, and the answer says so
  (`window=all recorded data`). It does not silently apply a default window and
  then report that window as though the rows had come from it.
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
    meta=Meta(window, tz, window_semantics, resolution, aggregate,
              aggregate_basis, row_count, truncated, catalog_total, accepted_for),
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
* **`next_steps` never suggests something the matrix would refuse.** A
  truncated catalogue is told to use `indicators` and narrow the window — not
  to aggregate, which the very next call would reject.

### Two renderings, one query

`render_compact` for the model: a pipe table with constant columns hoisted into
one `(constants: unit=mmol/L)` line, because a third-party MCP client pays per
token for the unit repeated on 200 rows. `render_rest` for the browser: arrays
of objects it can sort and paginate. Re-parsing a pipe table in JavaScript to
rebuild the dicts that existed two calls earlier is not a serialisation
strategy.

---

## Governance: what stops a loop

Four middlewares, outermost first:

1. **`ToolFaultMiddleware`** — a crashing tool becomes an error result instead
   of a dead turn. The text carries the tool name, the fault kind and the
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
4. **`ModelCallLimitMiddleware`** + **`ToolCallLimitMiddleware`** — the turn's
   budget, and a cap on how often the data tool may run.
   `exit_behavior="continue"` on the second: hitting the cap removes the tool
   for the rest of the turn rather than ending the turn, so the model still
   writes its answer from what it has.

The data tool itself **never raises**. The `eval` REPL can call it directly
(PTC), and a PTC call bypasses the tool middleware entirely — there is nothing
above it to contain a fault.

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
