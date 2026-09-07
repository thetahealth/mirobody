"""The toolbox a consumer runs against its own code: lints and contract
runners that encode what this project learned, so a downstream repository
does not have to relearn it.

* :mod:`.phi_lint` — a static check that log statements carry identifiers,
  counts and type names, never values.
* :mod:`.prompts` — a check that prompt templates only name tools that exist.
* :mod:`.samples` — run vendor sample payloads (ours or a consumer's) through a
  decoder and compare with the hand-computed expectations.
* :mod:`.contracts` — invariants any ``sink.Sink`` implementation must hold.
* :mod:`.golden` — snapshot files with an explicit update switch.
* :mod:`.coverage` — the provider coverage matrix, generated from the
  decoders so a documentation page cannot promise data the code does not
  produce, plus the CI check that it is current.

This package may use ``pytest`` conventions but does not import it: every
function returns findings and raises ``AssertionError`` only from the
``assert_*`` helpers, so the same code runs in CI and in a REPL.
"""
