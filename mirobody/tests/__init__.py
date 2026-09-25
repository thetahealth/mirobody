"""The tests that ship with a clone.

`test_engine_coverage.py` is EVIDENCE for a number the README prints and links,
so it stays in the repository. A benchmark nobody can run is an assertion.

`test_cross_language_identity.py` is what a terminology contributor checks a
change against: one analyte answers one code, whatever the language. The
splits that do not agree yet are strict xfails in the same file, so the open
fixes are listed where the fix is tested.

The rest of the regression suite lives in a gitignored `tests/` at the repo
root.

Nothing here reaches the wheel: `scripts/build_backend.py` drops the directory
and `scripts/check_wheel_data.py` fails the build if a test module comes back.
"""
