"""Rebuild the mirobody-gen corpus from its seed and keep each document's clean PDF.

    PYTHONPATH=<mirobody-gen> <its python> benchmarks/local_ocr/clean_pdfs.py --seed 7 --people 6 --out <dir>

Run with the GENERATOR's interpreter (it needs PyMuPDF), not mirobody's.

Why: `files.jsonl` gives each file's printed rows but not the page each row is
printed on, and a scanned check-up book is 5-13 pages. The benchmark scores
single pages, so it needs a page's own truth. The generator renders every PDF
family to a text-layer PDF first and only then rasterises it into a scan,
photo or fax (`mirobody_gen.delivery.render_doc`), so page k of the scan is
page k of that clean PDF. This wraps `mirobody_gen.render.pdf.render` (the one
call that makes it) to keep a copy under `<out>/clean/<doc_id>.pdf`, then runs
the ordinary `build --render`. The wrapper returns what it was given and draws
no random numbers, so the corpus it writes is the same corpus: `run.py`
checks every delivered file's sha256 against the corpus it scores.
"""

from __future__ import annotations

import argparse
import pathlib
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--people", type=int, default=6)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    from mirobody_gen import build
    from mirobody_gen.render import pdf

    clean = pathlib.Path(args.out) / "clean"
    clean.mkdir(parents=True, exist_ok=True)
    original = pdf.render

    def render(doc, reported_iso):
        data, pages = original(doc, reported_iso)
        (clean / f"{doc.doc_id}.pdf").write_bytes(data)
        return data, pages

    pdf.render = render
    sys.argv = ["mirobody-gen build", "--seed", str(args.seed), "--people", str(args.people),
                "--out", args.out, "--render"]
    build.main()


if __name__ == "__main__":
    main()
