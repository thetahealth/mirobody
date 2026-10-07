"""The results tables in README.md, from results/<model>/.

    python benchmarks/local_ocr/report.py --commits 6b5933b 490a0e1 e2044e9
    python benchmarks/local_ocr/report.py --set handwriting --commits e2044e9

`--commits` are the Mirobody commits the stored OCR outputs were scored with
(`summary-<commit>.json`, `extract-<commit>_summary.json`), oldest first: the
OCR outputs are the same for every commit, only the product's reading of them
differs. `--set handwriting` reports the handwriting sample
(results/<model>/handwriting/) at the last commit, every page counted, logs
included, by tier, language and kind.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
TIERS = ("T0", "T2", "T3", "T4", "T6")
KINDS = {"home_log": "notebook log", "outpatient_record": "doctor's note", "lab_slip": "hand-filled form"}
LANGUAGES = {"zh-Hans": "Chinese", "en": "English"}


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def pct(a, b) -> str:
    return "—" if not b or a is None else f"{a} ({100 * a / b:.1f}%)"


def gib(mib) -> str:
    return "—" if mib is None else f"{mib / 1024:.2f}"


def models_in(sub: str) -> tuple[dict, list[str]]:
    specs = json.loads((HERE / "models.json").read_text(encoding="utf-8"))
    return specs, [m for m in specs if (RESULTS / m / sub / "meta.json").exists()]


def printed(commits: list[str]) -> None:
    specs, models = models_in("")
    data = {m: {"meta": read(RESULTS / m / "meta.json"),
                **{f"a{c}": read(RESULTS / m / f"summary-{c}.json") for c in commits},
                **{f"b{c}": read(RESULTS / m / f"extract-{c}_summary.json") for c in commits}} for m in models}
    first = commits[0]

    def row(label: str, cell) -> str:
        return f"| {label} | " + " | ".join(cell(data[m], m) for m in models) + " |"

    def size(x, m):
        return f"{sum(f['bytes'] for k, f in x['meta']['files'].items() if k != 'template_file') / 1e9:.2f} GB"

    def a0(x):
        return x[f"a{first}"]["overall"]

    lines = ["| | " + " | ".join(specs[m]["name"] for m in models) + " |", "|---|" + "---|" * len(models),
             row("License", lambda x, m: specs[m]["license"].split(" ")[0]),
             row("Download (model + projector)", size),
             row("OCR seconds per page", lambda x, m: str(a0(x)["seconds_per_page"])),
             row("Peak footprint / max RSS (GiB)",
                 lambda x, m: f"{gib(x['meta']['memory_final'].get('phys_footprint_peak_mib'))} / "
                              f"{gib(x['meta'].get('memory_max_rss_mib'))}"),
             row("(a) printed values in the OCR text (scans, photos, screens)",
                 lambda x, m: pct(a0(x)["values_found"], a0(x)["values_total"])),
             row("(a) OCR rows: name and value", lambda x, m: pct(a0(x)["ocr_found"], a0(x)["truth_rows"])),
             row("(a) OCR rows: all four fields", lambda x, m: pct(a0(x)["ocr_complete"], a0(x)["truth_rows"]))]
    for c in commits:
        lines.append(row(f"(a) rows the table rules store, value right, {c}",
                         lambda x, m, c=c: pct(x[f"a{c}"]["overall"]["value_ok"], x[f"a{c}"]["overall"]["truth_rows"])
                         + f", {x[f'a{c}']['overall']['false']} false"))
    for c in commits:
        def b(x, c=c):
            return (x[f"b{c}"] or {}).get("overall")

        def s(x, c=c):
            return (x[f"b{c}"] or {}).get("series")

        lines += [
            row(f"**(b) end to end, value right as stored, {c}**",
                lambda x, m, b=b: "**" + pct(b(x)["value_ok"], b(x)["truth_rows"]) + "**" if b(x) else "—"),
            row(f"(b) unit right / range right, {c}",
                lambda x, m, b=b: f"{b(x)['unit_ok']} / {b(x)['range_ok']}" if b(x) else "—"),
            row(f"(b) false rows / rows stored twice, {c}",
                lambda x, m, b=b: f"{b(x)['false']} / {b(x).get('repeats', '—')}" if b(x) else "—"),
            row(f"(b) home logs, {c}",
                lambda x, m, s=s: f"{s(x)['value_ok']} of {s(x)['truth_rows']}" if s(x) else "—"),
        ]
    last = commits[-1]
    lines.append(row(f"(b) seconds per page, text model, {last}",
                     lambda x, m: str(((x[f"b{last}"] or {}).get("all") or {}).get("seconds_per_page", "—"))))
    print("\n".join(lines))
    print()
    print(f"Per tier, the 26 non-log pages: (a) OCR rows with name and value / (b) end to end at {last}.\n")
    tiers = data[models[0]][f"a{first}"]["by_tier"]
    print("| Model | " + " | ".join(f"{t} ({tiers[t]['truth_rows']} rows)" for t in TIERS) + " |")
    print("|---|" + "---:|" * len(TIERS))
    for m in models:
        a, b = data[m][f"a{first}"]["by_tier"], (data[m][f"b{last}"] or {}).get("by_tier") or {}
        print(f"| {specs[m]['name']} | " + " | ".join(
            f"{a[t]['ocr_found']} / {(b.get(t) or {}).get('value_ok', '—')}" for t in TIERS) + " |")


def handwriting(commit: str) -> None:
    specs, models = models_in("handwriting")
    a = {m: read(RESULTS / m / "handwriting" / f"summary-{commit}.json") for m in models}
    b = {m: read(RESULTS / m / "handwriting" / f"extract-{commit}_summary.json") for m in models}

    def cell(m: str, key: str | None, value: str | None) -> str:
        x = a[m]["all"] if key is None else a[m][key][value]
        y = (b[m].get("all") if key is None else (b[m].get(key) or {}).get(value)) or {}
        return f"{x['values_found']}/{x['values_total']} · {y.get('value_ok', '—')}/{y.get('truth_rows', '—')}"

    first = a[models[0]]
    groups = [("All pages", None, None)]
    groups += [(t, "by_tier_all", t) for t in sorted(first["by_tier_all"])]
    groups += [(LANGUAGES.get(v, v), "by_language", v) for v in sorted(first.get("by_language") or {})]
    groups += [(KINDS.get(v, v), "by_kind", v) for v in sorted(first.get("by_kind") or {})]
    print("Each cell: (a) printed values found in the OCR text · (b) rows stored with the value right, "
          f"at {commit}.\n")
    print("| Pages | " + " | ".join(specs[m]["name"] for m in models) + " |")
    print("|---|" + "---:|" * len(models))
    for label, key, value in groups:
        pages = first["all"]["pages"] if key is None else first[key][value]["pages"]
        print(f"| {label} ({pages}) | " + " | ".join(cell(m, key, value) for m in models) + " |")
    print()
    print("| | " + " | ".join(specs[m]["name"] for m in models) + " |")
    print("|---|" + "---:|" * len(models))

    def bb(m: str) -> dict:
        return b[m].get("all") or {}

    rows = [
        ("(a) rows with name and value on one line or table row", lambda m: f"{a[m]['all']['ocr_found']}"),
        ("(b) false rows / rows stored twice", lambda m: f"{bb(m).get('false', '—')} / {bb(m).get('repeats', '—')}"),
        ("(b) unit right / range right", lambda m: f"{bb(m).get('unit_ok', '—')} / {bb(m).get('range_ok', '—')}"),
        ("OCR passes that ran to the 8,192-token cap", lambda m: str(a[m]["all"]["capped_passes"])),
        ("(b) seconds per page, text model", lambda m: str(bb(m).get("seconds_per_page", "—"))),
        ("(b) pages the text model answered with nothing", lambda m: str(bb(m).get("empty_pages", "—"))),
    ]
    for label, f in rows:
        print(f"| {label} | " + " | ".join(f(m) for m in models) + " |")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commits", nargs="+", required=True)
    ap.add_argument("--set", default="")
    args = ap.parse_args()
    if args.set == "handwriting":
        handwriting(args.commits[-1])
    else:
        printed(args.commits)


if __name__ == "__main__":
    main()
