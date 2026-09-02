#!/usr/bin/env python3
"""Keep the README's star chart working after GitHub locked the stargazers API.

GitHub restricted `/repos/{owner}/{repo}/stargazers` to a repository's own
admins and collaborators on 2026-06-30. Every third-party chart service reads
that endpoint anonymously, so they all went blind: star-history.com now answers
with an error card instead of a chart, and the mirror the README used
(`star-history.dera.page`) serves a frozen pre-restriction snapshot behind a
24-hour cache — measured 2026-09-02, its data ended at 1,061 stars while the
repository had 1,202, so the two days that added 141 were simply missing.

The upstream workaround is to publish an access token in the README for their
service to use. We do not: a credential handed to a third party and committed
to a public file is a worse trade than rendering the chart ourselves.

So this script does both halves, with the standard library only:

    scripts/star_history.py               # fetch, then render
    scripts/star_history.py --fetch       # refresh docs/star-history.csv
    scripts/star_history.py --render      # redraw the SVGs from the CSV

`--fetch` needs a token for an account with admin/collaborator access — that is
the restriction, not a preference. It reads GH_TOKEN or GITHUB_TOKEN, else asks
`gh auth token`. The CSV it writes is the record: `--render` is offline and
reproducible from it, so the committed chart can be redrawn by anyone.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path

REPO = "thetahealth/mirobody"
ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "docs" / "star-history.csv"
IMAGES = ROOT / "docs" / "images"

#: The four READMEs are held to the same asset set by `test_readme_l10n.py`,
#: and a localized asset that exists on disk MUST be the one its translation
#: references — so the chart is generated once per language rather than showing
#: English axis titles to three of the four. The hand-drawn font stack has no
#: CJK glyphs; those strings fall back to a system CJK face, which is correct
#: (no hand-drawn CJK face exists to fall back to).
LABELS = {
    "":       ("Star History", "Date", "GitHub Stars", "stars"),
    "zh-CN":  ("Star 趋势", "日期", "GitHub Star 数", "stars"),
    "zh-TW":  ("Star 趨勢", "日期", "GitHub Star 數", "stars"),
    "ja":     ("Star の推移", "日付", "GitHub スター数", "stars"),
}

# Geometry and styling copied from star-history.com's own output, because the
# point of this script is that the chart keeps LOOKING like the one the README
# has always shown — only the data is ours. Measured off a real response
# (800x533.333 canvas, plot area translated (70,60), 700 wide, 423.333 tall).
WIDTH, HEIGHT = 800, 534
OX, OY = 70, 60                      # plot origin
PW, PH = 700, 423.333                # plot size

#: The hand-drawn wobble is a filter, not a font: fractal noise displacing the
#: source graphic. Reproduced with star-history's parameters (baseFrequency
#: .05, scale 5) and applied to exactly what they apply it to — the two axis
#: domain lines, the series, and the legend box.
XKCDIFY = (
    '<filter id="xkcdify" width="100%" height="100%" x="-5" y="-5" '
    'filterUnits="userSpaceOnUse">'
    '<feTurbulence baseFrequency=".05" result="noise" type="fractalNoise"/>'
    '<feDisplacementMap in="SourceGraphic" in2="noise" scale="5" '
    'xChannelSelector="R" yChannelSelector="G"/></filter>'
)

#: star-history embeds the xkcd typeface as a base64 WOFF in every SVG. We do
#: not: that font ships under CC BY-NC, which a commercial repository cannot
#: redistribute. The stack below lands on a hand-drawn face on macOS
#: (Chalkboard SE) and Windows (Comic Sans MS) and on the generic cursive
#: elsewhere — and the wobble above is what carries the look regardless.
FONT = "xkcd Script,Humor Sans,Chalkboard SE,Comic Sans MS,Segoe Print,cursive"

#: (ink, background, series) — series colour is star-history's own #dd4528.
THEMES = {
    "light": ("#000000", "#ffffff", "#dd4528"),
    "dark":  ("#e6edf3", "#0d1117", "#dd4528"),
}

def _token() -> str:
    for name in ("GH_TOKEN", "GITHUB_TOKEN"):
        if os.environ.get(name):
            return os.environ[name]
    try:
        out = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=15,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    sys.exit(
        "no GitHub token. Set GH_TOKEN or GITHUB_TOKEN, or run `gh auth login`.\n"
        "The stargazers endpoint has been admin/collaborator-only since "
        "2026-06-30, so an anonymous fetch cannot work."
    )


def fetch(repo: str) -> list[tuple[str, int]]:
    """Return [(YYYY-MM-DD, cumulative stars)] for every day the count changed."""
    token = _token()
    days: dict[str, int] = {}
    total = 0

    for page in range(1, 101):                     # 100 pages * 100 = 10k stars
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/stargazers?per_page=100&page={page}",
            headers={
                # Without this Accept header the endpoint returns bare users and
                # no `starred_at` — the timestamps ARE the chart.
                "Accept": "application/vnd.github.star+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "mirobody-star-history",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                batch = json.load(resp)
        except urllib.error.HTTPError as e:
            sys.exit(
                f"GitHub returned {e.code} for page {page}. Since 2026-06-30 this "
                "endpoint is restricted to the repository's admins and "
                "collaborators — a token without that access gets 403/404 here.\n"
                f"{e.read()[:300].decode('utf-8', 'replace')}"
            )
        if not batch:
            break
        for entry in batch:
            stamp = entry.get("starred_at")
            if not stamp:
                sys.exit(
                    "response has no `starred_at` — the star+json Accept header "
                    "was not honored, so there is nothing to plot."
                )
            total += 1
            days[stamp[:10]] = total

    if not days:
        sys.exit("no stargazers returned; refusing to overwrite the CSV with nothing.")
    return sorted(days.items())


def write_csv(series: list[tuple[str, int]], path: Path = CSV_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "stars"])
        w.writerows(series)
    print(f"wrote {path.relative_to(ROOT)}: {len(series)} change-days, "
          f"{series[-1][1]} stars as of {series[-1][0]}")


def read_csv(path: Path = CSV_PATH) -> list[tuple[str, int]]:
    with path.open(encoding="utf-8") as f:
        return [(row["date"], int(row["stars"])) for row in csv.DictReader(f)]


def _nice_ceiling(value: int) -> tuple[int, int]:
    """Round an axis maximum up to something a human reads, with its step."""
    for step in (10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 2500, 5000):
        if value <= step * 6:
            return ((value + step - 1) // step) * step, step
    step = 10000
    return ((value + step - 1) // step) * step, step


def _monotone_path(points: list[tuple[float, float]]) -> str:
    """Smooth like d3's curveMonotoneX, which is what star-history draws with.

    Fritsch-Carlson tangents rather than plain Catmull-Rom: a cumulative star
    count never decreases, and an unconstrained spline through a flat stretch
    followed by a jump overshoots into a visible dip — a chart that shows the
    project LOSING stars it never lost.
    """
    n = len(points)
    if n < 3:
        return " ".join(f"{'M' if i == 0 else 'L'}{x:.2f} {y:.2f}"
                        for i, (x, y) in enumerate(points))

    dx = [points[i + 1][0] - points[i][0] for i in range(n - 1)]
    dy = [points[i + 1][1] - points[i][1] for i in range(n - 1)]
    slope = [dy[i] / dx[i] if dx[i] else 0.0 for i in range(n - 1)]

    m = [slope[0]] + [0.0] * (n - 2) + [slope[-1]]
    for i in range(1, n - 1):
        if slope[i - 1] * slope[i] <= 0:
            m[i] = 0.0                      # local extremum: flat tangent
        else:
            w1, w2 = 2 * dx[i] + dx[i - 1], dx[i] + 2 * dx[i - 1]
            m[i] = (w1 + w2) / (w1 / slope[i - 1] + w2 / slope[i])

    out = [f"M{points[0][0]:.2f} {points[0][1]:.2f}"]
    for i in range(n - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        h = (x1 - x0) / 3
        out.append(f"C{x0 + h:.2f} {y0 + m[i] * h:.2f} "
                   f"{x1 - h:.2f} {y1 - m[i + 1] * h:.2f} {x1:.2f} {y1:.2f}")
    return " ".join(out)


def render(series: list[tuple[str, int]], theme: str, lang: str = "", repo: str = REPO) -> str:
    ink, bg, line = THEMES[theme]
    title, x_title, y_title, unit = LABELS[lang]

    first = date.fromisoformat(series[0][0])
    last = date.fromisoformat(series[-1][0])
    span = max((last - first).days, 1)
    y_max, y_step = _nice_ceiling(series[-1][1])

    def px(d: date | str) -> float:
        if isinstance(d, str):
            d = date.fromisoformat(d)
        return (d - first).days / span * PW

    def py(v: int) -> float:
        return PH - v / y_max * PH

    path = _monotone_path([(px(d), py(v)) for d, v in series])

    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" style="stroke-width:3;font-family:{FONT};'
        f'background:{bg}">',
        f'  <rect width="{WIDTH}" height="{HEIGHT}" fill="{bg}"/>',
        f'  <defs>{XKCDIFY}</defs>',
        f'  <text x="50%" y="30" text-anchor="middle" style="font-size:20px;'
        f'font-weight:700;fill:{ink}">{title}</text>',
        f'  <g transform="translate({OX} {OY})">',
        # Axis domain lines only — star-history draws no grid, and adding one
        # is the fastest way to stop looking like it.
        f'    <path d="M.5.5h{PW:.0f}" fill="none" stroke="{ink}" '
        f'filter="url(#xkcdify)" transform="translate(0 {PH:.3f})"/>',
        f'    <path d="M-1 {PH + .5:.3f}H.5V.5H-1" fill="none" stroke="{ink}" '
        f'filter="url(#xkcdify)"/>',
    ]

    for v in range(y_step, y_max + 1, y_step):
        o.append(f'    <text x="-7" y="{py(v):.2f}" dy=".32em" text-anchor="end" '
                 f'style="font-size:16px;fill:{ink}">{v}</text>')

    # x labels: month starts, the January one showing the year instead of the
    # month — d3's time axis does this, and so does the chart we are matching.
    # Anything within MIN_GAP of the previous label is dropped, which is what
    # keeps "Nov 2025" and "Dec 2025" from printing on top of each other (the
    # first star landed 2025-11-29, two days before the month boundary).
    MIN_GAP, drawn = 96, float("-inf")
    y, m = first.year, first.month
    while date(y, m, 1) <= last:
        start = date(y, m, 1)
        if start >= first:
            x = px(start)
            if x - drawn >= MIN_GAP:
                drawn = x
                if start.month == 1:
                    label = str(start.year)
                elif lang:
                    label = f"{start.month}月"      # natural in all three CJK locales
                else:
                    label = start.strftime("%B")
                o.append(f'    <text x="{x:.2f}" y="{PH:.3f}" dy=".71em" '
                         f'text-anchor="middle" style="font-size:16px;fill:{ink}">'
                         f'{label}</text>')
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)

    o += [
        f'    <path d="{path}" fill="none" stroke="{line}" filter="url(#xkcdify)"/>',
        # Legend: the same rounded, stroked, wobbled box, with the swatch.
        f'    <rect x="8" y="5" width="196" height="32" rx="5" ry="5" '
        f'fill="{bg}" fill-opacity=".85" stroke="{ink}" stroke-width="2" '
        f'filter="url(#xkcdify)"/>',
        f'    <rect x="15" y="17" width="8" height="8" rx="2" ry="2" fill="{line}" '
        f'filter="url(#xkcdify)"/>',
        f'    <text x="29" y="25" style="font-size:15px;fill:{ink}">{repo}</text>',
        # Where star-history puts its own domain (inside the plot group,
        # bottom-right) we put the provenance: this chart is generated from a
        # CSV in the repository, so anyone can redraw it.
        f'    <text x="{PW - 8}" y="{PH + 40:.0f}" text-anchor="end" '
        f'style="font-size:14px;fill:#8b949e">{series[-1][1]:,} {unit} · '
        f'{series[-1][0]} · docs/star-history.csv</text>',
        '  </g>',
        f'  <text x="50%" y="{HEIGHT - 11}" text-anchor="middle" '
        f'style="font-size:17px;fill:{ink}">{x_title}</text>',
        f'  <text x="-217" y="12" dy=".75em" text-anchor="end" transform="rotate(-90)" '
        f'style="font-size:17px;fill:{ink}">{y_title}</text>',
        '</svg>',
    ]
    return "\n".join(o) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--fetch", action="store_true", help="refresh the CSV from the API")
    ap.add_argument("--render", action="store_true", help="redraw the SVGs from the CSV")
    args = ap.parse_args()

    do_fetch = args.fetch or not args.render
    do_render = args.render or not args.fetch

    if do_fetch:
        write_csv(fetch(args.repo))
    if do_render:
        series = read_csv()
        IMAGES.mkdir(parents=True, exist_ok=True)
        for lang in LABELS:
            suffix = f".{lang}" if lang else ""
            for theme, stem in (("light", "star-history"), ("dark", "star-history-dark")):
                path = IMAGES / f"{stem}{suffix}.svg"
                path.write_text(render(series, theme, lang), encoding="utf-8")
                print(f"wrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
