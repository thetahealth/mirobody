#!/usr/bin/env python3
"""Render the README's terminal demo to a GIF, reproducibly.

The GIF is a build artifact, not a screen recording: `docs/demo/*.html` is the
source, a browser is the renderer, ffmpeg is the encoder. When the CLI's output
changes, you edit the scene and re-run this — nobody re-records a terminal
session by hand, and the frames cannot drift away from the command they claim
to show.

    python scripts/make_demo_gifs.py                     # capture + encode
    python scripts/make_demo_gifs.py --strip s.png       # encode a strip you saved
    python scripts/make_demo_gifs.py --frames d --name x # encode captured PNGs

The `--frames` mode is for the web-app walkthroughs, and it is deliberately not
automated: those frames are a person signing in and clicking through a running
stack, which is not something a script can assert is still true. Capture them at
one fixed viewport (they must all be the same size), name them so they sort in
order, and this encodes them with the same palette and scaling as the rest.

The scene renders every frame stacked vertically ("filmstrip") so ONE browser
screenshot yields all of them. That is not an optimisation: launching headless
Chrome once per frame took minutes, and on a machine already running Chrome
with a debug port it hung outright.

If the capture step fails (a running Chrome can block a headless launch), open
the scene in any browser at 916px wide, save a full-page PNG, and pass it with
--strip. The slicing is driven by the frame height below, not by the PNG size.
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import subprocess
import sys
import tempfile

from PIL import Image

ROOT = pathlib.Path(__file__).resolve().parent.parent
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

# Must match `.frame` in the scene's CSS and FRAMES in its script.
WIDTH, FRAME_H, FRAMES = 916, 344, 13
SCENE = "docs/demo/resolve.html"
# One GIF per README language. The terminal content is identical in all four --
# real CLI output, and language-neutral by construction; what differs is the
# window label and the closing caption, which are the only words a reader of a
# translated README would otherwise hit in English.
LANGS = ["", "zh-CN", "zh-TW", "ja"]


def gif_name(lang: str) -> str:
    return "resolve-demo.gif" if not lang else f"resolve-demo.{lang}.gif"
HOLD_CS, TAIL_CS = 22, 260          # centiseconds per frame / on the last one
OUT_WIDTH = 916                     # captured at 2x, downscaled to this


def capture(html: pathlib.Path, out: pathlib.Path, lang: str = "") -> None:
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(
            [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars",
             # A throwaway profile: the author's Chrome is usually running, and
             # sharing its user-data-dir makes this fail or disturb their session.
             f"--user-data-dir={pathlib.Path(td, 'profile')}",
             "--force-device-scale-factor=2",
             f"--window-size={WIDTH},{FRAME_H * FRAMES}",
             f"--screenshot={out}",
             html.as_uri() + (f"?lang={lang}" if lang else "")],
            check=True, capture_output=True, timeout=180,
        )


def slice_strip(strip: pathlib.Path, into: pathlib.Path) -> list[pathlib.Path]:
    im = Image.open(strip)
    scale = im.width / WIDTH
    if abs(scale - round(scale)) > 0.01:
        sys.exit(f"{strip} is {im.width}px wide; expected a multiple of {WIDTH}")
    h = round(FRAME_H * scale)
    if im.height < h * FRAMES:
        sys.exit(f"{strip} is {im.height}px tall; need {h * FRAMES} for {FRAMES} frames")
    out = []
    for i in range(FRAMES):
        f = into / f"{i:03d}.png"
        im.crop((0, i * h, im.width, (i + 1) * h)).save(f)
        out.append(f)
    return out


def encode(frames: list[pathlib.Path], gif: pathlib.Path, hold: int = HOLD_CS) -> None:
    """Two passes: a single-pass GIF bands visibly on antialiased text."""
    with tempfile.TemporaryDirectory() as td:
        listing = pathlib.Path(td, "frames.txt")
        rows = [f"file '{f}'\nduration {(TAIL_CS if i == len(frames) - 1 else hold) / 100}"
                for i, f in enumerate(frames)]
        rows.append(f"file '{frames[-1]}'")   # concat ignores the final duration
        listing.write_text("\n".join(rows))

        palette = pathlib.Path(td, "palette.png")
        src = ["-f", "concat", "-safe", "0", "-i", str(listing)]
        vf = f"scale={OUT_WIDTH}:-1:flags=lanczos"
        run = lambda a: subprocess.run(a, check=True, capture_output=True)
        run(["ffmpeg", "-y", *src, "-vf", f"{vf},palettegen=stats_mode=diff", str(palette)])
        run(["ffmpeg", "-y", *src, "-i", str(palette), "-lavfi",
             f"{vf}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3",
             "-loop", "0", str(gif)])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--strip", type=pathlib.Path,
                    help="a full-page PNG of the scene; skips the browser step")
    ap.add_argument("--lang", choices=LANGS,
                    help='one language only (\'\' for English); default: all four')
    ap.add_argument("--frames", type=pathlib.Path,
                    help="a directory of equally-sized PNGs, encoded in name order")
    ap.add_argument("--name", help="output stem for --frames (docs/images/<name>.gif)")
    ap.add_argument("--hold", type=int, default=HOLD_CS,
                    help=f"centiseconds per frame (default {HOLD_CS})")
    args = ap.parse_args()

    if not shutil.which("ffmpeg"):
        sys.exit("ffmpeg not found")

    out_dir = ROOT / "docs" / "images"
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.frames:
        if not args.name:
            sys.exit("--frames needs --name")
        frames = sorted(args.frames.glob("*.png"))
        if not frames:
            sys.exit(f"no PNGs in {args.frames}")
        sizes = {Image.open(f).size for f in frames}
        if len(sizes) != 1:
            sys.exit(f"frames must all be one size; found {sorted(sizes)}")
        gif = out_dir / f"{args.name}.gif"
        encode(frames, gif, hold=args.hold)
        print(f"  {gif.relative_to(ROOT)}  {gif.stat().st_size // 1024} KB  "
              f"{len(frames)} frames  {OUT_WIDTH}px")
        return

    langs = [args.lang] if args.lang is not None else LANGS

    for lang in langs:
        gif = out_dir / gif_name(lang)
        with tempfile.TemporaryDirectory() as td:
            td = pathlib.Path(td)
            strip = args.strip
            if strip is None:
                if not pathlib.Path(CHROME).exists():
                    sys.exit(f"Chrome not found at {CHROME}; capture manually, use --strip")
                strip = td / "strip.png"
                capture(ROOT / SCENE, strip, lang)
            encode(slice_strip(strip, td), gif)
        print(f"  {gif.relative_to(ROOT)}  {gif.stat().st_size // 1024} KB  "
              f"{FRAMES} frames  {OUT_WIDTH}px")


if __name__ == "__main__":
    main()
