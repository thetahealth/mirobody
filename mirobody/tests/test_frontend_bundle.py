"""The bundled web client names only files `frontend/` holds.

`frontend/` is another repository's build output, copied in by hand
(docs/frontend.md, "Replacing the client"). The sync of mirobody-web 985f3c2
committed the new chunks but kept the old `index.html`, whose entry script,
`/assets/index-mKXkiuxp.js`, the same sync had deleted: every page was a blank
shell while that script 404'd, and nothing outside a browser noticed. This
walks what a browser fetches, from `index.html` through each chunk it loads,
and fails on any name `frontend/` does not hold.

A `pip install` ships no `frontend/`, so there the real-tree test skips.
"""
from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"

_EXT = r"(?:js|css|svg|png|jpe?g|webp|gif|ico|woff2?|ttf|otf|json|wasm)"
# Vite spells a reference three ways inside a chunk: `/assets/x` (asset URLs,
# CSS `url()`), `assets/x` (the `__vite__mapDeps` preload list, relative to
# the base) and `"./x"` (one chunk importing a sibling in the same folder).
_IN_ASSETS = re.compile(rf"assets/([\w.-]+\.{_EXT})\b")
_SIBLING = re.compile(rf"""["']\./([\w.-]+\.{_EXT})["']""")


class _HtmlRefs(HTMLParser):
    """The same-origin `src` and `href` values of a page, and its module scripts."""

    def __init__(self) -> None:
        super().__init__()
        self.paths: list[str] = []
        self.modules: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        for key in ("src", "href"):
            url = values.get(key)
            if url and url.startswith("/") and not url.startswith("//"):
                self.paths.append(url.split("?", 1)[0].split("#", 1)[0].lstrip("/"))
        if tag == "script" and values.get("type") == "module" and values.get("src"):
            self.modules.append(values["src"].lstrip("/"))


def walk(frontend: Path) -> tuple[_HtmlRefs, set[str], list[str]]:
    """What a browser asks `frontend` for, starting at its `index.html`.

    Returns the page's own references, every path reached (relative to
    `frontend`), and each missing one as "referrer -> path".
    """
    page = _HtmlRefs()
    page.feed((frontend / "index.html").read_text(encoding="utf-8"))
    todo = [("index.html", path) for path in page.paths]
    reached: set[str] = set()
    missing: list[str] = []
    while todo:
        referrer, path = todo.pop()
        if path in reached:
            continue
        reached.add(path)
        file = frontend / path
        if not file.is_file():
            missing.append(f"{referrer} -> {path}")
            continue
        if file.suffix not in (".js", ".css"):
            continue
        text = file.read_text(encoding="utf-8", errors="replace")
        names = set(_IN_ASSETS.findall(text)) | set(_SIBLING.findall(text))
        todo.extend((path, f"assets/{name}") for name in names)
    return page, reached, sorted(missing)


@pytest.mark.skipif(not (FRONTEND / "index.html").is_file(), reason="no frontend/: a pip install ships none")
def test_the_bundled_client_names_only_files_it_holds():
    page, reached, missing = walk(FRONTEND)
    assert not missing, (
        "frontend/ names files it does not hold; rebuild and copy the whole dist/, "
        f"index.html included: {missing}"
    )
    # Without an entry script, or if the chunk patterns stopped matching what
    # Vite writes, the walk would check index.html alone and pass on nothing.
    assert page.modules, "index.html loads no module script"
    assert len(reached) > 2 * len(page.paths), f"the walk stopped at index.html's own {len(page.paths)} links"


def _write(root: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text, encoding="utf-8")
    return root


def test_an_entry_script_the_sync_deleted_is_reported(tmp_path):
    # The 985f3c2 shape: the new entry chunk is there, index.html names the old one.
    root = _write(tmp_path, {
        "index.html": '<script type="module" crossorigin src="/assets/index-mKXkiuxp.js"></script>'
                      '<link rel="stylesheet" href="/assets/index-IbnKvKED.css">',
        "assets/index-BeE5V1R2.js": "",
        "assets/index-IbnKvKED.css": "",
    })
    assert walk(root)[2] == ["index.html -> assets/index-mKXkiuxp.js"]


def test_each_way_a_chunk_names_another_is_followed(tmp_path):
    root = _write(tmp_path, {
        "index.html": '<script type="module" src="/assets/entry.js"></script>',
        "assets/entry.js": 'const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["assets/lazy.js"])))=>i.map(i=>d[i]);'
                           'import{a}from"./vendor.js";const logo="/assets/logo.svg";',
        "assets/vendor.js": 'import("./gone.js")',
    })
    assert walk(root)[2] == [
        "assets/entry.js -> assets/lazy.js",
        "assets/entry.js -> assets/logo.svg",
        "assets/vendor.js -> assets/gone.js",
    ]
