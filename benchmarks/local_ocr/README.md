# Local document OCR: GLM-OCR, PaddleOCR-VL and MinerU on synthetic reports

In 100%-local mode Mirobody reads report photos and PDF pages with a
document-OCR model on llama.cpp's `llama-server`: one request per task prompt
of the `local-ocr` entry (`config.llm.yaml`), a text pass and a tables pass.
The tables are read by their headers with no model
(`collect/files/services/table_indicators.py`); what the rules leave goes to
the text model, MiniCPM5-2B by default. The shipped OCR model is GLM-OCR-0.9B.
The question here: does another small, permissively licensed OCR model that
upstream llama.cpp serves do better, PaddleOCR-VL in particular, and does
MinerU read handwriting better?

## The answer

**Keep GLM-OCR-0.9B at Q8_0.** On the deciding run (Mirobody fcfbf78, the
product's whole extraction path with MiniCPM5-2B, every number against
printed truth):

- **Printed reports, 303 rows on 26 pages: GLM-OCR stores 283 with the right
  value (93.4%), MinerU2.5-Pro 279 (92.1%), PaddleOCR-VL-1.6 278 (91.7%);**
  with the generator's "SYNTHETIC SAMPLE" banner removed, as on a real
  report, 302 (99.7%), 299 and 282 ([The banner](#the-banner)).
  GLM-OCR stores no row the page does not print; MinerU 3; PaddleOCR-VL 12
  (range bounds stored as values on an English screenshot, invented ranges on
  an outpatient record's vitals). All three get 55 of the two home logs' 57
  rows. A repeat with another sampling seed moved a model's total by one row.
- **Handwriting, 301 rows on 28 pages: GLM-OCR stores 99, PaddleOCR-VL 62,
  MinerU 55; without the banner 219, 136 and 171.** MinerU does not read
  handwriting better here: its OCR text holds 283 of the 301 printed values,
  against 289 for GLM-OCR and 286 for PaddleOCR-VL, ahead only on the English
  pages and by one value (124 to 123); end to end it stores fewer rows than
  GLM-OCR in every tier, language and kind of page, with the banner or
  without. All three read about 95% of the handwritten values; what
  loses the rows is the text model (below).
- **PaddleOCR-VL reads the most and is the hardest to run.** 99% of the
  printed rows are in its OCR output (85% for the other two), because its
  tables pass returns the whole page as one grid; with the table rules
  rebuilt for that grid (e2044e9) the rules alone store 276 of its rows. But
  it writes LaTeX and OTSL that the product now has to clean, its passes
  looped to the token cap on 7 of 28 handwritten pages (about 61 s each, so
  23 s per handwritten page against 10–14), and its extra rows came with
  false ones.
- **GLM-OCR is the fastest OCR** (12.3 s per printed page, against 13.1 and
  14.8) and needs no change. It holds the most memory of the three (1.84 GiB
  peak footprint, against 1.31 and 1.24), well within an 8 GB machine beside
  MiniCPM5-2B.

The quant stays Q8_0, the one quantized build ggml-org publishes (the other is
f16). Switching would cost code and buy no rows; if the answer changes, the
sections a switch needs are in [If the default changes](#if-the-default-changes).

## Results: 28 printed pages

Measured 2026-10-06/07 on an Apple M1 Pro, 16 GB, macOS 26.6.2, beside a
colima VM running the Mirobody stack; llama.cpp 0.6.0 (build 11429, commit
d81235049). Each OCR model ran alone in its own server, with no other model
server up. The OCR outputs were made once; the four commits are the
`harness/small-model` code that read them, each later one carrying fixes this
benchmark found ([Product issues](#product-issues-found)). fcfbf78 is the
deciding one. Rows are of the 26 pages that are not home logs; the two logs
are a line of their own.

| | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro-2605 1.2B |
|---|---|---|---|
| License | MIT | Apache-2.0 | Apache-2.0 |
| Download (model + projector) | 1.43 GB | 1.82 GB | 1.24 GB |
| OCR seconds per page | 12.3 | 13.1 | 14.8 |
| Peak footprint / max RSS (GiB) | 1.84 / 2.37 | 1.31 / 2.16 | 1.24 / 1.65 |
| (a) printed values in the OCR text (scans, photos, screens) | 192 (96.0%) | 192 (96.0%) | 192 (96.0%) |
| (a) OCR rows: name and value | 256 (84.5%) | 300 (99.0%) | 258 (85.1%) |
| (a) OCR rows: all four fields | 250 (82.5%) | 266 (87.8%) | 253 (83.5%) |
| (a) rows the table rules store, value right, 6b5933b | 51 (16.8%), 0 false | 90 (29.7%), 0 false | 51 (16.8%), 0 false |
| (a) the same, 490a0e1 | 162 (53.5%), 0 false | 149 (49.2%), 0 false | 139 (45.9%), 0 false |
| (a) the same, e2044e9 and fcfbf78 | 204 (67.3%), 0 false | 276 (91.1%), 0 false | 196 (64.7%), 0 false |
| (b) end to end, value right as stored, 6b5933b | 265 (87.5%) | 264 (87.1%) | 261 (86.1%) |
| (b) the same, 490a0e1 | 281 (92.7%) | 272 (89.8%) | 281 (92.7%) |
| (b) the same, e2044e9 | 266 (87.8%) | 277 (91.4%) | 251 (82.8%) |
| **(b) the same, fcfbf78** | **283 (93.4%)** | **278 (91.7%)** | **279 (92.1%)** |
| (b) unit right / range right, fcfbf78 | 271 / 277 | 263 / 267 | 272 / 271 |
| (b) false rows / rows stored twice, fcfbf78 | 0 / 0 | 12 / 2 | 3 / 0 |
| (b) home logs, fcfbf78 | 55 of 57 | 55 of 57 | 55 of 57 |
| (b) text model seconds per page, fcfbf78 | 8.1 | 7.7 | 10.8 |

Per tier at fcfbf78: (a) OCR rows with name and value / (b) end to end.

| Model | T0 (103 rows) | T2 (135 rows) | T3 (21 rows) | T4 (29 rows) | T6 (15 rows) |
|---|---:|---:|---:|---:|---:|
| GLM-OCR 0.9B | 56 / 103 | 135 / 115 | 21 / 21 | 29 / 29 | 15 / 15 |
| PaddleOCR-VL-1.6 0.9B | 101 / 102 | 134 / 111 | 21 / 21 | 29 / 29 | 15 / 15 |
| MinerU2.5-Pro-2605 1.2B | 59 / 103 | 134 / 111 | 21 / 21 | 29 / 29 | 15 / 15 |

T0 (a) is the tables pass only: a text-layer page gets no text pass, its text
is exact. At fcfbf78 the rows every model loses are one page: a check-up scan
with this result and the previous one side by side (`p006_2023-09-11_e04a`
p8, 20 rows), which MiniCPM5-2B calls `non_health_related` and answers with
nothing, whatever OCR text it gets. Beyond it: GLM-OCR loses 2 rows of the
blood-pressure log; MinerU those 2, 2 of a multi-panel scan and 2 of the
outpatient record; PaddleOCR-VL those 2, 4 of the outpatient record and one
of a Traditional Chinese slip.

The commits, in order: 490a0e1 split a printed flag out of the model's value
and fixed doubled-dash ranges and the header vocabulary; e2044e9 cleaned every
OCR answer in the product (OTSL, LaTeX, loops), read whole-page grids and
stopped handing the text model rows the rules had read, which cost GLM-OCR
15 rows and MinerU 30, nearly all on two check-up scans where MiniCPM5-2B
judged the leftover non-medical; fcfbf78 tells the model the leftover belongs
to a medical report.

## Results: 28 handwritten pages

mirobody-gen b4a8c50 (`feat/handwriting`), `build --seed 42 --people 18
--render --handwriting`, `files.jsonl` sha256
`14bc3ed88179ba6d0b473c815609e5f2d6d704188ea2519b1ca9cd472b738057`; 28 pages,
301 rows, three tiers of hand (H1 neat, H2 running, H3 hard cursive), scanned
(9) or photographed (19). Every page counted, the 12 notebook logs included.
OCR runs at the same settings; end to end at fcfbf78.

Each cell: (a) printed values found in the OCR text · (b) rows stored with the
value right.

| Pages | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro-2605 1.2B |
|---|---:|---:|---:|
| All pages (28) | 289/301 · 99/301 | 286/301 · 62/301 | 283/301 · 55/301 |
| H1 (7) | 69/70 · 48/70 | 69/70 · 17/70 | 68/70 · 18/70 |
| H2 (12) | 167/171 · 32/171 | 166/171 · 37/171 | 164/171 · 30/171 |
| H3 (9) | 53/60 · 19/60 | 51/60 · 8/60 | 51/60 · 7/60 |
| English (13) | 123/128 · 79/128 | 121/128 · 37/128 | 124/128 · 36/128 |
| Chinese (15) | 166/173 · 20/173 | 165/173 · 25/173 | 159/173 · 19/173 |
| notebook log (12) | 197/197 · 52/197 | 196/197 · 15/197 | 197/197 · 17/197 |
| hand-filled form (4) | 32/32 · 31/32 | 32/32 · 32/32 | 30/32 · 30/32 |
| doctor's note (12) | 60/72 · 16/72 | 58/72 · 15/72 | 56/72 · 8/72 |

| | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro-2605 1.2B |
|---|---:|---:|---:|
| (a) rows with name and value on one line or table row | 94 | 94 | 83 |
| (b) false rows / rows stored twice | 57 / 0 | 22 / 0 | 35 / 0 |
| (b) unit right / range right | 98 / 99 | 62 / 53 | 57 / 53 |
| OCR passes that ran to the 8,192-token cap | 0 | 7 | 0 |
| OCR seconds per page | 9.6 (17 pages) | 23.2 | 14.4 |
| Peak footprint / max RSS (GiB) | 1.80 / 2.45 | 1.36 / 2.22 | 1.23 / 1.71 |
| (b) pages the text model answered with nothing | 10 | 18 | 18 |

GLM-OCR's first 11 handwritten pages ran while another agent's router loaded
GLM-OCR and MiniCPM5-2B on the same GPU; its seconds per page are of the other
17. The OCR models read handwriting nearly as well as print (95–96% of the
values); end to end the losses are MiniCPM5-2B's: it answered
`non_health_related` with no rows on 10 (GLM-OCR's text), 13 (PaddleOCR-VL's)
and 18 (MinerU's) of the 28 pages, and on the blood-pressure logs it put
values under the wrong column's name and stored a struck-out value beside its
correction (the false rows). These pages also carry the generator's banner
([below](#the-banner)). PaddleOCR-VL lost 5 more logs another way: its tables
pass looped on empty rows (`<ecel><ecel><nl>`, thousands of times) to the
token cap, the product's loop guard keeps markup-only repeats by design, and
the page's text, 72–73k characters once the grid is HTML, was a request of
34,811 tokens for MiniCPM5-2B's 32,768-token context: refused, nothing stored.

## What is measured

**The printed pages.** 28 pages of a synthetic corpus with row-level truth:
[mirobody-gen](https://github.com/thetahealth/mirobody-gen) at
248df0f2aead8051c81ef1f7a9fa431e627e8791, `build --seed 7 --people 6
--render`, `files.jsonl` sha256
`5bad865b1e5cba4979d453338f79b2d5bf342e9c19b8cd8061901ae1dab09b11`.
`sample.json` lists each page, its file's sha256 and why it was picked
(`handwriting_sample.json` in the handwriting build does the same there).

| Tier | Delivery | Pages |
| --- | --- | ---: |
| T0 | text-layer PDF page | 6 |
| T2 | scan (flatbed, grayscale, low resolution, phone-app "scan") | 10 |
| T3 | phone photo (flat, bent, oblique, WeChat-compressed) | 4 |
| T4 | photocopy, thermal fax, low-quality print, aged copy | 4 |
| T6 | screenshot, photo of a screen | 4 |

They include a multi-panel check-up lab page as a scan and as a text-layer
PDF, the narrative ECG and ultrasound page of a check-up (scan and text
layer), a home blood-pressure log and a home weight log, layouts with a
current and a previous result side by side, English and Traditional Chinese
reports, an outpatient record with vitals in running text, and slips with
units glued to values or to ranges. The corpus prints a two-panel (双栏)
table only as CSV, which no OCR reads, so no side-by-side panel is measured.

`files.jsonl` says which rows a document prints but not on which page, and a
scanned check-up book is 5–13 pages. `clean_pdfs.py` rebuilds the corpus from
the seed and keeps each document's clean text-layer PDF, from which every
scan, photo and fax is rasterised, so page *k* of a scan is page *k* of its
clean PDF; a row belongs to the page whose text layer prints its name and
value (a row a summary page repeats goes to its table's page). The rebuilt
corpus was byte-identical to an independent build of the same seed (all 70
files and `files.jsonl`). The handwriting build ships the same `clean/`.

**The path.** `run.py` hands each page to the model the way the product does
(`documents.extract`, `utils/llm/file_processors/media`): a PDF page rendered
at 150 dpi, a photo downscaled only past 6 MB, then fitted to a 1536 px JPEG;
image first, prompt after it. A text-layer PDF page gets only the tables pass,
appended to its text layer; a scan or photo gets every pass, joined as
`documents.ocr.vision_ocr` joins them. Each OCR model runs alone in its own
`llama-server -np 1 -c 16384 --cache-ram 0` on 127.0.0.1 with its own
official task prompts, and a pass stops at 8,192 tokens (the product's cap
from e2044e9). Each raw answer is then cleaned the way the measured commit
cleans it: `documents.ocr.clean_answer` from e2044e9 on, `convert.py` (the
same OTSL rule, written here) before.

**Three numbers per model**, every one against the printed truth:

- **(a) OCR**: the OCR model's own quality, read with no rule. *Values*: the
  printed values its text holds, each as often as the page prints it, as a
  whole token ("119/70", not the 70 of a date); scans, photos and screens
  only, a text-layer page's text being exact. *Rows*: printed rows it
  reproduces as a table row or a line holding the row's name and value;
  "all four fields" also holds its unit and range.
- **(a) rule rows**: what `table_indicators` stores from that output with no
  model (`rules:table@v1`).
- **(b) end to end**: what an upload would store. The page text goes through
  `IndicatorExtractor.extract_indicators_from_text(..., save_to_db=False)`:
  the table rules, MiniCPM5-2B for what they leave, the merge, the
  de-duplication; then the store's own value handling
  (`indicator_store.value_and_flag` where the commit has it). `extract.py`.
  MiniCPM5-2B runs as the preset runs it (`-c 32768 -np 2 -kvu --cache-ram
  1024`), plus `--seed 7`, so every OCR model's text is read by the same
  sampler. **(b) decides which OCR model ships.**

Rows are aligned to the truth with mirobody-gen's own aligner
(`mirobody_gen.harness.score.align`: the best one-to-one match on name
similarity and value). A field is compared as the product stores it: a value
through `translate.parse_value` ("42.4 %" is 42.4, right; "6.49↑" before
490a0e1 was a narrative with no number, wrong); a unit through
`normalize_unit`; a range as printed less spacing, brackets and the dash or
tilde between its ends. A stored row that matches no printed row is **stored
twice** when it holds the value of a printed row already stored under a name
alike; **printed elsewhere** when the page prints its name and value, or the
finding's text, outside the truth's rows (a check-up's "DOB值 3.1‰", an
ultrasound line under a composed label), counted apart; else **false**. A
distractor the generator planted ("异常项目数 0", a patient field) is false.

The Mirobody code was copied from the `harness/small-model` worktree at each
commit (`MIROBODY_SRC`, with a `.commit` file), so a branch that moved during
a run could not change what was measured.

### The banner

mirobody-gen prints "SYNTHETIC SAMPLE — GENERATED DATA, NOT A REAL PATIENT
RECORD" on every page; it is in 33 of the 84 printed and 68 of the 84
handwritten OCR answers. MiniCPM5-2B reads it literally: given what the table
rules left of an English check-up page (MinerU's text, e2044e9) it answered
`non_health_related` with no rows at seeds 7, 8 and 9, and with the line
removed `medical_report` with all 26. fcfbf78's prompt says a printed notice
does not make a document non-health and labels the rules' leftover as part of
a medical report; with the banner in place the same leftover then gave all 26
rows, and GLM-OCR's 6 of 6 (7 at seed 8). The page every model still loses
(`p006_2023-09-11_e04a` p8) has no table rows read, so no leftover note, and
the banner at its top. `extract.py --strip-banner` hands the product the page
without that line, as a real report would be:

| fcfbf78, (b) value right as stored | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro-2605 1.2B |
|---|---:|---:|---:|
| printed, 303 rows, banner in place | 283 (93.4%) | 278 (91.7%) | 279 (92.1%) |
| printed, banner stripped | **302 (99.7%)** | 282 (93.1%) | 299 (98.7%) |
| printed false rows, banner stripped | 0 | 8 | 4 |
| printed home logs (57 rows), banner stripped | 55 | 57 | 55 |
| handwritten, 301 rows, banner in place | 99 (32.9%) | 62 (20.6%) | 55 (18.3%) |
| handwritten, banner stripped | **219 (72.8%)** | 136 (45.2%) | 171 (56.8%) |
| handwritten H1 / H2 / H3, banner stripped (70 / 171 / 60 rows) | 51 / 127 / 41 | 38 / 65 / 33 | 35 / 107 / 29 |
| handwritten English / Chinese, banner stripped (128 / 173 rows) | 90 / 129 | 66 / 70 | 68 / 103 |
| handwritten logs / forms / notes, banner stripped (197 / 32 / 72 rows) | 148 / 31 / 40 | 69 / 31 / 36 | 119 / 30 / 22 |
| handwritten false rows, banner stripped | 61 | 73 | 70 |
| handwritten pages answered with nothing, banner stripped | 1 | 6 | 4 |

The banner is worth 19 printed rows to GLM-OCR and 20 to MinerU, nearly all
of them the 20-row page every model lost with it (from PaddleOCR-VL's text
that page stays empty without it too), and 120 handwritten rows to GLM-OCR.
GLM-OCR leads with and without it; on handwriting MinerU and PaddleOCR-VL
change places.

## Candidates

| Model | Measured | License | llama.cpp | Files (model + projector) |
|---|---|---|---|---|
| GLM-OCR 0.9B (`ggml-org/GLM-OCR-GGUF`, Q8_0) | yes, the baseline | MIT | upstream since PR #19677 | 1.43 GB |
| PaddleOCR-VL-1.6 0.9B (`PaddlePaddle/PaddleOCR-VL-1.6-GGUF`) | yes, asked for by name | Apache-2.0 | upstream since PR #18825 | 1.82 GB |
| MinerU2.5-Pro-2605 1.2B (`jinzhenj/MinerU2.5-Pro-2605-1.2B-GGUF`, Q8_0) | yes | Apache-2.0 (the earlier 2509 is AGPL-3.0) | Qwen2-VL architecture, upstream; needs `--special` | 1.24 GB |
| LightOnOCR-2-1B (`ggml-org/LightOnOCR-2-1B-GGUF`, Q8_0) | not measured: the owner limited the comparison to small OCR models | Apache-2.0 | upstream | 1.09 GB |
| Qwen3-VL-2B-Instruct (`Qwen/Qwen3-VL-2B-Instruct-GGUF`, Q4_K_M) | not measured: the owner limited the comparison to small OCR models | Apache-2.0 | upstream | 1.55 GB |
| dots.ocr 1.7B (`ggml-org/dots.ocr-GGUF`, Q8_0) | not measured: the owner limited the comparison to small OCR models | MIT | upstream since PR #17575 | 3.24 GB |
| DeepSeek-OCR 3B MoE (`ggml-org/DeepSeek-OCR-GGUF`, Q8_0) | not measured: the owner limited the comparison to small OCR models | MIT | upstream since PR #17400 (its author recommends the f16 language model: quantized builds loop) | 3.57 GB |

Not considered: PaddleOCR-VL-1.5 (1.6 is its drop-in successor, same
architecture); HunyuanOCR (Tencent Hunyuan community license, which excludes
regions), jina-ocr (CC-BY-NC), surya-ocr-2 (OpenRAIL with revenue limits):
not permissive; Unlimited-OCR, DeepSeek-OCR-2, NaviDC-OCR: each needs an
unmerged llama.cpp pull request or a patched build; olmOCR-2 and Chandra: 7B
and larger. MinerU2.5-Pro has no official GGUF; `jinzhenj`'s conversion is the
most downloaded, and its projector is byte-identical to `mradermacher`'s.

### Prompts and settings

| | Text pass | Tables pass | Sampling | Server | Tables come back as |
|---|---|---|---|---|---|
| GLM-OCR | `Text Recognition:` | `Table Recognition:` | temperature 0 | — | HTML |
| PaddleOCR-VL-1.6 | `OCR:` | `Table Recognition:` | temperature 0 | `--chat-template-file chat_template.jinja` (the repo's; the GGUF carries one too) | OTSL |
| MinerU2.5-Pro | `\nText Recognition:` | `\nTable Recognition:` | temperature 0, presence penalty 1.0, frequency penalty 0.005 (mineru-vl-utils' table settings) | `--special` | OTSL |

All prompts are the models' own: GLM-OCR's model card, PaddleOCR-VL's model
card, mineru-vl-utils' `DEFAULT_PROMPTS`. OTSL is a token grid (`<fcel>` a
cell, `<ecel>` an empty one, `<lcel>`/`<ucel>`/`<xcel>` spans, `<nl>` end of
row). All three are trained on layout-cropped regions in their own pipelines
(PP-DocLayout for GLM-OCR and PaddleOCR-VL, MinerU's own layout pass);
Mirobody sends whole pages, so that is what is measured.

### Files

| Model | File | Bytes | sha256 |
|---|---|---:|---|
| GLM-OCR | GLM-OCR-Q8_0.gguf | 950,433,408 | `45bc244a6446aff850521dc41f18bc8d7105ad5f0c2c8c28af04e7cc4f4d50b1` |
| | mmproj-GLM-OCR-Q8_0.gguf | 484,403,648 | `9c4b58e33e316ed142eb5dcb41abec3844d3e6e5dc361ffb782c3fa9d175141f` |
| PaddleOCR-VL-1.6 | PaddleOCR-VL-1.6-GGUF.gguf | 935,769,056 | `f3ae46ec885050acf4b3d31944431e1fd90d50664fb09126af4a3c050ba14ee8` |
| | PaddleOCR-VL-1.6-GGUF-mmproj.gguf | 881,770,560 | `204d757d7610d9b3faab10d506d69e5b244e32bf765e2bab2d0167e65e0a058a` |
| | chat_template.jinja | 1,831 | `1369e43952c7a84e834be95732d65111e96cbefe30aa32a4826eca344f5ef939` |
| MinerU2.5-Pro | MinerU2.5-Pro-2605-1.2B-Q8_0.gguf | 531,066,496 | `3f3523429c880d675c0330cb3de78240452bc40a961d037e24f91ab8488f03e7` |
| | mmproj-MinerU2.5-Pro-2605-1.2B-Q8_0.gguf | 709,409,600 | `b6c08ab50352677f7396af45ee60682f353f55488e26911d8154bfc37514d3f8` |
| text model | MiniCPM5-2B-Q4_K_M.gguf (`openbmb/MiniCPM5-2B-GGUF`) | 1,561,318,368 | `ec2d5801640099e97d8d7e8003ad4d81f336e757811f03a26173dddf386602fd` |

Every GGUF matches the sha256 huggingface.co publishes for it
(`/api/models/<repo>/tree/main`, `lfs.oid`). Hugging Face itself ran at about
100 KB/s on this network, so PaddleOCR-VL and MinerU were fetched anonymously
from ModelScope's mirror of the same repositories and from hf-mirror.com, in
parallel ranges, and checked against those hashes before use; the chat
template (not an LFS file) came from huggingface.co.

## What each model does with a whole page

- **GLM-OCR and MinerU's tables pass returns one table: the first.** On all
  28 printed pages the tables pass held exactly one table; on the
  multi-panel text-layer page (`p001_2025-11-26_e06a` p6, six panels, 34
  rows) that is 7 rows from both. The other panels reach the text model
  through the text pass.
- **PaddleOCR-VL returns the whole page as one grid** (44 rows there:
  section titles as spanning rows, every panel's header repeated). That is
  its whole (a) lead, and what e2044e9's rules were rebuilt to read.
- **PaddleOCR-VL writes LaTeX**: `\(\times 10^{9}/L\)`, `\(\mu mol/L\)`,
  `\(\gamma\)-谷氨酰基转移酶`, `6.49\(\uparrow\)`. Before the product cleaned
  it (e2044e9), 68 of its stored rows had a wrong unit against 40 for GLM-OCR
  and 14 for MinerU, and a row with a LaTeX name matched no printed row.
- **PaddleOCR-VL loops.** On the scanned narrative ECG/ultrasound page its
  `OCR:` pass wrote "未见异常" line after line to the 8,192-token cap (70 s);
  on 7 of the 28 handwritten pages its tables pass ran to the cap (about 61 s
  each). The product's loop guard (e2044e9) cuts the repeats from the text;
  the time is spent anyway.
- **MinerU's OTSL cell tokens are special tokens.** Without `--special`,
  `llama-server` drops them and the tables pass comes back as the cells run
  together (`1总胆固醇(TC)5.18<5.18&mmol/L`); with it the end-of-turn token
  is printed too (`<|im_end|>`), which the product does not strip (cut here).
  Its text pass separates columns with tabs.

## Product issues found

Found on these pages, reported, and fixed on `harness/small-model` by another
agent, then measured here:

| Fixed in | What was wrong | Measured effect |
|---|---|---|
| d6c9880 | `translate.parse_range("35.0--45.0")` was (-45.0, 35.0), stored as `ref_low`/`ref_high` for every doubled-dash range | ranges right |
| d6c9880 | a flag left after the model's number ("6.49↑") was stored as a narrative with no number | all 16 of GLM-OCR's (b) value errors at 6b5933b |
| 5c1b36f | the table rules knew few headers (检测结果, 化验结果, 数值, Test Item, REF.RANGE, Traditional 檢驗項目 …) and took "Measurement" for the name column | rule rows 51 → 162 (GLM-OCR), none wrong |
| e2044e9 | no OCR answer was cleaned: OTSL unread (0 of PaddleOCR-VL's rows), LaTeX units, loops kept, no token cap (a loop ran to the 16k context) | PaddleOCR-VL rule rows 0 → 276 through the product path |
| 48a50aa | extraction asked for up to 32,000 tokens: MiniCPM5-2B looped 13 minutes on one handwritten log (30,067 tokens) and stored nothing | budget now bounded by the text |
| fcfbf78 | the text model, handed only the rules' leftover, called it non-health (with the banner) and dropped it | GLM-OCR 266 → 283, MinerU 251 → 279 |

Still open:

- **MiniCPM5-2B drops whole pages as `non_health_related`**: the 20-row
  printed page above for every OCR model, and 10–18 of the 28 handwritten
  pages. The banner contributes ([above](#the-banner)); where the rules read
  nothing there is no leftover note to counter it.
- **MiniCPM5-2B misfiles values on handwritten logs**: blood pressures under
  "Heart rate", a struck-out reading stored beside its correction.
- **A looped empty grid overflows the text model.** `documents.ocr._cut_loops`
  keeps a repeated unit with no word in it (an empty table row) as layout; on
  5 handwritten logs PaddleOCR-VL's tables pass was thousands of empty rows,
  and the extraction request (34,811 tokens) exceeded MiniCPM5-2B's context
  and failed. Nothing bounds the text handed to the model.
- **A whole-page tables pass sees one panel** with GLM-OCR and MinerU; the
  rest reaches the text model as the text pass.

## If the default changes

**PaddleOCR-VL-1.6** already has a preset section (e2044e9) and its answers
are cleaned in the product; `config.llm.yaml`'s `local-ocr` entry would need:

```yaml
    model: paddleocr-vl
    ocr_prompts:
      text: "OCR:"
      tables: "Table Recognition:"
```

**MinerU2.5-Pro.** Preset section (`docker/local-models.ini`):

```ini
[mineru2.5-pro]
hf-repo = jinzhenj/MinerU2.5-Pro-2605-1.2B-GGUF:Q8_0
c = 16384
cache-ram = 0
; Its table cells are special tokens: without this the tables pass has no separators.
special = true
```

```yaml
    model: mineru2.5-pro
    ocr_prompts:
      text: "\nText Recognition:"
      tables: "\nTable Recognition:"
    extra_body:
      temperature: 0
      presence_penalty: 1.0
      frequency_penalty: 0.005
```

and in code, `<|im_end|>` stripped from each answer (`documents.ocr` has no
such step). Measured from local files; neither preset's `hf-repo` download was
run here.

## Reproduce

```bash
# 1. the corpus, with clean PDFs (the generator's own Python: it needs PyMuPDF)
PYTHONPATH=<mirobody-gen> <its python> benchmarks/local_ocr/clean_pdfs.py --seed 7 --people 6 --out <corpus>
#    the handwriting build: mirobody-gen feat/handwriting, build --seed 42 --people 18 --render --handwriting

# 2. the GGUFs of the table above under <models>/<org>__<repo>/ (or in the
#    Hugging Face cache), each checked against its sha256

# 3. each model: OCR, then end to end (MiniCPM5-2B from the Hugging Face cache)
export MIROBODY_SRC=<a mirobody checkout at the commit to measure> PYTHONPATH=<mirobody-gen>
python benchmarks/local_ocr/run.py --model glm-ocr-q8 --corpus <corpus> --models-dir <models>
python benchmarks/local_ocr/extract.py --model glm-ocr-q8 --corpus <corpus>
python benchmarks/local_ocr/report.py --commits 6b5933b 490a0e1 e2044e9 fcfbf78

# handwriting: the same with --sample <hw corpus>/handwriting_sample.json --set handwriting
python benchmarks/local_ocr/report.py --set handwriting --commits fcfbf78
```

Model ids: `glm-ocr-q8`, `paddleocr-vl-1.6`, `mineru2.5-pro-q8` (`models.json`).
`run.py --rescore` and `extract.py --rescore` score the stored outputs again
without a model (the raw OCR answers are in `results/<model>/pages/` and
`results/<model>/handwriting/pages/`); `--min-free-gb` and `--min-free-mem`
stop a run before it starves a shared machine, and a rerun resumes at the
first page with no stored output. `results/run.json` records each corpus,
every file's hash, the command, the llama.cpp build and the machine.

## Limits

- **Small and synthetic.** 28 printed pages (303 rows and 57 log rows) and 28
  handwritten (301 rows), one seed each. A repeat at another sampling seed
  moved a model's total by one row, and pages move whole: one page is 20
  rows for every model. A few rows apart is not a ranking.
- **Single pages, not documents.** A page is scored alone; in an upload a
  continuation page without a header borrows the previous page's.
- **One text model.** (b) is MiniCPM5-2B, the default; a larger answering
  model would read more of what the rules leave, and would drop fewer
  handwritten pages.
- **The banner** steers MiniCPM5-2B ([above](#the-banner)); real reports do
  not carry it.
- **Speed on one machine.** Seconds per page are one M1 Pro's, with a colima
  VM beside; GLM-OCR's handwriting figure is of 17 pages (see above).
- **Whole pages, not the models' own pipelines.** All three are built to read
  regions a layout model cuts out; with that step each might do better. The
  product has no such step.
