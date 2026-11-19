# Images

**English** · **[中文](README.zh-CN.md)**

Everything in this directory ships with the repository under
[Apache-2.0](../../LICENSE) and can be used as it is in an article, a talk or a
post.

Every picture of the app was taken on the two synthetic demo accounts in
[`demo/`](../../demo/README.md): the values are generated and no picture shows
a real person's record. The LOINC codes and test names a picture carries come
from LOINC; [`LICENSE-3RD-PARTY`](../../LICENSE-3RD-PARTY) lists the
terminology licences.

A file with no language in its name is the English one; `.zh-CN` is Simplified
Chinese and `.ja` is Japanese. Each table says which languages exist, so a
missing one is a gap, not an oversight in your checkout.

## Screenshots for a post

Still pictures, 1600 × 900 (16:9), of the Simplified Chinese interface. The
five page pictures are the pages of release 1.6.1 as the browser drew them, in
a window 880 to 1056 px wide and cut to 16:9, so the subject stays readable
when a feed shows the picture about 500 px wide. They come from a stack started
with `./deploy.sh`: the first-run page before it had a model, the others with
one model key, after the four 2026 files in `demo/upload/` were uploaded to the
accounts they belong to (`you_annual_checkup_2026-05.pdf` and
`you_lipid_panel_2026-08.csv` as `you`, `mom_physical_2026-06.jpg` and
`mom_clinic_visit_2026-07.xlsx` as `mom`). The three earlier years for `you`
came later and are not in the pictures; with them uploaded too, `you`'s
cholesterol carries more readings than the pictures show.

| File | What it shows | The README line it backs | Languages |
| --- | --- | --- | --- |
| [`ask-own-shot.zh-CN.png`](ask-own-shot.zh-CN.png) | The answer to "我的胆固醇这几年是怎么变化的？" on the Ask page: the chart the agent drew of four lipid tests across three reports, the table under it, and one underlined value opened. The popover shows the reading as the report printed it (`Total Cholesterol-TC`, 4.45 mmol/L), its reference range and the PDF it was read from. | "Three reports, three names for the same test. One trend, with the file behind every number." | 中文 |
| [`upload-shot.zh-CN.png`](upload-shot.zh-CN.png) | Total cholesterol's drawer on the Indicators page after `you`'s two 2026 uploads. Two reports print the test as `Total Cholesterol-TC` and the third as `Cholesterol, Total`; the drawer shows one indicator with one unit, one LOINC code (14647-2), three readings, and the start of their trend. | "② Standardize: … resolve to one code, units to one standard" and "Compare reports across labs." | 中文 |
| [`ask-circle-shot.zh-CN.png`](ask-circle-shot.zh-CN.png) | The Ask page with 帮他问 (Query for) set to Mom's record, which the signed-in demo account may only view: the question, the first lines of the answer, and the chart of her total cholesterol across three reports. | "Look after your family. Invite someone who shares their own record, or keep one for a parent who never signs in." | 中文 |
| [`resolve-shot.zh-CN.png`](resolve-shot.zh-CN.png) | `uvx --python 3.12 mirobody resolve 血红蛋白 ヘモグロビン 血脂` and what it printed, set as a terminal window. Both names get LOINC 718-7 and 血脂 stays unresolved. The output is the command's own (1.6.1), character for character and uncoloured, wrapped the way a 90-column terminal wraps it. The two lines under the window are a caption, not output. | "`血红蛋白` and `ヘモグロビン` both resolve to `718-7`. `血脂` stays unresolved: it names a category, not one test." | 中文 caption; the output is the same in every language |
| [`setup-shot.zh-CN.png`](setup-shot.zh-CN.png) | The first-run page of a deployment that has no model yet: a model service with one API key, or 100% on this machine with llama.cpp and no key, and what each choice means for speed, privacy and requirements. | "Where the app runs and where the model runs are separate choices." | 中文 |
| [`journal-shot.zh-CN.png`](journal-shot.zh-CN.png) | Data › Records just after saving `昨晚开始头疼，血压150/95，没发烧，每天早晚吃二甲双胍500mg`: the blood pressure as two coded readings (8480-6 and 8462-4), the headache as ICPC-3 NS01, metformin on the medication list, and 没发烧 ("no fever") left out. The box is empty again after saving, so the sentence itself is not in the picture: quote it beside the picture. | "Write a health journal. … 'no fever' is not logged as a fever." | 中文 |

## Recordings

Animated GIFs, 880 px wide (the terminal one is 916). The README and the guides
embed them; the screenshots above are stills of the same pages.

| File | What it shows | Where it is shown | Languages |
| --- | --- | --- | --- |
| [`ask-own-demo.gif`](ask-own-demo.gif)<br>[`ask-own-demo.zh-CN.gif`](ask-own-demo.zh-CN.gif)<br>[`ask-own-demo.ja.gif`](ask-own-demo.ja.gif) | Asking how cholesterol has changed: the agent shows each step as it works, charts three reports that name the test differently as one trend, and an underlined number opens the reading and the file it came from. | The top of the README, under "Three reports, three names for the same test. One trend, with the file behind every number."; the walkthrough | English · 中文 · 日本語 |
| [`ask-circle-demo.gif`](ask-circle-demo.gif)<br>[`ask-circle-demo.zh-CN.gif`](ask-circle-demo.zh-CN.gif) | The same question asked about the record Mom shares; the agent answers from a record the asker can only view. | The walkthrough, which is where "Care for your family" in the README sends the reader | English · 中文 |
| [`upload-demo.gif`](upload-demo.gif)<br>[`upload-demo.zh-CN.gif`](upload-demo.zh-CN.gif)<br>[`upload-demo.ja.gif`](upload-demo.ja.gif) | Uploading a lab-report PDF on the Data page; once it is processed, the Indicators list names each test plainly, and one test's drawer shows its specimen, what it measures and its LOINC code. | The walkthrough. Backs "Compare reports across labs." | English · 中文 · 日本語 |
| [`journal-demo.gif`](journal-demo.gif)<br>[`journal-demo.zh-CN.gif`](journal-demo.zh-CN.gif) | One typed sentence becomes a headache coded NS01, blood pressure coded 8480-6 and 8462-4, and metformin on the medication list; "no fever" is not logged. | The walkthrough. Backs "Write a health journal." | English · 中文 |
| [`resolve-demo.gif`](resolve-demo.gif)<br>[`resolve-demo.zh-CN.gif`](resolve-demo.zh-CN.gif)<br>[`resolve-demo.ja.gif`](resolve-demo.ja.gif) | `mirobody resolve` in a terminal: Chinese and Japanese names for haemoglobin resolve to the same LOINC code, and a broad category stays unresolved. | "Try the engine with one command" in the README; the quickstart | English · 中文 · 日本語 |
| [`setup-demo.gif`](setup-demo.gif)<br>[`setup-demo.zh-CN.gif`](setup-demo.zh-CN.gif)<br>[`setup-demo.ja.gif`](setup-demo.ja.gif) | What `./deploy.sh` prints, then the first-run page: a model name edited beside an OpenRouter key, then 100% on this machine, where the page finds the llama.cpp server and both models are ready. | "Run everything 100% locally" in the README | English · 中文 · 日本語 |

## Diagrams

SVG, drawn for a light and a dark page. The care-circle diagram also has a
portrait layout for a phone, and PNG copies (1840 × 1272) for places that take
no SVG.

| File | What it shows | Where it is shown | Languages |
| --- | --- | --- | --- |
| [`collect-standardize-agent.svg`](collect-standardize-agent.svg)<br>[`collect-standardize-agent.zh-CN.svg`](collect-standardize-agent.zh-CN.svg)<br>[`collect-standardize-agent.ja.svg`](collect-standardize-agent.ja.svg)<br>dark: [`collect-standardize-agent-dark.svg`](collect-standardize-agent-dark.svg)<br>[`collect-standardize-agent-dark.zh-CN.svg`](collect-standardize-agent-dark.zh-CN.svg)<br>[`collect-standardize-agent-dark.ja.svg`](collect-standardize-agent-dark.ja.svg) | The three stages: collect reports, device data and genetics; standardize names and units so sources compare; ask across the history and trace each answer to its source. | "Collect · Standardize · Agent" in the README; the standardization and pipeline guides | English · 中文 · 日本語 |
| [`care-circle-sharing.svg`](care-circle-sharing.svg)<br>[`care-circle-sharing.zh-CN.svg`](care-circle-sharing.zh-CN.svg)<br>[`care-circle-sharing.ja.svg`](care-circle-sharing.ja.svg)<br>dark: [`care-circle-sharing-dark.svg`](care-circle-sharing-dark.svg)<br>[`care-circle-sharing-dark.zh-CN.svg`](care-circle-sharing-dark.zh-CN.svg)<br>[`care-circle-sharing-dark.ja.svg`](care-circle-sharing-dark.ja.svg) | Invite a family member; they choose whether you may view or edit their own record; ask about the record they share. A family member who does not sign in can be managed by you until they take over and choose your access. | "Care for your family" in the README, with "Joining a care circle shares nothing by itself."; the walkthrough | English · 中文 · 日本語 |
| [`care-circle-sharing-mobile.svg`](care-circle-sharing-mobile.svg)<br>[`care-circle-sharing-mobile.zh-CN.svg`](care-circle-sharing-mobile.zh-CN.svg)<br>[`care-circle-sharing-mobile.ja.svg`](care-circle-sharing-mobile.ja.svg)<br>dark: [`care-circle-sharing-mobile-dark.svg`](care-circle-sharing-mobile-dark.svg)<br>[`care-circle-sharing-mobile-dark.zh-CN.svg`](care-circle-sharing-mobile-dark.zh-CN.svg)<br>[`care-circle-sharing-mobile-dark.ja.svg`](care-circle-sharing-mobile-dark.ja.svg) | The same care-circle diagram in a portrait layout. | The same places, below 640 px of width | English · 中文 · 日本語 |
| [`care-circle-sharing.png`](care-circle-sharing.png)<br>[`care-circle-sharing.zh-CN.png`](care-circle-sharing.zh-CN.png)<br>dark: [`care-circle-sharing-dark.png`](care-circle-sharing-dark.png)<br>[`care-circle-sharing-dark.zh-CN.png`](care-circle-sharing-dark.zh-CN.png) | The wide care-circle diagram as a PNG. | No page in this repository embeds it | English · 中文 |

## Logo and social card

| File | What it shows | Where it is shown | Languages |
| --- | --- | --- | --- |
| [`mirobody-icon.svg`](mirobody-icon.svg)<br>dark: [`mirobody-icon-dark.svg`](mirobody-icon-dark.svg) | The Mirobody mark, for a light and for a dark page. | The top of every README | no text |
| [`social-preview.png`](social-preview.png) | The card a link to the repository unfolds into (2560 × 1280): the pitch, the two ways to run it, a frame of the Ask page and four spellings of haemoglobin resolving to 718-7. It also prints a benchmark score, which is the README's figure on the day the card was rendered. | The repository's social preview | English |
| [`social-preview.html`](social-preview.html)<br>[`social-preview-frame.png`](social-preview-frame.png) | The page the card is rendered from, and the frame of `ask-own-demo.gif` it embeds. | Source only | English |
