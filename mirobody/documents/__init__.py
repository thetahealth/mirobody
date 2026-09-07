"""Documents → text, once.

A health record arrives as a file: a lab report PDF (born-digital or scanned),
a photo of a printout, a spreadsheet, a Word or PowerPoint export, a text
dump. Before anything downstream can read it — the indicator extractor, the
agent's virtual filesystem, a search index — it has to become text, and every
consumer used to do that itself with its own detection table, its own PDF
library and its own idea of a table.

`detect` answers "what is this file" from extension, content type and, when
those lie (a multipart part with a generic type and no name), the bytes.
`extract` turns it into text: the PDF text layer page by page, with only the
scanned pages rendered and handed to an injected OCR callable; images
downscaled and OCR'd; spreadsheets as markdown tables under a row budget; Word
and PowerPoint as markdown; text decoded through the encodings a report is
actually saved in. One PDF library (pypdfium2) for both text and rendering;
Pillow for images. The model that reads a page is the caller's (`Ocr`), and so
is the cache that stops the same bytes being OCR'd twice (`TextCache`).
"""
