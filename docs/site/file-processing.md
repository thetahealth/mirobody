# Uploading health documents

Upload a report on the web client's **Data** page. Mirobody keeps the source
file and extracts the readings it can identify, including printed units,
reference ranges and report dates.

## Supported documents

| Format | Processing |
| --- | --- |
| PDF | Read embedded text; recognize scanned pages with OCR or vision. |
| Images | Recognize text using the configured OCR or vision model. |
| Excel | Read `.xlsx` and `.xlsm` tables. |
| Word and PowerPoint | Read `.docx` and `.pptx` content. |
| Text and structured text | Read supported text formats such as TXT, CSV, JSON and HTML. |

Legacy `.xls`, `.xlsb`, `.doc` and `.ppt` files are not supported. Convert them
to a supported format first. Raw genotype exports follow a separate
[genetics workflow](genetics.md).

## Upload and review

1. Select the person's record and upload the document.
2. Wait for both upload and extraction to finish. A stored file can still have
   failed extraction or contain no usable readings.
3. Review the extracted date, values, units and reference ranges against the
   original. Correct uncertain results before relying on them.
4. Ask a question or open the indicators view. Citations lead back to the source.

Text tables are read locally where possible. Other content uses the configured
models. To keep images local, follow the local or hybrid instructions in
[Choosing a Model](../model-choice.md).

## API integration

Use `POST /files/upload` for REST uploads, or
`/ws/upload-health-report` for progress events. The REST endpoint only stores
the file unless the request also sets `file=true`, which files it into the
record and starts extraction — the same path the Data page uses. WebSocket
clients must wait for `extraction_completed`; `upload_completed` only
confirms file storage. Requests require authentication and permission to
write to the target record. The deployment's `/docs` page lists current
request schemas.

If a file is stored but no readings appear, check the extraction status, model
configuration and `ENABLE_INDICATOR_EXTRACTION` setting. Interface examples,
limits and error formats are in the [file-processing reference](../file-processing.md).
