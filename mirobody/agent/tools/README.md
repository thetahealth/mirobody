# 🛠️ Developing Mirobody Tools

Mirobody follows a **"Tools First"** philosophy. You write standard Python code, and the system automatically converts it into MCP (Model Context Protocol) tools for AI agents.

## 🔍 Tool Discovery

Tool directories are configured by `MCP_TOOL_DIRS` in `config.{env}.yaml`. The defaults:

1. **Built-in tools**: `mirobody/agent/tools/` (this directory) — the whole shipped tool surface: terminology (② Translate), health records, medications, genetics and pharmacogenomics.

**Place your own tools in your own directory and add it to `MCP_TOOL_DIRS`** — the list is ordinary config, so a deployment can extend it without touching the package:

```yaml
# config.{env}.yaml
MCP_TOOL_DIRS:
  - mirobody/agent/tools
  - my_tools          # yours, relative to the working directory
```

**Or ship them as a package.** A distribution that declares a `mirobody.tools`
entry point pointing at a module of tool classes is loaded at boot the moment
it is `pip install`ed — same registry, same schema generation, served over
`/mcp` and handed to the agent like the built-ins:

```toml
[project.entry-points."mirobody.tools"]
labs = "my_plugin.tools"
```

`examples/mirobody_example_plugin/` is a complete example.

### Discovery Rules

1. **File Location**: Must be a `.py` file inside a configured tool directory.
2. **Ignored Files**: Files starting with `_` (e.g., `_utils.py`) are ignored.
3. **Eligible Code**:
   * **Functions**: Top-level functions are automatically registered.
   * **Classes**: Must end with `Service` (e.g., `FinanceService`) to be registered; their public methods become tools.
4. **`__tools__`**: a module or class that sets `__tools__ = ("name", ...)` publishes exactly those names, and no other public method or function. Every shipped tool class declares it, so a public helper can never become an undocumented tool.

### Conditional Registration (`_enabled`)

A Service may define a static `_enabled() -> bool`; returning `False` **skips the whole class** (none of its methods register). Use it to gate optional integrations on config (e.g. an API key).


## 📝 Implementation Guide

Your Python code *is* the definition. No separate configuration or JSON schema is needed.

### 1. Type Hints (Required)

Mirobody uses Python type hints (`str`, `int`, `bool`, `float`) to generate the tool's input schema.

* **Fundamental Types**: `str`, `int`, `float`, `bool`
* **Complex Types**: `list[str]`, `dict` (parsed as generic object)

### 2. Docstrings (Required)

Docstrings are parsed to provide descriptions to the AI. We recommend the standard format:

```python
def my_tool(arg1: str):
    """
    Brief description of what the tool does.

    Args:
        arg1: Description of the argument.
  
    Returns:
        Description of the return value.
    """
```

### 3. Authentication & Context

If your tool needs user information (like a User ID from a JWT), add a `user_info` parameter.

* **Injection**: Mirobody automatically injects this value; the AI agent does *not* see or provide it.
* **Structure**: `{"user_id": "..."}`, the authenticated account the call reads. Inside a chat it also
  carries `session_id`, the conversation the call belongs to (see Citations). An MCP or REST caller has none.

### 4. Citations: every number points back to its row

An answer cites what it rests on, in the format of `mirobody/kernel/citations.py`:

```
<statement>LDL fell from 3.8 to 2.9 mmol/L<cite>[r3][r9]</cite></statement>
```

* **The handle.** Inside a chat, each row the readings tool returns carries a short `rid` (`r1`, `r2`, ...),
  the first column of its table. `collect/citations.py` mints it per conversation and stores only the row's
  identity: an observation id, or for a `stats` line or a day, week or month bucket, what it was computed
  over. The same row keeps its rid for the life of the conversation, across restarts and workers.
* **Three kinds of cite.** A row (`r3`); a reference passage (`ref:<source>:<id>`); lines of a document as
  `read_file` numbered them (`/library/<file>#L12-L14`).
* **Resolving.** `GET /api/citations?session_id=…&rids=r3,r9` returns what each rid is now: the reading with
  its file, or an aggregate with its readings. A reading the person has deleted returns `gone`, a rid the
  conversation never showed returns `unknown`. Only the conversation's owner can resolve, while they can
  still read the record.
* **Checking.** `kernel.citations.check(answer, support)` lists every number that is not traced: outside a
  statement, cited to nothing, or neither in its cited rows nor derived from them (a difference, ratio,
  percentage change, sum or mean). The benchmark and the training judge use the same function.
* **A new record tool** that returns rows can join: mint keys for its rows with `mint_rids` while
  `CITATION_SESSION` is set, and put the rid first in its table.


## 📚 Medical knowledge: one capability, two tiers

The record tools answer "what does my record say". General medical knowledge ("what is metformin for",
"what does a high ALT mean", "what do guidelines say about statins") comes from a second capability that
never reads the record, so a small model does not answer it from memory. It lives in `agent/knowledge/`,
not in this directory: it is agent-only, and the MCP surface stays the record tools.

| | Offline, the default | Online, optional |
| --- | --- | --- |
| Sources | MedlinePlus health topics and lab test pages (public domain, credited "MedlinePlus, National Library of Medicine"); FDA labels of the 200 most-labelled prescription generics (openFDA, CC0) | PubMed reviews, meta-analyses, guidelines and trials with abstracts (through Europe PMC); ClinicalTrials.gov |
| Network at answer time | none | each search sends its words to `www.ebi.ac.uk` or `clinicaltrials.gov`; never record data |
| Turned on by | an index built once with `mirobody fetch knowledge` (the Docker image ships one) | `KNOWLEDGE_ONLINE: true` |
| Cites | `ref:medlineplus:<topic id>`, `ref:medlineplus_test:<page>`, `ref:openfda:<label set id>#<section>` | `ref:pmid:<id>`, `ref:pmc:<id>`, `ref:doi:<doi>`, `ref:nct:<id>` |

**Two tools**, offered only for the tiers a deployment has. `search_medical_knowledge(query, scope)` searches
`"reference"` (offline), `"literature"` or `"trials"` (online); `read_medical_source(ref)` reads one result in
full. Both return passages with `ref:` ids, which the answer cites the way it cites a row (Citations):
`<statement>ALT is an enzyme found mainly in the liver<cite>[ref:medlineplus_test:alt-blood-test]</cite></statement>`.
`/api/citations` resolves a ref to its title, source and public page without a network call, so the chip
opens the page the passage came from.

**Rules the prompt states.** A general medical fact comes only from what these tools returned, never from
memory; "your record shows" and "the reference says" stay separate statements; a label's dosage never
becomes an instruction for this person. Each turn may search 3 times and read 2 sources. An unreachable
source is reported as an outage, never as "no evidence".

**The index** is a SQLite FTS5 file at `KNOWLEDGE_INDEX` (default `~/.mirobody/knowledge/medref.sqlite3`),
about 15 MB, built in about 13 minutes from the sources above and kept out of git and the wheel. Without it the offline tier is
off. Queries are matched in English: the model turns a Chinese or Japanese question into English medical
terms, as the tool description asks.

**Privacy.** An online search carries the words of the query, so the online tier is off by default, and
the tool tells the model never to put the person's name, values or dates in a query.

## 💡 Examples

### Basic Function Tool

Save this as `my_tools/calculator.py`:

```python
def add_numbers(a: float, b: float) -> dict:
    """
    Adds two numbers together.

    Args:
        a: The first number.
        b: The second number.

    Returns:
        A dictionary containing the sum.
    """
    return {"result": a + b}
```

### Advanced Service Class

Save this as `my_tools/stocks.py`:

```python
from typing import Any

class StockService:
    """
    Service for retrieving stock market data.
    """

    __tools__ = ("get_stock_price",)

    def get_stock_price(self, ticker: str, user_info: dict[str, Any]) -> dict[str, Any]:
        """
        Gets the current price of a stock.

        Args:
            ticker: The stock ticker symbol (e.g., AAPL).

        Returns:
            The current stock price.
        """
        # `user_info` is injected: the account the call reads, never a value
        # the model chose.
        if not user_info.get("user_id"):
            return {"success": False, "error": "Authorization required."}

        return {
            "ticker": ticker,
            "price": 150.00,
            "currency": "USD"
        }
```

## 🧩 Reference

For the core implementation details of how tools are parsed, refer to:
[`mirobody/mcp/tool.py`](../../mcp/tool.py)
