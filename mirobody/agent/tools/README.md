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
