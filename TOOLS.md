# 🛠️ Developing Mirobody Tools

Mirobody follows a **"Tools First"** philosophy. You write standard Python code, and the system automatically converts it into MCP (Model Context Protocol) tools for AI agents.

## 🔍 Tool Discovery

Mirobody automatically discovers tools in the following locations:

1.  **Custom Tools**: `tools/` (Root directory) - **Place your own tools here.**
2.  **Core Tools**: `mirobody/tools/` - Built-in system tools (only for public releases).

### Discovery Rules
1.  **File Location**: Must be a `.py` file inside `tools/`.
2.  **Ignored Files**: Files starting with `_` (e.g., `_utils.py`) are ignored.
3.  **Eligible Code**:
    *   **Functions**: Top-level functions are automatically registered.
    *   **Classes**: Must end with `Service` (e.g., `FinanceService`) to be registered.

## 📝 Implementation Guide

Your Python code *is* the definition. No separate configuration or JSON schema is needed.

### 1. Type Hints (Required)
Mirobody uses Python type hints (`str`, `int`, `bool`, `float`) to generate the tool's input schema.
*   **Fundamental Types**: `str`, `int`, `float`, `bool`
*   **Complex Types**: `list[str]`, `dict` (parsed as generic object)

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
*   **Injection**: Mirobody automatically injects this value; the AI agent does *not* see or provide it.
*   **Structure**: `{"user_id": "...", "success": True}`.

## 💡 Examples

### Basic Function Tool
Save this as `tools/calculator.py`:

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
Save this as `tools/finance.py`:

```python
from typing import Dict, Any

class StockService:
    """
    Service for retrieving stock market data.
    """

    def get_stock_price(self, ticker: str, user_info: dict) -> Dict[str, Any]:
        """
        Gets the current price of a stock.

        Args:
            ticker: The stock ticker symbol (e.g., AAPL).
        
        Returns:
            The current stock price.
        """
        # user_info is automatically injected
        user_id = user_info.get("user_id")
        print(f"User {user_id} requested price for {ticker}")

        return {
            "ticker": ticker,
            "price": 150.00,
            "currency": "USD"
        }
```

## 🧩 Reference

For the core implementation details of how tools are parsed, refer to:
[`mirobody/mcp/tool.py`](mirobody/mcp/tool.py)
