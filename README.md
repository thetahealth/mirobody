<div align="center">

# 🚀 Mirobody

**Open Source AI-Native Data Engine for Your Personal Data**

[![Demos](https://img.shields.io/badge/Live%20Demos-mirobody.ai-blue)](https://mirobody.ai)
[![Theta Wellness](https://img.shields.io/badge/Theta%20Wellness-thetahealth.ai-green)](https://www.thetahealth.ai/)

*Privacy-first data platform that bridges your data with the latest AI capabilities*

**Key Features:**
- 🌐 **HTTP Remote MCP Server** - Deploy and access MCP tools over HTTPS
- 🎯 **Claude Agent Skills Support** - Develop tools using standard Skills format (SKILL.md)
- 🔄 **Universal Tool Adapter** - Works with ChatGPT, Claude, Cursor, and more
- 🔌 **Pluggable Data Providers** - Connect any data source via [Providers API](PROVIDERS.md)
- 🤖 **Custom Agents** - Create your own conversational agents via [Agents API](AGENTS.md)

</div>

---

## 📖 Table of Contents

- [Why Mirobody?](#-why-mirobody)
- [Theta Wellness: Our Health Intelligence App](#-theta-wellness-our-health-intelligence-app)
- [Quick Start](#-quick-start)
- [Access & Authentication](#-access--authentication)
- [API Reference](#-api-reference)

---

## ✨ Why Mirobody?

### 🔄 Write Tools Once, Run Everywhere

Forget about complex JSON schemas, manual bindings, or router configurations. In Mirobody, **your Python code is the only definition required.**

- Tools built here instantly work in **ChatGPT** (via Apps-SDK) and the entire **MCP Ecosystem** (Claude, Cursor, IDEs).
- Mirobody works simultaneously as an **MCP Client** (to use tools) and an **OAuth-enabled MCP Server** (to provide data), creating a complete data loop.
- **🌐 HTTP Remote MCP Support**: Mirobody supports **HTTP-based remote MCP servers**, enabling cloud deployments and cross-network tool access. Configure `MCP_PUBLIC_URL` to expose your MCP server over HTTPS for ChatGPT Apps and other remote integrations.

### 💎 Your Data Is an Asset, Not a Payload

Mirobody is built for **Personal Intelligence**, not just local storage. We believe the next frontier of AI is not knowing more about the world, but knowing more about *you*.

- General AI creates generic answers. Mirobody uses your data to create a **Personal Knowledge Base**, enabling AI to give answers that are truly relevant to your life.
- You can run the entire engine **locally** on your machine. We provide the architecture to unlock your data's value without ever compromising ownership.

### 🤖 Native Agent Engine

- Powered by a **self-developed agent engine** that fully reproduces **Claude Code's** autonomous capabilities locally.
- **🎯 Skills-Based Tool Development**: Mirobody supports developing tools using **Claude Agent Skills** format (SKILL.md files). You can create reusable tools that work seamlessly across the MCP ecosystem. Simply structure your tools as Skills and drop them into the `skills/` directory - Mirobody will automatically discover and expose them.
- Designed to load **Claude Agent Skills** SKILL.md files, turning your private data into an actionable knowledge base.

---

## 🏥 Theta Wellness: Our Health Intelligence App

[**Theta Wellness**](https://www.thetahealth.ai/) is our flagship application built on Mirobody, demonstrating the platform's capabilities in the **Personal Health** domain. We have built a professional-grade **Health Data Analysis** suite that showcases how Mirobody can handle the most complex, multi-modal, and sensitive data environments.

### Key Features

- **📱 Broad Integration**: Connects with **300+ devices**, Apple Health, and Google Health.
- **🏥 EHR Ready**: Compatible with systems covering **90% of the US population's** Electronic Health Records.
- **🎯 Multi-Modal Analysis**: Analyze health data via Voice, Image, Files, or Text.

> **💡 Empowering the Community**
>
> We are open-sourcing the Mirobody engine because the same architecture that powers our medical-grade Health Agent can power **your business**.
>
> Whether you want to build a **Finance Analyzer**, **Legal Assistant**, or **DevOps Bot**, the infrastructure is ready. We focus on Health; you build the rest. Simply swap the files in the `tools/` directory to start your own vertical.

---

## ⚡ Quick Start

> **👉 See [QUICKSTART.md](QUICKSTART.md) for a detailed, step-by-step guide.**

### 1. Deploy via Docker

Initialize your environment in seconds:

```bash
git clone https://github.com/thetahealth/mirobody.git
cd mirobody
./deploy.sh
```

Then open `http://localhost:18080` in your web browser.

> **📝 Configuration Notes:**
>
> - A `.env` file will be created automatically with two variables:
>   - `ENV`: The name of the current config.
>   - `CONFIG_ENCRYPTION_KEY`: A 32-byte string used for encrypting sensitive variablThe default configuration template is [`config.yaml`](config.yaml).
**👉 See [CONFIG.md](CONFIG.md) for a detailed configuration guide.**
**👉 See [DATABASE.md](DATABASE.md) for database schema and initialization details.**

During deployment, a `config.localdb.yaml` (or similar env-specific file) is created for your overrides. default config values in this file. 
>   - **Tip**: Check `EMAIL_PREDEFINE_CODES` for predefined email accounts and verification codes used for user login.
> - **LLM Setup**: `OPENROUTER_API_KEY` is required for the Deep agent.
> - **Auth Setup**: To enable **Google/Apple OAuth** or **Email Verification**, set the respective variables in `config.{env}.yaml`.
> - All API keys will be encrypted automatically once Mirobody loads them using the `CONFIG_ENCRYPTION_KEY` value.

### 2. Create Your Tools

Mirobody adopts a **"Tools-First"** philosophy. No complex binding logic is required:

- **Python Tools**: Drop your Python scripts into the `tools/` directory. **👉 See [TOOLS.md](TOOLS.md) for a developer guide.**
- **Claude Agent Skills**: Place SKILL.md files in the `skills/` directory (content loaded directly as agent instructions)
- ✨ **Zero Config**: The system auto-discovers your funtions and skills.
- 🐍 **Pure Python**: Use the libraries you love (Pandas, NumPy, etc.).
- 🎯 **Skills Support**: Develop tools using **Claude Agent Skills** SKILL.md format - write instructions freely, they become agent context.
- 🔧 **Universal**: A single tool file works for both REST API and MCP (local and remote HTTP).

Example Tools structure:
```python
# tools/my_tools.py
def analyze_data(input_data: str) -> dict:
    """
    Description of this tool.

    Args:
        input_data: Description of this argument.

    Returns:
        Description of the return value.
    """
    return {"result": "analysis"}
```

> **🔐 JWT Authentication**: If your tool requires JWT authentication, add a `user_id: str` parameter. This parameter will be automatically injected by Mirobody from the JWT token and **should NOT be included in the docstring's Args section**. Example:
> 
> ```python
> # tools/my_authenticated_tools.py
> def get_user_data(user_id: str, query: str) -> dict:
>     """
>     Retrieves user-specific data.
> 
>     Args:
>         query: The search query.
> 
>     Returns:
>         User data matching the query.
>     """
>     # user_id is automatically provided by Mirobody from JWT
>     return {"user_id": user_id, "data": "..."}
> ```

#### 🎯 Developing Tools with Claude Agent Skills

Mirobody supports the **[Claude Agent Skills specification](https://agentskills.io/specification)**, allowing you to create sophisticated, reusable tools:

- **📋 Standards Compliant**: Follows the official Agent Skills format with YAML frontmatter
- **🔍 Auto-Discovery**: Place Skills in the `skills/` directory - Mirobody automatically detects and loads them
- **✍️ Flexible Content**: SKILL.md body is loaded directly into agent context - write comprehensive instructions freely
- **🌐 MCP Native**: Skills work seamlessly across the entire MCP ecosystem

> **⚠️ Current Implementation Status**
>
> Mirobody currently supports **SKILL.md files with metadata.json**. The following features from the full Agent Skills specification are not yet implemented:
> - ❌ `scripts/` directory execution
> - ❌ `references/` directory loading
> - ❌ `assets/` directory resources
> - ❌ Sandbox environment for script execution
> - ❌ `allowed-tools` field enforcement
>
> **What works**: 
> - ✅ **SKILL.md files** with YAML frontmatter - loaded directly into agent context
> - ✅ **metadata.json files** - required by Mirobody for skill discovery
>
> **💡 Simple but Powerful**: The entire SKILL.md body becomes part of the agent's instructions. Write comprehensive guides, detailed workflows, examples, and troubleshooting tips - all will be available to the agent. Full specification support is planned for future releases.

A skill is a directory containing a `SKILL.md` file and a `metadata.json` file:

```
skills/
└── my-custom-skill/
    ├── metadata.json     # Required by Mirobody: Skill metadata for discovery
    └── SKILL.md          # Required by spec: Skill definition with YAML frontmatter
```

(Optional directories like `scripts/`, `references/`, and `assets/` are defined in the specification but not yet supported by Mirobody)

**metadata.json** (Required by Mirobody):

```json
{
  "name": "My Custom Skill",
  "summary": "Extract and analyze data from structured documents",
  "when_to_use": [
    "When user needs to process CSV, Excel, or JSON files",
    "When data extraction or transformation is required",
    "When statistical analysis of structured data is needed"
  ],
  "when_not_to_use": [
    "For unstructured text documents",
    "For image or video processing",
    "When simple file reading is sufficient"
  ],
  "tags": ["data-analysis", "csv", "excel", "statistics"]
}
```

| Field | Description | Required |
|-------|-------------|----------|
| `name` | Display name of the skill (can be human-readable with spaces) | Yes |
| `summary` | Brief description for quick reference | Yes |
| `when_to_use` | Array of use case scenarios | Yes |
| `when_not_to_use` | Array of scenarios to avoid this skill | Yes |
| `tags` | Array of tags for categorization | Yes |

> **📝 Note**: 
> - `metadata.json` is a **Mirobody-specific requirement** for skill discovery and IDE integration. It's not part of the official Agent Skills specification.
> - The `name` in `metadata.json` is for display purposes (can contain spaces and capitals).
> - The `name` in SKILL.md frontmatter must follow the strict naming convention (lowercase, hyphens only, matching directory name).

**SKILL.md Example** (Required by Specification):

```markdown
---
name: my-custom-skill
description: Extract and analyze data from structured documents. Use when working with CSV, Excel, or JSON files that need parsing, transformation, or statistical analysis.
license: MIT
metadata:
  author: your-org
  version: "1.0.0"
---

# My Custom Skill

This skill provides comprehensive data extraction and analysis capabilities for structured documents.

## Instructions

1. **Identify the file format** - Check if the input is CSV, Excel, or JSON
2. **Parse the document** - Use appropriate parsing techniques for the file type
3. **Validate data** - Ensure data integrity and handle missing values
4. **Perform analysis** - Apply requested statistical or transformation operations
5. **Return results** - Format output according to user preferences

## Available Tools

You can use the following MCP tools to accomplish this task:
- Use file reading tools to access the document
- Use data processing tools for transformation
- Use statistical analysis tools for calculations

## Edge Cases

- Handle missing or malformed data gracefully
- Support multiple encodings (UTF-8, Latin-1, etc.)
- For large files, consider processing in manageable chunks

## Example Usage

When user provides sales_data.csv with columns: date, product, revenue
1. Read and parse the CSV file
2. Group data by month
3. Calculate monthly revenue totals
4. Identify trends and generate summary report
```

> **💡 SKILL.md Flexibility**
>
> The SKILL.md file content is **loaded directly into the agent's context** when the skill is activated. This means:
> - ✍️ **Write freely**: Structure your instructions however works best for your use case
> - 📝 **No format restrictions**: Use any markdown format - lists, tables, code blocks, etc.
> - 🎯 **Be as detailed as needed**: Include step-by-step guides, examples, edge cases, or troubleshooting tips
> - 🧩 **Think of it as a prompt**: The content becomes part of the agent's instructions, so write clearly and comprehensively
>
> The agent will read and follow everything you write in the body section, so make it as helpful and detailed as necessary!

**Required Frontmatter Fields:**

| Field | Description | Constraints |
|-------|-------------|-------------|
| `name` | Skill identifier (must match directory name) | 1-64 chars, lowercase, hyphens only, no leading/trailing hyphens |
| `description` | What the skill does and when to use it | 1-1024 chars, include keywords for discoverability |

**Optional Frontmatter Fields:**

| Field | Description | Example |
|-------|-------------|---------|
| `license` | License identifier | `MIT`, `Apache-2.0`, `Proprietary` |
| `compatibility` | Environment requirements | `Requires pandas, numpy, and network access` |
| `metadata` | Additional custom properties | `author`, `version`, `category` |
| `allowed-tools` | Pre-approved tools (not yet enforced) | `Bash(git:*) Read Write` |

> **💡 Mirobody Requirements**: In addition to the standard SKILL.md file, Mirobody requires a `metadata.json` file for skill discovery and categorization. This is a Mirobody-specific requirement and not part of the official Agent Skills specification.


#### 🌐 HTTP Remote MCP Server

Mirobody's MCP server supports **HTTP/HTTPS remote access**, enabling:

- **Cloud Deployments**: Deploy your MCP server on any cloud platform
- **ChatGPT Apps**: Integrate with OpenAI's ChatGPT Apps via HTTPS
- **Cross-Network Access**: Access tools from anywhere, not just localhost
- **OAuth Security**: Secure remote access with OAuth authentication

To enable remote HTTP access, set `MCP_PUBLIC_URL` in your `config.{env}.yaml`:
```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

Your MCP server will then be accessible at the configured HTTPS endpoint, ready for remote integrations.

---

## 🔐 Access & Authentication

Once deployed, you can access the platform through the local web interface or our official hosted client.

### 1. Access Interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| **Local Web App** | `http://localhost:18080` | Fully self-hosted web interface running locally. |
| **Official Client** | [https://mirobody.ai](https://mirobody.ai) | **Recommended.** Our official web client that connects securely to your local backend service. |
| **MCP Server (Local)** | `http://localhost:18080/mcp` | For Claude Desktop / Cursor integration via local connection. |
| **MCP Server (Remote HTTP)** | `https://yourdomain.com/mcp` | **🌐 HTTP Remote MCP Support** - For ChatGPT Apps and remote integrations. Set `MCP_PUBLIC_URL` in your config file to enable HTTPS access. |

#### MCP Integration

Mirobody supports both **local** and **remote HTTP** MCP connections:

**Local Connection (Cursor/Claude Desktop):**

```json
{
  "mirobody_mcp": {
    "command": "npx",
    "args": [
      "-y",
      "universal-mcp-proxy"
    ],
    "env": {
      "UMCP_ENDPOINT": "http://localhost:18080/mcp"
    }
  }
}
```

**Remote HTTP Connection (ChatGPT Apps, Cloud Deployments):**

Configure `MCP_PUBLIC_URL` in your `config.{env}.yaml`:

```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

Then access your MCP server via HTTPS at the configured URL. This enables:
- ✅ ChatGPT Apps integration
- ✅ Cross-network tool access
- ✅ Cloud-based deployments
- ✅ Secure OAuth-enabled remote MCP access

### 2. Login Methods

You can choose to configure your own authentication providers or use the pre-set demo account.

- **🔐 Social Login**: Google Account / Apple Account (Requires configuration in `config.yaml`)
- **📧 Email Login**: Email Verification Code (Requires configuration in `config.yaml`)
- **🎮 Demo Account** (Instant Access):
  - **Users**: `demo1@mirobody.ai`, `demo2@mirobody.ai`, `demo3@mirobody.ai` (More demo users configurable in `config.yaml`)
  - **Password**: `777777`

---

## 🔌 API Reference

Mirobody provides standard endpoints for integration:

| Endpoint | Description | Protocol |
|----------|-------------|----------|
| `/mcp` | MCP Protocol Interface | JSON-RPC 2.0 |
| `/api/chat` | AI Chat Interface | OpenAI Compatible |
| `/api/history` | Session Management | REST |

---

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to submit pull requests, report issues, and contribute to the project.

---

<div align="center">

**Built with ❤️ for the AI Open Source Community**

</div>