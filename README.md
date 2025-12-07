# Mirobody

### Open Source AI-Native Data Engine for Your Personal Data

Mirobody is not just another chatbot wrapper. It is a privacy-first data platform designed to bridge your data with the latest AI capabilities. It serves as a universal adapter for your tools, fully compliant with the Model Context Protocol (MCP).

---

### Why Mirobody?

* **Write Tools Once, Run Everywhere**
    
    Forget about complex JSON schemas, manual bindings, or router configurations. In Mirobody, **your Python code is the only definition required.**
    * Tools built here instantly work in **ChatGPT** (via Apps-SDK) and the entire **MCP Ecosystem** (Claude, Cursor, IDEs).
    * Mirobody works simultaneously as an **MCP Client** (to use tools) and an **OAuth-enabled MCP Server** (to provide data), creating a complete data loop.

* **Your Data Is an Asset, Not a Payload**
    
    Mirobody is built for **Personal Intelligence**, not just local storage. We believe the next frontier of AI is not knowing more about the world, but knowing more about *you*.
    * General AI creates generic answers. Mirobody uses your data to create a **Personal Knowledge Base**, enabling AI to give answers that are truly relevant to your life.
    * You can run the entire engine **locally** on your machine. We provide the architecture to unlock your data's value without ever compromising ownership.

* **Native Agent Engine**
    * Powered by a **self-developed agent engine** that fully reproduces **Claude Code's** autonomous capabilities locally.
    * Designed to directly load standard **Claude Agent Skills** (Coming Soon), turning your private data into an actionable knowledge base.


---

## 🏥 Theta Wellness: Our Health Intelligence App

**Theta Wellness** is our flagship application built on Mirobody, demonstrating the platform's capabilities in the **Personal Health** domain. We have built a professional-grade **Health Data Analysis** suite that showcases how Mirobody can handle the most complex, multi-modal, and sensitive data environments.

* **Broad Integration**: Connects with **300+ device manufacturers**, Apple Health, and Google Health.
* **EHR Ready**: Compatible with systems covering **90% of the US population's** Electronic Health Records.
* **Multi-Modal Analysis**: Analyze health data via Voice, Image, Files, or Text.

> **💡 Empowering the Community**
>
> We are open-sourcing the Mirobody engine because the same architecture that powers our medical-grade Health Agent can power **your business**.
>
> Whether you want to build a **Finance Analyzer**, **Legal Assistant**, or **DevOps Bot**, the infrastructure is ready. We focus on Health; you build the rest. Simply swap the files in the `tools/` directory to start your own vertical.


---

## ⚡ Quick Start

### 1. Configuration
Initialize your environment in seconds:

```bash
cd config
cp config.example.yaml config.yaml
```

> **Note**:
>
>   * **LLM Setup**: `OPENROUTER_API_KEY` is required.
>   * **Auth Setup**: To enable **Google/Apple OAuth** or **Email Verification**, fill in the respective fields in `config.yaml`.
>   * All API keys are encrypted automatically.

### 2\. Create Your Tools

Mirobody adopts a **"Tools-First"** philosophy. No complex binding logic is required. Simply drop your Python scripts into the `tools/` directory:

  * ✨ **Zero Config**: The system auto-discovers your functions.
  * 🐍 **Pure Python**: Use the libraries you love (Pandas, NumPy, etc.).
  * 🔧 **Universal**: A single tool file works for both REST API and MCP.

### 3\. Deployment

Launch the platform using our unified deployment script.

**Option A: Image Mode** ⭐ **(Recommended)**
*Downloads pre-built images.*
*Faster deployment with synthetic test user data included in the database (coming soon).*

```bash
./deploy.sh --mode=image
```

**Option B: Build Mode**
*Builds everything from scratch.*

```bash
./deploy.sh --mode=build
```



**Daily Startup**
*For regular use after initial setup, simply run:*

```bash
./deploy.sh
```

-----

## 🔐 Access & Authentication

Once deployed, you can access the platform through the local web interface or our official hosted client.

### 1\. Access Interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| **Local Web App** | `http://localhost:18080` | Fully self-hosted web interface running locally. |
| **Official Client**| [https://my.mirobody.ai](https://my.mirobody.ai) | **Recommended.** Our official web client that connects securely to your local backend service. |
| **MCP Server** | `http://localhost:18080/mcp` | For Claude Desktop / Cursor integration. |
To use Mirobody with Cursor, add the following configuration to your MCP settings:

"mirobady_mcp": {
  "command": "npx",
  "args": [
    "-y",
    "universal-mcp-proxy"
  ],
  "env": {
    "UMCP_ENDPOINT": "http://localhost:18080/mcp"
  }
}

### 2\. Login Methods

You can choose to configure your own authentication providers or use the pre-set demo account.

  * **Social Login**: Google Account / Apple Account (Requires configuration in `config.yaml`)
  * **Email Login**: Email Verification Code (Requires configuration in `config.yaml`)
  * **Demo Account** (Instant Access):
      * **Users:** `demo1@mirobody.ai`, `demo2@mirobody.ai`, `demo3@mirobody.ai` (More demo users configurable in `config.yaml`)
      * **Password:** `777777`

-----

## 🔌 API Reference

Mirobody provides standard endpoints for integration:

| Endpoint | Description | Protocol |
|----------|-------------|----------|
| `/mcp` | MCP Protocol Interface | JSON-RPC 2.0 |
| `/api/chat` | AI Chat Interface | OpenAI Compatible |
| `/api/history` | Session Management | REST |

-----

<p align="center">
<sub>Built with ❤️ for the AI Open Source Community.</sub>
</p>
