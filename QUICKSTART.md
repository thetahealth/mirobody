# ⚡ Quick Start Guide

Get Mirobody up and running in minutes.

## 📋 Prerequisites

- **Docker & Docker Compose**: Ensure these are installed and running.
- **Git**: To clone the repository.

## 🚀 Installation & Deployment

1.  **Clone the repository**
    ```bash
    git clone https://github.com/thetahealth/mirobody.git
    cd mirobody
    ```

2.  **Deploy via Script**
    Run the deployment script to set up the environment and start containers:
    ```bash
    ./deploy.sh
    ```
    
    This script will:
    - Generate a secure `.env` file.
    - Create a default configuration file (`config.localdb.yaml`).
    - Build the Docker image.
    - Start the services (Postgres, Redis, Mirobody).

3.  **Access the Application**
    Once deployed, open your browser and navigate to:
    
    [**http://localhost:18080**](http://localhost:18080)

## 🐍 Local Python Development

If you prefer to run the Mirobody agent code locally (for debugging or development) while keeping the database and cache in Docker:

### 1. Start Backing Services
Run Postgres and Redis via Docker Compose:
```bash
docker compose up -d pg redis
```

### 2. Environment Setup
Prerequisites:
- **Python**: 3.10 or higher
- **Node.js**: 18.0.0 or higher (for chart renderer)

Setup your isolated environment:
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies
pip install -r requirements.txt

# Install Node.js dependencies (for chart rendering)
npm install --omit=dev
```

### 3. Configuration
Generate the required configuration files if they don't exist:

```bash
# Create .env
echo "ENV=localdb" > .env
# Generate a random encryption key (optional but recommended)
echo "CONFIG_ENCRYPTION_KEY=$(openssl rand -hex 32)" >> .env

# Create config file from template or let the app generate default on first run
# For now, ensure you have config.localdb.yaml settings as described in the Configuration section.
```

### 4. Run the Application
Start the Mirobody agent:
```bash
python -m main
```

The server will start at `http://localhost:18080`.

## ⚙️ Configuration

A `config.localdb.yaml` file is automatically created during his first deployment. You **must** configure at least the LLM API keys for the agent to function correctly.

1.  Open `config.localdb.yaml`.
2.  Add your API keys:

    ```yaml
    # OpenRouter API key (Required for Deep Agent)
    OPENROUTER_API_KEY: 'sk-or-...'
    
    # Optional: OpenAI or Google keys
    OPENAI_API_KEY: 'sk-...'
    GOOGLE_API_KEY: '...'
    ```
    
    > **Note:** Sensitive keys are automatically encrypted by the system using the `CONFIG_ENCRYPTION_KEY` found in your `.env` file.

3.  Restart services if you made changes:
    ```bash
    docker compose down
    docker compose up -d
    ```

## 👤 First Login

You can use the pre-configured demo accounts to log in immediately:

- **Email**: `demo1@mirobody.ai`
- **Password**: `777777`

## 🔌 MCP Integration

To use Mirobody as an MCP server with Cursor or Claude Desktop:

1.  **Local server URL**: `http://localhost:18080/mcp`
2.  **Configuration** (for `claude_desktop_config.json`):
    ```json
    {
      "mcpServers": {
        "mirobody": {
          "command": "npx",
          "args": ["-y", "universal-mcp-proxy"],
          "env": {
            "UMCP_ENDPOINT": "http://localhost:18080/mcp"
          }
        }
      }
    }
    ```

## 📚 Next Steps

- Check out [README.md](./README.md) for full details.
- Explore the `tools/` directory to add your own Python tools.
- Add skills to `skills/` directory.
