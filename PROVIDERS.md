# 🏥 Developing Data Providers

Providers are modules in the **Theta Pulse** platform that connect Mirobody to external data sources (wearables, databases, health APIs).

## 📂 Architecture

Providers can be located in two places:

1.  **Custom Providers** (Recommended): Place them in the root `providers/` directory.
2.  **Core Providers** (Public releases): Located in `mirobody/pulse/theta/`.

Each provider gets its own directory following the naming convention: `mirobody_<slug>`.

### Directory Structure
```
providers/                      # Root directory for custom providers
└── mirobody_mydevice/          # Provider directory
    ├── __init__.py             # Exports
    └── provider_mydevice.py    # Main implementation
```

## 🏗️ Implementation Guide

To create a new provider, inherit from `BaseThetaProvider` and implement the required methods.

### 1. Basic Structure

```python
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.models import ConnectInfoField

class MyDeviceProvider(BaseThetaProvider):
    def __init__(self):
        super().__init__()
        # Initialize your client/service here

    @classmethod
    def create_provider(cls, config):
        # Factory method
        return cls()
```

### 2. Provider Metadata (`info`)

Define how your provider appears in the UI and what credentials it needs.

```python
    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            slug="theta_mydevice",
            name="My Device",
            description="Integration for My Device health data",
            logo="https://example.com/logo.png",
            supported=True,
            auth_type=LinkType.PASSWORD,  # or CUSTOMIZED
            status=ProviderStatus.AVAILABLE,
            # If LinkType.CUSTOMIZED, define fields:
            connect_info_fields=[
                ConnectInfoField(
                    field_name="api_key",
                    field_type="string",
                    required=True,
                    label="API Key"
                )
            ]
        )
```

### 3. Authentication (`_validate_credentials_v2`)

Validate that the user's credentials work.

```python
    async def _validate_credentials_v2(self, credentials: dict) -> None:
        # For LinkType.PASSWORD
        username = credentials.get("username")
        password = credentials.get("password")
        
        # For LinkType.CUSTOMIZED
        connect_info = credentials.get("connect_info", {})
        api_key = connect_info.get("api_key")

        if not self.my_client.test_connection(api_key):
             raise ValueError("Invalid credentials")
```

### 4. Data Pulling (Optional)

If your provider pulls data periodically, implement `pull_from_vendor_api`.

```python
    def register_pull_task(self) -> bool:
        return True  # Enable scheduled pulling

    async def pull_from_vendor_api(self, username, password) -> list:
        # Fetch data from external API
        raw_data = await self.my_client.get_data()
        return raw_data
```

## 🧩 Reference

- **Base Class**: [`mirobody/pulse/theta/platform/base.py`](mirobody/pulse/theta/platform/base.py)
- **Example**: [`mirobody/pulse/theta/mirobody_pgsql/provider_pgsql.py`](mirobody/pulse/theta/mirobody_pgsql/provider_pgsql.py)
