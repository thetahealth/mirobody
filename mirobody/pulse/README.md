# Provider Integration Guide

This guide provides comprehensive instructions for integrating new device/service providers into the Mirobody Health platform.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Provider Architecture Overview](#provider-architecture-overview)
3. [Implementation Requirements](#implementation-requirements)
4. [Core Methods Reference](#core-methods-reference)
5. [Data Flow & Scopes](#data-flow--scopes)
6. [Testing Your Provider](#testing-your-provider)
7. [Best Practices](#best-practices)

---

## 1. Prerequisites

Before integrating a new provider, ensure you have:

### Technical Requirements
- Python 3.8+ environment
- Access to the target device/service API documentation
- OAuth credentials (OAuth1 or OAuth2) from the device vendor
- Understanding of async/await patterns in Python
- Familiarity with REST APIs and JSON data formats

### Configuration Requirements
You'll need to configure the following in your `config.yaml`:

```yaml
# OAuth Credentials
<PROVIDER>_CLIENT_ID: "your_client_id"
<PROVIDER>_CLIENT_SECRET: "your_client_secret"
<PROVIDER>_REDIRECT_URL: "your_callback_url"

# Optional: API Endpoints (if different from defaults)
<PROVIDER>_AUTH_URL: "https://..."
<PROVIDER>_TOKEN_URL: "https://..."
<PROVIDER>_API_BASE_URL: "https://..."

# Optional: OAuth Scopes
<PROVIDER>_SCOPES: "scope1 scope2 scope3"

# Optional: Performance Tuning
OAUTH_TEMP_TTL_SECONDS: 900
<PROVIDER>_REQUEST_TIMEOUT: 30
<PROVIDER>_CONCURRENT_REQUESTS: 5
```

### Database Schema
Ensure your database has the provider-specific table:

```sql
CREATE TABLE IF NOT EXISTS health_data_<provider> (
    id SERIAL PRIMARY KEY,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_del BOOLEAN DEFAULT FALSE,
    msg_id VARCHAR(255) UNIQUE NOT NULL,
    raw_data JSONB NOT NULL,
    theta_user_id VARCHAR(255) NOT NULL,
    external_user_id VARCHAR(255)
);

CREATE INDEX idx_health_data_<provider>_theta_user_id 
    ON health_data_<provider>(theta_user_id);
CREATE INDEX idx_health_data_<provider>_msg_id 
    ON health_data_<provider>(msg_id);
```

---

## 2. Provider Architecture Overview

### Directory Structure

```
connect/
├── __init__.py
└── theta/
    └── mirobody_<provider>/
        ├── __init__.py
        └── provider_<provider>.py
```

### Class Hierarchy

```
BaseThetaProvider (from mirobody.pulse.theta.platform.base)
    ↓
ThetaYourProvider (your implementation)
```

### Key Components

1. **OAuth Flow Handler**: Manages user authentication
2. **Data Puller**: Fetches data from vendor API
3. **Data Formatter**: Transforms vendor data to standard format
4. **Database Service**: Persists raw and formatted data

---

## 3. Implementation Requirements

### Step 1: Create Provider Directory

Create a new directory under `connect/theta/`:

```bash
mkdir -p connect/theta/mirobody_<provider>
touch connect/theta/mirobody_<provider>/__init__.py
touch connect/theta/mirobody_<provider>/provider_<provider>.py
```

### Step 2: Define Provider Class

Your provider must inherit from `BaseThetaProvider` and implement all required methods:

```python
"""
Theta <Provider> Provider

<Provider> OAuth data provider with authentication and data pulling functionality
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

# Core imports
from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.indicators_info import StandardIndicator
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.core.units import UNIT_CONVERSIONS
from mirobody.pulse.data_upload.models.requests import (
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.theta.platform.utils import ThetaDataFormatter, ThetaTimeUtils
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg, global_config


class ThetaYourProvider(BaseThetaProvider):
    """Theta <Provider> Provider - Data Integration"""
    
    def __init__(self):
        super().__init__()
        # Initialize your provider-specific configuration here
        pass
    
    @classmethod
    def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaYourProvider']:
        """Factory method to create provider instance"""
        pass
    
    @property
    def info(self) -> ProviderInfo:
        """Provider metadata"""
        pass
    
    async def link(self, request: Any) -> Dict[str, Any]:
        """Initiate OAuth flow"""
        pass
    
    async def callback(self, *args, **kwargs) -> Dict[str, Any]:
        """Handle OAuth callback"""
        pass
    
    async def unlink(self, user_id: str) -> Dict[str, Any]:
        """Unlink user connection"""
        pass
    
    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        """Format raw data to standard format"""
        pass
    
    async def pull_from_vendor_api(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """Pull data from vendor API"""
        pass
    
    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Save raw data to database"""
        pass
    
    async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
        """Check if data is already processed"""
        pass
    
    async def _pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool:
        """Pull and push data for a specific user"""
        pass
```

### Step 3: Define Data Mapping

Create a mapping between vendor API fields and standard indicators:

```python
# Example data mapping
DATA_MAPPING = {
    "data_type_1": {
        "vendor_field_1": (
            StandardIndicator.HEART_RATE.value.name,
            lambda x: x,  # Converter function
            StandardIndicator.HEART_RATE.value.standard_unit
        ),
        "vendor_field_2": (
            StandardIndicator.STEPS.value.name,
            lambda x: x,
            StandardIndicator.STEPS.value.standard_unit
        ),
    },
    "data_type_2": {
        # More mappings...
    }
}
```

---

## 4. Core Methods Reference

### 4.1 `__init__(self)`

**Purpose**: Initialize provider configuration and credentials

**Scope**: Instance initialization

**Implementation**:
```python
def __init__(self):
    super().__init__()
    
    # Load OAuth credentials
    self.client_id = safe_read_cfg("<PROVIDER>_CLIENT_ID")
    self.client_secret = safe_read_cfg("<PROVIDER>_CLIENT_SECRET")
    self.redirect_url = safe_read_cfg("<PROVIDER>_REDIRECT_URL")
    
    # Load API endpoints
    self.auth_url = safe_read_cfg("<PROVIDER>_AUTH_URL") or "https://..."
    self.token_url = safe_read_cfg("<PROVIDER>_TOKEN_URL") or "https://..."
    self.api_base_url = safe_read_cfg("<PROVIDER>_API_BASE_URL") or "https://..."
    
    # Load scopes
    self.scopes = safe_read_cfg("<PROVIDER>_SCOPES") or "default_scope"
    
    # Configuration validation
    if not self.client_id or not self.client_secret:
        logging.error("<Provider> OAuth credentials not configured")
```

**Key Points**:
- Always call `super().__init__()` first
- Use `safe_read_cfg()` for configuration values
- Provide sensible defaults where possible
- Validate critical configuration

---

### 4.2 `create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaYourProvider']`

**Purpose**: Factory method for conditional provider instantiation

**Scope**: Class method (called before instance creation)

**Implementation**:
```python
@classmethod
def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaYourProvider']:
    """
    Factory method to create provider from config
    
    Required config keys:
    - <PROVIDER>_CLIENT_ID
    - <PROVIDER>_CLIENT_SECRET
    
    Returns:
        Provider instance if config is valid, None otherwise
    """
    try:
        client_id = safe_read_cfg("<PROVIDER>_CLIENT_ID")
        client_secret = safe_read_cfg("<PROVIDER>_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            logging.info("<Provider> disabled: missing credentials")
            return None
        
        return cls()
    except Exception as e:
        logging.warning(f"Failed to create <Provider> provider: {e}")
        return None
```

**Key Points**:
- Returns `None` if provider cannot be initialized
- Graceful failure - don't raise exceptions
- Log informational messages for debugging

---

### 4.3 `info(self) -> ProviderInfo`

**Purpose**: Provide metadata about the provider

**Scope**: Property, accessed frequently for display/routing

**Implementation**:
```python
@property
def info(self) -> ProviderInfo:
    """Get Provider information"""
    return ProviderInfo(
        slug="theta_<provider>",              # Unique identifier
        name="<Provider Name>",               # Display name
        description="<Provider> health data integration via OAuth",
        logo="https://static.thetahealth.ai/res/<provider>.png",
        supported=True,                       # Whether provider is active
        auth_type=LinkType.OAUTH2,           # OAUTH1 or OAUTH2
        status=ProviderStatus.AVAILABLE,     # Status
    )
```

**Key Points**:
- `slug` must be unique across all providers
- `auth_type` must match your OAuth implementation
- Logo should be hosted on CDN

---

### 4.4 `link(self, request: Any) -> Dict[str, Any]`

**Purpose**: Initiate OAuth authentication flow

**Scope**: User-triggered, begins the linking process

**Implementation for OAuth2**:
```python
async def link(self, request: Any) -> Dict[str, Any]:
    """
    Initiate OAuth2 flow
    
    Args:
        request: Contains user_id and options (redirect_url, return_url)
        
    Returns:
        Dict with 'link_web_url' for user to visit
        
    Raises:
        RuntimeError: If OAuth configuration is invalid
    """
    user_id = request.user_id
    options = request.options or {}
    
    try:
        # Generate state parameter
        state_payload = {"s": str(uuid.uuid4()), "r": options.get("return_url", "")}
        state = urlencode(state_payload)
        
        # Store state in Redis (TTL: 15 minutes)
        cfg = global_config()
        redis_client = await cfg.get_redis().get_async_client()
        await redis_client.setex(f"oauth2:state:{state}", 900, user_id)
        await redis_client.setex(f"oauth2:redir:{state}", 900, self.redirect_url)
        await redis_client.aclose()
        
        # Build authorization URL
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_url,
            "scope": self.scopes,
            "state": state,
        }
        authorization_url = f"{self.auth_url}?{urlencode(params)}"
        
        logging.info(f"Generated OAuth2 URL for user {user_id}")
        return {"link_web_url": authorization_url}
        
    except Exception as e:
        logging.error(f"Error linking provider: {str(e)}")
        raise RuntimeError(str(e))
```

**Implementation for OAuth1**:
```python
async def link(self, request: Any) -> Dict[str, Any]:
    """Initiate OAuth1 flow"""
    user_id = request.user_id
    options = request.options or {}
    
    try:
        # Create OAuth1Session
        oauth = OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
        )
        
        # Get request token
        resp = oauth.post(self.request_token_url)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to get request token: {resp.text}")
        
        # Parse response
        params = parse_qs(resp.text)
        oauth_token = params['oauth_token'][0]
        oauth_token_secret = params['oauth_token_secret'][0]
        
        # Store token secret in Redis
        cfg = global_config()
        redis_client = await cfg.get_redis().get_async_client()
        await redis_client.setex(f"oauth:secret:{oauth_token}", 900, oauth_token_secret)
        await redis_client.setex(f"oauth:user:{oauth_token}", 900, user_id)
        await redis_client.aclose()
        
        # Build authorization URL
        auth_params = {
            "oauth_token": oauth_token,
            "oauth_callback": self.redirect_url
        }
        authorization_url = f"{self.auth_url}?{urlencode(auth_params)}"
        
        return {"link_web_url": authorization_url}
        
    except Exception as e:
        logging.error(f"Error linking provider: {str(e)}")
        raise RuntimeError(str(e))
```

**Key Points**:
- Store temporary OAuth state/tokens in Redis with TTL
- Include user_id mapping for callback retrieval
- Return URL that user must visit
- Handle both OAuth1 and OAuth2 flows appropriately

---

### 4.5 `callback(self, *args, **kwargs) -> Dict[str, Any]`

**Purpose**: Handle OAuth callback and complete authentication

**Scope**: Triggered by OAuth provider redirect

**Implementation for OAuth2**:
```python
async def callback(self, code: str, state: str) -> Dict[str, Any]:
    """
    Handle OAuth2 callback
    
    Args:
        code: Authorization code from provider
        state: State parameter for validation
        
    Returns:
        Dict with provider_slug, access_token, and stage="completed"
        
    Raises:
        RuntimeError: If token exchange fails
    """
    try:
        # Retrieve user_id and redirect_uri from Redis
        cfg = global_config()
        redis_client = await cfg.get_redis().get_async_client()
        user_id = await redis_client.get(f"oauth2:state:{state}")
        redirect_uri = await redis_client.get(f"oauth2:redir:{state}")
        await redis_client.delete(f"oauth2:state:{state}")
        await redis_client.delete(f"oauth2:redir:{state}")
        await redis_client.aclose()
        
        if isinstance(user_id, bytes):
            user_id = user_id.decode("utf-8")
        if isinstance(redirect_uri, bytes):
            redirect_uri = redirect_uri.decode("utf-8")
        
        if not user_id:
            raise ValueError("Missing user_id for callback")
        
        # Exchange code for tokens
        async with aiohttp.ClientSession() as session:
            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            
            async with session.post(self.token_url, data=data, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Token exchange failed: {resp.status} - {text}")
                
                token_data = await resp.json()
        
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in")
        
        if not access_token:
            raise RuntimeError("Missing access_token in response")
        
        # Calculate expiry timestamp
        expires_at = None
        if expires_in:
            expires_at = int(time.time()) + int(expires_in)
        
        # Save credentials to database
        success = await self.db_service.save_oauth2_credentials(
            user_id, self.info.slug, access_token, refresh_token, expires_at
        )
        if not success:
            raise RuntimeError("Failed to save credentials")
        
        logging.info(f"Successfully linked provider for user {user_id}")
        
        # Trigger initial data pull
        creds_payload = {
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
        }
        asyncio.create_task(self._pull_and_push_for_user(creds_payload))
        
        return {
            "provider_slug": self.info.slug,
            "access_token": access_token[:20] + "...",
            "stage": "completed"
        }
        
    except Exception as e:
        logging.error(f"Error in callback: {str(e)}")
        raise RuntimeError(str(e))
```

**Implementation for OAuth1**:
```python
async def callback(self, oauth_token: str, oauth_verifier: str) -> Dict[str, Any]:
    """Handle OAuth1 callback"""
    try:
        # Retrieve token secret and user_id from Redis
        cfg = global_config()
        redis_client = await cfg.get_redis().get_async_client()
        oauth_token_secret = await redis_client.get(f"oauth:secret:{oauth_token}")
        user_id = await redis_client.get(f"oauth:user:{oauth_token}")
        await redis_client.delete(f"oauth:secret:{oauth_token}")
        await redis_client.delete(f"oauth:user:{oauth_token}")
        await redis_client.aclose()
        
        if isinstance(oauth_token_secret, bytes):
            oauth_token_secret = oauth_token_secret.decode("utf-8")
        if isinstance(user_id, bytes):
            user_id = user_id.decode("utf-8")
        
        if not oauth_token_secret or not user_id:
            raise ValueError("Missing OAuth state from stage 1")
        
        # Create OAuth1Session for token exchange
        oauth = OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
            resource_owner_key=oauth_token,
            resource_owner_secret=oauth_token_secret,
            verifier=oauth_verifier
        )
        
        # Get access token
        resp = oauth.post(self.access_token_url)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to get access token: {resp.text}")
        
        # Parse tokens
        params = parse_qs(resp.text)
        access_token = params['oauth_token'][0]
        access_token_secret = params['oauth_token_secret'][0]
        
        # Save credentials
        success = await self.db_service.save_oauth1_credentials(
            user_id, self.info.slug, access_token, access_token_secret
        )
        if not success:
            raise RuntimeError("Failed to save credentials")
        
        logging.info(f"Successfully linked provider for user {user_id}")
        
        # Trigger initial data pull
        creds_payload = {
            "user_id": user_id,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }
        asyncio.create_task(self._pull_and_push_for_user(creds_payload))
        
        return {
            "provider_slug": self.info.slug,
            "access_token": access_token[:20] + "...",
            "stage": "completed"
        }
        
    except Exception as e:
        logging.error(f"Error in callback: {str(e)}")
        raise RuntimeError(str(e))
```

**Key Points**:
- Retrieve stored state/tokens from Redis
- Exchange temporary tokens for permanent ones
- Save credentials using appropriate db_service method
- Trigger immediate data pull after successful linking
- Clean up temporary Redis keys

---

### 4.6 `unlink(self, user_id: str) -> Dict[str, Any]`

**Purpose**: Remove user connection and revoke access

**Scope**: User-triggered, removes all provider data

**Implementation**:
```python
async def unlink(self, user_id: str) -> Dict[str, Any]:
    """
    Unlink provider connection
    
    Args:
        user_id: User ID to unlink
        
    Returns:
        Dict with success status and message
        
    Raises:
        RuntimeError: If unlinking fails
    """
    try:
        logging.info(f"Unlinking provider for user: {user_id}")
        
        # Get stored credentials
        credentials = await self.db_service.get_user_credentials(
            user_id, self.info.slug, self.info.auth_type
        )
        
        if not credentials:
            logging.warning(f"No credentials found for user {user_id}")
            return {"success": True, "message": "No credentials found"}
        
        # Optional: Call vendor API to revoke access
        # (Not all providers support this)
        try:
            # For OAuth2:
            if self.info.auth_type == LinkType.OAUTH2:
                access_token = credentials.get("access_token")
                if access_token:
                    # Example revocation call
                    async with aiohttp.ClientSession() as session:
                        headers = {"Authorization": f"Bearer {access_token}"}
                        await session.delete(
                            f"{self.api_base_url}/revoke",
                            headers=headers
                        )
            
            # For OAuth1:
            elif self.info.auth_type == LinkType.OAUTH1:
                access_token = credentials.get("access_token")
                token_secret = credentials.get("access_token_secret")
                if access_token and token_secret:
                    oauth = OAuth1Session(
                        client_key=self.client_id,
                        client_secret=self.client_secret,
                        resource_owner_key=access_token,
                        resource_owner_secret=token_secret
                    )
                    oauth.delete(f"{self.api_base_url}/revoke")
        except Exception as e:
            logging.warning(f"Failed to revoke access at vendor: {str(e)}")
        
        # Always remove from database
        await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
        
        logging.info(f"Successfully unlinked provider for user {user_id}")
        return {"success": True, "message": "Successfully unlinked"}
        
    except Exception as e:
        logging.error(f"Failed to unlink provider: {str(e)}")
        raise RuntimeError(f"Failed to unlink provider: {str(e)}")
```

**Key Points**:
- Attempt to revoke access at vendor (optional)
- Always clean up local database regardless of vendor response
- Log warnings for partial failures
- Return success if database cleanup succeeds

---

### 4.7 `format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData`

**Purpose**: Transform vendor-specific data to standardized format

**Scope**: Called for every data batch received

**Implementation**:
```python
async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
    """
    Format raw data to StandardPulseData
    
    Args:
        raw_data: Raw data from vendor, including:
            - user_id: User identifier
            - data_type: Type of data (e.g., 'sleeps', 'workouts')
            - data: Actual data payload
            - timestamp: Pull timestamp
            - msg_id: Message ID for deduplication
            
    Returns:
        StandardPulseData with formatted health records
    """
    start_time = time.time()
    
    try:
        request_id = self.generate_request_id()
        user_id = raw_data.get("user_id", "")
        data_type = raw_data.get("data_type", "unknown")
        data_content = raw_data.get("data", [])
        
        if not user_id:
            logging.error("No user_id in raw_data")
            return self._create_empty_response(request_id, "")
        
        # Get user timezone
        user_timezone = await self._get_user_timezone(user_id)
        
        # Initialize processing context
        processing_info = {
            "provider": self.info.slug,
            "start_time": start_time,
            "processed_indicators": 0,
            "skipped_indicators": 0,
            "errors": [],
            "msg_id": raw_data.get("msg_id", ""),
            "user_timezone": user_timezone,
        }
        
        # Ensure data_content is a list
        if not isinstance(data_content, list):
            data_content = [data_content]
        
        health_records: List[StandardPulseRecord] = []
        
        # Process data based on type
        if data_type == "sleeps":
            health_records.extend(
                self._process_sleep_data(data_content, processing_info)
            )
        elif data_type == "workouts":
            health_records.extend(
                self._process_workout_data(data_content, processing_info)
            )
        # Add more data types as needed
        else:
            logging.warning(f"Unknown data type: {data_type}")
        
        # Update processing info
        processing_info.update({
            "end_time": time.time(),
            "processing_duration_ms": int((time.time() - start_time) * 1000),
            "total_records": len(health_records),
        })
        
        # Create result
        meta_info = StandardPulseMetaInfo(
            userId=user_id,
            requestId=request_id,
            source="theta",
            timezone=user_timezone
        )
        
        result = StandardPulseData(
            metaInfo=meta_info,
            healthData=health_records,
            processingInfo=processing_info,
        )
        
        logging.info(
            f"Formatted {len(health_records)} records for user {user_id}"
        )
        return result
        
    except Exception as e:
        logging.error(f"Error formatting data: {str(e)}")
        request_id = self.generate_request_id()
        return self._create_empty_response(request_id, raw_data.get("user_id", ""))

def _process_sleep_data(
    self, data: List[Dict], processing_info: Dict
) -> List[StandardPulseRecord]:
    """Process sleep data using mapping configuration"""
    records = []
    
    for item in data:
        try:
            # Parse timestamp
            timestamp_str = item.get("start") or item.get("created_at")
            timestamp_ms = (
                ThetaTimeUtils.parse_time_to_timestamp(timestamp_str)
                if timestamp_str
                else int(time.time() * 1000)
            )
            
            # Apply data mapping
            for field_path, (indicator_name, converter, unit) in \
                    self.DATA_MAPPING.get("sleeps", {}).items():
                
                # Navigate nested fields
                value = item
                for field in field_path.split("."):
                    value = value.get(field) if isinstance(value, dict) else None
                    if value is None:
                        break
                
                if value is not None:
                    record = StandardPulseRecord(
                        source=ThetaDataFormatter.format_source_name(self.info.slug),
                        type=indicator_name,
                        timestamp=timestamp_ms,
                        unit=unit,
                        value=float(converter(value)),
                        timezone=processing_info.get("user_timezone", "UTC"),
                        source_id=processing_info.get("msg_id", ""),
                    )
                    records.append(record)
                    processing_info["processed_indicators"] += 1
                    
        except Exception as e:
            logging.error(f"Error processing sleep item: {str(e)}")
            processing_info["errors"].append(f"Sleep: {str(e)}")
            processing_info["skipped_indicators"] += 1
    
    return records
```

**Key Points**:
- Always retrieve user timezone for accurate timestamps
- Use data mapping configuration for consistency
- Handle nested field navigation gracefully
- Track processing metrics in `processing_info`
- Return empty response on fatal errors

---

### 4.8 `pull_from_vendor_api(self, *args, **kwargs) -> List[Dict[str, Any]]`

**Purpose**: Fetch data from vendor API

**Scope**: Called periodically or on-demand for data sync

**Implementation**:
```python
async def pull_from_vendor_api(
    self,
    access_token: str,
    refresh_token: Optional[str] = None,
    days: Optional[int] = 1
) -> List[Dict[str, Any]]:
    """
    Pull data from vendor API
    
    Args:
        access_token: OAuth access token
        refresh_token: OAuth refresh token (OAuth2 only)
        days: Number of days to pull (default: 1)
        
    Returns:
        List of raw data dicts, each containing:
            - user_id: External user identifier
            - data_type: Type of data
            - data: Raw data payload
            - timestamp: Pull timestamp
    """
    try:
        logging.info(f"Starting data pull (last {days} days)")
        
        if not access_token:
            raise ValueError("Access token is required")
        
        # Prepare headers
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        
        # Calculate date range
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
        
        all_raw_data = []
        timestamp = int(time.time() * 1000)
        
        async with aiohttp.ClientSession() as session:
            # Fetch different data types
            data_endpoints = {
                "sleeps": f"{self.api_base_url}/sleep",
                "workouts": f"{self.api_base_url}/workouts",
                "cycles": f"{self.api_base_url}/cycles",
            }
            
            for data_type, endpoint in data_endpoints.items():
                try:
                    params = {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat(),
                    }
                    
                    # Fetch paginated data
                    data = await self._fetch_paginated_data(
                        session, endpoint, headers, params
                    )
                    
                    if data:
                        all_raw_data.append({
                            "user_id": "",  # Will be filled by caller
                            "data_type": data_type,
                            "data": data,
                            "timestamp": timestamp,
                        })
                        logging.info(f"Pulled {len(data)} {data_type} records")
                        
                except Exception as e:
                    logging.error(f"Error pulling {data_type}: {str(e)}")
                    continue
        
        logging.info(f"Completed data pull: {len(all_raw_data)} data sets")
        return all_raw_data
        
    except Exception as e:
        logging.error(f"Error in data pull: {str(e)}")
        return []

async def _fetch_paginated_data(
    self,
    session: aiohttp.ClientSession,
    endpoint: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Generic paginated data fetcher
    
    Handles pagination, rate limiting, and retries
    """
    all_records = []
    next_token = None
    params = params or {}
    max_retries = 3
    
    while True:
        if next_token:
            params["nextToken"] = next_token
        
        retry_count = 0
        data = {}
        
        while retry_count <= max_retries:
            try:
                async with session.get(
                    endpoint,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    # Handle rate limiting
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "60"))
                        if retry_count < max_retries:
                            logging.warning(
                                f"Rate limited, retrying after {retry_after}s"
                            )
                            await asyncio.sleep(min(retry_after, 60))
                            retry_count += 1
                            continue
                        else:
                            logging.error("Max retries exceeded for rate limiting")
                            break
                    
                    # Handle auth errors
                    elif resp.status == 401:
                        text = await resp.text()
                        logging.error(f"Authentication failed: {resp.status} - {text}")
                        break
                    
                    # Handle other errors
                    elif resp.status != 200:
                        text = await resp.text()
                        logging.error(f"Request failed: {resp.status} - {text}")
                        break
                    
                    # Success
                    else:
                        data = await resp.json()
                        break
                        
            except asyncio.TimeoutError:
                if retry_count < max_retries:
                    retry_count += 1
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    logging.error("Request timeout exceeded")
                    break
            except Exception as e:
                logging.error(f"Request error: {str(e)}")
                if retry_count < max_retries:
                    retry_count += 1
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    break
        
        # Check if we got data
        if retry_count > max_retries:
            break
        
        # Extract records and next token
        if "records" in data:
            records = data.get("records", [])
            all_records.extend(records)
            next_token = data.get("next_token")
            
            logging.info(
                f"Fetched {len(records)} records from {endpoint}, "
                f"total: {len(all_records)}"
            )
            
            if not next_token:
                break
        else:
            # Non-paginated response
            all_records.append(data)
            break
    
    return all_records
```

**Key Points**:
- Implement pagination support
- Handle rate limiting with retries
- Use exponential backoff for transient errors
- Support date range filtering
- Return structured data for each data type

---

### 4.9 `save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]`

**Purpose**: Persist raw vendor data to database

**Scope**: Called before data formatting for audit trail

**Implementation**:
```python
async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Save raw data to database
    
    Args:
        raw_data: Raw data payload
        
    Returns:
        List of saved data dicts with msg_id added
    """
    try:
        if not isinstance(raw_data, dict):
            return []
        
        # Extract user ID
        user_id = raw_data.get("user_id", "")
        
        # Generate message ID for deduplication
        msg_id = f"{self.info.slug}_{user_id}_{int(time.time())}"
        
        # Insert into database
        insert_sql = (
            f"INSERT INTO health_data_{self.info.slug.replace('theta_', '')} "
            "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
            "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :is_del, :msg_id, "
            ":raw_data, :theta_user_id, :external_user_id) "
            "ON CONFLICT (msg_id) DO NOTHING"
        )
        
        params = {
            "is_del": False,
            "msg_id": msg_id,
            "raw_data": json.dumps(raw_data, ensure_ascii=False),
            "theta_user_id": user_id,
            "external_user_id": user_id,
        }
        
        await execute_query(query=insert_sql, params=params)
        
        # Return data with msg_id
        result_data = raw_data.copy()
        result_data["msg_id"] = msg_id
        
        logging.info(f"Saved raw data with msg_id: {msg_id}")
        return [result_data]
        
    except Exception as e:
        logging.error(f"Error saving raw data: {str(e)}")
        return []
```

**Key Points**:
- Generate unique `msg_id` for deduplication
- Use `ON CONFLICT DO NOTHING` to handle duplicates
- Store complete raw payload as JSONB
- Include both theta_user_id and external_user_id
- Return augmented data with msg_id

---

### 4.10 `is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool`

**Purpose**: Check if data has already been processed

**Scope**: Called before saving/formatting to avoid duplicates

**Implementation**:
```python
async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
    """
    Check if data is already processed
    
    Args:
        raw_data: Raw data to check
        
    Returns:
        True if already processed, False otherwise
    """
    # Most providers can rely on database constraints (msg_id uniqueness)
    # Return False to let database handle deduplication
    return False
    
    # Alternative: Query database for existing msg_id
    # try:
    #     msg_id = raw_data.get("msg_id")
    #     if not msg_id:
    #         return False
    #     
    #     query = f"""
    #         SELECT EXISTS(
    #             SELECT 1 FROM health_data_{self.info.slug.replace('theta_', '')}
    #             WHERE msg_id = :msg_id
    #         ) as exists
    #     """
    #     result = await execute_query(query=query, params={"msg_id": msg_id})
    #     return result[0]["exists"] if result else False
    # except Exception as e:
    #     logging.error(f"Error checking if data processed: {str(e)}")
    #     return False
```

**Key Points**:
- Simple return False if using database constraints
- Implement explicit checks only if needed
- Log errors but don't fail the pipeline

---

### 4.11 `_pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool`

**Purpose**: Unified pull and push workflow for a single user

**Scope**: Called after OAuth linking or by scheduled tasks

**Implementation**:
```python
async def _pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool:
    """
    Pull and push data for a specific user
    
    Args:
        credentials: Dict containing:
            - user_id: User identifier
            - access_token: OAuth access token
            - refresh_token: OAuth refresh token (OAuth2 only)
            
    Returns:
        True if successful, False otherwise
    """
    try:
        user_id = credentials.get("user_id")
        if not user_id:
            logging.error("Missing user_id in credentials")
            return False
        
        # For OAuth2: Ensure token is valid (refresh if needed)
        if self.info.auth_type == LinkType.OAUTH2:
            access_token = await self.get_valid_access_token(user_id)
            if not access_token:
                logging.error(f"Unable to get valid token for user {user_id}")
                return False
            
            # Get latest credentials (may have updated refresh token)
            credentials = await self.db_service.get_user_credentials(
                user_id, self.info.slug, self.info.auth_type
            )
            if not credentials:
                logging.error(f"Unable to get latest credentials for user {user_id}")
                return False
        else:
            # OAuth1: Extract tokens from credentials
            access_token = credentials.get("access_token")
            token_secret = credentials.get("access_token_secret")
            if not access_token or not token_secret:
                logging.error(f"Invalid OAuth1 credentials for user {user_id}")
                return False
        
        # Pull data from vendor API
        raw_data_list = await self.pull_from_vendor_api(
            access_token,
            credentials.get("refresh_token"),
            days=2  # Pull last 2 days
        )
        
        if not raw_data_list:
            logging.info(f"No data pulled for user {user_id}")
            return True
        
        # Push data through the pipeline
        success_count = 0
        error_count = 0
        
        for raw_data in raw_data_list:
            try:
                # Add user_id to raw data
                raw_data["user_id"] = user_id
                
                # Check if already processed (optional)
                if await self.is_data_already_processed(raw_data):
                    continue
                
                # Push to data pipeline
                msg_id = str(uuid.uuid4())
                push_success = await push_service.push_data(
                    platform="theta",
                    provider_slug=self.info.slug,
                    data=raw_data,
                    msg_id=msg_id,
                )
                
                if push_success:
                    success_count += 1
                else:
                    error_count += 1
                    logging.error(
                        f"Failed to push data for user {user_id} "
                        f"with msg_id {msg_id}"
                    )
                    
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing data for user {user_id}: {str(e)}")
                continue
        
        logging.info(
            f"Processed data for user {user_id}: "
            f"success={success_count}, errors={error_count}"
        )
        return error_count == 0
        
    except Exception as e:
        logging.error(f"Error in _pull_and_push_for_user: {str(e)}")
        return False

async def get_valid_access_token(self, user_id: str) -> Optional[str]:
    """
    Get valid access token, refreshing if necessary (OAuth2 only)
    
    Args:
        user_id: User identifier
        
    Returns:
        Valid access token or None
    """
    try:
        # Get stored credentials
        credentials = await self.db_service.get_user_credentials(
            user_id, self.info.slug, self.info.auth_type
        )
        if not credentials:
            return None
        
        access_token = credentials.get("access_token")
        refresh_token = credentials.get("refresh_token")
        expires_at = credentials.get("expires_at")
        
        if not access_token:
            return None
        
        # Check if token is expired
        current_time = int(time.time())
        
        if isinstance(expires_at, datetime):
            expires_at = int(expires_at.timestamp())
        elif expires_at:
            expires_at = int(expires_at)
        
        # Token still valid
        if expires_at and current_time < expires_at:
            logging.info(
                f"Token valid for user {user_id}, "
                f"expires in {expires_at - current_time}s"
            )
            return access_token
        
        # Token expired, try to refresh
        if not refresh_token:
            logging.error(f"No refresh token for user {user_id}")
            return None
        
        logging.info(f"Refreshing token for user {user_id}")
        
        # Refresh token
        async with aiohttp.ClientSession() as session:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            
            async with session.post(
                self.token_url, data=data, headers=headers
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logging.error(f"Token refresh failed: {resp.status} - {text}")
                    # Clean up invalid credentials
                    await self.db_service.delete_user_theta_provider(
                        user_id, self.info.slug
                    )
                    return None
                
                token_data = await resp.json()
        
        new_access_token = token_data.get("access_token")
        new_refresh_token = token_data.get("refresh_token", refresh_token)
        expires_in = token_data.get("expires_in")
        
        if not new_access_token:
            return None
        
        # Calculate new expiry
        new_expires_at = None
        if expires_in:
            new_expires_at = int(time.time()) + int(expires_in)
        
        # Save updated credentials
        await self.db_service.save_oauth2_credentials(
            user_id, self.info.slug, new_access_token, new_refresh_token, new_expires_at
        )
        
        logging.info(f"Successfully refreshed token for user {user_id}")
        return new_access_token
        
    except Exception as e:
        logging.error(f"Error getting valid token for user {user_id}: {str(e)}")
        return None
```

**Key Points**:
- Handle token refresh for OAuth2
- Pull recent data (e.g., last 2 days)
- Push data through the pipeline asynchronously
- Track success/error counts
- Clean up invalid credentials on auth failure

---

## 5. Data Flow & Scopes

### Overall Data Flow

```
1. User Initiates Link
   ↓
2. link() → Generate OAuth URL
   ↓
3. User Authorizes at Vendor
   ↓
4. callback() → Exchange for tokens, save credentials
   ↓
5. _pull_and_push_for_user() → Immediate data pull
   ↓
6. pull_from_vendor_api() → Fetch raw data
   ↓
7. push_service.push_data() → Push to pipeline
   ↓
8. save_raw_data_to_db() → Store raw data
   ↓
9. format_data() → Transform to standard format
   ↓
10. Data Upload Service → Upload to platform
```

### Scope Definitions

#### Public Methods (Called by Framework)
- `create_provider()`: Class method, called during provider registration
- `info`: Property, accessed for routing and display
- `link()`: Endpoint-triggered by user action
- `callback()`: Endpoint-triggered by OAuth redirect
- `unlink()`: Endpoint-triggered by user action
- `format_data()`: Pipeline-triggered for data transformation
- `save_raw_data_to_db()`: Pipeline-triggered before formatting
- `is_data_already_processed()`: Pipeline-triggered for deduplication

#### Private/Internal Methods
- `_pull_and_push_for_user()`: Internal, triggered after linking or by scheduler
- `pull_from_vendor_api()`: Internal, called by _pull_and_push_for_user
- `_process_*_data()`: Internal helpers for format_data
- `_fetch_paginated_data()`: Internal helper for API calls
- `get_valid_access_token()`: Internal, OAuth2 token management
- `_generate_authorization_url()`: Internal, OAuth flow helper
- `_handle_oauth_callback()`: Internal, OAuth flow helper

### Threading & Concurrency

- All methods are `async` and use `await` for I/O operations
- Use `asyncio.create_task()` to trigger background jobs
- Use `asyncio.gather()` for concurrent API calls
- Use `asyncio.Semaphore()` to limit concurrent requests

---

## 6. Testing Your Provider

### Unit Testing

Create `test_provider_<provider>.py`:

```python
import pytest
from connect.theta.mirobody_<provider>.provider_<provider> import ThetaYourProvider

@pytest.fixture
def provider():
    """Create provider instance for testing"""
    return ThetaYourProvider()

@pytest.mark.asyncio
async def test_provider_info(provider):
    """Test provider metadata"""
    info = provider.info
    assert info.slug == "theta_<provider>"
    assert info.auth_type in [LinkType.OAUTH1, LinkType.OAUTH2]
    assert info.supported is True

@pytest.mark.asyncio
async def test_link_generates_url(provider):
    """Test OAuth link generation"""
    class MockRequest:
        user_id = "test_user_123"
        options = {}
    
    result = await provider.link(MockRequest())
    assert "link_web_url" in result
    assert result["link_web_url"].startswith("https://")

@pytest.mark.asyncio
async def test_format_data_empty(provider):
    """Test format_data with empty input"""
    raw_data = {"user_id": "test_user", "data": []}
    result = await provider.format_data(raw_data)
    assert result.metaInfo.userId == "test_user"
    assert len(result.healthData) == 0

@pytest.mark.asyncio
async def test_format_data_with_samples(provider):
    """Test format_data with sample data"""
    raw_data = {
        "user_id": "test_user",
        "data_type": "sleeps",
        "data": [
            {
                "start": "2024-01-01T00:00:00Z",
                "score": {"sleep_duration": 28800}
            }
        ]
    }
    result = await provider.format_data(raw_data)
    assert result.metaInfo.userId == "test_user"
    assert len(result.healthData) > 0
```

### Integration Testing

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_oauth_flow():
    """Test complete OAuth flow (requires test credentials)"""
    provider = ThetaYourProvider()
    
    # 1. Test link
    class MockRequest:
        user_id = "integration_test_user"
        options = {}
    
    link_result = await provider.link(MockRequest())
    assert "link_web_url" in link_result
    
    # 2. Simulate callback (requires manual authorization)
    # This part typically requires manual intervention or mocking
    
    # 3. Test data pull
    # credentials = await provider.db_service.get_user_credentials(...)
    # raw_data = await provider.pull_from_vendor_api(...)
    # assert len(raw_data) > 0

@pytest.mark.integration
@pytest.mark.asyncio
async def test_data_pipeline():
    """Test data pull, save, and format"""
    provider = ThetaYourProvider()
    
    # Mock credentials
    credentials = {
        "user_id": "test_user",
        "access_token": "test_token",
        "refresh_token": "test_refresh"
    }
    
    # Pull data (mocked response)
    with patch.object(provider, 'pull_from_vendor_api') as mock_pull:
        mock_pull.return_value = [
            {
                "user_id": "test_user",
                "data_type": "sleeps",
                "data": [{"start": "2024-01-01T00:00:00Z"}]
            }
        ]
        
        # Save to DB
        raw_data = mock_pull.return_value[0]
        saved = await provider.save_raw_data_to_db(raw_data)
        assert len(saved) == 1
        assert "msg_id" in saved[0]
        
        # Format data
        formatted = await provider.format_data(saved[0])
        assert formatted.metaInfo.userId == "test_user"
```

### Manual Testing

1. **OAuth Flow**:
   ```bash
   # Start the server
   python main.py
   
   # Navigate to link endpoint
   # http://localhost:8000/api/v1/pulse/theta/<provider>/link?user_id=test_user
   
   # Complete OAuth authorization
   # Verify callback is handled correctly
   ```

2. **Data Pull**:
   ```python
   # In Python console
   from connect.theta.mirobody_<provider>.provider_<provider> import ThetaYourProvider
   
   provider = ThetaYourProvider()
   
   # Pull data
   import asyncio
   raw_data = asyncio.run(
       provider.pull_from_vendor_api("your_access_token", "refresh_token", days=1)
   )
   print(f"Pulled {len(raw_data)} data sets")
   ```

3. **Data Formatting**:
   ```python
   # Format sample data
   sample_data = {
       "user_id": "test_user",
       "data_type": "sleeps",
       "data": [...]
   }
   
   formatted = asyncio.run(provider.format_data(sample_data))
   print(f"Formatted {len(formatted.healthData)} records")
   ```

### Testing Checklist

- [ ] Provider can be instantiated with valid config
- [ ] Provider returns correct metadata via `info`
- [ ] `link()` generates valid OAuth URL
- [ ] `callback()` successfully exchanges tokens
- [ ] Credentials are saved to database correctly
- [ ] `unlink()` removes credentials from database
- [ ] `pull_from_vendor_api()` fetches data successfully
- [ ] `save_raw_data_to_db()` persists raw data
- [ ] `format_data()` transforms data correctly
- [ ] All StandardIndicators are mapped correctly
- [ ] Timezones are handled properly
- [ ] Rate limiting is handled gracefully
- [ ] Token refresh works (OAuth2)
- [ ] Error handling is robust
- [ ] Logging is comprehensive

---

## 7. Best Practices

### Cttonfiguration Management

1. **Use Environment-Specific Configs**:
   ```yaml
   # config.yaml (development)
   <PROVIDER>_CLIENT_ID: "dev_client_id"
   
   # config.production.yaml
   <PROVIDER>_CLIENT_ID: "prod_client_id"
   ```

2. **Validate Configuration on Startup**:
   ```python
   def __init__(self):
       super().__init__()
       self.client_id = safe_read_cfg("<PROVIDER>_CLIENT_ID")
       if not self.client_id:
           logging.error("Missing <PROVIDER>_CLIENT_ID")
   ```

3. **Provide Sensible Defaults**:
   ```python
   self.api_base_url = safe_read_cfg("<PROVIDER>_API_BASE_URL") or \
                       "https://api.<provider>.com/v1"
   ```

### Error Handling

1. **Log All Errors with Context**:
   ```python
   try:
       result = await self.some_operation()
   except Exception as e:
       logging.error(
           f"Error in some_operation for user {user_id}: {str(e)}",
           exc_info=True  # Include stack trace
       )
   ```

2. **Fail Gracefully**:
   ```python
   # Don't raise exceptions in create_provider
   @classmethod
   def create_provider(cls, config):
       try:
           return cls()
       except Exception as e:
           logging.warning(f"Failed to create provider: {e}")
           return None  # Return None instead of raising
   ```

3. **Retry Transient Errors**:
   ```python
   retry_count = 0
   max_retries = 3
   while retry_count <= max_retries:
       try:
           result = await self.api_call()
           break
       except TransientError:
           retry_count += 1
           await asyncio.sleep(2 ** retry_count)  # Exponential backoff
   ```

### Data Mapping

1. **Use Configuration-Driven Mapping**:
   ```python
   DATA_MAPPING = {
       "data_type": {
           "vendor_field": (
               StandardIndicator.NAME.value.name,
               lambda x: x * conversion_factor,
               StandardIndicator.NAME.value.standard_unit
           )
       }
   }
   ```

2. **Handle Missing Fields**:
   ```python
   value = item.get("field")
   if value is None:
       continue  # Skip missing fields, don't error
   ```

3. **Validate Data Types**:
   ```python
   try:
       value = float(raw_value)
   except (ValueError, TypeError):
       logging.warning(f"Invalid value: {raw_value}")
       continue
   ```

### Performance Optimization

1. **Use Concurrent Requests**:
   ```python
   semaphore = asyncio.Semaphore(5)  # Limit concurrency
   
   async def fetch_one(item_id):
       async with semaphore:
           return await self.api_call(item_id)
   
   tasks = [fetch_one(id) for id in item_ids]
   results = await asyncio.gather(*tasks)
   ```

2. **Implement Pagination**:
   ```python
   all_records = []
   next_token = None
   
   while True:
       page = await self.fetch_page(next_token)
       all_records.extend(page["records"])
       next_token = page.get("next_token")
       if not next_token:
           break
   ```

3. **Cache Expensive Operations**:
   ```python
   # Use Redis for temporary caching
   cached = await redis_client.get(f"cache:{key}")
   if cached:
       return json.loads(cached)
   
   result = await expensive_operation()
   await redis_client.setex(f"cache:{key}", 3600, json.dumps(result))
   return result
   ```

### Security

1. **Never Log Sensitive Data**:
   ```python
   # BAD
   logging.info(f"Token: {access_token}")
   
   # GOOD
   logging.info(f"Token: {access_token[:10]}...")
   ```

2. **Validate Input**:
   ```python
   if not user_id or not isinstance(user_id, str):
       raise ValueError("Invalid user_id")
   ```

3. **Use Secure Token Storage**:
   ```python
   # Tokens are stored encrypted in database
   await self.db_service.save_oauth2_credentials(...)
   ```

### Monitoring & Observability

1. **Track Key Metrics**:
   ```python
   processing_info = {
       "start_time": time.time(),
       "processed_indicators": 0,
       "skipped_indicators": 0,
       "errors": [],
   }
   
   # Update throughout processing
   processing_info["processed_indicators"] += 1
   
   # Log summary
   logging.info(
       f"Processed {processing_info['processed_indicators']} indicators, "
       f"skipped {processing_info['skipped_indicators']}"
   )
   ```

2. **Use Structured Logging**:
   ```python
   logging.info(
       "Data pull completed",
       extra={
           "user_id": user_id,
           "provider": self.info.slug,
           "record_count": len(raw_data),
           "duration_ms": duration,
       }
   )
   ```

3. **Implement Health Checks**:
   ```python
   async def health_check(self) -> bool:
       """Check if provider is healthy"""
       try:
           # Test configuration
           if not self.client_id:
               return False
           
           # Test API connectivity (optional)
           async with aiohttp.ClientSession() as session:
               async with session.get(
                   f"{self.api_base_url}/health",
                   timeout=aiohttp.ClientTimeout(total=5)
               ) as resp:
                   return resp.status == 200
       except Exception:
           return False
   ```

---

## Appendix A: StandardIndicator Reference

Common indicators you'll map to:

### Activity Indicators
- `STEPS`: Step count
- `DISTANCE`: Distance traveled (meters)
- `CALORIES_ACTIVE`: Active calories burned
- `CALORIES_BASAL`: Basal metabolic rate calories

### Heart Rate Indicators
- `HEART_RATE`: Heart rate (bpm)
- `HEART_RATE_MAX`: Maximum heart rate
- `RESTING_HEART_RATE`: Resting heart rate
- `HRV_RMSSD`: Heart rate variability (RMSSD in ms)

### Sleep Indicators
- `SLEEP_IN_BED`: Time in bed (milliseconds)
- `SLEEP_ANALYSIS_AWAKE`: Time awake during sleep (ms)
- `SLEEP_ANALYSIS_ASLEEP_CORE`: Light sleep time (ms)
- `SLEEP_ANALYSIS_ASLEEP_DEEP`: Deep sleep time (ms)
- `SLEEP_ANALYSIS_ASLEEP_REM`: REM sleep time (ms)
- `SLEEP_EFFICIENCY`: Sleep efficiency (percentage)
- `SLEEP_DISTURBANCES`: Number of sleep disturbances

### Body Metrics
- `WEIGHT`: Body weight (kg)
- `HEIGHT`: Height (meters)
- `BMI`: Body mass index
- `BODY_FAT_PERCENTAGE`: Body fat percentage
- `BLOOD_OXYGEN`: SpO2 percentage

### Workout Indicators
- `WORKOUT_DURATION_LOW`: Low intensity duration (minutes)
- `WORKOUT_DURATION_MEDIUM`: Medium intensity duration (minutes)
- `WORKOUT_DURATION_HIGH`: High intensity duration (minutes)
- `ALTITUDE_GAIN`: Altitude gained (meters)
- `SPEED`: Average speed (m/s)

---

## Appendix B: Common Issues & Solutions

### Issue: OAuth Callback Not Working

**Symptoms**: Callback returns 404 or fails silently

**Solutions**:
1. Verify redirect URL matches exactly in vendor dashboard
2. Check Redis connectivity for state storage
3. Ensure callback route is registered in main.py
4. Verify state parameter is URL-encoded properly

### Issue: Token Expired During Data Pull

**Symptoms**: 401 errors from vendor API

**Solutions**:
1. Implement token refresh (OAuth2)
2. Check `expires_at` calculation
3. Verify refresh token is saved correctly
4. Test `get_valid_access_token()` method

### Issue: Data Not Formatting Correctly

**Symptoms**: Empty health records or missing indicators

**Solutions**:
1. Check data mapping configuration
2. Verify field names match vendor API response
3. Log raw data to inspect structure
4. Test converter functions separately
5. Ensure timezone is retrieved correctly

### Issue: Rate Limiting

**Symptoms**: 429 errors from vendor API

**Solutions**:
1. Implement exponential backoff
2. Respect Retry-After header
3. Reduce concurrent request limit
4. Cache frequently accessed data

### Issue: Duplicate Data

**Symptoms**: Same data processed multiple times

**Solutions**:
1. Ensure msg_id is unique and consistent
2. Add database constraint: `UNIQUE(msg_id)`
3. Implement `is_data_already_processed()` check
4. Use `ON CONFLICT DO NOTHING` in insert queries

---

## Support & Resources

- **Internal Documentation**: `/docs/providers/`
- **Example Providers**:
   - Garmin: `connect/theta/mirobody_garmin_connect/provider_garmin.py`
   - Whoop: `connect/theta/mirobody_whoop/provider_whoop.py`
- **Testing Guide**: `/docs/testing/provider_testing.md`
- **API Reference**: `/docs/api/pulse_api.md`

For questions or assistance, contact the platform team or create an issue in the repository.

---

**Document Version**: 1.0  
**Last Updated**: December 2024  
**Author**: Platform Team
