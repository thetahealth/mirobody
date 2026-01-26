# 🗄️ Database Structure & Initialization

Mirobody uses PostgreSQL with the `theta_ai` schema.

## 🏗️ Initialization Process

Database initialization is **automatic** for local and development environments.

1.  **Trigger**: When the server starts (`mirobody/server/server.py`).
2.  **Condition**: The `ENV` environment variable is **NOT** `TEST`, `GRAY`, or `PROD`.
3.  **Action**: The server executes all SQL files found in `mirobody/res/sql/` in alphabetical order.

### Bootstrap Files (`mirobody/res/sql/`)
- `00_init_schema.sql`: Creates extensions, schema, and base tables.
- `01_basedata.sql`: Inserts static dictionary data.
- `02_settings.sql`: application settings.
- ...and other migration scripts.

## 🧩 Schema Overview

All tables reside in the `theta_ai` schema.

### Extensions
The following PostgreSQL extensions are enabled:
- **`vector`**: For AI embeddings and semantic search.
- **`pg_trgm`**: For fast text similarity search.
- **`pgcrypto`**: For cryptographic functions.

### Core Tables

#### User & Auth
- **`health_app_user`**: Main user profile table.
- **`health_user_provider`**: Stores connection info for external providers (Google, Apple, etc.).

#### Data Sharing
- **`th_share_relationship`**: Tracks who shares data with whom.
- **`th_share_permission_type`**: Defines granular permissions (e.g., "All Data", "Device Data").

#### Health Data
- **`health_data_{provider}`**: Raw data storage for specific providers (e.g., `health_data_garmin`).
- **`th_task_flow`**: Tracks data processing tasks and status.

## 🛠️ Manual Initialization

If you need to manually initialize the database (e.g., for production):

1.  Ensure the database exists.
2.  Run the SQL files in order using `psql`:

```bash
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/res/sql/00_init_schema.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/res/sql/01_basedata.sql
# ... run remaining files
```
