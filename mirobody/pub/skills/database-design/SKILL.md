---
name: database-design
description: Database schema design, optimization, and migration patterns for PostgreSQL, MySQL, and NoSQL databases. Use for designing schemas, writing migrations, or optimizing queries.
source: wshobson/agents
license: MIT
---

# Database Design

## Schema Design Principles

### Normalization Guidelines
```sql
-- 1NF: Atomic values, no repeating groups
-- 2NF: No partial dependencies on composite keys
-- 3NF: No transitive dependencies

-- Users table (normalized)
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Addresses table (separate entity)
CREATE TABLE addresses (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
  street VARCHAR(255),
  city VARCHAR(100),
  country VARCHAR(100),
  is_primary BOOLEAN DEFAULT false
);
```

### Denormalization for Performance
```sql
-- When read performance matters more than write consistency
CREATE TABLE order_summaries (
  id SERIAL PRIMARY KEY,
  order_id INTEGER REFERENCES orders(id),
  customer_name VARCHAR(255),  -- Denormalized from customers
  total_amount DECIMAL(10,2),
  item_count INTEGER,
  last_updated TIMESTAMPTZ DEFAULT NOW()
);
```

## Index Design

### Common Index Patterns
```sql
-- B-tree (default) for equality and range queries
CREATE INDEX idx_users_email ON users(email);

-- Composite index (order matters!)
CREATE INDEX idx_orders_user_date ON orders(user_id, created_at DESC);

-- Partial index for specific conditions
CREATE INDEX idx_active_users ON users(email) WHERE deleted_at IS NULL;

-- GIN index for array/JSONB columns
CREATE INDEX idx_posts_tags ON posts USING GIN(tags);

-- Covering index (includes additional columns)
CREATE INDEX idx_orders_covering ON orders(user_id) INCLUDE (total, status);
```

### Index Analysis
```sql
-- Check index usage
SELECT
  schemaname, tablename, indexname,
  idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- Find missing indexes
SELECT
  relname, seq_scan, seq_tup_read,
  idx_scan, idx_tup_fetch
FROM pg_stat_user_tables
WHERE seq_scan > idx_scan
ORDER BY seq_tup_read DESC;
```

## Migration Patterns

### Safe Migration Template
```sql
-- Always use transactions
BEGIN;

-- Add column with default (non-blocking in PG 11+)
ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active';

-- Create index concurrently (doesn't lock table)
CREATE INDEX CONCURRENTLY idx_users_status ON users(status);

-- Backfill data in batches
UPDATE users SET status = 'active' WHERE status IS NULL AND id BETWEEN 1 AND 10000;

COMMIT;
```

### Zero-Downtime Migrations
```
1. Add new column (nullable)
2. Deploy code that writes to both columns
3. Backfill old data
4. Deploy code that reads from new column
5. Remove old column
```

## Query Optimization

### EXPLAIN Analysis
```sql
-- Always use EXPLAIN ANALYZE
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM orders WHERE user_id = 123 AND status = 'pending';

-- Key metrics to watch:
-- - Seq Scan vs Index Scan
-- - Actual rows vs Estimated rows
-- - Buffers: shared hit vs read
```

### Common Optimizations
```sql
-- Use EXISTS instead of IN for large sets
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- Pagination with keyset (cursor) instead of OFFSET
SELECT * FROM posts
WHERE created_at < '2024-01-01'
ORDER BY created_at DESC
LIMIT 20;

-- Use CTEs for complex queries
WITH active_users AS (
  SELECT id FROM users WHERE last_login > NOW() - INTERVAL '30 days'
)
SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users);
```

## Constraints & Data Integrity

```sql
-- Primary key
ALTER TABLE users ADD PRIMARY KEY (id);

-- Foreign key with cascade
ALTER TABLE orders ADD CONSTRAINT fk_orders_user
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;

-- Check constraint
ALTER TABLE products ADD CONSTRAINT chk_price_positive
  CHECK (price >= 0);

-- Unique constraint
ALTER TABLE users ADD CONSTRAINT uniq_users_email UNIQUE (email);

-- Exclusion constraint (no overlapping ranges)
ALTER TABLE reservations ADD CONSTRAINT excl_no_overlap
  EXCLUDE USING gist (room_id WITH =, tsrange(start_time, end_time) WITH &&);
```

## Best Practices

- Use UUIDs for public-facing IDs, SERIAL/BIGSERIAL for internal
- Always add `created_at` and `updated_at` timestamps
- Use soft deletes (`deleted_at`) for important data
- Design for eventual consistency in distributed systems
- Document schema decisions in migration files
- Test migrations on production-size data before deploying
