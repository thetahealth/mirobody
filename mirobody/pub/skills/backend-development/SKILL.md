---
name: backend-development
description: Backend API design, database architecture, microservices patterns, and test-driven development. Use for designing APIs, database schemas, or backend system architecture.
source: wshobson/agents
license: MIT
---

# Backend Development

## API Design

### RESTful Conventions
```
GET    /users          # List users
POST   /users          # Create user
GET    /users/:id      # Get user
PUT    /users/:id      # Update user (full)
PATCH  /users/:id      # Update user (partial)
DELETE /users/:id      # Delete user

GET    /users/:id/posts  # List user's posts
POST   /users/:id/posts  # Create post for user
```

### Response Format
```json
{
  "data": { ... },
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 100
  }
}
```

### Error Format
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input",
    "details": [
      { "field": "email", "message": "Invalid format" }
    ]
  }
}
```

## Database Patterns

### Schema Design
```sql
-- Use UUIDs for public IDs
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  public_id UUID DEFAULT gen_random_uuid() UNIQUE,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Soft deletes
ALTER TABLE users ADD COLUMN deleted_at TIMESTAMPTZ;

-- Indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created ON users(created_at DESC);
```

### Query Patterns
```sql
-- Pagination with cursor
SELECT * FROM posts
WHERE created_at < $cursor
ORDER BY created_at DESC
LIMIT 20;

-- Efficient counting
SELECT reltuples::bigint AS estimate
FROM pg_class WHERE relname = 'users';
```

## Authentication

### JWT Pattern
```typescript
interface TokenPayload {
  sub: string;      // User ID
  iat: number;      // Issued at
  exp: number;      // Expiration
  scope: string[];  // Permissions
}

function verifyToken(token: string): TokenPayload {
  return jwt.verify(token, SECRET) as TokenPayload;
}
```

### Middleware
```typescript
async function authenticate(req: Request, res: Response, next: Next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    req.user = verifyToken(token);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
}
```

## Caching Strategy

```typescript
// Cache-aside pattern
async function getUser(id: string): Promise<User> {
  const cached = await redis.get(`user:${id}`);
  if (cached) return JSON.parse(cached);

  const user = await db.users.findById(id);
  await redis.setex(`user:${id}`, 3600, JSON.stringify(user));
  return user;
}

// Cache invalidation
async function updateUser(id: string, data: Partial<User>) {
  await db.users.update(id, data);
  await redis.del(`user:${id}`);
}
```

## Rate Limiting

```typescript
const limiter = rateLimit({
  windowMs: 60 * 1000,  // 1 minute
  max: 100,             // 100 requests per window
  keyGenerator: (req) => req.ip,
  handler: (req, res) => {
    res.status(429).json({ error: 'Too many requests' });
  }
});
```

## Observability

- **Logging**: Structured JSON logs with request IDs
- **Metrics**: Request latency, error rates, queue depths
- **Tracing**: Distributed tracing with correlation IDs
- **Health checks**: `/health` and `/ready` endpoints
