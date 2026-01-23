---
name: code-documentation
description: Writing effective code documentation - API docs, README files, inline comments, and technical guides. Use for documenting codebases, APIs, or writing developer guides.
source: wshobson/agents
license: MIT
---

# Code Documentation

## README Structure

### Standard README Template
```markdown
# Project Name

Brief description of what this project does.

## Quick Start

\`\`\`bash
npm install
npm run dev
\`\`\`

## Installation

Detailed installation instructions...

## Usage

\`\`\`typescript
import { something } from 'project';

// Example usage
const result = something.doThing();
\`\`\`

## API Reference

### `functionName(param: Type): ReturnType`

Description of what the function does.

**Parameters:**
- `param` - Description of parameter

**Returns:** Description of return value

**Example:**
\`\`\`typescript
const result = functionName('value');
\`\`\`

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `option1` | `string` | `'default'` | What it does |

## Contributing

How to contribute...

## License

MIT
```

## API Documentation

### JSDoc/TSDoc Style
```typescript
/**
 * Creates a new user account.
 *
 * @param userData - The user data for account creation
 * @param options - Optional configuration
 * @returns The created user object
 * @throws {ValidationError} If email is invalid
 * @example
 * ```ts
 * const user = await createUser({
 *   email: 'user@example.com',
 *   name: 'John'
 * });
 * ```
 */
async function createUser(
  userData: UserInput,
  options?: CreateOptions
): Promise<User> {
  // Implementation
}

/**
 * Configuration options for the API client.
 */
interface ClientConfig {
  /** The API base URL */
  baseUrl: string;
  /** Request timeout in milliseconds @default 5000 */
  timeout?: number;
  /** Custom headers to include in requests */
  headers?: Record<string, string>;
}
```

### OpenAPI/Swagger
```yaml
openapi: 3.0.0
info:
  title: My API
  version: 1.0.0

paths:
  /users:
    post:
      summary: Create a user
      description: Creates a new user account
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/UserInput'
      responses:
        '201':
          description: User created successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/User'
        '400':
          description: Invalid input

components:
  schemas:
    UserInput:
      type: object
      required:
        - email
        - name
      properties:
        email:
          type: string
          format: email
        name:
          type: string
    User:
      type: object
      properties:
        id:
          type: string
        email:
          type: string
        name:
          type: string
        createdAt:
          type: string
          format: date-time
```

## Inline Comments

### When to Comment
```typescript
// GOOD: Explain WHY, not WHAT

// Use binary search because the list is always sorted and
// can contain millions of items - O(log n) vs O(n)
const index = binarySearch(items, target);

// GOOD: Explain complex business logic
// Users get 20% discount if they've been members for 2+ years
// AND have made 10+ purchases (per marketing team decision Q4 2024)
if (user.memberYears >= 2 && user.purchaseCount >= 10) {
  applyDiscount(0.2);
}

// GOOD: Document workarounds
// HACK: Safari doesn't support this API, fallback to polling
// TODO: Remove when Safari adds support (tracking: webkit.org/b/12345)
if (!window.IntersectionObserver) {
  startPolling();
}
```

### When NOT to Comment
```typescript
// BAD: Stating the obvious
// Increment counter by 1
counter++;

// BAD: Explaining clear code
// Check if user is admin
if (user.role === 'admin') { ... }

// BAD: Outdated comments (worse than no comment)
// Returns the user's full name  <-- Actually returns email now!
function getUserIdentifier(user) {
  return user.email;
}
```

## Architecture Documentation

### ADR (Architecture Decision Record)
```markdown
# ADR-001: Use PostgreSQL for Primary Database

## Status
Accepted

## Context
We need a database for storing user data and transactions.
Options considered: PostgreSQL, MySQL, MongoDB, DynamoDB.

## Decision
Use PostgreSQL with Supabase hosting.

## Rationale
- Strong ACID compliance needed for financial data
- Team has PostgreSQL experience
- Supabase provides auth and realtime features
- pgvector extension for future AI features

## Consequences
- Need to manage schema migrations
- May need read replicas for scale
- Team needs to learn Supabase-specific features
```

### Component Documentation
```markdown
## Authentication Module

### Overview
Handles user authentication using JWT tokens with refresh rotation.

### Flow
1. User submits credentials to `/auth/login`
2. Server validates and returns access + refresh tokens
3. Access token used for API requests (15min expiry)
4. Refresh token used to get new access token (7d expiry)

### Dependencies
- `jsonwebtoken` - Token generation/validation
- `bcrypt` - Password hashing
- `redis` - Refresh token storage

### Configuration
- `JWT_SECRET` - Secret for signing tokens
- `ACCESS_TOKEN_EXPIRY` - Access token lifetime
- `REFRESH_TOKEN_EXPIRY` - Refresh token lifetime
```

## Documentation Principles

1. **Write for your audience** - New devs vs API consumers
2. **Keep it close to code** - Docs in same repo, near relevant code
3. **Update with code** - Stale docs are worse than none
4. **Examples over explanations** - Show, don't just tell
5. **Progressive disclosure** - Quick start first, details later
