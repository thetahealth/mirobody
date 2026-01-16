---
name: javascript-typescript
description: JavaScript and TypeScript development with ES6+, Node.js, React, and modern web frameworks. Use for frontend, backend, or full-stack JavaScript/TypeScript projects.
source: wshobson/agents
license: MIT
---

# JavaScript/TypeScript Development

## TypeScript Configuration

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitOverride": true,
    "skipLibCheck": true,
    "declaration": true,
    "outDir": "./dist"
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

## Type Patterns

### Utility Types
```typescript
// Pick specific properties
type UserPreview = Pick<User, 'id' | 'name'>;

// Omit properties
type CreateUser = Omit<User, 'id' | 'createdAt'>;

// Make all properties optional
type PartialUser = Partial<User>;

// Make all properties required
type RequiredUser = Required<User>;

// Extract union types
type Status = 'pending' | 'active' | 'inactive';
type ActiveStatus = Extract<Status, 'active' | 'pending'>;
```

### Discriminated Unions
```typescript
type Result<T> =
  | { success: true; data: T }
  | { success: false; error: Error };

function handleResult<T>(result: Result<T>) {
  if (result.success) {
    console.log(result.data); // T
  } else {
    console.error(result.error); // Error
  }
}
```

### Generic Constraints
```typescript
interface HasId {
  id: string | number;
}

function findById<T extends HasId>(items: T[], id: T['id']): T | undefined {
  return items.find(item => item.id === id);
}
```

## Modern JavaScript

### Destructuring & Spread
```javascript
const { name, ...rest } = user;
const merged = { ...defaults, ...options };
const [first, ...others] = items;
```

### Optional Chaining & Nullish Coalescing
```javascript
const city = user?.address?.city ?? 'Unknown';
const count = data?.items?.length ?? 0;
```

### Array Methods
```javascript
const adults = users.filter(u => u.age >= 18);
const names = users.map(u => u.name);
const total = items.reduce((sum, item) => sum + item.price, 0);
const hasAdmin = users.some(u => u.role === 'admin');
const allActive = users.every(u => u.active);
```

## React Patterns

```typescript
// Props with children
interface CardProps {
  title: string;
  children: React.ReactNode;
}

// Event handlers
interface ButtonProps {
  onClick: (event: React.MouseEvent<HTMLButtonElement>) => void;
}

// Custom hooks
function useLocalStorage<T>(key: string, initial: T) {
  const [value, setValue] = useState<T>(() => {
    const stored = localStorage.getItem(key);
    return stored ? JSON.parse(stored) : initial;
  });

  useEffect(() => {
    localStorage.setItem(key, JSON.stringify(value));
  }, [key, value]);

  return [value, setValue] as const;
}
```

## Node.js Patterns

```typescript
// ES Modules
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

// Error handling
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
  process.exit(1);
});
```
