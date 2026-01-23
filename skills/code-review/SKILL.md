---
name: code-review
description: Automated code review for pull requests using specialized review patterns. Analyzes code for quality, security, performance, and best practices. Use when reviewing code changes, PRs, or doing code audits.
source: anthropics/claude-code
license: Apache-2.0
---

# Code Review

## Review Categories

### 1. Security Review
Check for:
- SQL injection vulnerabilities
- XSS (Cross-Site Scripting)
- Command injection
- Insecure deserialization
- Hardcoded secrets/credentials
- Improper authentication/authorization
- Insecure direct object references

### 2. Performance Review
Check for:
- N+1 queries
- Missing database indexes
- Unnecessary re-renders (React)
- Memory leaks
- Blocking operations in async code
- Missing caching opportunities
- Large bundle sizes

### 3. Code Quality Review
Check for:
- Code duplication (DRY violations)
- Functions doing too much (SRP violations)
- Deep nesting / complex conditionals
- Magic numbers/strings
- Poor naming
- Missing error handling
- Incomplete type coverage

### 4. Testing Review
Check for:
- Missing test coverage for new code
- Tests that don't test behavior
- Flaky test patterns
- Missing edge cases
- Mocked external dependencies

## Review Output Format

```markdown
## Code Review Summary

### 🔴 Critical (Must Fix)
- **[File:Line]** [Issue description]
  - **Why:** [Explanation]
  - **Fix:** [Suggested fix]

### 🟡 Suggestions (Should Consider)
- **[File:Line]** [Issue description]
  - **Why:** [Explanation]
  - **Fix:** [Suggested fix]

### 🟢 Nits (Optional)
- **[File:Line]** [Minor suggestion]

### ✅ What's Good
- [Positive feedback on good patterns]
```

## Common Patterns to Flag

### Security
```javascript
// BAD: SQL injection
const query = `SELECT * FROM users WHERE id = ${userId}`;

// GOOD: Parameterized query
const query = 'SELECT * FROM users WHERE id = $1';
await db.query(query, [userId]);
```

### Performance
```javascript
// BAD: N+1 query
users.forEach(async user => {
  const posts = await getPosts(user.id);
});

// GOOD: Batch query
const userIds = users.map(u => u.id);
const posts = await getPostsForUsers(userIds);
```

### Error Handling
```javascript
// BAD: Swallowing errors
try {
  await riskyOperation();
} catch (e) {}

// GOOD: Handle or propagate
try {
  await riskyOperation();
} catch (e) {
  logger.error('Operation failed', { error: e });
  throw new AppError('Operation failed', { cause: e });
}
```

## Review Checklist

- [ ] No hardcoded secrets
- [ ] Input validation present
- [ ] Error handling complete
- [ ] Types/interfaces defined
- [ ] Tests added for new code
- [ ] No obvious performance issues
- [ ] Code is readable and documented
- [ ] Breaking changes documented
