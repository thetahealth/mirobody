---
name: qa-regression
description: Automate QA regression testing with reusable test skills. Create login flows, dashboard checks, user creation, and other common test scenarios that run consistently.
license: MIT
---

# QA Regression Testing

Build and run automated regression tests using Playwright. Each test is a reusable skill that can be composed into full test suites.

## Setup

```bash
npm init -y
npm install playwright @playwright/test
npx playwright install
```

## Test Structure

Create tests in `tests/` folder:

```
tests/
├── auth/
│   ├── login.spec.ts
│   └── logout.spec.ts
├── dashboard/
│   └── load.spec.ts
├── users/
│   ├── create.spec.ts
│   └── delete.spec.ts
└── regression.spec.ts   # Full suite
```

## Common Test Skills

### Login Test

```typescript
// tests/auth/login.spec.ts
import { test, expect } from '@playwright/test';

test.describe('Login Flow', () => {
  test('should login with valid credentials', async ({ page }) => {
    await page.goto('/login');

    await page.fill('[data-testid="email"]', process.env.TEST_EMAIL!);
    await page.fill('[data-testid="password"]', process.env.TEST_PASSWORD!);
    await page.click('[data-testid="submit"]');

    // Verify redirect to dashboard
    await expect(page).toHaveURL(/dashboard/);
    await expect(page.locator('[data-testid="user-menu"]')).toBeVisible();
  });

  test('should show error for invalid credentials', async ({ page }) => {
    await page.goto('/login');

    await page.fill('[data-testid="email"]', 'wrong@example.com');
    await page.fill('[data-testid="password"]', 'wrongpassword');
    await page.click('[data-testid="submit"]');

    await expect(page.locator('[data-testid="error-message"]')).toBeVisible();
  });
});
```

### Dashboard Load Test

```typescript
// tests/dashboard/load.spec.ts
import { test, expect } from '@playwright/test';
import { login } from '../helpers/auth';

test.describe('Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('should load dashboard within 3 seconds', async ({ page }) => {
    const start = Date.now();
    await page.goto('/dashboard');
    await page.waitForSelector('[data-testid="dashboard-content"]');
    const loadTime = Date.now() - start;

    expect(loadTime).toBeLessThan(3000);
  });

  test('should display all widgets', async ({ page }) => {
    await page.goto('/dashboard');

    await expect(page.locator('[data-testid="stats-widget"]')).toBeVisible();
    await expect(page.locator('[data-testid="chart-widget"]')).toBeVisible();
    await expect(page.locator('[data-testid="activity-widget"]')).toBeVisible();
  });

  test('should refresh data on button click', async ({ page }) => {
    await page.goto('/dashboard');

    const initialValue = await page.locator('[data-testid="last-updated"]').textContent();
    await page.click('[data-testid="refresh-button"]');
    await page.waitForTimeout(1000);
    const newValue = await page.locator('[data-testid="last-updated"]').textContent();

    expect(newValue).not.toBe(initialValue);
  });
});
```

### Create User Test

```typescript
// tests/users/create.spec.ts
import { test, expect } from '@playwright/test';
import { login } from '../helpers/auth';
import { generateTestUser, deleteTestUser } from '../helpers/users';

test.describe('User Creation', () => {
  let testUser: { email: string; name: string };

  test.beforeEach(async ({ page }) => {
    await login(page);
    testUser = generateTestUser();
  });

  test.afterEach(async () => {
    // Cleanup
    await deleteTestUser(testUser.email);
  });

  test('should create new user successfully', async ({ page }) => {
    await page.goto('/users/new');

    await page.fill('[data-testid="user-name"]', testUser.name);
    await page.fill('[data-testid="user-email"]', testUser.email);
    await page.selectOption('[data-testid="user-role"]', 'member');
    await page.click('[data-testid="create-user-btn"]');

    // Verify success
    await expect(page.locator('[data-testid="success-toast"]')).toBeVisible();
    await expect(page).toHaveURL(/users/);

    // Verify user appears in list
    await expect(page.locator(`text=${testUser.email}`)).toBeVisible();
  });

  test('should validate required fields', async ({ page }) => {
    await page.goto('/users/new');
    await page.click('[data-testid="create-user-btn"]');

    await expect(page.locator('[data-testid="name-error"]')).toBeVisible();
    await expect(page.locator('[data-testid="email-error"]')).toBeVisible();
  });
});
```

## Shared Helpers

```typescript
// tests/helpers/auth.ts
import { Page } from '@playwright/test';

export async function login(page: Page) {
  await page.goto('/login');
  await page.fill('[data-testid="email"]', process.env.TEST_EMAIL!);
  await page.fill('[data-testid="password"]', process.env.TEST_PASSWORD!);
  await page.click('[data-testid="submit"]');
  await page.waitForURL(/dashboard/);
}

export async function logout(page: Page) {
  await page.click('[data-testid="user-menu"]');
  await page.click('[data-testid="logout"]');
  await page.waitForURL(/login/);
}
```

```typescript
// tests/helpers/users.ts
export function generateTestUser() {
  const id = Date.now();
  return {
    name: `Test User ${id}`,
    email: `test-${id}@example.com`,
  };
}

export async function deleteTestUser(email: string) {
  // API call to cleanup test user
  await fetch(`${process.env.API_URL}/admin/users`, {
    method: 'DELETE',
    headers: {
      'Authorization': `Bearer ${process.env.ADMIN_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ email }),
  });
}
```

## Full Regression Suite

```typescript
// tests/regression.spec.ts
import { test } from '@playwright/test';

// Import all test suites
import './auth/login.spec';
import './auth/logout.spec';
import './dashboard/load.spec';
import './users/create.spec';
import './users/delete.spec';

test.describe('Full Regression Suite', () => {
  // Tests run in order defined above
});
```

## Playwright Config

```typescript
// playwright.config.ts
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [
    ['html'],
    ['json', { outputFile: 'test-results.json' }],
  ],
  use: {
    baseURL: process.env.BASE_URL || 'http://localhost:3000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
  ],
});
```

## Running Tests

```bash
# Run all tests
npx playwright test

# Run specific test file
npx playwright test tests/auth/login.spec.ts

# Run tests with UI
npx playwright test --ui

# Run in headed mode (see browser)
npx playwright test --headed

# Generate report
npx playwright show-report
```

## CI Integration

```yaml
# .github/workflows/regression.yml
name: Regression Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Install dependencies
        run: npm ci

      - name: Install Playwright
        run: npx playwright install --with-deps

      - name: Run tests
        run: npx playwright test
        env:
          BASE_URL: ${{ secrets.STAGING_URL }}
          TEST_EMAIL: ${{ secrets.TEST_EMAIL }}
          TEST_PASSWORD: ${{ secrets.TEST_PASSWORD }}

      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: playwright-report
          path: playwright-report/
```

## Best Practices

1. **Use data-testid attributes** - More stable than CSS selectors
2. **Clean up test data** - Always delete what you create
3. **Avoid hardcoded waits** - Use `waitForSelector` instead of `waitForTimeout`
4. **Run in parallel** - Faster feedback on CI
5. **Screenshot on failure** - Easier debugging
6. **Environment variables** - Never commit credentials

## Quick Commands

| Task | Command |
|------|---------|
| Run all | `npx playwright test` |
| Run one file | `npx playwright test login.spec.ts` |
| Debug mode | `npx playwright test --debug` |
| UI mode | `npx playwright test --ui` |
| Update snapshots | `npx playwright test --update-snapshots` |
