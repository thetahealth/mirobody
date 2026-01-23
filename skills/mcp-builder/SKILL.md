---
name: mcp-builder
description: Guide for creating high-quality MCP (Model Context Protocol) servers that enable LLMs to interact with external services through well-designed tools. Use when building MCP servers to integrate external APIs or services, whether in Python (FastMCP) or Node/TypeScript (MCP SDK).
source: anthropics/skills
license: Apache-2.0
---

# MCP Server Development Guide

Create MCP servers that enable LLMs to interact with external services through well-designed tools.

## High-Level Workflow

### Phase 1: Research and Planning

**Understand Modern MCP Design:**
- Balance comprehensive API coverage with specialized workflow tools
- Use clear, descriptive tool names with consistent prefixes (e.g., `github_create_issue`)
- Design tools that return focused, relevant data
- Provide actionable error messages

**Study MCP Protocol:**
- Start with sitemap: `https://modelcontextprotocol.io/sitemap.xml`
- Key pages: specification, transport mechanisms, tool definitions

### Phase 2: Implementation

**Recommended Stack:**
- **Language**: TypeScript (best SDK support)
- **Transport**: Streamable HTTP for remote, stdio for local

**Project Structure:**
```
my-mcp-server/
├── src/
│   ├── index.ts      # Server entry point
│   ├── tools/        # Tool implementations
│   └── utils/        # Shared utilities
├── package.json
└── tsconfig.json
```

**Tool Implementation Pattern:**
```typescript
server.registerTool({
  name: "github_create_issue",
  description: "Create a new GitHub issue",
  inputSchema: z.object({
    repo: z.string().describe("Repository name (owner/repo)"),
    title: z.string().describe("Issue title"),
    body: z.string().optional().describe("Issue body")
  }),
  outputSchema: z.object({
    id: z.number(),
    url: z.string()
  }),
  annotations: {
    readOnlyHint: false,
    destructiveHint: false,
    idempotentHint: false
  },
  handler: async (input) => {
    // Implementation
    return { id: 123, url: "https://..." };
  }
});
```

### Phase 3: Test

```bash
# TypeScript
npm run build
npx @modelcontextprotocol/inspector

# Python
python -m py_compile your_server.py
```

### Phase 4: Create Evaluations

Create 10 complex, realistic questions to test your MCP server:

```xml
<evaluation>
  <qa_pair>
    <question>Find all open issues labeled 'bug' in the repo</question>
    <answer>5</answer>
  </qa_pair>
</evaluation>
```

## Tool Design Best Practices

- Use Zod (TS) or Pydantic (Python) for schemas
- Include constraints and examples in field descriptions
- Define `outputSchema` for structured data
- Support pagination where applicable
- Add tool annotations (readOnly, destructive, idempotent)
