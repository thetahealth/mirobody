# Agent skills

Install a skill to give a coding assistant the Mirobody workflow and commands.
These skills run in your assistant, separately from the built-in chat agent.

| Skill | Use |
| --- | --- |
| `standardize-health-data` | Use the Python library to standardize readings and read health documents. |
| `mirobody` | Run the application and connect an assistant through MCP. |

## Install

```bash
npx skills add thetahealth/mirobody --skill standardize-health-data
npx skills add thetahealth/mirobody --skill mirobody
```

Library tasks require Python 3.12+ and `mirobody`; document reading also needs
the parse dependencies and a model key. Application tasks need Docker and a
configured cloud or local model.

If you already have a running deployment, you can connect directly using
[MCP](https://docs.mirobody.ai/en/tools/mcp-integration).
Other installation methods are in the [skills reference](../../skills/README.md).
