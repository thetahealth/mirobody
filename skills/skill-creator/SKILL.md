---
name: skill-creator
description: Guide for creating effective skills. Use when users want to create a new skill (or update an existing skill) that extends Claude's capabilities with specialized knowledge, workflows, or tool integrations.
source: anthropics/skills
license: Apache-2.0
---

# Skill Creator

Skills are modular packages that extend Claude's capabilities by providing specialized knowledge, workflows, and tools.

## Core Principles

### Concise is Key
The context window is a shared resource. Only add context Claude doesn't already have. Challenge each piece: "Does Claude really need this?"

### Anatomy of a Skill

```
skill-name/
├── SKILL.md (required)
│   ├── YAML frontmatter (name, description)
│   └── Markdown instructions
└── Bundled Resources (optional)
    ├── scripts/      - Executable code
    ├── references/   - Documentation
    └── assets/       - Templates, images
```

### SKILL.md Format

```markdown
---
name: my-skill-name
description: A clear description of what this skill does and when to use it
---

# My Skill Name

[Instructions for Claude when this skill is active]

## Examples
- Example usage 1
- Example usage 2

## Guidelines
- Guideline 1
- Guideline 2
```

## Skill Creation Process

### Step 1: Understand with Examples
Gather concrete examples of how the skill will be used. Ask:
- "What functionality should this skill support?"
- "What would a user say that should trigger this skill?"

### Step 2: Plan Reusable Contents
Analyze examples to identify:
- **Scripts**: Code that gets rewritten repeatedly
- **References**: Documentation Claude needs to reference
- **Assets**: Templates, images for output

### Step 3: Initialize
Create the skill directory structure with SKILL.md and resource folders.

### Step 4: Implement
- Start with reusable resources (scripts, references, assets)
- Write clear SKILL.md with proper frontmatter
- Test scripts by actually running them

### Step 5: Iterate
Use the skill on real tasks, notice struggles, improve.

## Progressive Disclosure

Keep SKILL.md under 500 lines. Split content:

```markdown
# PDF Processing

## Quick start
[code example]

## Advanced features
- **Form filling**: See [FORMS.md](FORMS.md)
- **API reference**: See [REFERENCE.md](REFERENCE.md)
```

## What NOT to Include

- README.md
- INSTALLATION_GUIDE.md
- CHANGELOG.md
- User-facing documentation

Skills are for AI agents, not humans.
