---
name: internal-comms
description: Write internal communications using company formats. Use when writing status reports, leadership updates, company newsletters, FAQs, incident reports, project updates, or any internal communications.
source: anthropics/skills
license: Apache-2.0
---

# Internal Communications

## Document Types

### Status Report
```markdown
# [Project Name] Status Report
**Date:** [Date]
**Author:** [Name]
**Status:** 🟢 On Track / 🟡 At Risk / 🔴 Blocked

## Summary
[2-3 sentence overview]

## Progress This Week
- Completed: [items]
- In Progress: [items]
- Blocked: [items with owners]

## Key Metrics
| Metric | Target | Actual | Trend |
|--------|--------|--------|-------|
| [Metric] | [Target] | [Actual] | ⬆️/➡️/⬇️ |

## Next Week
- [Planned items]

## Risks & Mitigations
| Risk | Impact | Mitigation | Owner |
|------|--------|------------|-------|
| [Risk] | H/M/L | [Action] | [Name] |

## Asks
- [Any blockers needing escalation]
```

### Leadership Update
```markdown
# [Team] Update - [Date]

## TL;DR
[One paragraph executive summary - the only thing busy execs will read]

## Wins
- [Key accomplishment with impact]
- [Key accomplishment with impact]

## Challenges
- [Challenge]: [What we're doing about it]

## Key Decisions Needed
1. [Decision]: [Context, options, recommendation]

## Metrics Dashboard
[Include 3-5 key metrics with trends]
```

### Incident Report
```markdown
# Incident Report: [Title]

**Severity:** P0/P1/P2/P3
**Duration:** [Start] - [End]
**Impact:** [User/revenue impact]
**Status:** Resolved/Monitoring/Active

## Timeline
| Time (UTC) | Event |
|------------|-------|
| [Time] | [What happened] |

## Root Cause
[Clear explanation of what went wrong]

## Resolution
[What was done to fix it]

## Action Items
| Item | Owner | Due Date | Status |
|------|-------|----------|--------|
| [Action] | [Name] | [Date] | ⬜/✅ |

## Lessons Learned
- [What we learned]
- [What we'll do differently]
```

### All-Hands Announcement
```markdown
# [Announcement Title]

Hey team,

[Opening that sets context]

**What's happening:** [Clear, simple explanation]

**Why it matters:** [Impact and benefits]

**What you need to do:** [Specific actions if any]

**Timeline:**
- [Date]: [Milestone]
- [Date]: [Milestone]

**Questions?** [Where to ask]

[Sign-off]
```

## Writing Principles

1. **Lead with the bottom line** - Busy readers skim
2. **Be specific** - Numbers > adjectives
3. **Own problems** - "We missed" not "It was missed"
4. **Action-oriented** - Every problem has a next step
5. **Appropriate tone** - Match urgency to content
