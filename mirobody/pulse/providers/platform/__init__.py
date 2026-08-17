"""
provider platform internals

    base.py           — BasePullProvider abstract base class (inherit for new providers)
    platform.py       — ProviderPlatform: provider loading, registration, data routing
    database_service.py — ProviderDatabaseService: credential storage, user-provider mapping
    pull_task.py      — Scheduled pull task factory (one task per provider)
    startup.py        — provider platform startup sequence
    normalize.py      — vendor timestamps and source names -> our canonical forms
"""
