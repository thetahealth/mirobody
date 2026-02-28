"""
Theta platform internals

    base.py           — BaseThetaProvider abstract base class (inherit for new providers)
    platform.py       — ThetaPlatform: provider loading, registration, data routing
    database_service.py — ThetaDatabaseService: credential storage, user-provider mapping
    pull_task.py      — Scheduled pull task factory (one task per provider)
    startup.py        — Theta startup sequence
    utils.py          — ThetaDataFormatter, ThetaTimeUtils helpers
"""
