"""
provider platform internals

    base.py           BasePullProvider abstract base class (inherit for new providers)
    platform.py       ProviderPlatform: provider loading, registration, data routing
    database_service.py ProviderDatabaseService: credential storage, user-provider mapping
    pull_task.py      ProviderPullTask: one provider's scheduled pull
    startup.py        provider platform startup sequence
    normalize.py      decoded facts -> stored records, and their source name
    oauth2.py         OAuth2Client: authorization, token exchange and refresh
    http.py           the bounded GET and paginator every pull uses
"""
