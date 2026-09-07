"""How a person proves who they are.

    jwt.py            the token this server issues and verifies
    email.py          an emailed code, and the Mandrill validator behind it
    apple.py          Sign in with Apple
    google.py         Google identity tokens
    firebase.py       Firebase identity tokens
    webauthn.py       passkeys
    oauth_service.py  the OAuth flow a third-party client runs

Everything here answers one question and stops: *is this caller who they say
they are*. What they may then READ is the parent package — `care_circle.py`
decides whose record a person may open, and no module in here has an opinion
about that. `user_service.py` is the seam: it picks a validator from here and
lands the answer in an identity record.

Nothing in this subpackage is imported eagerly by `mirobody.user`: `email`
pulls in mandrill and `oauth_service` reaches the database, both `[app]`-only,
and `mirobody.pulse` imports `mirobody.user` at engine level. The parent's
lazy `__getattr__` is what keeps that honest — see its docstring for the
install this once broke.
"""
