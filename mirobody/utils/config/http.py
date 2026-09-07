import os

#-----------------------------------------------------------------------------

class HttpConfig:
    def __init__(
        self,
        name        : str = "",
        version     : str = "",
        host        : str = "",
        port        : int = 0,
        uri_prefix  : str = "",
        htdoc       : str = "",
        headers     : dict[str, str] | None = None
    ):
        if headers is None:
            headers = {}
        self.name   = name
        self.version= version

        self.host   = host if host else "0.0.0.0"
        self.port   = port if port > 0 else 80

        stripped_uri_prefix = uri_prefix.strip().strip("/")
        if stripped_uri_prefix:
            self.uri_prefix = f"/{stripped_uri_prefix}"
        else:
            self.uri_prefix = ""

        # The web client's static assets live at repo-root `frontend/` — OUTSIDE
        # the Python package, so the wheel ships the engine, not 8MB of
        # JavaScript. Resolution: HTTP_ROOT from config, else `frontend/`
        # relative to the working directory (the Docker/source layout). When
        # neither exists the server runs API+MCP only and says so at startup —
        # a pip-installed server pairs with the hosted client at mirobody.ai.
        self.htdoc = htdoc.strip() if htdoc else ""
        if self.htdoc and not os.path.exists(self.htdoc):
            self.htdoc = ""
        if not self.htdoc and os.path.isdir("frontend"):
            self.htdoc = "frontend"

        #-------------------------------------------------

        self.headers = []
        has_server_header = False

        for key, value in headers.items():
            if not key or not value:
                continue

            self.headers.append((key, value))

            if not has_server_header and key.lower() == "server":
                has_server_header = True

        if not has_server_header and name:
            self.headers.append(("Server", f"{name}/{version}" if version else name))

    #-----------------------------------------------------

    def print(self):
        print(f"http            : {self.host}:{self.port}{self.uri_prefix}")
        print(f"                : {self.htdoc if self.htdoc else "Static Files are disabled."}")
        for header in self.headers:
            print(f"                   {header[0]}: {header[1]}")

#-----------------------------------------------------------------------------
