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
        headers     : dict[str, str] = {}
    ):
        self.name   = name
        self.version= version

        self.host   = host if host else "0.0.0.0"
        self.port   = port if port > 0 else 80

        stripped_uri_prefix = uri_prefix.strip().strip("/")
        if stripped_uri_prefix:
            self.uri_prefix = f"/{stripped_uri_prefix}"
        else:
            self.uri_prefix = ""

        self.htdoc = htdoc.strip() if htdoc else ""
        if self.htdoc:
            if not os.path.exists(self.htdoc):
                self.htdoc = ""
        if not self.htdoc:
            self.htdoc = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pub", "htdoc")

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
