import logging

#-----------------------------------------------------------------------------

class LogConfig:
    def __init__(
        self,
        name        : str = "",
        dir         : str = "",
        level       : int = logging.INFO,
        secret_key  : str = ""
    ):
        self.name       = name
        self.dir        = dir
        self.level      = level
        self.secret_key = secret_key


    def print(self):
        print(f"log             : {self.dir}/{self.name}:{self.level}")

#-----------------------------------------------------------------------------
