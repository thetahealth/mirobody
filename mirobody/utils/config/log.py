import logging

#-----------------------------------------------------------------------------

class LogConfig:
    def __init__(
        self,
        name        : str = "",
        dir         : str = "",
        level       : int = logging.INFO,
    ):
        self.name       = name
        self.dir        = dir
        self.level      = level


    def print(self):
        print(f"log             : {self.dir}/{self.name}:{self.level}")

#-----------------------------------------------------------------------------
