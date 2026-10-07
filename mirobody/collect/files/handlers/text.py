from mirobody.collect.files.handlers.base import BaseFileHandler


class TextHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "text"
