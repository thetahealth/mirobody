from mirobody.collect.files.handlers.base import BaseFileHandler


class ImageHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "image"
