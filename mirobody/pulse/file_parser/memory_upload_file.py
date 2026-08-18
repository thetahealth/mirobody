"""In-memory stand-in for FastAPI's UploadFile.

Both places that feed raw bytes into ``FileProcessor.process_single_file``
(the WebSocket chunked-upload manager and the chat-attachment background
processor) used to carry their own hand-written shim. The copies diverged in
practice: one shim was patched to accept ``read(size)`` and ``seek(0, 2)``
because the genetic-file detector calls them (without the params the
TypeError was swallowed and genetic files were misread as plain-text
reports), while another copy never received the same fix. One class, one set
of file semantics.
"""


class MemoryUploadFile:
    """Mimics FastAPI UploadFile for in-memory content."""

    def __init__(self, content: bytes, filename: str, content_type: str):
        self.content = content
        self.filename = filename
        self.content_type = content_type
        self._position = 0
        self.size = len(content)
        # Dummy file attribute if accessed directly
        self.file = self

    async def read(self, size: int = -1):
        if size == -1:
            result = self.content[self._position:]
            self._position = len(self.content)
        else:
            end_pos = min(self._position + size, len(self.content))
            result = self.content[self._position:end_pos]
            self._position = end_pos
        return result

    async def seek(self, position: int, whence: int = 0):
        if whence == 0:
            self._position = max(0, min(position, len(self.content)))
        elif whence == 1:
            self._position = max(0, min(self._position + position, len(self.content)))
        elif whence == 2:
            self._position = max(0, min(len(self.content) + position, len(self.content)))
        return self._position

    def tell(self):
        return self._position

    async def close(self):
        pass
