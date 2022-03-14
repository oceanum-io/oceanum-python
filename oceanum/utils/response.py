import requests
from io import BytesIO, SEEK_SET, SEEK_END
from tempfile import NamedTemporaryFile
from pathlib import Path


class ResponseFile(object):
    def __init__(self, content):
        self._tmpfile = NamedTemporaryFile("wb")
        self._tmpfile.write(content)
        self._tmpfile.seek(0)

    @property
    def name(self):
        return self._tmpfile.name

    def open(self):
        return self._tmpfile


class ResponseStream(object):
    def __init__(self, request_iter):
        self._bytes = BytesIO()
        self._iterator = request_iter

    def _load_all(self):
        self._bytes.seek(0, SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, end_position):
        current_position = self._bytes.seek(0, SEEK_END)
        while current_position < end_position:
            try:
                current_position += self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        current_position = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            end_position = current_position + size
            self._load_until(end_position)

        self._bytes.seek(current_position)
        return self._bytes.read(size)

    def seek(self, position, whence=SEEK_SET):
        if whence == SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)