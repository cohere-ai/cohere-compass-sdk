"""Exceptions raised by the upload workflow."""


class SourceError(Exception):
    """An upload source cannot be classified, discovered, or read."""


class UploadStateError(RuntimeError):
    """An upload job's saved state could not be read, written, or found."""
