"""
Exception hierarchy for the Cohere Compass SDK.

This module defines custom exceptions used throughout the SDK for error handling and
reporting.
"""


class CompassError(Exception):
    """Base class for all exceptions raised by the Compass client."""

    pass


class CompassClientError(CompassError):
    """Exception raised for all 4xx client errors in the Compass client."""

    def __init__(
        self,
        message: str = "Client error occurred.",
        code: int | None = 400,
    ):
        """
        Initialize CompassClientError.

        CompassClientError is raised for all 4xx client errors from Compass APIs.

        :param message: Error message to display.
        :param code: HTTP status code associated with the error.

        """
        self.message = message
        self.code = code
        super().__init__(self.message)


class CompassAuthError(CompassClientError):
    """Exception raised for authentication errors in the Compass client."""

    def __init__(
        self,
        message: str = (
            "CompassAuthError - check your bearer token or username and password."
        ),
    ):
        """
        Initialize CompassAuthError.

        A specialization of CompassClientError used for authenticated-related client
        errors.

        :param message: Error message to display.

        """
        self.message = message
        super().__init__(self.message)


class CompassInsertionError(CompassError):
    """Exception raised for insertion errors in the Compass client."""

    def __init__(
        self,
        message: str = "Insertion error occurred.",
        errors: list[dict[str, str]] = [],
    ):
        """
        Initialize CompassInsertionError.

        :param message: Error message to display.
        :param errors: List of errors.

        """
        self.message = message
        self.errors = errors
        super().__init__(self.message)


class CompassMaxErrorRateExceeded(Exception):
    """
    Exception raised when error rate during document insertion exceeds threshold.

    This exception is raised when the insert_docs() method encounters too many errors
    during batch document insertion. The max_error_rate parameter controls the
    threshold, and when exceeded, the insertion process stops to prevent further
    failures.

    Attributes:
        message: Description of the error condition.

    """

    def __init__(
        self,
        message: str = "The maximum error rate was exceeded. Stopping the insertion.",
    ):
        """
        Initialize CompassMaxErrorRateExceeded.

        :param message: Error message to display.

        """
        self.message = message
        super().__init__(self.message)
