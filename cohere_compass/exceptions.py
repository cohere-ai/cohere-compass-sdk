"""
Exception hierarchy for the Cohere Compass SDK.

This module defines custom exceptions used throughout the SDK for error handling and
reporting.
"""

from contextlib import contextmanager

import httpx


class CompassError(Exception):
    """Base class for all exceptions raised by the Compass client."""

    pass


class CompassNetworkError(CompassError):
    """Network-related error when talking to the Compass API."""

    def __init__(self, message: str) -> None:
        """
        Initialize CompassNetworkError.

        CompassNetworkError is raised for network-related issues when communicating
        with the Compass API.

        :param message: Error message to display.
        """
        self.message = message
        super().__init__(message)


class CompassTimeoutError(CompassNetworkError):
    """Request to the Compass API timed out."""

    def __init__(self, message: str) -> None:
        """
        Initialize CompassTimeoutError.

        CompassTimeoutError is raised when a request to the Compass API times out.

        :param message: Error message to display.
        """
        super().__init__(message)


class CompassServerError(CompassError):
    """Exception raised for all 5xx server errors."""

    def __init__(
        self,
        message: str,
        code: int,
    ):
        """
        Initialize CompassServerError.

        CompassServerError is raised for all 5xx server errors from Compass APIs.

        :param message: Error message.
        :param code: HTTP status code associated with the error.

        """
        self.message = message
        self.code = code
        super().__init__(self.message)


class CompassClientError(CompassError):
    """Exception raised for all 4xx client errors in the Compass client."""

    def __init__(
        self,
        message: str,
        code: int,
    ):
        """
        Initialize CompassClientError.

        CompassClientError is raised for all 4xx client errors from Compass APIs.

        :param message: Error message.
        :param code: HTTP status code associated with the error.

        """
        self.message = message
        self.code = code
        super().__init__(self.message)


class CompassAuthError(CompassClientError):
    """Exception raised for authentication errors in the Compass client."""

    def __init__(
        self,
        message: str,
        code: int,
    ):
        """
        Initialize CompassAuthError.

        A specialization of CompassClientError used for authenticated-related client
        errors.

        :param message: Error message to display.

        """
        self.message = message
        super().__init__(self.message, code)


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


@contextmanager
def handle_httpx_exceptions():
    """Context manager that converts httpx exceptions into Compass exceptions."""
    try:
        yield
    except Exception as exc:
        if not isinstance(exc, httpx.HTTPError):
            raise  # not httpx; bubble up

        # ----- Timeout -----
        if isinstance(exc, httpx.TimeoutException):
            msg = f"Timeout error: {exc}"
            raise CompassTimeoutError(msg) from exc

        # ----- Network errors -----
        if isinstance(exc, httpx.NetworkError):
            msg = f"Network error: {exc}"
            raise CompassNetworkError(msg) from exc

        # ----- HTTP status errors -----
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            body_text = exc.response.text

            if status in (401, 403):
                msg = "Unauthorized. Please check your bearer token."
                raise CompassAuthError(msg, status) from exc

            elif 400 <= status < 500:
                msg = f"Client error {status}: {body_text}"
                raise CompassClientError(message=msg, code=status) from exc

            elif 500 <= status < 600:
                msg = f"Server error {status}: {body_text}"
                raise CompassServerError(message=msg, code=status) from exc
            else:
                msg = f"Unexpected HTTP status {status}: {body_text}"
                raise CompassError(msg) from exc

        # ----- Catch-all -----
        if isinstance(exc, httpx.RequestError):
            msg = f"Request error: {exc}"
            raise CompassNetworkError(msg) from exc

        # Shouldn't reach here; fallback
        raise CompassError(f"Unexpected error: {exc}") from exc
