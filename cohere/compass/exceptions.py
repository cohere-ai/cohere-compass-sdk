from typing import Optional


class CompassError(Exception):
    """Base class for all exceptions raised by the Compass client."""

    pass


class CompassClientError(CompassError):
    """Exception raised for all 4xx client errors in the Compass client."""

    def __init__(  # noqa: D107
        self,
        message: str = "Client error occurred.",
        code: Optional[int] = 400,
    ):
        self.message = message
        self.code = code
        super().__init__(self.message)


class CompassAuthError(CompassClientError):
    """Exception raised for authentication errors in the Compass client."""

    def __init__(  # noqa: D107
        self,
        message: str = (
            "CompassAuthError - check your bearer token or username and password."
        ),
    ):
        self.message = message
        super().__init__(self.message)


class CompassMaxErrorRateExceeded(Exception):
    """
    Exception raised if the error rate during document insertion exceeds the max.

    When the user calls the insert_docs() method, an optional max_error_rate parameter
    can be passed to the method. If the error rate during the insertion process exceeds
    this max_error_rate, the insertion process will be stopped and this exception will
    be raised.
    """

    def __init__(  # noqa: D107
        self,
        message: str = "The maximum error rate was exceeded. Stopping the insertion.",
    ):
        self.message = message
        super().__init__(self.message)
