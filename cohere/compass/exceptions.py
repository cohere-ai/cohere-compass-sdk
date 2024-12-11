
class CompassClientError(Exception):
    """Exception raised for all 4xx client errors in the Compass client."""

    def __init__(self, message: str = "Client error occurred.", code: int = 400):
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
        self.message = message
        super().__init__(self.message)


class CompassMaxErrorRateExceeded(Exception):
    """Exception raised when the error rate exceeds the maximum allowed error rate in
    the Compass client."""

    def __init__(
        self,
        message: str = "The maximum error rate was exceeded. Stopping the insertion process.",
    ):
        self.message = message
        super().__init__(self.message)
