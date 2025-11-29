import httpx

from cohere_compass.exceptions import CompassServerError


def is_retryable_httpx_exception(exc: BaseException) -> bool:
    """
    Determine if it is a beneficial to retry the given httpx exception.

    This is useful when using tenacity's @retry decorator on functions that make HTTP
    requests. In such cases, one typically wants to retry only network or server errors
    rather than, for example, client errors (4xx status codes).

    Example usage with tenacity:

    @retry(
        retry=retry_if_exception(is_retryable_httpx_exception),
        ... # other tenacity settings
    )
    def make_request():
        ...

    Args:
        exc (BaseException): The exception to evaluate.

    Returns:
        bool: True if the exception is retryable, False otherwise.

    """
    if isinstance(exc, httpx.TimeoutException):
        return True

    # NetworkError is a subtype of RequestError for low-level networking issues
    if isinstance(exc, httpx.NetworkError):
        return True

    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        # Retry 5xx, caller will eventually see CompassServerError
        if 500 <= status < 600:
            return True

    return False


def is_retryable_compass_exception(exc: BaseException) -> bool:
    """
    Determine if it is a beneficial to retry the given httpx exception.

    This is useful when using tenacity's @retry decorator on methods of CompassClient,
    CompassParserClient, etc. In case of failures, we want to retry server-related
    exceptions, i.e. CompassServerException, but not other exceptions.

    Example usage with tenacity:

    @retry(
        retry=retry_if_exception(is_retryable_compass_exception),
        ... # other tenacity settings
    )
    def my_method():
        ...

    Args:
        exc (BaseException): The exception to evaluate.

    Returns:
        bool: True if the exception is retryable, False otherwise.

    """
    if isinstance(exc, CompassServerError):
        # Retry on 5xx server errors
        return True
    return False
