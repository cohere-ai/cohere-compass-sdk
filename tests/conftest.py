import pytest
from requests_mock import ANY as requests_mock_ANY
from requests_mock import Mocker


@pytest.fixture
def requests_mock_200s(requests_mock: Mocker) -> Mocker:
    requests_mock.register_uri(requests_mock_ANY, requests_mock_ANY)
    return requests_mock
