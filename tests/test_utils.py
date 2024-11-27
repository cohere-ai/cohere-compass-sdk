from concurrent.futures import ThreadPoolExecutor

from compass_sdk.utils import imap_queued


def test_imap_queued():
    input = range(10)
    expected = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

    with ThreadPoolExecutor(4) as pool:
        actual = imap_queued(
            pool,
            lambda x: x * 10,
            input,
            max_queued=8,
        )
        assert sorted(list(actual)) == sorted(expected)
