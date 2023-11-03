import time
import logging

# https://stackoverflow.com/a/64030200/5204002

logger = logging.getLogger(__name__)


def retry(times: int, exceptions, sleep=0, exc_to_return: dict = None, prefix = None):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :param exceptions: Lists of exceptions that trigger a retry attempt
    :param exc_to_return: return value if exception is in exc_to_return
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            def __msg_error(msg_prefix, e):
                    if callable(msg_prefix):
                        msg_prefix = msg_prefix(*args, **kwargs)
                    if msg_prefix is None:
                        return str(e)
                    return f'{msg_prefix} - {str(e)}'

            attempt = 0
            last_exc = None
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(__msg_error(prefix, e))
                    last_exc = e
                    attempt += 1
                    time.sleep(sleep)
            if exc_to_return:
                for k, v in exc_to_return.items():
                    if isinstance(last_exc, k):
                        return v
            return func(*args, **kwargs)
        return newfn
    return decorator

