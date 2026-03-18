class _Logger:
    def info(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None


def get_logger(*args, **kwargs):
    return _Logger()


def configure(*args, **kwargs):
    return None


def make_filtering_bound_logger(*args, **kwargs):
    return _Logger


class PrintLoggerFactory:
    def __call__(self, *args, **kwargs):
        return _Logger()


class contextvars:
    @staticmethod
    def merge_contextvars(*args, **kwargs):
        return None


class stdlib:
    @staticmethod
    def add_log_level(*args, **kwargs):
        return None


class processors:
    class TimeStamper:
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            return args[-1] if args else None

    class StackInfoRenderer:
        def __call__(self, *args, **kwargs):
            return args[-1] if args else None

    class JSONRenderer:
        def __call__(self, *args, **kwargs):
            return args[-1] if args else None

    @staticmethod
    def format_exc_info(*args, **kwargs):
        return args[-1] if args else None


class dev:
    class ConsoleRenderer:
        def __call__(self, *args, **kwargs):
            return args[-1] if args else None
