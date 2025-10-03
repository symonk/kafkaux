import typing

from .model import ReportableMessage

# FILTER_REGISTRY tracks filter names to the callable functions.
# The keys are the literal strategy function names.
FILTER_REGISTRY: dict[str, typing.Callable[[str, ReportableMessage], None]] = {}


def auto_register(f):
    FILTER_REGISTRY[f.__name__] = f


@auto_register
def key_was(expected: str, message: ReportableMessage) -> bool:
    """key_was marks messages where the key was an exact match to the user
    provided filter."""
    return message.key == expected
