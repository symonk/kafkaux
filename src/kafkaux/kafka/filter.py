import re
import typing

from .model import Event

# FILTER_REGISTRY tracks filter names to the callable functions.
# The keys are the literal strategy function names.
FILTER_REGISTRY: dict[str, typing.Callable[[str, Event], None]] = {}


def auto_register(f):
    FILTER_REGISTRY[f.__name__] = f


@auto_register
def key_matched(expected: str, message: Event) -> bool:
    """key_was marks messages where the key was an exact match to the user
    provided filter."""
    pattern = re.compile(expected)
    return pattern.fullmatch(message.key) is not None
