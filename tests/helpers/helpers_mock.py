# tests/helpers/helpers_mock.py
import inspect
from typing import Any, Callable, cast
from unittest.mock import Mock


def as_mock(obj: Any) -> Mock:
    """Helper function to cast an object to a Mock for type checking purposes."""
    return cast(Mock, obj)  # type: ignore


def get_bound_args(mock_obj: Mock, func: Callable) -> dict[str, Any]:
    """Return the bound arguments for a call recorded by a Mock, using the signature of an original function.

    This inspects the Mock's most recent call (mock_obj.call_args) and binds its positional
    and keyword arguments to the parameter names of the provided function `func` using
    inspect.signature(func).bind(...). Useful to obtain a mapping from parameter names
    to the values actually passed to the mocked function.

    Parameters
    ----------
    mock_obj : unittest.mock.Mock
        The Mock object whose call arguments will be analyzed. The mock must have been called.
    func : Callable
        The original (unmocked) function whose signature will be used to bind the mock's call args.
    """
    sig = inspect.signature(func)
    args, kwargs = mock_obj.call_args
    return sig.bind(*args, **kwargs).arguments
