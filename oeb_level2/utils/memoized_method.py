#!/usr/bin/env python

# This code was obtained from https://stackoverflow.com/a/33672499
# which was under CC BY-SA 3.0
import functools
import weakref

from typing import (
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Callable,
        Hashable,
        Mapping,
        Optional,
        TypeVar,
        Union,
    )
    
    from typing_extensions import (
        ParamSpec,
        Concatenate,
    )
    
    from mypy_extensions import (
        Arg,
        KwArg,
        VarArg,
    )
    
    RV = TypeVar('RV')
    RO = TypeVar('RO')
    PVS = ParamSpec('PVS')

def memoized_method(maxsize: "Optional[int]" = 128, typed: "bool" = False) -> "Callable[[Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]],Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]]":
    def decorator(func: "Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]") -> "Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]":
        @functools.wraps(func)
        def wrapped_func(self: "RO", *args: "PVS.args", **kwargs: "PVS.kwargs") -> "RV":
            # We're storing the wrapped method inside the instance. If we had
            # a strong reference to self the instance would never die.
            self_weak = weakref.ref(self)
            @functools.wraps(func)
            @functools.lru_cache(maxsize=maxsize,typed=typed)
            def cached_method(*args: "PVS.args", **kwargs: "PVS.kwargs") -> "RV":
                # This cast is needed because func expects a value which is not optional
                return func(cast("RO", self_weak()), *args, **kwargs)
            setattr(self, func.__name__, cached_method)
            # This cast is needed because _lru_cache_wrapper expects hashable contents 
            return cached_method(*args, **cast("Mapping[str, Hashable]",kwargs))
        return wrapped_func
    return decorator
