#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: GPL-3.0-only
# Copyright (C) 2020 Barcelona Supercomputing Center, Javier Garrayo Ventas
# Copyright (C) 2020-2022 Barcelona Supercomputing Center, Meritxell Ferret
# Copyright (C) 2020-2023 Barcelona Supercomputing Center, José M. Fernández
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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

def memoized_method(maxsize: "Optional[int]" = 128, typed: "bool" = False) -> "Callable[[Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]],Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]]": # type: ignore[valid-type]
    def decorator(func: "Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]") -> "Callable[[Arg(RO, 'self'), VarArg(PVS.args), KwArg(PVS.kwargs)], RV]":  # type: ignore[valid-type]
        @functools.wraps(func)
        def wrapped_func(self: "RO", *args: "PVS.args", **kwargs: "PVS.kwargs") -> "RV": # type: ignore[valid-type]
            # We're storing the wrapped method inside the instance. If we had
            # a strong reference to self the instance would never die.
            self_weak = weakref.ref(self)
            @functools.wraps(func)
            @functools.lru_cache(maxsize=maxsize,typed=typed)
            def cached_method(*args: "PVS.args", **kwargs: "PVS.kwargs") -> "RV":  # type: ignore[valid-type]
                # This cast is needed because func expects a value which is not optional
                return func(cast("RO", self_weak()), *args, **kwargs)
            setattr(self, func.__name__, cached_method)
            # This cast is needed because _lru_cache_wrapper expects hashable contents 
            return cached_method(*args, **cast("Mapping[str, Hashable]",kwargs))
        return wrapped_func
    return decorator
