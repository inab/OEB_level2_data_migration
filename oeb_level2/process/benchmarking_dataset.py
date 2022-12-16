#!/usr/bin/env python

from typing import TYPE_CHECKING

if TYPE_CHECKING:
        from typing import (
                Any,
                Mapping,
                Optional,
                Sequence,
        )

class BenchmarkingDataset:

    def __init__(self, schemaMappings: "Mapping[str, Any]", id: "str", data_type: "str", visibility: "bool", name: "str", description: "str", challenge_ids: "Sequence[str]", version: "str"):

        self._id = id
        self._schema = schemaMappings['Dataset']
        self.type = data_type
        self.visibility = visibility
        self.name = name
        self.description = description
        self.challenge_ids = challenge_ids
        self.version = version
        self.datalink = Datalink(schemaMappings, inline_data={"test": None})


class Datalink:

    def __init__(self, schemaMappings: "Mapping[str, Any]", inline_data: "Optional[Mapping[str, Any]]" = None, filename: "Optional[str]" = None):
        
        self.inline_data: "Optional[Mapping[str, Any]]"= None
        self.filename: "Optional[str]" = None
        if inline_data is not None:
            self.inline_data = inline_data
        elif filename is not None:
            self.filename = filename
