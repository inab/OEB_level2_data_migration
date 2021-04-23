#!/usr/bin/env python

class BenchmarkingDataset:

    def __init__(self, schemaMappings, id, data_type, visibility, name, description, challenge_ids, version):

        self._id = id
        self._schema = schemaMappings['Dataset']
        self.type = data_type
        self.visibility = visibility
        self.name = name
        self.description = description
        self.challenge_ids = challenge_ids
        self.version = version
        self.datalink = Datalink(schemaMappings, inline_data="{test}")


class Datalink:

    def __init__(self, schemaMappings, inline_data=None, file=None):

        if inline_data is not None:
            self.inline_data = inline_data
        elif file is not None:
            self.file = file
