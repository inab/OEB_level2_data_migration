#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: GPL-3.0-only
# Copyright (C) 2020 Barcelona Supercomputing Center, Javier Garrayo Ventas
# Copyright (C) 2020-2022 Barcelona Supercomputing Center, Meritxell Ferret
# Copyright (C) 2020-2025 Barcelona Supercomputing Center, José M. Fernández
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

import os
import sys
import copy
import datetime
import enum
import hashlib
import inspect
import itertools
import tempfile
import logging
import json
import urllib.parse

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from typing import(
        Any,
        Iterator,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Set,
        Tuple,
        Type,
        Union,
    )
    
    from typing_extensions import (
        Final,
        NotRequired,
        TypedDict,
    )
    
    from ..schemas.typed_schemas.submission_form_schema import DatasetsVisibility
	
    from extended_json_schema_validator.extensible_validator import (
        ExtensibleValidator,
        ParsedContentEntry,
    )
    from extended_json_schema_validator.extensions.abstract_check import SchemaHashEntry
    
    from oebtools.auth import OEBCredentials
    
    from oebtools.fetch import IdAndOrigId

    from numbers import Real

    class MetricsLimits(TypedDict):
        min: "NotRequired[Real]"
        min_inclusive: "NotRequired[bool]"
        max: "NotRequired[Real]"
        max_inclusive: "NotRequired[bool]"

    class RepresentationHints(TypedDict):
        limits: "NotRequired[MetricsLimits]"

import requests

from oebtools.common import(
    DEFAULT_BDM_TAG,
    FLAVOR_SANDBOX,
    FLAVOR_STAGED,
    YieldedInput,
)

from oebtools.fetch import (
    DEFAULT_CONCEPT_DESC,
    FetchedInlineData,
    OEBFetcher,
)

from oebtools.uploader import (
    OEBUploader,
    PayloadMode,
)

from .memoized_method import memoized_method

from ..schemas import (
    LEVEL2_SCHEMA_IDS,
    META_TYPE_AGG_DATA_SERIES,
    VIS_2D_PLOT,
    VIS_BAR_PLOT,
    VIS_BOX_PLOT,
    VIS_LINE_PLOT,
    VIS_RADAR_PLOT,
    TYPE2SCHEMA_ID,
    get_oeb_level2_schemas_path,
    create_validator_for_oeb_level2,
)


class OEBDatasetType(enum.Enum):
    Input = "input"
    Participant = "participant"
    Assessment = "assessment"
    Aggregation = "aggregation"
    MetricsReference = "metrics_reference"
    PublicReference = "public_reference"

ASSESSMENT_CATEGORY_LABEL: "Final[str]" = "assessment"
AGGREGATION_CATEGORY_LABEL: "Final[str]" = "aggregation"

DATASET_ORIG_ID_SUFFIX = {
    OEBDatasetType.Participant: "_P",
    OEBDatasetType.Assessment: "_A",
}

TEST_ACTION_ORIG_ID_SUFFIX = {
    "AggregationEvent": "_Event",
    "MetricsEvent": "_MetricsEvent",
}


GRAPHQL_POSTFIX = "/graphql"

ORIG_ID_SEPARATOR_KEY = "level_2:orig_id_separator"

AGGREGATION_SEPARATOR_KEY = "level_2:aggregation_separator"

METRICS_LABEL_SEPARATOR_KEY = "level_2:metric_separator"

COMMUNITY_LABEL_KEY = "level_2:community_label"

CHALLENGE_LABEL_KEY = "level_2:challenge_id"

PARTICIPANT_ID_KEY = "level_2:participant_id"

EXCLUDE_PARTICIPANT_KEY = "level_2:exclude_participant"

METRIC_ID_KEY = "level_2:metric_id"

DEFAULT_ORIG_ID_SEPARATOR = "_"

DEFAULT_AGGREGATION_SEPARATOR = "agg"

DEFAULT_METRICS_LABEL_SEPARATOR = "+"


class GraphQLQueryLabel(enum.Enum):
    Input = "input"
    MetricsReference = "metrics_reference"
    Participant = "participant"
    Aggregation = "aggregation"

class ChallengeLabelAndSep(NamedTuple):
    ch_id: "str"
    label: "str"
    sep: "str" = DEFAULT_ORIG_ID_SEPARATOR
    aggregation_sep: "str" = DEFAULT_AGGREGATION_SEPARATOR
    metrics_label_sep: "str" = DEFAULT_METRICS_LABEL_SEPARATOR

class BenchmarkingEventPrefixEtAl(NamedTuple):
    prefix: "str" = ""
    sep: "str" = DEFAULT_ORIG_ID_SEPARATOR
    aggregation_sep: "str" = DEFAULT_AGGREGATION_SEPARATOR
    metrics_label_sep: "str" = DEFAULT_METRICS_LABEL_SEPARATOR

class MetricsTrio(NamedTuple):
    metrics_id: "str"
    tool_id: "Optional[str]"
    proposed_label: "str"
    all_labels: "Sequence[str]"
    representation_hints: "Optional[RepresentationHints]"

class OpenEBenchUtils():
    DEFAULT_OEB_API = "https://dev-openebench.bsc.es/api/scientific/graphql"
    DEFAULT_OEB_SUBMISSION_API = "https://dev-openebench.bsc.es/api/scientific/submission/"
    DEFAULT_DATA_MODEL_DIR = "benchmarking_data_model"

    TEST_EVENT_INFIX = '_testEvent_'

    def __init__(
        self,
        oeb_credentials: "Mapping[str, Any]",
        workdir: "str",
        oeb_token: "Optional[str]" = None,
        cache_entry_expire: "Optional[Union[int, float]]" = None,
        override_cache: "bool" = False,
        level2_min_validator: "Optional[Any]" = None,
    ):
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        self.data_model_repo_dir = os.path.join(workdir, self.DEFAULT_DATA_MODEL_DIR)
        self.oeb_api = oeb_credentials.get("graphqlURI", self.DEFAULT_OEB_API)
        
        # Dealing with endpoints
        if self.oeb_api.endswith(GRAPHQL_POSTFIX):
            self.oeb_api_base = self.oeb_api[0:-len(GRAPHQL_POSTFIX)+1]
        else:
            self.oeb_api_base = self.oeb_api
            if self.oeb_api_base.endswith("/"):
                self.oeb_api = self.oeb_api_base[0:-1]
            else:
                self.oeb_api = self.oeb_api_base
            
            self.oeb_api += GRAPHQL_POSTFIX
            
        self.admin_tools = OEBUploader(
            self.oeb_api_base,
            oeb_credentials=cast("OEBCredentials", oeb_credentials) if oeb_token is None  else  oeb_token,
            cache_entry_expire=cache_entry_expire,
        )
        self.override_cache = override_cache
        
        self.oeb_submission_api = oeb_credentials.get('submissionURI', self.DEFAULT_OEB_SUBMISSION_API)
        oebIdProviders = oeb_credentials['accessURI']
        if not isinstance(oebIdProviders, list):
            oebIdProviders = [ oebIdProviders ]
        self.oeb_access_api_points = oebIdProviders
        
        storage_server_block = oeb_credentials.get('storageServer', {})
        self.storage_server_type = storage_server_block.get("type")
        self.storage_server_endpoint = storage_server_block.get("endpoint")
        self.storage_server_community = storage_server_block.get("community")
        self.storage_server_token = storage_server_block.get("token")

        #logging.basicConfig(level=logging.INFO)
        
        # TODO: to be removed, as it is not needed any more
        local_config = {
            'primary_key': {
                'provider': [
                    *oebIdProviders,
                    self.oeb_submission_api
                ],
                # To be set on instantiation
                # 'schema_prefix': None,
                "allow_provider_duplicates": True,
                'accept': 'text/uri-list'
            }
        }
        
        if level2_min_validator is None:
            level2_min_validator, num_level2_schemas, expected_level2_schemas = create_validator_for_oeb_level2()
            if num_level2_schemas < expected_level2_schemas:
                self.logger.error(f"OEB level2 operational JSON Schemas not found ({num_level2_schemas} vs {expected_level2_schemas})")
                sys.exit(1)
            
            # This is to avoid too much verbosity
            # level2_min_validator.logger.setLevel(logging.CRITICAL)
        
        self.level2_min_validator = level2_min_validator
        
        self.schema_validators_local_config = local_config

        self.schema_validators: "Optional[ExtensibleValidator]" = None
        self.schema_prefix: "Optional[str]" = None
        self.num_schemas = 0
        
        self.schemaMappings: "Optional[Mapping[str, str]]" = None

    # function to pull a github repo obtained from https://github.com/inab/vre-process_nextflow-executor/blob/master/tool/VRE_NF.py
    
    @staticmethod
    def get_community_label(community: "Mapping[str, Any]") -> "str":
        comm_meta = cast("Optional[Mapping[str, Any]]", community.get('_metadata'))
        if comm_meta is None:
            comm_meta = {}
        community_label = comm_meta.get(COMMUNITY_LABEL_KEY)
        if community_label is None:
            community_label = community["acronym"]
        
        return cast("str", community_label)

    @staticmethod
    def gen_community_prefix(community: "Mapping[str, Any]") -> "str":
        return OpenEBenchUtils.gen_community_prefix_from_label(OpenEBenchUtils.get_community_label(community))

    @staticmethod
    def gen_community_prefix_from_label(community_label: "str") -> "str":
        community_prefix = community_label + ':'
        
        return community_prefix

    def gen_benchmarking_event_prefix(self, bench_event: "Mapping[str, Any]", community_prefix: "str") -> "BenchmarkingEventPrefixEtAl":
        if not bench_event.get("orig_id", "").startswith(community_prefix):
            self.logger.warning(f"Benchmarking event {bench_event['_id']} original id {bench_event.get('orig_id')} does not start with community prefix {community_prefix}")
        
        # This code is needed to get custom orig_id separators
        bench_meta = cast("Optional[Mapping[str, Any]]", bench_event.get('_metadata'))
        if bench_meta is None:
            bench_meta = {}
        
        bench_event_orig_id_separator = bench_meta.get(ORIG_ID_SEPARATOR_KEY)
        if bench_event_orig_id_separator is None:
            bench_event_orig_id_separator = DEFAULT_ORIG_ID_SEPARATOR
        
        bench_event_aggregation_separator = bench_meta.get(AGGREGATION_SEPARATOR_KEY)
        if bench_event_aggregation_separator is None:
            bench_event_aggregation_separator = DEFAULT_AGGREGATION_SEPARATOR
        
        bench_event_metrics_label_separator = bench_meta.get(METRICS_LABEL_SEPARATOR_KEY)
        if bench_event_metrics_label_separator is None:
            bench_event_metrics_label_separator = DEFAULT_METRICS_LABEL_SEPARATOR
        
        # Prefixes about benchmarking events
        benchmarking_event_prefix = bench_event.get("orig_id")
        if benchmarking_event_prefix is not None:
            benchmarking_event_prefix += bench_event_orig_id_separator
        else:
            benchmarking_event_prefix = community_prefix
        
        return BenchmarkingEventPrefixEtAl(
            prefix=benchmarking_event_prefix,
            sep=bench_event_orig_id_separator,
            aggregation_sep=bench_event_aggregation_separator,
            metrics_label_sep=bench_event_metrics_label_separator,
        )
    
    @staticmethod
    def gen_test_event_original_id(challenge: "Mapping[str, Any]", participant_label: "str") -> "str":
        return cast("str", challenge.get("orig_id", challenge["_id"])) + OpenEBenchUtils.TEST_EVENT_INFIX + participant_label
    
    @staticmethod
    def gen_metrics_event_original_id(assessment_dataset: "Mapping[str, Any]") -> "str":
        ass_d_id = assessment_dataset.get("orig_id", assessment_dataset["_id"])
        return rchop(ass_d_id, DATASET_ORIG_ID_SUFFIX[OEBDatasetType.Assessment]) + TEST_ACTION_ORIG_ID_SUFFIX["MetricsEvent"]
    
    @staticmethod
    def gen_aggregation_event_original_id(aggregation_dataset: "Mapping[str, Any]") -> "str":
        agg_d_id = cast("str", aggregation_dataset.get("orig_id", aggregation_dataset["_id"]))
        return agg_d_id + TEST_ACTION_ORIG_ID_SUFFIX["AggregationEvent"]
    
    def gen_expected_dataset_prefix(
        self,
        dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        challenge_ids: "Optional[Sequence[str]]" = None,
    ) ->  "Tuple[str, str]":
        # This method is assuming the received challenge ids are OEB ones,
        # not challenge labels
        
        if challenge_ids is None:
            challenge_ids = dataset["challenge_ids"]
        # First, decide the prefix
        the_prefix = None
        
        if len(challenge_ids) == 1:
            # Fetching the challenge prefix
            challenge_elem = challenge_ids[0]
            challenge_id = challenge_elem["_id"] if isinstance(challenge_elem, dict)  else challenge_elem
            challenge = self.fetchStagedEntry(dataType="Challenge", the_id=challenge_id)
            
            # Getting a possible custom original id separator for this challenge
            c_meta = challenge.get("_metadata")
            if c_meta is None:
                c_meta = {}
            challenge_orig_id_separator = c_meta.get(ORIG_ID_SEPARATOR_KEY)
            if challenge_orig_id_separator is None:
                challenge_orig_id_separator = bench_event_prefix_et_al.sep
            
            the_prefix = challenge.get("orig_id", "")
            if len(the_prefix) > 0:
                the_prefix += challenge_orig_id_separator
        elif ("community_ids" in dataset) and len(dataset["community_ids"]) > 1:
            the_prefix = ""
            challenge_orig_id_separator = DEFAULT_ORIG_ID_SEPARATOR
        else:
            benchmarking_event_id = None
            challenge_orig_id_separator = None
            for challenge_elem in challenge_ids:
                challenge_id = challenge_elem["_id"] if isinstance(challenge_elem, dict)  else challenge_elem
                    
                challenge = self.fetchStagedEntry(dataType="Challenge", the_id=challenge_id)
                if benchmarking_event_id is None:
                    benchmarking_event_id = challenge["benchmarking_event_id"]
                    challenge_orig_id_separator = bench_event_prefix_et_al.sep
                elif benchmarking_event_id != challenge["benchmarking_event_id"]:
                    the_prefix = community_prefix
                    challenge_orig_id_separator = DEFAULT_ORIG_ID_SEPARATOR
                    break
            else:
                the_prefix = bench_event_prefix_et_al.prefix
                if challenge_orig_id_separator is None:
                    challenge_orig_id_separator = DEFAULT_ORIG_ID_SEPARATOR
        
        return the_prefix, challenge_orig_id_separator
    
    def gen_participant_original_id_from_dataset(
        self,
        dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        participant_label: "str",
    ) -> "str":
        # First, obtain the prefix
        the_prefix, challenge_orig_id_separator = self.gen_expected_dataset_prefix(dataset, community_prefix, bench_event_prefix_et_al)
        
        # Then, dig in to get the participant label
        the_metadata = dataset.get("_metadata")
        if the_metadata is not None:
            participant_label = the_metadata.get(PARTICIPANT_ID_KEY, participant_label)
        
        try:
            dataset_type = OEBDatasetType(dataset["type"])
            the_suffix = DATASET_ORIG_ID_SUFFIX.get(dataset_type, "")
        except ValueError:
            the_suffix = ""
            
        expected_orig_id = the_prefix + participant_label + the_suffix
        
        return expected_orig_id
        
    def gen_participant_original_id_from_min_dataset(
        self,
        min_dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        challenge_ids: "Sequence[str]",
    ) -> "str":
        # First, obtain the prefix
        the_prefix, challenge_orig_id_separator = self.gen_expected_dataset_prefix(
            min_dataset,
            community_prefix,
            bench_event_prefix_et_al,
            challenge_ids=challenge_ids,
        )
        
        # Then, dig in to get the participant label
        participant_label = cast("str", min_dataset["participant_id"])
        
        try:
            dataset_type = OEBDatasetType(min_dataset["type"])
            the_suffix = DATASET_ORIG_ID_SUFFIX.get(dataset_type, "")
        except ValueError:
            the_suffix = ""
            
        expected_orig_id = the_prefix + participant_label + the_suffix
        
        return expected_orig_id
        
    def fix_participant_original_id(
        self,
        dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        participant_label: "str",
        do_fix_orig_id: "bool",
    ) ->  "Optional[Mapping[str, Any]]":
        """
        It works only for participant and assessment datasets
        """
        expected_orig_id = self.gen_participant_original_id_from_dataset(
            dataset,
            community_prefix,
            bench_event_prefix_et_al,
            participant_label,
        )
        o_key = "orig_id"  if "orig_id" in dataset else "_id"
        orig_id = dataset[o_key]
        fixable_dataset = None
        if expected_orig_id != orig_id:
            if do_fix_orig_id:
                fixable_dataset = cast("MutableMapping[str, Any]", copy.copy(dataset))
                fixable_dataset[o_key] = expected_orig_id
                self.logger.info(f"For {dataset['type']} dataset {dataset['_id']}, renamed from {orig_id} to {expected_orig_id}")
            else:
                self.logger.warning(f"For {dataset['type']} dataset {dataset['_id']}, expected original id was {expected_orig_id}, but got {orig_id}. Fix it in order to avoid problems")
        
        return fixable_dataset
    
    def gen_assessment_original_id_from_dataset(
        self,
        dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        participant_label: "str",
        metrics_label: "str",
    ) -> "str":
        # First, obtain the prefix
        the_prefix, challenge_orig_id_separator = self.gen_expected_dataset_prefix(dataset, community_prefix, bench_event_prefix_et_al)
        
        # Then, dig in to get the participant label and metrics label
        the_metadata = dataset.get("_metadata")
        if the_metadata is not None:
            participant_label = the_metadata.get(PARTICIPANT_ID_KEY, participant_label)
            possible_metrics_label: "Union[str, Sequence[str]]" = the_metadata.get(METRIC_ID_KEY, metrics_label)
            if isinstance(possible_metrics_label, list):
                # Only the first element is taken into account
                if len(possible_metrics_label) > 0:
                    metrics_label = possible_metrics_label[0]
                else:
                    self.logger.error(f"Ill-formed {METRIC_ID_KEY} in entry {dataset['_id']}. Please fix it in the database")
            else:
                metrics_label = cast("str", possible_metrics_label)
        try:
            dataset_type = OEBDatasetType(dataset["type"])
            the_suffix = DATASET_ORIG_ID_SUFFIX.get(dataset_type, "")
        except ValueError:
            the_suffix = ""
            
        expected_orig_id = the_prefix + metrics_label + challenge_orig_id_separator + participant_label + the_suffix
        
        return expected_orig_id
    
    def gen_assessment_original_id_from_min_dataset(
        self,
        min_dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        challenge_ids: "Sequence[str]",
    ) -> "str":
        # First, obtain the prefix
        the_prefix, challenge_orig_id_separator = self.gen_expected_dataset_prefix(
            min_dataset,
            community_prefix,
            bench_event_prefix_et_al,
            challenge_ids=challenge_ids,
        )
        
        # Then, dig in to get the participant label and metrics label
        participant_label = cast("str", min_dataset["participant_id"])
        metrics_label = cast("str", min_dataset["metrics"]["metric_id"])
        
        try:
            dataset_type = OEBDatasetType(min_dataset["type"])
            the_suffix = DATASET_ORIG_ID_SUFFIX.get(dataset_type, "")
        except ValueError:
            the_suffix = ""
            
        expected_orig_id = the_prefix + metrics_label + challenge_orig_id_separator + participant_label + the_suffix
        
        return expected_orig_id
        
    def fix_assessment_original_id(
        self,
        dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        participant_label: "str",
        metrics_label: "str",
        do_fix_orig_id: "bool",
    ) ->  "Optional[Mapping[str, Any]]":
        """
        It works only for participant and assessment datasets
        """
        expected_orig_id = self.gen_assessment_original_id_from_dataset(
            dataset,
            community_prefix,
            bench_event_prefix_et_al,
            participant_label,
            metrics_label,
        )
        
        o_key = "orig_id"  if "orig_id" in dataset else "_id"
        orig_id = dataset[o_key]
        fixable_dataset = None
        if expected_orig_id != orig_id:
            if do_fix_orig_id:
                fixable_dataset = cast("MutableMapping[str, Any]", copy.copy(dataset))
                fixable_dataset[o_key] = expected_orig_id
                self.logger.info(f"For {dataset['type']} dataset {dataset['_id']}, renamed from {orig_id} to {expected_orig_id}")
            else:
                self.logger.warning(f"For {dataset['type']} dataset {dataset['_id']}, expected original id was {expected_orig_id}, but got {orig_id}. Fix it in order to avoid problems")
        
        return fixable_dataset
    
    def gen_aggregation_original_id_from_min_dataset(
        self,
        min_dataset: "Mapping[str, Any]",
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        ch_to: "Sequence[ChallengeLabelAndSep]",
    ) -> "str":
        # First, obtain the prefix
        challenge_ids = list(map(lambda ct: ct.ch_id, ch_to))
        the_prefix, challenge_orig_id_separator = self.gen_expected_dataset_prefix(
            min_dataset,
            community_prefix,
            bench_event_prefix_et_al,
            challenge_ids=challenge_ids,
        )
        
        inline_data = min_dataset["datalink"]["inline_data"]
        vis = inline_data["visualization"]
        vis_type = vis["type"]
        # Then, dig in to get the metrics labels
        metrics_labels: "Sequence[str]"
        if inline_data.get("series_type") == META_TYPE_AGG_DATA_SERIES:
            metrics_labels = vis.get("available_metrics", [])
        elif vis_type == VIS_2D_PLOT:
            metrics_labels = [ vis.get("x_axis"), vis.get("y_axis") ]
        elif vis_type == VIS_BAR_PLOT:
            metrics_labels = [ vis.get("metric") ]
        else:
            self.logger.fatal(f"Please implement aggregation {vis_type} minimal id generation")
            raise NotImplementedError(f"Please implement aggregation {vis_type} minimal id generation")
        
        # TODO: validate metrics labels

        if len(ch_to) == 1:
            challenge_orig_id_separator = ch_to[0].sep
            aggregation_sep = ch_to[0].aggregation_sep
            metrics_label_sep = ch_to[0].metrics_label_sep
        else:
            aggregation_sep = bench_event_prefix_et_al.aggregation_sep
            metrics_label_sep = bench_event_prefix_et_al.metrics_label_sep
        
        expected_orig_id = the_prefix + aggregation_sep + \
            challenge_orig_id_separator + metrics_label_sep.join(metrics_labels)
        
        return expected_orig_id

    @staticmethod
    def get_challenge_label_from_challenge(
        the_challenge: "Mapping[str, Any]",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        community_prefix: "str"
    ) -> "ChallengeLabelAndSep":
        """
        It returns both the challenge label as well as
        the challenge orig_id separator
        """
        challenge_label = the_challenge.get("acronym")
        _metadata = the_challenge.get("_metadata")
        challenge_orig_id_separator = bench_event_prefix_et_al.sep
        challenge_aggregation_separator = bench_event_prefix_et_al.aggregation_sep
        challenge_metrics_label_separator = bench_event_prefix_et_al.metrics_label_sep
        if isinstance(_metadata, dict):
            the_label = _metadata.get(CHALLENGE_LABEL_KEY)
            if the_label is not None:
                challenge_label = the_label
            the_challenge_orig_id_separator = _metadata.get(ORIG_ID_SEPARATOR_KEY)
            
            if the_challenge_orig_id_separator is not None:
                challenge_orig_id_separator = the_challenge_orig_id_separator
            
            the_challenge_aggregation_separator = _metadata.get(AGGREGATION_SEPARATOR_KEY)
            if the_challenge_aggregation_separator is not None:
                challenge_aggregation_separator = the_challenge_aggregation_separator
            
            the_metrics_label_separator = _metadata.get(METRICS_LABEL_SEPARATOR_KEY)
            if the_metrics_label_separator is not None:
                challenge_metrics_label_separator = the_metrics_label_separator
        
        if challenge_label is None:
            # Very old school label
            challenge_label = the_challenge.get("orig_id")
            if challenge_label is not None:
                if challenge_label.startswith(bench_event_prefix_et_al.prefix):
                    challenge_label = challenge_label[len(bench_event_prefix_et_al.prefix):]
                elif challenge_label.startswith(community_prefix):
                    challenge_label = challenge_label[len(community_prefix):]
            else:
                challenge_label = the_challenge["_id"]
        
        return ChallengeLabelAndSep(
            ch_id=cast("str", the_challenge["_id"]),
            label=cast("str", challenge_label),
            sep=challenge_orig_id_separator,
            aggregation_sep=challenge_aggregation_separator,
            metrics_label_sep=challenge_metrics_label_separator,
        )

    @staticmethod
    def gen_ch_id_to_label_and_sep(
        challenges: "Sequence[Mapping[str, Any]]",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        community_prefix: "str",
    ) -> "Mapping[str, ChallengeLabelAndSep]":
        ch_id_to_label_and_sep = {}
        for challenge in challenges:
            challenge_label_and_sep = OpenEBenchUtils.get_challenge_label_from_challenge(
                challenge,
                bench_event_prefix_et_al,
                community_prefix,
            )
            ch_id_to_label_and_sep[challenge_label_and_sep.ch_id] = challenge_label_and_sep
            ch_orig_id = challenge.get("orig_id")
            if ch_orig_id is not None:
                ch_id_to_label_and_sep[ch_orig_id] = challenge_label_and_sep
        
        return ch_id_to_label_and_sep

    def doMaterializeRepo(self, git_uri: "str", git_tag: "str") -> "str":

        repo_hashed_id = hashlib.sha1(git_uri.encode('utf-8')).hexdigest()
        repo_hashed_tag_id = hashlib.sha1(git_tag.encode('utf-8')).hexdigest()

        # Assure directory exists before next step
        repo_destdir = os.path.join(
            self.data_model_repo_dir, repo_hashed_id)
        if not os.path.exists(repo_destdir):
            try:
                os.makedirs(repo_destdir)
            except IOError as error:
                errstr = "ERROR: Unable to create intermediate directories for repo {}. ".format(
                    git_uri,)
                raise Exception(errstr)

        repo_tag_destdir = os.path.join(repo_destdir, repo_hashed_tag_id)
        return cast("str", self.admin_tools.schemas_manager.checkoutSchemas(
            checkoutDir=repo_tag_destdir,
            git_repo=git_uri,
            tag=git_tag,
            override_cache=self.override_cache,
        ))

    # function that retrieves all the required metadata from OEB database
    def graphql_query_OEB_DB(self, data_type: GraphQLQueryLabel, bench_event_id: "str") -> "Mapping[str, Mapping[str, Any]]":

        if data_type == GraphQLQueryLabel.Input:
            query_name = "InputQuery"
            query = """
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
        _metadata
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        datasets(datasetFilters: {type: "input"}) {
            _id
        }
    }
    getContacts {
        _id
        email
    }
"""
            variables = {
                'bench_event_id': bench_event_id,
            }
        elif data_type == GraphQLQueryLabel.MetricsReference:
            query_name = "MetricsReferenceQuery"
            query = """
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
        _metadata
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        orig_id
        challenge_contact_ids
        metrics_categories {
          category
          description
          metrics {
            metrics_id
            orig_id
            tool_id
          }
        }
        datasets(datasetFilters: {type: "metrics_reference"}) {
            _id
        }
    }
    getMetrics {
        _id
        _metadata
        representation_hints
        orig_id
    }
"""
            variables = {
                'bench_event_id': bench_event_id,
            }
        elif data_type == GraphQLQueryLabel.Participant:
            query_name = "ParticipantsQuery"
            query = """
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
        _metadata
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        orig_id
        challenge_contact_ids
        metrics_categories {
          category
          description
          metrics {
            metrics_id
            orig_id
            tool_id
          }
        }
        participant_datasets: datasets(datasetFilters: {type: "participant"}) {
            _id
            _schema
            orig_id
            community_ids
            challenge_ids
            visibility
            name
            version
            description
            dates {
                creation
                modification
            }
            type
            datalink {
                inline_data
                schema_url
                uri
                schema_uri
            }
            dataset_contact_ids
            depends_on {
                tool_id
                metrics_id
                rel_dataset_ids {
                    dataset_id
                }
            }
            _metadata
        }
    }
    getMetrics {
        _id
        _metadata
        representation_hints
        orig_id
    }
"""
            variables = {
                'bench_event_id': bench_event_id,
            }
        elif data_type == GraphQLQueryLabel.Aggregation:
            query_name = "AggregationQuery"
            query = """
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
        _metadata
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        orig_id
        challenge_contact_ids
        metrics_categories {
          category
          description
          metrics {
            metrics_id
            orig_id
            tool_id
          }
        }
        event_test_actions: test_actions(testActionFilters: {action_type: "TestEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        metrics_test_actions: test_actions(testActionFilters: {action_type: "MetricsEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        aggregation_test_actions: test_actions(testActionFilters: {action_type: "AggregationEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        public_reference_datasets: datasets(datasetFilters: {type: "public_reference"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        metrics_reference_datasets: datasets(datasetFilters: {type: "metrics_reference"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        input_datasets: datasets(datasetFilters: {type: "input"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        participant_datasets: datasets(datasetFilters: {type: "participant"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        assessment_datasets: datasets(datasetFilters: {type: "assessment"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        aggregation_datasets: datasets(datasetFilters: {type: "aggregation"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                    schema_url
                    uri
                    schema_uri
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
    }
    getMetrics {
        _id
        _metadata
        representation_hints
        orig_id
    }
"""
            variables = {
                'bench_event_id': bench_event_id,
            }
        else:
            self.logger.fatal("Unable to generate graphQL query: Unknown datatype {}".format(data_type))
            sys.exit(2)
        
        
        try:
            response = self.admin_tools.query_graphql(
                query,
                variables=variables,
                query_name=query_name,
                override_cache=self.override_cache,
            )
            
            # get challenges and input datasets for provided benchmarking event
            data = response.get('data')
            if not isinstance(data, dict):
                self.logger.fatal(f"GraphQL query:\n{query}\nreturned errors:\n{response}")
                sys.exit(2)
            
            if len(data["getBenchmarkingEvents"]) == 0:
                self.logger.fatal(f"Benchmarking event {bench_event_id} is not available in OEB. Please double check the id, or contact OpenEBench support for information about how to open a new benchmarking event")
                sys.exit(2)
            if len(data["getChallenges"]) == 0:

                self.logger.fatal("No challenges associated to benchmarking event " + bench_event_id +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit(2)
            
            # Cleaning up a corner case
            for challenge_ql in data["getChallenges"]:
                if challenge_ql.get("metrics_categories", []) is None:
                    del challenge_ql["metrics_categories"]
            
            # Deserializing _metadata
            benchmarking_events = data.get('getBenchmarkingEvents')
            if benchmarking_events is not None:
                for benchmarking_event in benchmarking_events:
                    metadata = benchmarking_event.get('_metadata')
                    # Deserialize the metadata
                    if isinstance(metadata, str):
                        benchmarking_event['_metadata'] = json.loads(metadata)

            challenges = data.get('getChallenges')
            # mapping from challenge _id to challenge label
            if challenges is not None:
                for challenge in challenges:
                    metadata = challenge.get('_metadata')
                    # Deserialize the metadata
                    if isinstance(metadata,str):
                        challenge_metadata = json.loads(metadata)
                        challenge['_metadata'] = challenge_metadata
                    else:
                        challenge_metadata = None
                    
                    # Deserializing inline_data
                    #datasets = challenge.get('datasets',[])
                    #for dataset in datasets:
                    #    datalink = dataset.get('datalink')
                    #    if datalink is not None:
                    #        inline_data = datalink.get('inline_data')
                    #        if isinstance(inline_data, str):
                    #            datalink['inline_data'] = json.loads(inline_data)
                    
                    # And now, for the embedded datasets and test actions
                    for sub_k in ('datasets', 'event_test_actions', 'metrics_test_actions', 'public_reference_datasets', 'metrics_reference_datasets', 'input_datasets', 'participant_datasets', 'assessment_datasets', 'aggregation_datasets'):
                        sub_v_l = challenge.get(sub_k)
                        if isinstance(sub_v_l, list):
                            for sub_v in sub_v_l:
                                sub_inline_data = sub_v.get('datalink',{}).get('inline_data')
                                if isinstance(sub_inline_data, str):
                                    sub_v['datalink']['inline_data'] = json.loads(sub_inline_data)
                                
                                sub_metadata = sub_v.get('_metadata')
                                if isinstance(sub_metadata, str):
                                    sub_v['_metadata'] = json.loads(sub_metadata)
                            
            
            metrics = data.get('getMetrics')
            if metrics is not None:
                for metric in metrics:
                    metadata = metric.get('_metadata')
                    # Deserialize the metadata
                    if isinstance(metadata,str):
                        metric['_metadata'] = json.loads(metadata)
                    # and representation hints
                    representation_hints = metric.get("representation_hints")
                    if isinstance(representation_hints, str):
                        metric["representation_hints"] = json.loads(representation_hints)
            
            return cast("Mapping[str, Mapping[str, Any]]", response)
        except Exception as e:

            self.logger.exception(e)
            raise e

    # function that uploads the predictions file to a remote server for it long-term storage, and produces a DOI
    def upload_to_storage_service(self, participant_data: "Mapping[str, str]", file_location: "str", contact_email: "str", data_version: "str") -> "str":
        # First, check whether file_location is an URL
        file_location_parsed = urllib.parse.urlparse(file_location)
        
        if len(file_location_parsed.scheme) > 0:
            self.logger.info(
                "Participant's predictions file already has an assigned URI: " + file_location)
            return file_location
        
        if self.storage_server_type == 'b2share':
            endpoint = self.storage_server_endpoint
            # 1. create new record
            self.logger.info("Uploading participant's predictions file to " +
                         endpoint + " for permanent storage")
            header = {"Content-Type": "application/json"}
            params = {'access_token': self.storage_server_token}
            metadata = {"titles": [{"title": "Predictions made by " + participant_data["participant_id"] + " participant in OpenEBench Virtual Research Environment"}],
                        "community": self.storage_server_community,
                        "community_specific": {},
                        "contact_email": contact_email,
                        "version": data_version,
                        "open_access": True}
            r = requests.post(endpoint + "records/", params=params,
                              data=json.dumps(metadata), headers=header)

            result = json.loads(r.text)
            # check whether request was succesful
            if r.status_code != 201:
                self.logger.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            # 2. add file to new record
            filebucketid = result["links"]["files"].split('/')[-1]
            record_id = result["id"]

            try:
                upload_file = open(file_location, 'rb')
            except OSError as exc:
                self.logger.fatal("OS error: {0}".format(exc))
                sys.exit()

            url = endpoint + 'files/' + filebucketid
            header = {"Accept": "application/json",
                      "Content-Type": "application/octet-stream"}

            r = requests.put(url + '/' + os.path.basename(file_location),
                             data=upload_file, params=params, headers=header)

            # check whether request was succesful
            if r.status_code != 200:
                self.logger.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            # 3. publish the new record
            header = {'Content-Type': 'application/json-patch+json'}
            commit_msg = [
                {
                    "op": "add",
                    "path": "/publication_state",
                    "value": "submitted"
                }
            ]
            commit_str = json.dumps(commit_msg)

            url = endpoint + "records/" + record_id + "/draft"
            r = requests.patch(url, data=commit_str, params=params, headers=header)

            # check whether request was succesful
            if r.status_code != 200:
                self.logger.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            published_result = json.loads(r.text)

            data_doi = published_result["metadata"]["DOI"]
            # print(record_id) https://trng-b2share.eudat.eu/api/records/637a25e86dbf43729d30217613f1218b
            self.logger.info("File '" + file_location +
                         "' uploaded and permanent ID assigned: " + data_doi)
            return cast("str", data_doi)
        else:
            self.logger.fatal('Unsupported storage server type {}'.format(self.storage_server_type))
            sys.exit(5)

    def _setup_oeb_validator(self, data_model_source: "Union[str, Tuple[str, Sequence[SchemaHashEntry]]]") -> "None":
        # create the cached json schemas for validation
        self.concept_ids_map = self.admin_tools.fetchIdsAndOrigIds(override_cache=self.override_cache)
        self.schema_validators, self.schema_prefix = self.admin_tools.schemas_manager._setupValidator(
            data_model_source,
            concept_ids_map=self.concept_ids_map,
            flavor=FLAVOR_SANDBOX,
        )

        num_schemas = 0
        schemaMappings: "MutableMapping[str, str]" = {}
        for key in self.schema_validators.getValidSchemas().keys():
            num_schemas += 1
            if key.startswith(self.schema_prefix):
                concept = key[len(self.schema_prefix):]
            else:
                # This "else" should be discarded in the future
                concept = key[key.rindex('/')+1:]
            if concept:
                schemaMappings[concept] = key
        
        if num_schemas == 0:
            print(
                "FATAL ERROR: No schema was successfully loaded. Exiting...\n", file=sys.stderr)
            sys.exit(1)
        
        self.schemaMappings = schemaMappings
        self.num_schemas = num_schemas
        
    def load_schemas_from_repo(self, data_model_dir: "str", tag: "str" = DEFAULT_BDM_TAG) -> "Mapping[str, str]":
        if self.schema_validators is None:
            self._setup_oeb_validator(data_model_dir)
        
        assert self.schemaMappings is not None
        return self.schemaMappings

    def load_schemas_from_server(self) -> "Mapping[str, str]":
        if self.schema_validators is None:
            # fetch in memory the cached json schemas for validation
            data_model_in_memory = self.admin_tools.schemas_manager.checkoutSchemas(fetchFromREST=True, override_cache=self.override_cache)
            self._setup_oeb_validator(data_model_in_memory)
        
        assert self.schemaMappings is not None
        return self.schemaMappings

    def schemas_validation(self, json_data_array: "Sequence[Any]", val_result_filename: "str") -> "None":
        # validate the newly annotated dataset against https://github.com/inab/benchmarking-data-model

        assert self.schema_validators is not None
        self.logger.info(
            "\n\t==================================\n\t8. Validating datasets and TestActions\n\t==================================\n")

        cached_jsons: "MutableSequence[ParsedContentEntry]" = []
        for element in json_data_array:

            cached_jsons.append(
                {'json': element, 'file': "inline " + element["_id"], 'errors': []})

        self.schema_validators.warmUpCaches()
        val_res = self.schema_validators.jsonValidate(
            *cached_jsons, verbose=True)
        
        if val_result_filename is not None:
            self.logger.info("Saving validation result to {}".format(val_result_filename))
            with open(val_result_filename, mode="w", encoding="utf-8") as wb:
                json.dump(val_res, wb)
        
        # check for errors in the validation results
        # skipping the duplicate keys case
        to_error = 0
        to_warning = 0
        to_obj_warning = 0
        to_obj_error = 0
        for val_obj in val_res:
            to_p_error = 0
            to_p_warning = 0
            for error in val_obj['errors']:
                if error['reason'] not in ("dup_pk",):
                    self.logger.fatal("\nObjects validation Failed:\n " + json.dumps(val_obj, indent=4))
                    # self.logger.fatal("\nSee full validation logs:\n " + str(val_res))
                    to_p_error += 1
                else:
                    to_p_warning += 1
            to_warning += to_p_warning
            to_error += to_p_error
            if to_p_warning > 0:
                to_obj_warning += 1
            if to_p_error > 0:
                to_obj_error += 1
        
        if to_error > 0:
            self.logger.error(
                "\n\t==================================\n\t Some objects did not validate\n\t==================================\n")
            
            self.logger.error("Report: {} errors in {} of {} documents".format(to_error, to_obj_error, len(val_res)))
            sys.exit(3)
        
        self.logger.info(
            "\n\t==================================\n\t Objects validated\n\t==================================\n")
        
        self.logger.info("Report: {} duplicated keys in {} of {} documents".format(to_warning, to_obj_warning, len(val_res)))

    def fetchStagedAndSandboxData(self, dataType: "str", filtering_keys: "Mapping[str, Union[Sequence[str], Set[str]]]" = {}) -> "Iterator[Mapping[str, Any]]":
        # sandbox entries take precedence over other ones
        seen: "Set[str]" = set()
        for flavor in (FLAVOR_SANDBOX, FLAVOR_STAGED):
            this_seen: "Set[str]" = set()
            for d in self.admin_tools.iterateConceptEntries(
                dataType,
                flavor=flavor,
                filtering_keys=filtering_keys,
                negative_filtering_keys={
                    "_id": seen
                },
                override_cache=self.override_cache,
            ):
                this_seen.add(d["_id"])
                yield d
            seen.update(this_seen)
    
    def fetchSandboxAndGraphQLStagedData(self, data_type: "str", filtering_keys: "Mapping[str, Union[Sequence[str], Set[str]]]" = {}) -> "Iterator[Mapping[str, Any]]":
        # Special management for _id
        if "_id" in filtering_keys:
            orig_filtering_keys = cast("MutableMapping[str, Union[Sequence[str], Set[str]]]", copy.copy(filtering_keys))
            orig_filtering_keys["orig_id"] = orig_filtering_keys["_id"]
            del orig_filtering_keys["_id"]
        else:
            orig_filtering_keys = None
        
        # sandbox entries take precedence over other ones
        seen: "Set[str]" = set()
        # First, sandbox data
        for d in self.admin_tools.iterateConceptEntries(
            data_type,
            flavor=FLAVOR_SANDBOX,
            filtering_keys=filtering_keys,
            override_cache=self.override_cache,
        ):
            seen.add(d["_id"])
            yield d
        
        if orig_filtering_keys is not None:
            this_seen: "Set[str]" = set()
            for d in self.admin_tools.iterateConceptEntries(
                data_type,
                flavor=FLAVOR_SANDBOX,
                filtering_keys=filtering_keys,
                negative_filtering_keys={
                    "_id": seen
                },
                override_cache=self.override_cache,
            ):
                this_seen.add(d["_id"])
                yield d
            
            seen.update(this_seen)
        
        # Then, query through graphql to fetch the ids
        # and getting the staged entries one by one
        concept_desc = DEFAULT_CONCEPT_DESC.get(data_type)
        if concept_desc is not None and concept_desc[0] is not None:
            result_key = concept_desc[0]
            
            all_conditions = [ filtering_keys ]
            if orig_filtering_keys is not None:
                all_conditions.append(orig_filtering_keys)
            
            for the_filtering_keys in all_conditions:
                varkeys = []
                varvals = []
                # Guessing from the 
                q_filter = data_type[0].lower() + data_type[1:] + "Filters"
                query = f"{q_filter}: {{"
                for key, vals in the_filtering_keys.items():
                    # Plurals are translated to singular
                    if key.endswith("s"):
                        t_key = key[:-1]
                    else:
                        t_key = key
                    if query[-1] != "{":
                        query += ", "
                    query += f"{t_key}: ${t_key}"
                    varkeys.append(t_key)
                    varvals.append(vals)
                query += "}"
                graphql_query = f"""	{result_key}({query}) {{
		_id
	}}
"""
                self.logger.debug(f"graphql query {graphql_query}")
                for comb in itertools.product(*varvals):
                    variables = {
                        k: v
                        for k, v in zip(varkeys, comb)
                    }
                    response = self.admin_tools.query_graphql(
                        graphql_query,
                        variables=variables,
                        override_cache=self.override_cache,
                    )
                    # get challenges and input datasets for provided benchmarking event
                    data = response.get('data')
                    if not isinstance(data, dict):
                        self.logger.fatal(f"graphQL query:\n{query}\nreturned errors:\n{response}")
                        sys.exit(2)
                    for res in data[result_key]:
                        the_id = res["_id"]
                        if the_id in seen:
                            continue
                        seen.add(the_id)
                        yield self.fetchStagedEntry(data_type, the_id)
    
    @memoized_method(maxsize=None)
    def fetchStagedEntry(self, dataType: "str", the_id: "str") -> "Mapping[str, Any]":
        for fetched_data_type, datares_raw in self.admin_tools.fetchEntriesFromIds(
            [the_id],
            flavor=FLAVOR_STAGED,
            override_cache=self.override_cache,
        ):
            if dataType != fetched_data_type:
                raise ValueError(f"Expected {the_id} to be {dataType} instead of {fetched_data_type}")
            return datares_raw
        
        raise LookupError(f"Unable to fetch {the_id} ({dataType})")
        
    def check_min_dataset_collisions(self, input_min_datasets: "Sequence[Mapping[str, Any]]", ch_id_to_label_and_sep: "Mapping[str, ChallengeLabelAndSep]") -> "Sequence[Tuple[str, Optional[str], str]]":
        # This method is only valid for minimal dataset of type participant and assessment
        # Those ones with original ids
        input_d_dict = dict(map(lambda i_d: (i_d["_id"],i_d), input_min_datasets))
        collisions = []
        i_keys = list(input_d_dict.keys())
        
        known_datasets_map: "MutableMapping[str, MutableSequence[IdAndOrigId]]" = {}
        for tp_id in self.admin_tools.getFetchedConceptIdsMap(override_cache=self.override_cache)["Dataset"]:
            known_datasets_map.setdefault(tp_id[0], []).append(tp_id)
            if tp_id[1] is not None:
                known_datasets_map.setdefault(tp_id[1], []).append(tp_id)
            
        for input_min_dataset in input_min_datasets:
            possible_collisions = known_datasets_map.get(input_min_dataset["_id"])
            if possible_collisions is None:
                self.logger.debug(f"No collision {input_min_dataset['_id']}")
                continue
            
            for possible_collision in possible_collisions:
                db_dataset = self.fetchStagedEntry(dataType="Dataset", the_id=possible_collision[0])
                
                has_coll = False
                # The dataset must be of the same type
                if db_dataset["type"] != input_min_dataset["type"]:
                    self.logger.error(f"Dataset type mismatch: orig id {input_min_dataset['_id']} has type {input_min_dataset['type']}, database entry ({db_dataset['_id']}) has {db_dataset['type']}")
                    has_coll = True
                
                # The new challenge ids must be equal or a superset
                # of the dataset in the database
                if isinstance(input_min_dataset["challenge_id"], list):
                    i_ch_set = set(input_min_dataset["challenge_id"])
                else:
                    i_ch_set = set()
                    i_ch_set.add(input_min_dataset["challenge_id"])
                
                # Translation from challenge id to challenge label
                
                db_ch_set: "Set[str]" = set()
                for d_id in db_dataset["challenge_ids"]:
                    c_l = ch_id_to_label_and_sep.get(d_id)
                    if c_l is not None:
                        db_ch_set.add(c_l.label)
                if not db_ch_set.issubset(i_ch_set):
                    self.logger.error(f"Challenges where new dataset {input_min_dataset['_id']} appears is not a superset of the new dataset challenges: {db_ch_set - i_ch_set} ({i_ch_set} vs {db_ch_set})")
                    has_coll = True
            
                # 
                
                if has_coll:
                    collisions.append((cast("str", db_dataset['_id']), cast("Optional[str]", db_dataset.get('orig_id')), cast("str", input_min_dataset['_id'])))
        
        return collisions
    
    def check_dataset_collisions(
        self,
        output_datasets: "Sequence[Mapping[str, Any]]",
#        community_prefix: "str",
#        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        default_schema_url: "Optional[Union[Sequence[str], bool]]" = None
    ) -> "Sequence[Tuple[str, Optional[str], str]]":
        
        # Those ones with original ids
        output_d_list = list(map(lambda o_d: (o_d.get("orig_id"), o_d["_id"], o_d), output_datasets))
        output_d_dict = dict(map(lambda odl: (odl[0], odl[2]) , filter(lambda odl: odl[0] is not None, output_d_list)))
        output_d_dict.update(map(lambda odl: (odl[1], odl[2]) , filter(lambda odl: odl[0] is None, output_d_list)))
        
        collisions = []
        
        db_d_map: "MutableMapping[str, IdAndOrigId]" = {}
        db_orig_d_map: "MutableMapping[str, MutableSequence[IdAndOrigId]]" = {}
        for tp_id in self.admin_tools.getFetchedConceptIdsMap(override_cache=self.override_cache)["Dataset"]:
            db_d_map[tp_id[0]] = tp_id
            if tp_id[1] is not None:
                db_orig_d_map.setdefault(tp_id[1], []).append(tp_id)

        
        o_keys = list(output_d_dict.keys())
        for o_dataset in output_datasets:
#            # Early check
#            if o_dataset["type"] == OEBDatasetType.Participant.value:
#                self.gen_expected_participant_original_id(o_dataset, community_prefix, bench_event_prefix_et_al, "")
#            elif o_dataset["type"] == OEBDatasetType.Assessment.value:
#                self.gen_expected_assessment_original_id(o_dataset, community_prefix, bench_event_prefix_et_al, "", "")
            
            should_exit = False
            # Should we validate the inline data?
            o_datalink = o_dataset.get("datalink")
            o_type = o_dataset["type"]
            o_id = cast("str", o_dataset["_id"])
            o_orig_id = o_dataset.get("orig_id")
            if (not isinstance(default_schema_url, bool) or default_schema_url) and isinstance(o_datalink, dict):
                the_error, fetched_inline_data = self.admin_tools.fetchInlineDataFromDatalink(
                    o_id,
                    o_dataset["datalink"],
                    discard_unvalidable=o_type == OEBDatasetType.Participant.value,
                    override_cache=self.override_cache,
                )
                
                if the_error is not None:
                    self.logger.error(f"Fetchable inline data errors from dataset {o_id} ({o_orig_id})\n{the_error}")
                    should_exit = True
                elif fetched_inline_data is not None:
                    if isinstance(fetched_inline_data, FetchedInlineData):
                        inline_data = fetched_inline_data.data
                        # Is this dataset an inline one?
                        schema_uri = fetched_inline_data.schema_uri
                        
                        guess_unmatched: "Union[bool, Sequence[str]]"
                        if schema_uri is None:
                            guess_unmatched = True
                        else:
                            guess_unmatched = [
                                schema_uri
                            ]
                        
                        # Now, time to validate the dataset
                        config_val_list = self.level2_min_validator.jsonValidate({
                            "json": inline_data,
                            "file": "inline " + o_id,
                            "errors": [],
                        }, guess_unmatched=guess_unmatched)
                        assert len(config_val_list) > 0
                        config_val_block = config_val_list[0]
                        config_val_block_errors = list(filter(lambda ve: (schema_uri is None) or (ve.get("schema_id") == schema_uri), config_val_block.get("errors", [])))
                        if len(config_val_block_errors) > 0:
                            self.logger.error(f"Validation errors in inline data from dataset {o_id} ({o_orig_id}) using {schema_uri}\n{config_val_block_errors}")
                            should_exit = True
                    else:
                        self.logger.error(f"Fetched data from dataset {o_id} ({o_orig_id}) cannot be validated, as it does not have schema_uri")
                        should_exit = True
            
            
            db_match = db_d_map.get(o_id)
            db_o_matches = db_orig_d_map.get(o_id)
            
            # The different ambiguity corner cases (take 1)
            if db_match is not None and db_o_matches is not None:
                self.logger.error(f"Database could be poisoned, as {o_id} output matched more than one entry, {o_id} and {', '.join(map(lambda aa: aa[0], db_o_matches))}")
                self.logger.error(json.dumps(o_dataset, sort_keys=True, indent=4))
                for db_o_match in db_o_matches:
                    collisions.append((o_id, o_orig_id, db_o_match[0]))
                continue
            
            if o_orig_id is not None:
                db_i_orig_match = db_d_map.get(o_orig_id)
                db_orig_matches = db_orig_d_map.get(o_orig_id)
            
                if db_i_orig_match is not None:
                    if db_orig_matches is not None:
                        self.logger.error(f"Database could be poisoned, as {o_orig_id} orig output matched two entries, {o_orig_id} and {', '.join(map(lambda aa: aa[0], db_orig_matches))}")
                        #self.logger.error(json.dumps(db_orig_dataset, sort_keys=True, indent=4))
                    else:
                        self.logger.error(f"Database could be poisoned, as {o_orig_id} orig output matched entry with {o_orig_id} id")
                    
                    #self.logger.error(json.dumps(db_i_orig_dataset, sort_keys=True, indent=4))
                    self.logger.error(json.dumps(o_dataset, sort_keys=True, indent=4))
                    if isinstance(db_orig_matches, list):
                        for db_orig_match in db_orig_matches:
                            collisions.append((o_id, o_orig_id, db_orig_match[0]))
                    else:
                        collisions.append((o_id, o_orig_id, "MISSING_DB_ORIG_DATASET"))
                    continue
                elif db_orig_matches is not None:
                    if db_match is None:
                        db_match = db_orig_matches[0]
                    elif db_orig_matches[0][0] != db_match[0]:
                        self.logger.error(f"Output dataset {o_id} (orig {o_orig_id}) matches two different database datasets:")
                        #self.logger.error(json.dumps(d_dataset, sort_keys=True, indent=4))
                        #self.logger.error(json.dumps(db_orig_dataset, sort_keys=True, indent=4))
                        for db_orig_match in db_orig_matches:
                            collisions.append((o_id, o_orig_id, db_orig_match[0]))
                        continue

            if db_match is not None:
                d_id = db_match[0]
                d_orig_id = db_match[1]
            elif db_o_matches is not None:
                d_id = db_o_matches[0][0]
                d_orig_id = db_o_matches[0][1]
            else:
                self.logger.info(f"Nothing matched output dataset {o_id} (orig {o_orig_id})")
                continue
            
            d_dataset = self.fetchStagedEntry('Dataset', d_id)
            
            # Now, corner cases
            # Case 1 o_id matches d_orig_id => good
            # Case 2 o_id matches d_id
            # Case 2.a o_orig_id matches d_orig_id => good
            # Case 2.b o_orig_id does not match d_orig_id => bad
            
            has_coll = False
            if d_orig_id is not None and d_orig_id == o_id:
                pass
            elif d_id == o_id and o_orig_id is not None and d_orig_id is not None and o_orig_id != d_orig_id:
                self.logger.error(f"Mismatches in the pairs of (_id, orig_id): ({o_id}, {o_orig_id}) vs ({d_id}, {d_orig_id})")
                has_coll = True
            
            # The dataset must be of the same type
            if d_dataset["type"] != o_dataset["type"]:
                self.logger.error(f"Dataset type mismatch: id {o_id} has type {o_dataset['type']}, database entry ({d_id}) has {d_dataset['type']}")
                has_coll = True
            
            # The new challenge ids must be equal or a superset
            # of the dataset in the database
            o_ch_set = set(o_dataset["challenge_ids"])
            d_ch_set = set(d_dataset["challenge_ids"])
            if not d_ch_set.issubset(o_ch_set):
                self.logger.error(f"New dataset {o_id} challenges are not a superset of the dataset challenges: {d_ch_set} vs {o_ch_set}. (Tip: ill management of original ids on community side?)")
                has_coll = True
            
            # 
            
            if has_coll or should_exit:
                collisions.append((d_id, d_orig_id, o_id))
        
        return collisions
    
    def generate_manifest_dataset(
        self,
        dataset_submission_id: "str",
        community_id: "str",
        benchmarking_event_id: "str",
        version: "str",
        data_visibility: "DatasetsVisibility",
        final_data: "Sequence[Any]"
    ) -> "Mapping[str, Any]":
        """
        This method receives both a dataset submission id and
        the array of data elements (datasets, testactions) to
        be stored in the database
        """
        
        assert self.schemaMappings is not None
        dataset_schema = self.schemaMappings['Dataset']
        umbrella_assembling_timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        
        unique_contacts = set()
        unique_challenges = set()
        rel_dataset_ids = []
        for elem in final_data:
            if elem['_schema'] == dataset_schema:
                for dataset_contact_id in elem['dataset_contact_ids']:
                    unique_contacts.add(dataset_contact_id)
                for challenge_id in elem['challenge_ids']:
                    unique_challenges.add(challenge_id)
                rel_dataset_ids.append({
                    'dataset_id': elem['_id'],
                    'role': 'dependency'
                })
        
        umbrella = {
            '_id': dataset_submission_id,
            '_schema': dataset_schema,
            'community_ids': [ community_id ],
            'challenge_ids': list(unique_challenges),
            'visibility': data_visibility,
            'name': dataset_submission_id,
            'version': version,
            'description': f'Manifest dataset {dataset_submission_id} from consolidated data',
            'dates': {
                'creation': umbrella_assembling_timestamp,
                'modification': umbrella_assembling_timestamp,
            },
            'type': 'other',
            'datalink': {
                'uri': f'oeb:{benchmarking_event_id}',
                'attrs': [
                    'curie'
                ]
            },
            'dataset_contact_ids': list(unique_contacts),
            'depends_on': {
                'rel_dataset_ids': rel_dataset_ids,
            }
        }
        
        return umbrella
    
    def validate_and_submit_oeb_buffer(
        self,
        community_id: "str",
        json_data: "Sequence[Mapping[str, Any]]",
        val_result_filename: "str",
        payload_mode: "PayloadMode" = PayloadMode.THRESHOLD,
    ) -> "None":
        self.logger.info(f"\n\t==================================\n\t8. Uploading workflow results to {self.oeb_submission_api}\n\t==================================\n")
        
        assert self.schema_prefix is not None
        assert self.schema_validators is not None
        
        sendable_entries = list(map(lambda e: YieldedInput(content=e, path="inline " + e["_id"]), json_data))
        
        try:
            self.admin_tools.data_upload(
                community_id,
                sendable_entries,
                val_result_filename,
                data_model_dir=(self.schema_prefix, list(self.schema_validators.getValidSchemas().values())),
                deep_schemas_dir=get_oeb_level2_schemas_path(),
                payload_mode=payload_mode,
                override_cache=self.override_cache,
            )
        except urllib.error.HTTPError as he:
            self.logger.fatal(f"Error in uploading data to OpenEBench. Bad request: {he.code} {he.reason}\n{he.read()!r}")
            sys.exit()
        else:
            self.logger.info(
                "\n\tData uploaded correctly...finalizing migration\n\n")

    @staticmethod
    def getMetricsTrioFromMetric(mmi: "Mapping[str, Any]", community_prefix: "str", tool_id: "Optional[str]") -> "MetricsTrio":
        # Getting a proposed label
        proposed_label: "str"
        all_labels: "MutableSequence[str]" = []
        mmi_metadata = mmi.get("_metadata")
        
        if isinstance(mmi_metadata, dict) and (METRIC_ID_KEY in mmi_metadata):
            possible_proposed_label = cast("Optional[Union[str, Sequence[str]]]", mmi_metadata[METRIC_ID_KEY])
            if possible_proposed_label is not None:
                if isinstance(possible_proposed_label, list):
                    all_labels.extend(possible_proposed_label)
                else:
                    all_labels.append(cast("str",possible_proposed_label))
        
        if mmi["orig_id"].startswith(community_prefix):
            proposed_label = cast("str", mmi["orig_id"][len(community_prefix):])
            if proposed_label is not None:
                all_labels.append(proposed_label)
        
        proposed_label = cast("str", mmi["orig_id"])
        if proposed_label is not None:
            all_labels.append(proposed_label)
        
        assert len(all_labels) > 0
        return MetricsTrio(
            metrics_id=mmi["_id"],
            tool_id=tool_id,
            proposed_label=all_labels[0],
            all_labels=all_labels,
            representation_hints=mmi.get("representation_hints"),
        )
    
    def getMetricsTrioFromMetricsId(self, metrics_id: "str", community_prefix: "str", tool_id: "Optional[str]") -> "MetricsTrio":
        mmi = self.fetchStagedEntry("Metrics", metrics_id)
        return self.getMetricsTrioFromMetric(mmi, community_prefix, tool_id)

def rchop(s: "str", sub: "str") -> "str":
    return s[:-len(sub)] if s.endswith(sub) else s
