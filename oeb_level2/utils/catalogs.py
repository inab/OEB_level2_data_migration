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

import dataclasses
import json
import logging
import re
import sys

from oebtools.fetch import (
    FetchedInlineData,
    OEB_CONCEPT_PREFIXES,
    OEBFetcher,
)

from ..schemas import (
    ASSESSMENT_INLINE_SCHEMAS,
    AGGREGATION_INLINE_SCHEMAS,
)

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from types import ModuleType
    
    from typing import (
        Any,
        Iterable,
        Iterator,
        KeysView,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Tuple,
        Union,
    )
    
    from typing_extensions import (
        Literal,
        NotRequired,
        TypedDict,
    )
    
    from .migration_utils import (
        ChallengeLabelAndSep,
    )
    
    class InlineDataLabel(TypedDict):
        """
        This typed dict mimics the one in the inline data,
        so it cannot be altered or renamed
        """
        label: "str"
        dataset_orig_id: "str"
    
    from extended_json_schema_validator.extensible_validator import (
        ExtensibleValidator
    )

class DatasetValidationSchema(NamedTuple):
    dataset_id: "str"
    schema_id: "Optional[str]"

class InlineDataLabelPair(NamedTuple):
    label: "InlineDataLabel"
    dataset_id: "str"

from .migration_utils import (
    ASSESSMENT_CATEGORY_LABEL,
    BenchmarkingEventPrefixEtAl,
    DATASET_ORIG_ID_SUFFIX,
    EXCLUDE_PARTICIPANT_KEY,
    METRIC_ID_KEY,
    MetricsTrio,
    OEBDatasetType,
    OpenEBenchUtils,
    PARTICIPANT_ID_KEY,
    TEST_ACTION_ORIG_ID_SUFFIX,
)

from ..schemas import (
    TYPE2SCHEMA_ID,
    VIS_2D_PLOT,
    VIS_AGG_DATA_SERIES,
    VIS_BAR_PLOT,
)


def gen_challenge_assessment_metrics_dict(the_challenge: "Mapping[str, Any]") -> "Mapping[str, Mapping[str, str]]":
    for metrics_category in the_challenge.get("metrics_categories",[]):
        if metrics_category.get("category") == ASSESSMENT_CATEGORY_LABEL:
            challenge_assessment_metrics = metrics_category.get("metrics", [])
            cam_d = {
                cam["metrics_id"]: cam
                for cam in challenge_assessment_metrics
            }
            
            return cam_d
    
    return {}

def gen_inline_data_label_from_participant_dataset(par_dataset: "Mapping[str, Any]", default_label: "Optional[str]" = None) -> "InlineDataLabelPair":
    # First, look for the label in the participant dataset
    par_metadata = par_dataset.get("_metadata",{})
    par_label = None if par_metadata is None else par_metadata.get(PARTICIPANT_ID_KEY)
    # Then, look for it in the assessment dataset
    if par_label is None:
        par_label = default_label
    
    # Now, trying pattern matching
    # to extract the label
    if par_label is None:
        match_p = re.search(r"Predictions made by (.*) participant", par_dataset["description"])
        if match_p:
            par_label = match_p.group(1)
    
    # Last chance is guessing from the original id!!!!
    if par_label is None:
        par_orig_id = par_dataset.get("orig_id", par_dataset["_id"])
        if ':' in par_orig_id:
            par_label = par_orig_id[par_orig_id.index(':') + 1 :]
        else:
            par_label = par_orig_id
        
        # Removing suffix
        if par_label.endswith("_P"):
            par_label = par_label[:-2]
    
    par_dataset_id = cast("str", par_dataset["_id"])
    inline_data_label: "InlineDataLabel" = {
        "label": par_label,
        "dataset_orig_id": par_dataset.get("orig_id", par_dataset_id),
    }
    return InlineDataLabelPair(
        label=inline_data_label,
        dataset_id=par_dataset_id,
    )


def gen_inline_data_label_from_assessment_and_participant_dataset(
    ass_dataset: "Mapping[str, Any]",
    par_dataset: "Mapping[str, Any]",
    proposed_metrics_label: "Optional[str]" = None,
    metrics_id: "Optional[str]" = None,
) -> "InlineDataLabelPair":
    # With this change, minimal assessment datasets also work
    default_ass_label = ass_dataset.get("participant_id")
    metrics_label: "Optional[str]"
    if default_ass_label is None:
        ass_metadata = ass_dataset.get("_metadata",{})
        if ass_metadata is None:
            default_ass_label = None
        else:
            default_ass_label = ass_metadata.get(PARTICIPANT_ID_KEY)

        if metrics_id is not None:
            metrics_label = metrics_id
        else:
            metrics_label = ass_dataset.get("depends_on", {}).get("metrics_id", "")
    elif proposed_metrics_label is not None:
        metrics_label = proposed_metrics_label
    elif metrics_id is not None:
        metrics_label = metrics_id
    else:
        # Not so correct, but it should work
        metrics_label = ass_dataset.get("metrics", {}).get("metric_id", "")

    assert metrics_label is not None

    part_label_pair = gen_inline_data_label_from_participant_dataset(par_dataset, default_label=default_ass_label)

    ass_dataset_id = cast("str", ass_dataset["_id"])
    inline_data_label: "InlineDataLabel" = {
        "label": part_label_pair.label["label"] + " " + metrics_label,
        "dataset_orig_id": ass_dataset.get("orig_id", ass_dataset_id),
    }
    return InlineDataLabelPair(
        label=inline_data_label,
        dataset_id=ass_dataset_id,
    )


def gen_inline_data_label(met_dataset: "Mapping[str, Any]", par_datasets: "Sequence[Mapping[str, Any]]") -> "Optional[InlineDataLabelPair]":
    met_metadata = met_dataset.get("_metadata",{})
    met_label = None if met_metadata is None else met_metadata.get(PARTICIPANT_ID_KEY)
    if len(par_datasets) > 0:
        return gen_inline_data_label_from_participant_dataset(par_datasets[0], default_label=met_label)
    
    return None

def match_metric_from_label(logger: "Union[logging.Logger, ModuleType]", metrics_graphql: "Sequence[Mapping[str, Any]]", community_prefix: "str", metrics_label: "str", challenge_id: "str", challenge_acronym: "str", challenge_assessment_metrics_d: "Mapping[str, Mapping[str, str]]", dataset_id: "Optional[str]" = None) -> "Optional[MetricsTrio]":
    # Select the metrics just "guessing"
    guessed_metrics = []
    exact_match = False
    dataset_metrics_id_u = metrics_label.upper()
    # There should be a cleaner way to have this
    preferred_id_prefix = "OEBM" + challenge_id[4:7]
    
    # This sort is to give some precedence to metrics from the same community as the challenge
    # over all the other metrics
    for metric in sorted(metrics_graphql, key=lambda e: e["_id"][0:7] != preferred_id_prefix):
        # Could the metrics label match the metrics id?
        if metric['_id'] == metrics_label:
            guessed_metrics = [ metric ]
            exact_match = True
            break
        metric_metadata = metric.get("_metadata")
        # First guess
        no_guess = True
        if isinstance(metric_metadata, dict) and METRIC_ID_KEY in metric_metadata:
            possible_metric_labels = cast("Optional[Union[str, Sequence[str]]]", metric_metadata[METRIC_ID_KEY])
            if possible_metric_labels is not None:
                if isinstance(possible_metric_labels, str):
                    possible_metric_labels = [ possible_metric_labels ]
                for possible_metric_label in possible_metric_labels:
                    if possible_metric_label.upper() == dataset_metrics_id_u:
                        if metric["_id"].startswith(preferred_id_prefix):
                            guessed_metrics = [ metric ]
                            exact_match = True
                            no_guess = False
                            break
                        else:
                            # Different community, we cannot assure
                            guessed_metrics.append(metric)
                            no_guess = False
        
        if no_guess and metric['orig_id'].startswith(community_prefix):
            # Second guess (it can introduce false crosses)
            if metric["orig_id"][len(community_prefix):].upper() == dataset_metrics_id_u:
                guessed_metrics = [ metric ]
                exact_match = True
                break
            elif metric["orig_id"][len(community_prefix):].upper().startswith(dataset_metrics_id_u):
                guessed_metrics.append(metric)
    
    if len(guessed_metrics) == 0:
        logger.critical(f"For {dataset_id}, unable to match in OEB a metric to label {metrics_label} . Please contact OpenEBench support for information about how to register your own metrics and link them to the challenge {challenge_id} (acronym {challenge_acronym})")
        return None
        #should_end.append((the_challenge['_id'], the_challenge['acronym']))
        #continue
    
    matched_metrics = []
    for guessed_metric in guessed_metrics:
        cam = challenge_assessment_metrics_d.get(guessed_metric["_id"])
        if cam is not None:
            matched_metrics.append((cam, guessed_metric))
    
    metric_id: "Optional[str]" = None
    tool_id: "Optional[str]" = None
    mmi = None
    if len(matched_metrics) == 0:
        if len(guessed_metrics) == 1:
            mmi = guessed_metrics[0]
            metric_id = mmi["_id"]
            logger.warning(f"Metric {metric_id} (guessed from {metrics_label} at dataset {dataset_id}) is not registered as an assessment metric at challenge {challenge_id} (acronym {challenge_acronym}). Consider register it")
        else:
            logger.critical(f"Several metrics {', '.join(map(lambda gm: cast('str', gm['_id']), guessed_metrics))} were guessed from {metrics_label} at dataset {dataset_id} . No clever heuristic can be applied. Please properly register some of them as an assessment metric at challenge {challenge_id} (acronym {challenge_acronym}).")
            #should_end.append((the_challenge['_id'], the_challenge['acronym']))
            #continue
    elif len(matched_metrics) == 1:
        mmi_cam, mmi = matched_metrics[0]
        metric_id = mmi_cam['metrics_id']
        tool_id = mmi_cam.get('tool_id')
    else:
        logger.critical(f"{len(matched_metrics)} metrics registered at challenge {challenge_id} (acronym {challenge_acronym}) matched from {metrics_label} at dataset {dataset_id} : {', '.join(map(lambda mm: mm[0]['metrics_id'], matched_metrics))}. Fix the challenge declaration.")
        #should_end.append((the_challenge['_id'], the_challenge['acronym']))
        #continue
    
    
    if mmi is not None:
        return OpenEBenchUtils.getMetricsTrioFromMetric(mmi, community_prefix, tool_id)
    else:
        return None


DATASET_ID_PREFIX = OEB_CONCEPT_PREFIXES['Dataset']
TEST_ACTION_ID_PREFIX = OEB_CONCEPT_PREFIXES['TestAction']

@dataclasses.dataclass
class IndexedDatasets:
    # The dataset type of all the datasets
    type: "OEBDatasetType"
    # Which logger to use
    logger: "Union[logging.Logger, ModuleType]"
    # Validator for inline data
    level2_min_validator: "ExtensibleValidator"
    admin_tools: "OEBFetcher"
    override_cache: "bool"
    metrics_graphql: "Sequence[Mapping[str, Any]]"
    community_prefix: "str"
    challenge_prefix: "str"
    challenge: "Mapping[str, Any]"
    challenge_label_and_sep: "ChallengeLabelAndSep"
    cam_d: "Mapping[str, Mapping[str, str]]"
    # dataset categories
    d_categories: "Optional[Sequence[Mapping[str, Any]]]"
    # Benchmarking event prefix, separator, and aggregation separator
    bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl" = dataclasses.field(default_factory=BenchmarkingEventPrefixEtAl)
    # Let's index the datasets
    # by _id and orig_id
    # In the case of assessment datasets
    # they are combined to get the aggregation datasets
    d_list: "MutableSequence[Mapping[str, Any]]" = dataclasses.field(default_factory=list)
    # Maps the dataset ids and orig_ids to their
    # place in the previous list
    d_dict: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    # Maps the dataset ids and orig_ids to their
    # inline data contents
    d_inline_dict: "MutableMapping[str, FetchedInlineData]" = dataclasses.field(default_factory=dict)
    # (assessment and aggregation)
    # Groups the assessment datasets by metrics
    d_m_dict: "MutableMapping[str, MutableSequence[int]]" = dataclasses.field(default_factory=dict)
    # (aggregation)
    # Maps datasets to lists of metric mappings
    metrics_by_d: "MutableMapping[str, Sequence[Optional[MetricsTrio]]]" = dataclasses.field(default_factory=dict)
    
    def index_dataset(self, raw_dataset: "Mapping[str, Any]") -> "Optional[DatasetValidationSchema]":
        d_type = raw_dataset["type"]
        
        if d_type != self.type.value:
            self.logger.error(f"This instance is focused on datasets of type {self.type.value}, not of type {d_type}")
            return None
        
        is_participant = self.type == OEBDatasetType.Participant
        is_assessment = self.type == OEBDatasetType.Assessment
        is_aggregation = self.type == OEBDatasetType.Aggregation
        
        # Now, time to record the position where the assessment
        # dataset is going to be in the list of assessment datasets
        index_id_orig = cast("Optional[str]", raw_dataset.get("orig_id"))
        index_id = cast("str", raw_dataset["_id"])
        d_pos = self.d_dict.get(index_id_orig) if index_id_orig is not None else None
        if d_pos is None:
            d_pos = self.d_dict.get(index_id)
        
        # Restrictions at the dataset level
        id_to_check = None
        if index_id_orig is not None:
            id_to_check = index_id_orig
        if (id_to_check is None) and not index_id.startswith(DATASET_ID_PREFIX):
            id_to_check = index_id
        
        if id_to_check is not None:
            if not id_to_check.startswith(self.community_prefix):
                self.logger.warning(f"Dataset {index_id} with orig id {id_to_check} (type {self.type.value}) does not start with the community prefix {self.community_prefix}. You should fix it to avoid possible duplicates")
            if is_participant or is_assessment or is_aggregation:
                if not id_to_check.startswith(self.bench_event_prefix_et_al.prefix):
                    self.logger.warning(f"Dataset {index_id} with orig id {id_to_check} (type {self.type.value}) does not start with the benchmarking event prefix {self.bench_event_prefix_et_al.prefix}. You should fix it to avoid possible duplicates")
            if len(raw_dataset.get("challenge_ids",[])) == 1 or is_assessment or is_aggregation:
                if not id_to_check.startswith(self.challenge_prefix):
                    self.logger.warning(f"Dataset {index_id} with orig id {id_to_check} (type {self.type.value}) does not start with the challenge prefix {self.challenge_prefix}. You should fix it to avoid possible duplicates")
        
            expected_suffix = DATASET_ORIG_ID_SUFFIX.get(self.type)
            if (expected_suffix is not None) and not id_to_check.endswith(expected_suffix):
                self.logger.warning(f"Dataset {index_id} with orig id {id_to_check} (type {self.type.value}) does not end with the expected suffix {expected_suffix}. You should fix it to avoid possible duplicates")
        
        # Some validations
        # Validating the category where it matches
        d_on_metrics_id = None
        d_on_tool_id = None
        if is_assessment or is_aggregation:
            raw_challenge_ids = raw_dataset.get("challenge_ids",[])
            if len(raw_challenge_ids) > 1:
                self.logger.warning(f"The number of challenges for dataset {index_id} ({index_id_orig} , type {self.type.value}) should be 1, but it is {len(raw_challenge_ids)}. Fix the entry keeping only the rightful challenge id ({self.challenge['_id']})")
            
            d_on = raw_dataset.get("depends_on", {})
            d_on_metrics_id = d_on.get("metrics_id")
            d_on_tool_id = d_on.get("tool_id")
            
            m_matched = None
            m_tool_should = None
            if self.d_categories is not None:
                for m_cat in self.d_categories:
                    for m in m_cat.get("metrics", []):
                        if m.get("metrics_id") == d_on_metrics_id:
                            potential_tool_id = m.get("tool_id")
                            
                            if potential_tool_id == d_on_tool_id:
                                m_matched = d_on_metrics_id
                                break
                            elif m_tool_should is not None:
                                self.logger.warning(f"Metrics {d_on_metrics_id}, used for {self.type.value}, registered more than once in challenge {raw_dataset['challenge_ids'][0]}. Database contents should be curated")
                            else:
                                m_tool_should = potential_tool_id
                    if m_matched is not None:
                        break
            
            if m_matched is None:
                if m_tool_should is None:
                    self.logger.error(f"{self.type.value.capitalize()} dataset {index_id} ({'in database' if d_pos is None else 'to be submitted'}) from challenge {', '.join(raw_dataset['challenge_ids'])} depends on metric {d_on_metrics_id} implemented by {d_on_tool_id}, which are not registered as valid {self.type.value} challenge metrics. Fix it")
                else:
                    self.logger.error(f"{self.type.value.capitalize()} dataset {index_id} ({'in database' if d_pos is None else 'to be submitted'}) from challenge {', '.join(raw_dataset['challenge_ids'])} matched metric {d_on_metrics_id}, but mismatched implementation ({d_on_tool_id} instead of {m_tool_should}). Fix it")
                    
                return None
        
        # Some additional validations for inline data
        o_datalink = raw_dataset.get("datalink")
        the_data = None
        fetchable_uri: "str" = "(unknown source)"
        schema_uri: "Optional[str]" = None
        validable_schemas: "Optional[Sequence[str]]" = None
        fetched_inline_data: "Optional[FetchedInlineData]" = None
        if isinstance(o_datalink, dict):
            the_error, fetched_payload = self.admin_tools.fetchInlineDataFromDatalink(index_id, o_datalink, discard_unvalidable=not is_assessment and not is_aggregation, override_cache=self.override_cache)
            
            # Is this dataset an inline one?
            if the_error is None and isinstance(fetched_payload, FetchedInlineData):
                fetched_inline_data = fetched_payload
                the_data = fetched_inline_data.data
                # Learning / guessing the schema_url
                schema_uri = fetched_inline_data.schema_uri
                if schema_uri is None:
                    if is_assessment:
                        validable_schemas = ASSESSMENT_INLINE_SCHEMAS
                    elif is_aggregation:
                        validable_schemas = AGGREGATION_INLINE_SCHEMAS
                else:
                    validable_schemas = [
                        schema_uri
                    ]
                
                if fetched_inline_data.remote is not None:
                    fetchable_uri = fetched_inline_data.remote.fetchable_uri
                else:
                    fetchable_uri = "inline " + index_id
        
        found_schema_url: "Optional[str]" = None
        if the_data is not None and validable_schemas is not None:
            # Now, time to validate the dataset
            config_val_list = self.level2_min_validator.jsonValidate({
                "json": the_data,
                "file": fetchable_uri,
                "errors": [],
            }, guess_unmatched=validable_schemas)
            assert len(config_val_list) > 0
            config_val_block = config_val_list[0]
            found_schema_url = config_val_block.get("schema_id")
            config_val_block_errors = list(filter(lambda ve: (schema_uri is None) or (ve.get("schema_id") == schema_uri), config_val_block.get("errors", [])))
            
            if len(config_val_block_errors) > 0:
                self.logger.error(f"Validation errors in {fetchable_uri} from {self.type.value} dataset {index_id} ({index_id_orig}) using {validable_schemas}. It should be rebuilt\n{json.dumps(config_val_block_errors, indent=4)}")
                # This commented out in order to give a try to propose a fixed version
                #return None
        
        if is_aggregation:
            if not isinstance(o_datalink, dict):
                # Skip it, we cannot work with it
                self.logger.info(f"Skipping {self.type.value} dataset {index_id} indexing, as it does not have a datalink")
                return None
                
            # Is this dataset an inline one?
            if the_data is None:
                # Skip it, we cannot work with it
                self.logger.info(f"Skipping {self.type.value} dataset {index_id} indexing, as it does not contain (inline) data")
                return None
        
            vis_hints = the_data.get("visualization", {})
            vis_type = vis_hints.get("type")
            if len(vis_hints) == 0:
                self.logger.warning(f"No visualization type for {self.type.value} dataset {index_id}. Is it missing or intentional??")
                # TODO: What should we do????
            elif vis_type in TYPE2SCHEMA_ID:
                suffix = None
                proposed_suffix: "str" = "FIX_UNDEFINED_METRIC_LABEL"
                
                if vis_type == VIS_2D_PLOT:
                    x_axis_metric_label = vis_hints.get("x_axis")
                    y_axis_metric_label = vis_hints.get("y_axis")
                    if x_axis_metric_label is None:
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} of visualization type {vis_type} did not define x_axis label. Fix it")
                    
                    if y_axis_metric_label is None:
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} of visualization type {vis_type} did not define y_axis label. Fix it")
                    
                    if x_axis_metric_label is not None and y_axis_metric_label is not None:
                        # Check there is some matching metric
                        x_trio = match_metric_from_label(
                            logger=self.logger,
                            metrics_graphql=self.metrics_graphql,
                            community_prefix=self.community_prefix,
                            metrics_label=x_axis_metric_label,
                            challenge_id=self.challenge['_id'],
                            challenge_acronym=self.challenge['acronym'],
                            challenge_assessment_metrics_d=self.cam_d,
                            dataset_id=index_id,
                        )
                        if x_trio is not None:
                            x_trio_proposed_label = x_trio.proposed_label
                        else:
                            self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} uses for x axis unmatched metric {x_axis_metric_label}. Fix it")
                            x_trio_proposed_label = "FIX_UNMATCHED_METRIC_LABEL"
                            
                        y_trio = match_metric_from_label(
                            logger=self.logger,
                            metrics_graphql=self.metrics_graphql,
                            community_prefix=self.community_prefix,
                            metrics_label=y_axis_metric_label,
                            challenge_id=self.challenge['_id'],
                            challenge_acronym=self.challenge['acronym'],
                            challenge_assessment_metrics_d=self.cam_d,
                            dataset_id=index_id,
                        )
                        if y_trio is not None:
                            y_trio_proposed_label = y_trio.proposed_label
                        else:
                            self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} uses for y axis unmatched metric {y_axis_metric_label}. Fix it")
                            y_trio_proposed_label = "FIX_UNMATCHED_METRIC_LABEL"
                        
                        # Saving it for later usage
                        self.metrics_by_d[index_id] = [
                            x_trio,
                            y_trio,
                        ]
                        
                        # Check the suffix
                        suffix = self.challenge_label_and_sep.sep + f"{x_axis_metric_label}{self.challenge_label_and_sep.metrics_label_sep}{y_axis_metric_label}"
                        proposed_suffix = self.challenge_label_and_sep.sep + f"{x_trio_proposed_label}{self.challenge_label_and_sep.metrics_label_sep}{y_trio_proposed_label}"
                elif vis_type == VIS_BAR_PLOT:
                    metrics_label = vis_hints.get("metric")
                    if metrics_label is None:
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} of visualization type {vis_type} did not define metric label. Fix it")
                    else:
                        # Check there is some matching metric
                        trio = match_metric_from_label(
                            logger=self.logger,
                            metrics_graphql=self.metrics_graphql,
                            community_prefix=self.community_prefix,
                            metrics_label=metrics_label,
                            challenge_id=self.challenge['_id'],
                            challenge_acronym=self.challenge['acronym'],
                            challenge_assessment_metrics_d=self.cam_d,
                            dataset_id=index_id,
                        )
                        if trio is not None:
                            trio_proposed_label = trio.proposed_label
                        else:
                            self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} uses unmatched metric {metrics_label}. Fix it")
                            trio_proposed_label = "FIX_UNMATCHED_METRIC_LABEL"
                        
                        # Saving it for later usage
                        self.metrics_by_d[index_id] = [
                            trio,
                        ]
                        
                        # Check the suffix
                        suffix = self.challenge_label_and_sep.sep + metrics_label
                        proposed_suffix = self.challenge_label_and_sep.sep + trio_proposed_label
                elif vis_type in VIS_AGG_DATA_SERIES:
                    available_metrics = vis_hints.get("available_metrics")
                    if available_metrics is None:
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} of visualization type {vis_type} did not define available metrics labels. Fix it")
                    else:
                        matched_trios = []
                        proposed_labels = []
                        for metrics_label in available_metrics:
                            # Check there is some matching metric
                            trio = match_metric_from_label(
                                logger=self.logger,
                                metrics_graphql=self.metrics_graphql,
                                community_prefix=self.community_prefix,
                                metrics_label=metrics_label,
                                challenge_id=self.challenge['_id'],
                                challenge_acronym=self.challenge['acronym'],
                                challenge_assessment_metrics_d=self.cam_d,
                                dataset_id=index_id,
                            )
                            matched_trios.append(trio)
                            if trio is not None:
                                trio_proposed_label = trio.proposed_label
                            else:
                                self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} uses unmatched metric {metrics_label}. Fix it")
                                trio_proposed_label = "FIX_UNMATCHED_METRIC_LABEL"
                            proposed_labels.append(trio_proposed_label)
                        
                        # Saving it for later usage
                        self.metrics_by_d[index_id] = matched_trios
                        
                        # Check the suffix
                        suffix = self.challenge_label_and_sep.sep + self.challenge_label_and_sep.metrics_label_sep.join(available_metrics)
                        proposed_suffix = self.challenge_label_and_sep.sep + self.challenge_label_and_sep.metrics_label_sep.join(proposed_labels)
                
                if suffix is not None:
                    if index_id_orig is None or not (index_id_orig.endswith(suffix) or index_id_orig.endswith(proposed_suffix)):
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} orig id {index_id_orig} does not end with either computed metrics suffix {suffix} or proposed computed metrics suffix {proposed_suffix}. Fix it")
                    
                    proposed_orig_id = self.challenge_prefix + self.challenge_label_and_sep.aggregation_sep + proposed_suffix
                    if index_id_orig is None or index_id_orig != proposed_orig_id:
                        self.logger.critical(f"{self.type.value.capitalize()} dataset {index_id} orig id {index_id_orig} does not match with proposed original id {proposed_orig_id}. Fix it")
                        
            else:
                self.logger.warning(f"Unhandled visualization type {vis_type} for {self.type.value} dataset {index_id}. Is it a new visualization or a typo??")
            
        # New dataset to be tracked
        if d_pos is None:
            d_pos = len(self.d_list)
            
            if index_id_orig is not None:
                self.d_dict[index_id_orig] = d_pos
            self.d_dict[index_id] = d_pos
            
            if isinstance(fetched_inline_data, FetchedInlineData):
                if index_id_orig is not None:
                    self.d_inline_dict[index_id_orig] = fetched_inline_data
                self.d_inline_dict[index_id] = fetched_inline_data
            
            # Only happens to assessment datasets
            # As we do not have access to the participant dataset here
            # the code cannot know whether the dataset should be added
            # here. So, a clean up task has to be performed in DatasetsCatalog.
            # The optimal place is check_dataset_depends_on, because it
            # is already fetching the related datasets.
            if d_on_metrics_id is not None:
                self.d_m_dict.setdefault(d_on_metrics_id, []).append(d_pos)
            
            # And at last, store the dataset in the list
            self.d_list.append(raw_dataset)
        else:
            # Overwrite tracked dataset with future version
            self.d_list[d_pos] = raw_dataset
            if isinstance(fetched_inline_data, FetchedInlineData):
                if index_id_orig is not None:
                    self.d_inline_dict[index_id_orig] = fetched_inline_data
                self.d_inline_dict[index_id] = fetched_inline_data
            else:
                if index_id_orig is not None and index_id_orig in self.d_inline_dict:
                    del self.d_inline_dict[index_id_orig]
                if index_id in self.d_inline_dict:
                    del self.d_inline_dict[index_id]
        
        return DatasetValidationSchema(
            dataset_id=index_id,
            schema_id=found_schema_url,
        )
    
    def get(self, dataset_id: "str") -> "Optional[Mapping[str, Any]]":
        the_id = self.d_dict.get(dataset_id)
        
        return self.d_list[the_id] if the_id is not None else None

    def get_payload(self, dataset_id: "str") -> "Optional[FetchedInlineData]":
        return self.d_inline_dict.get(dataset_id)

    def get_metrics_trio(self, dataset_id: "str") -> "Optional[Sequence[Optional[MetricsTrio]]]":
        return self.metrics_by_d.get(dataset_id)

    def datasets_from_metric(self, metrics_id: "str") -> "Iterator[Mapping[str, Any]]":
        return map(lambda d_index: self.d_list[d_index], self.d_m_dict.get(metrics_id, []))

    def exclude_dataset_from_metrics(self, dataset_id: "str") -> "bool":
        was_excluded = False
        # First, get its index
        the_id = self.d_dict.get(dataset_id)
        # And now ... search for it and remove!
        if the_id is not None:
            for d_indexes in self.d_m_dict.values():
                if the_id in d_indexes:
                    was_excluded = True
                    d_indexes.remove(the_id)

        return was_excluded

    def keys(self) -> "KeysView[str]":
        return self.d_dict.keys()
    
    @property
    def datasets(self) -> "Sequence[Mapping[str, Any]]":
        return self.d_list

@dataclasses.dataclass
class DatasetsCatalog:
    # Level2 min validator
    level2_min_validator: "ExtensibleValidator"
    admin_tools: "OEBFetcher"
    override_cache: "bool"
    # Which logger to use
    logger: "Union[logging.Logger, ModuleType]" = logging
    metrics_graphql: "Sequence[Mapping[str, Any]]" = dataclasses.field(default_factory=list)
    community_prefix: "str" = ""
    bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl" = dataclasses.field(default_factory=BenchmarkingEventPrefixEtAl)
    challenge_prefix: "str" = ""
    challenge: "Mapping[str, Any]" = dataclasses.field(default_factory=dict)
    catalogs: "MutableMapping[OEBDatasetType, IndexedDatasets]" = dataclasses.field(default_factory=dict)
    
    def merge_datasets(self, raw_datasets: "Iterable[Mapping[str, Any]]", d_categories: "Optional[Sequence[Mapping[str, Any]]]" = None, cam_d: "Optional[Mapping[str, Mapping[str, str]]]" = None) -> "Sequence[DatasetValidationSchema]":
        d_indexed = []
        for raw_dataset in raw_datasets:
            try:
                d_type = OEBDatasetType(raw_dataset["type"])
            except ValueError:
                self.logger.exception(f"Unknown type {raw_dataset['type']} for dataset {raw_dataset['_id']}. Skipping (or FIXME)")
                continue
            
            idat = self.catalogs.get(d_type)
            if idat is None:
                # Although it is only needed for aggregation dataset checks
                # it could be reused outside
                if cam_d is None:
                    cam_d = gen_challenge_assessment_metrics_dict(self.challenge)
                
                idat = IndexedDatasets(
                    type=d_type,
                    logger=self.logger,
                    admin_tools=self.admin_tools,
                    override_cache=self.override_cache,
                    metrics_graphql=self.metrics_graphql,
                    level2_min_validator=self.level2_min_validator,
                    community_prefix=self.community_prefix,
                    bench_event_prefix_et_al=self.bench_event_prefix_et_al,
                    challenge_prefix=self.challenge_prefix,
                    challenge=self.challenge,
                    challenge_label_and_sep=OpenEBenchUtils.get_challenge_label_from_challenge(
                        self.challenge,
                        self.bench_event_prefix_et_al,
                        self.community_prefix,
                    ),
                    cam_d=cam_d,
                    d_categories=d_categories,
                )
                self.catalogs[d_type] = idat
            
            res_index = idat.index_dataset(raw_dataset)
            if res_index is not None:
                # Count only the indexed ones
                d_indexed.append(res_index)
                self.check_dataset_depends_on(raw_dataset)
        
        return d_indexed
    
    def check_dataset_depends_on(self, raw_dataset: "Mapping[str, Any]") -> "bool":
        d_type_str = raw_dataset["type"]
        
        d_type: "Optional[OEBDatasetType]"
        try:
            d_type = OEBDatasetType(d_type_str)
            idat = self.catalogs.get(d_type)
        except ValueError:
            d_type = None
            idat = None

        # We are not going to validate uningested datasets
        if idat is None:
            self.logger.error(f"Revoked check of datasets of type {d_type_str}, as they have not been indexed yet")
            return False
        
        if idat.get(raw_dataset["_id"]) is None:
            self.logger.error(f"Revoked check of dataset {raw_dataset['_id']} of type {d_type_str}, as it either has not been indexed yet or some previous validation failed")
            return False
            
        failed = False
        do_exclude_from_m_dict = False
        d_on = raw_dataset.get("depends_on")
        if d_on is not None:
            # Let's check datasets which are declared in depends_on
            challenge_ids_set = set(raw_dataset.get("challenge_ids", []))
            missing_dataset_ids = []
            ambiguous_dataset_ids = []
            unmatching_dataset_ids = []
            for d_on_entry in d_on.get("rel_dataset_ids",[]):
                d_on_id = d_on_entry.get("dataset_id")
                d_on_role = d_on_entry.get("role", "dependency")
                if (d_on_id is not None) and d_on_role == "dependency":
                    d_on_datasets = self.get_dataset(d_on_id)
                    if len(d_on_datasets) == 0:
                        missing_dataset_ids.append(d_on_id)
                    elif len(d_on_datasets) > 1:
                        ambiguous_dataset_ids.append(d_on_id)
                    elif len(challenge_ids_set.intersection(d_on_datasets[0].get("challenge_ids", []))) == 0:
                        unmatching_dataset_ids.append(d_on_id)
                    
                    # Now detect exclusion cases
                    if d_type == OEBDatasetType.Assessment and len(d_on_datasets) > 0:
                        for d_on_dataset in d_on_datasets:
                            if d_on_dataset.get("type") == OEBDatasetType.Participant.value and d_on_dataset.get("_metadata", {}).get(EXCLUDE_PARTICIPANT_KEY, False):
                                do_exclude_from_m_dict = True
            
            if len(ambiguous_dataset_ids) > 0:
                self.logger.error(f"{raw_dataset['_id']} depends on these ambiguous datasets: {', '.join(ambiguous_dataset_ids)}")
                failed = True
            
            if len(missing_dataset_ids) > 0:
                self.logger.warning(f"{raw_dataset['_id']} depends on these unindexed datasets: {', '.join(missing_dataset_ids)}")
            
            if len(unmatching_dataset_ids) > 0:
                self.logger.error(f"{raw_dataset['_id']} does not share challenges with these datasets: {', '.join(unmatching_dataset_ids)}")
                failed = True
        
        # Clean up task to exclude assessment datasets from
        # further processing when aggregation datasets are analysed
        if do_exclude_from_m_dict:
            idat.exclude_dataset_from_metrics(raw_dataset["_id"])

        return failed

    def get_participant_labels(self) -> "Sequence[InlineDataLabelPair]":
        """
        This method gets all the inline data labels from participant datasets
        """
        
        participant_labels: "MutableSequence[InlineDataLabelPair]" = []

        # We need the participant datasets
        idat_part = self.get(OEBDatasetType.Participant)
        if idat_part is None:
            self.logger.warning(f"No {OEBDatasetType.Participant.value} dataset in challenge {self.challenge['_id']}")
            return participant_labels
        
        # And we are building the list
        for raw_p_dataset in idat_part.datasets:
            inline_data_label_pair = gen_inline_data_label_from_participant_dataset(raw_p_dataset)
            participant_labels.append(inline_data_label_pair)
        
        return participant_labels
    
    def get_assessment_labels(self) -> "Sequence[InlineDataLabelPair]":
        """
        This method gets all the inline data labels
        """
        
        assessment_labels: "MutableSequence[InlineDataLabelPair]" = []

        # We need the assessment datasets
        idat_ass = self.get(OEBDatasetType.Assessment)
        if idat_ass is None:
            self.logger.warning(f"No {OEBDatasetType.Assessment.value} dataset in challenge {self.challenge['_id']}")
            return assessment_labels
        
        # And we are building the list
        for raw_a_dataset in idat_ass.datasets:
            raw_a_depends_on = raw_a_dataset.get("depends_on", {})

            # Get the metrics, and skip if they are not available
            raw_a_metrics_id = raw_a_depends_on.get("metrics_id")
            if raw_a_metrics_id is None:
                continue

            # And get the participant dataset to obtain the assessment inline_data label
            l_raw_p_dataset_ids = raw_a_depends_on.get("rel_dataset_ids", [])
            for raw_p_dataset_id in l_raw_p_dataset_ids:
                if raw_p_dataset_id.get("role") in (None, "dependency"):
                    raw_p_datasets = self.get_dataset(raw_p_dataset_id["dataset_id"])
                    if len(raw_p_datasets) > 0:
                        raw_p_dataset = None
                        for cand_raw_p_dataset in raw_p_datasets:
                            if cand_raw_p_dataset["type"] == OEBDatasetType.Participant.value:
                                raw_p_dataset = cand_raw_p_dataset
                                break

                        if raw_p_dataset is None:
                            continue

                        inline_data_label_pair = gen_inline_data_label_from_assessment_and_participant_dataset(raw_a_dataset, par_dataset=raw_p_dataset)
                        if inline_data_label_pair is not None:
                            assessment_labels.append(inline_data_label_pair)
        
        return assessment_labels
    
    def get(self, dataset_type: "OEBDatasetType") -> "Optional[IndexedDatasets]":
        return self.catalogs.get(dataset_type)
    
    def get_dataset(self, dataset_id: "str") -> "Sequence[Mapping[str, Any]]":
        retvals = []
        for idat in self.catalogs.values():
            retval = idat.get(dataset_id)
            if retval is not None:
                retvals.append(retval)
        
        return retvals
    
    def get_dataset_payload(self, dataset_id: "str") -> "Sequence[FetchedInlineData]":
        retvals = []
        for idat in self.catalogs.values():
            retval = idat.get_payload(dataset_id)
            if retval is not None:
                retvals.append(retval)
        
        return retvals

@dataclasses.dataclass
class TestActionRel:
    action: "Mapping[str, Any]"
    in_d: "Sequence[Mapping[str, Any]]"
    other_d: "Sequence[Mapping[str, Any]]"
    out_d: "Sequence[Mapping[str, Any]]"

@dataclasses.dataclass
class IndexedTestActions:
    # The test action type of all the test actions
    action_type: "str"
    in_d_catalog: "Optional[IndexedDatasets]"
    out_d_catalog: "Optional[IndexedDatasets]"
    community_prefix: "str"
    challenge: "Mapping[str, Any]"
    # Which logger to use
    logger: "Union[logging.Logger, ModuleType]" = logging
    
    # Let's index the test actions
    # by _id and orig_id
    a_list: "MutableSequence[TestActionRel]" = dataclasses.field(default_factory=list)
    # Maps the test action ids and orig_ids to their
    # place in the previous list
    a_dict: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    
    od_to_a: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    
    # These catalogs allow checking other kind of input datasets
    other_d_catalogs: "Mapping[OEBDatasetType, IndexedDatasets]" = dataclasses.field(default_factory=dict)
    
    def index_test_action(self, raw_test_action: "Mapping[str, Any]") -> "Optional[IndexedTestActions]":
        a_type = raw_test_action["action_type"]
        
        if a_type != self.action_type:
            self.logger.error(f"This instance is focused on test actions of type {self.action_type}, not of type {a_type}")
            return None
        
        # Now, time to record the position where the assessment
        # dataset is going to be in the list of assessment datasets
        index_id_orig = cast("Optional[str]", raw_test_action.get("orig_id"))
        index_id = cast("str", raw_test_action["_id"])
        ter_pos = self.a_dict.get(index_id_orig) if index_id_orig is not None else None
        if ter_pos is None:
            ter_pos = self.a_dict.get(index_id)
        
        # Restrictions at the dataset level
        id_to_check = None
        if index_id_orig is not None:
            id_to_check = index_id_orig
        if (id_to_check is None) and not index_id.startswith(TEST_ACTION_ID_PREFIX):
            id_to_check = index_id
        
        if id_to_check is not None:
            if not id_to_check.startswith(self.community_prefix):
                self.logger.warning(f"TestAction {index_id} with original id {id_to_check} (type {self.action_type}) does not start with the community prefix {self.community_prefix}. You should fix it to avoid possible duplicates")
            elif self.action_type == "TestEvent":
                expected_test_event_prefix = OpenEBenchUtils.gen_test_event_original_id(self.challenge, "")
                if not id_to_check.startswith(expected_test_event_prefix):
                    self.logger.warning(f"TestAction {index_id} with original id {id_to_check} (type {self.action_type}) does not start with the test action prefix {expected_test_event_prefix}. You should fix it to avoid possible duplicates")
            
            expected_suffix = TEST_ACTION_ORIG_ID_SUFFIX.get(self.action_type)
            if (expected_suffix is not None) and not id_to_check.endswith(expected_suffix):
                self.logger.warning(f"TestAction {index_id} with original id {id_to_check} (type {self.action_type}) does not end with the expected suffix {expected_suffix}. You should fix it to avoid possible duplicates")
        
        # This is needed to check the provenance, as
        # all the outgoing datasets should depend on this tool_id
        transforming_tool_id = raw_test_action.get("tool_id")
        
        # Checking availability of input and output datasets
        unmatched_in_dataset_ids = []
        in_datasets = []
        unmatched_out_dataset_ids = []
        out_datasets = []
        other_datasets = []
        should_fail = False
        challenge_id = raw_test_action["challenge_id"]
        for involved_d in raw_test_action.get("involved_datasets", []):
            d_role = involved_d.get("role")
            candidate_d_id = involved_d.get("dataset_id")
            if d_role == "incoming":
                # Search for it to match
                candidate_d = None if self.in_d_catalog is None else self.in_d_catalog.get(candidate_d_id)
                if candidate_d is not None:
                    in_datasets.append(candidate_d)
                else:
                    for other_d_catalog in self.other_d_catalogs.values():
                        if other_d_catalog is not None:
                            candidate_d = other_d_catalog.get(candidate_d_id)
                            if candidate_d is not None:
                                other_datasets.append(candidate_d)
                                break
                    else:
                        unmatched_in_dataset_ids.append(candidate_d_id)
                        should_fail = True
                        self.logger.debug(f"Unmatched {d_role} dataset {candidate_d_id} as {'(none)' if self.in_d_catalog is None else self.in_d_catalog.type.value} or others like {', '.join(map(lambda k: k.value, self.other_d_catalogs.keys()))}")
            elif d_role == "outgoing":
                candidate_d = None if self.out_d_catalog is None else self.out_d_catalog.get(candidate_d_id)
                # Search for it to match
                if candidate_d is not None:
                    out_datasets.append(candidate_d)
                    
                    # Checking the dataset is in the same challenge id
                    if challenge_id not in candidate_d.get("challenge_ids", []):
                        self.logger.error(f"Entry {raw_test_action['_id']} (type {self.action_type}) generates dataset {candidate_d['_id']}, but dataset is not in challenge {challenge_id}")
                        should_fail = True
                    
                    o_tool_id = candidate_d.get("depends_on", {}).get("tool_id")
                    if o_tool_id != transforming_tool_id:
                        self.logger.error(f"Entry {raw_test_action['_id']} (type {self.action_type}) reflects transformation due tool {transforming_tool_id}, but {d_role} dataset {candidate_d_id} depends on {o_tool_id}. Fix it")
                        should_fail = True
                    
                    if self.action_type == "MetricsEvent":
                        expected_metrics_event_id = OpenEBenchUtils.gen_metrics_event_original_id(candidate_d)
                        if id_to_check != expected_metrics_event_id:
                            self.logger.warning(f"TestAction {index_id} with original id {id_to_check} (type {self.action_type}) is not the proposed {expected_metrics_event_id}. You should fix it to avoid possible duplicates")
                    elif self.action_type == "AggregationEvent":
                        expected_aggregation_event_id = OpenEBenchUtils.gen_aggregation_event_original_id(candidate_d)
                        if id_to_check != expected_aggregation_event_id:
                            self.logger.warning(f"TestAction {index_id} with original id {id_to_check} (type {self.action_type}) is not the proposed {expected_aggregation_event_id}. You should fix it to avoid possible duplicates")

                else:
                    unmatched_out_dataset_ids.append(candidate_d_id)
                    should_fail = True
                    self.logger.debug(f"Unmatched {d_role} dataset {candidate_d_id} as {'(none)' if self.out_d_catalog is None else self.out_d_catalog.type.value}")
            else:
                self.logger.critical(f"Unexpected {d_role} dataset {candidate_d_id} in {index_id}. This program does not know how to handle it.")
        
        if len(unmatched_in_dataset_ids) > 0 or len(unmatched_out_dataset_ids) > 0:
            if len(unmatched_in_dataset_ids) > 0:
                self.logger.error(f"In {self.action_type} entry {raw_test_action['_id']} from challenge {raw_test_action['challenge_id']}, {len(unmatched_in_dataset_ids)} unmatched {'(none)' if self.in_d_catalog is None else self.in_d_catalog.type.value} input datasets: {', '.join(unmatched_in_dataset_ids)}")
                #if self.in_d_catalog:
                #    self.logger.error('\n'.join(self.in_d_catalog.keys()))
                #sys.exit(18)
            if len(unmatched_out_dataset_ids) > 0:
                self.logger.error(f"In {self.action_type} entry {raw_test_action['_id']} from challenge {raw_test_action['challenge_id']}, {len(unmatched_out_dataset_ids)} unmatched {'(none)' if self.out_d_catalog is None else self.out_d_catalog.type.value} output datasets: {', '.join(unmatched_out_dataset_ids)}")
        
        if should_fail:
            return None
        
        ter = TestActionRel(
            action=raw_test_action,
            in_d=in_datasets,
            other_d=other_datasets,
            out_d=out_datasets,
        )
        
        # After all the validations, put it in place
        if ter_pos is None:
            ter_pos = len(self.a_list)
            
            if index_id_orig is not None:
                self.a_dict[index_id_orig] = ter_pos
            self.a_dict[index_id] = ter_pos
            
            # And at last, store the test action in the list
            self.a_list.append(ter)
        else:
            # Overwrite tracked dataset with future version
            self.a_list[ter_pos] = ter
        
        # Epilogue: index all the outgoing datasets
        for out_dataset in out_datasets:
            self.od_to_a[out_dataset["_id"]] = ter_pos
        
        return self
    
    def get_by_outgoing_dataset(self, dataset_id: "str") -> "Optional[TestActionRel]":
        ter_pos = self.od_to_a.get(dataset_id)
        return self.a_list[ter_pos]  if ter_pos is not None else None
    
    def get_by_original_id(self, original_id: "str") -> "Optional[Mapping[str, Any]]":
        a_pos = self.a_dict.get(original_id)
        if a_pos is None:
            return None
        
        return self.a_list[a_pos].action
            
ActionType2InOutDatasetTypes: "Mapping[str, Tuple[OEBDatasetType, Sequence[OEBDatasetType], OEBDatasetType]]" = {
    # "SetupEvent": (None, ),
    "TestEvent": (OEBDatasetType.Input, [OEBDatasetType.PublicReference], OEBDatasetType.Participant),
    "MetricsEvent": (OEBDatasetType.Participant, [OEBDatasetType.MetricsReference], OEBDatasetType.Assessment),
    "AggregationEvent": (OEBDatasetType.Assessment, [OEBDatasetType.PublicReference, OEBDatasetType.MetricsReference], OEBDatasetType.Aggregation),
    # "StatisticsEvent": (OEBDatasetType.Aggregation, [???], OEBDatasetType.Aggregation),
}

@dataclasses.dataclass
class TestActionsCatalog:
    d_catalog: "DatasetsCatalog"
    catalogs: "MutableMapping[str, IndexedTestActions]" = dataclasses.field(default_factory=dict)
    # Which logger to use
    logger: "Union[logging.Logger, ModuleType]" = logging
    
    def merge_test_actions(self, raw_test_actions: "Iterable[Mapping[str, Any]]") -> "int":
        num_indexed = 0
        for raw_test_action in raw_test_actions:
            a_type = raw_test_action["action_type"]
            
            ita = self.catalogs.get(a_type)
            if ita is None:
                # Derive the in and out dataset types
                a_type_got = ActionType2InOutDatasetTypes.get(a_type)
                
                if a_type_got is None:
                    self.logger.critical(f"Test action {raw_test_action['_id']} is of unhandable action type {a_type}. Either this program or the database have serious problems")
                    continue
                
                in_d_type, other_d_types, out_d_type = a_type_got
                
                # Get the in and out dataset catalogs
                in_d_catalog = self.d_catalog.get(in_d_type)
                out_d_catalog = self.d_catalog.get(out_d_type)
                other_d_catalogs = {}
                for d_c_t in other_d_types:
                    d_c = self.d_catalog.get(d_c_t)
                    if d_c is not None:
                        other_d_catalogs[d_c_t] = d_c
                
                ita = IndexedTestActions(
                    action_type=a_type,
                    in_d_catalog=in_d_catalog,
                    out_d_catalog=out_d_catalog,
                    other_d_catalogs=other_d_catalogs,
                    community_prefix=self.d_catalog.community_prefix,
                    challenge=self.d_catalog.challenge,
                    logger=self.logger
                )
                self.catalogs[a_type] = ita
            
            if ita.index_test_action(raw_test_action) is not None:
                # Count only the indexed ones
                num_indexed += 1
        
        return num_indexed
    
    def get(self, action_type: "str") -> "Optional[IndexedTestActions]":
        return self.catalogs.get(action_type)

@dataclasses.dataclass
class IndexedChallenge:
    challenge: "Mapping[str, Any]"
    challenge_id: "str"
    challenge_label_and_sep: "ChallengeLabelAndSep"
    d_catalog: "DatasetsCatalog"
    ta_catalog: "TestActionsCatalog"
    # This one should be generated in next way
    # gen_challenge_assessment_metrics_dict(self.challenge)
    cam_d: "Mapping[str, Mapping[str, str]]"
    # Assessment metrics categories catalog
    ass_cat: "Sequence[Mapping[str, Any]]"
    # Which logger to use
    logger: "Union[logging.Logger, ModuleType]" = logging
    
    def match_metric_from_metrics_label(self, metrics_label: "str", dataset_id: "str" = "(unknown)") -> "Optional[MetricsTrio]":
        return match_metric_from_label(
            logger=self.logger,
            metrics_graphql=self.d_catalog.metrics_graphql,
            community_prefix=self.d_catalog.community_prefix,
            metrics_label=metrics_label,
            challenge_id=self.challenge_id,
            challenge_acronym=self.challenge['acronym'],
            challenge_assessment_metrics_d=self.cam_d,
            dataset_id=dataset_id,
        )
