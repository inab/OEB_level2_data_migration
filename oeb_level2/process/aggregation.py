#!/usr/bin/env python

import dataclasses
import inspect
import logging
import sys
import os
import datetime
import json

from .benchmarking_dataset import BenchmarkingDataset
from .assessment import (
    gen_challenge_assessment_metrics_dict,
    match_metric_from_label,
)
from ..schemas import (
    AGGREGATION_2D_PLOT_SCHEMA_ID,
    AGGREGATION_BAR_PLOT_SCHEMA_ID,
    SINGLE_METRIC_SCHEMA_ID,
)

from typing import (
    NamedTuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        Sequence,
        MutableSequence,
        Tuple,
    )
    from .assessment import AssessmentTuple
    from ..utils.migration_utils import OpenEBenchUtils

class AggregationTuple(NamedTuple):
    aggregation_dataset: "Mapping[str, Any]"
    at_l: "Sequence[AssessmentTuple]"

@dataclasses.dataclass
class IndexedDatasets:
    # The dataset type of all the datasets
    type: "str"
    # Which logger to use
    logger: "Any"
    # Validator for inline data
    level2_min_validator: "Any"
    metrics_graphql: "Sequence[Mapping[str, Any]]"
    community_acronym: "str"
    challenge: "Mapping[str, Any]"
    cam_d: "Optional[Mapping[str,str]]"
    # dataset categories
    d_categories: "Optional[Sequence[]]"
    # Let's index the datasets
    # by _id and orig_id
    # In the case of assessment datasets
    # they are combined to get the aggregation datasets
    d_list: "MutableSequence[Mapping[str, Any]]" = dataclasses.field(default_factory=list)
    # Maps the dataset ids and orig_ids to their
    # place in the previous list
    d_dict: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    # Groups the assessment datasets by metrics
    d_m_dict: "MutableMapping[str, MutableSequence[int]]" = dataclasses.field(default_factory=dict)
    
    def index_dataset(self, raw_dataset: "Mapping[str, Any]") -> "Optional[IndexedDatasets]":
        d_type = raw_dataset["type"]
        
        if d_type != self.type:
            self.logger.error(f"This instance is focused on datasets of type {self.type}, not of type {d_type}")
            return None
        
        is_assessment = self.type == "assessment"
        is_aggregation = self.type == "aggregation"
        
        # Now, time to record the position where the assessment
        # dataset is going to be in the list of assessment datasets
        index_id_orig = raw_dataset.get("orig_id")
        index_id = raw_dataset["_id"]
        d_pos = self.d_dict.get(index_id_orig)
        if d_pos is None:
            d_pos = self.d_dict.get(index_id)
        
        # Some validations
        # Validating the category where it matches
        d_on_metrics_id = None
        if is_assessment or is_aggregation:
            d_on = raw_dataset.get("depends_on", {})
            d_on_metrics_id = d_on.get("metrics_id")
            d_on_tool_id = d_on.get("tool_id")
            
            m_matched = None
            m_tool_should = None
            for m_cat in self.d_categories:
                for m in m_cat.get("metrics", []):
                    if m.get("metrics_id") == d_on_metrics_id:
                        potential_tool_id = m.get("tool_id")
                        
                        if potential_tool_id == d_on_tool_id:
                            m_matched = d_on_metrics_id
                            break
                        elif m_tool_should is not None:
                            self.logger.warning(f"Metrics {d_on_metrics_id}, used for {self.type}, registered more than once in challenge {raw_dataset['challenge_ids'][0]}. Database contents should be curated")
                        else:
                            m_tool_should = potential_tool_id
                if m_matched is not None:
                    break
            
            if m_matched is None:
                if m_tool_should is None:
                    self.logger.error(f"{self.type.capitalize()} dataset {index_id} ({'in database' if d_pos is None else 'to be submitted'}) from challenge {', '.join(raw_dataset['challenge_ids'])} depends on metric {d_on_metrics_id} implemented by {d_on_tool_id}, which are not registered as valid {self.type} challenge metrics. Fix it")
                else:
                    self.logger.error(f"{self.type.capitalize()} dataset {index_id} ({'in database' if d_pos is None else 'to be submitted'}) from challenge {', '.join(raw_dataset['challenge_ids'])} matched metric {d_on_metrics_id}, but mismatched implementation ({d_on_tool_id} instead of {m_tool_should}). Fix it")
                    
                return None
        
        # Some additional validations for inline data
        o_datalink = raw_dataset.get("datalink")
        inline_data = None
        if isinstance(o_datalink, dict):
            inline_data = o_datalink.get("inline_data")
            # Is this dataset an inline one?
            if inline_data is not None:
                # Learning / guessing the schema_url
                schema_url = o_datalink.get("schema_url")
                inline_schemas = None
                if schema_url is None:
                    if is_assessment:
                        inline_schemas = [
                            SINGLE_METRIC_SCHEMA_ID,
                        ]
                    elif is_aggregation:
                        inline_schemas = [
                            AGGREGATION_2D_PLOT_SCHEMA_ID,
                            AGGREGATION_BAR_PLOT_SCHEMA_ID,
                        ]
                else:
                    inline_schemas = [
                        schema_url
                    ]
                
                if inline_schemas is not None:
                    # Now, time to validate the dataset
                    config_val_list = self.level2_min_validator.jsonValidate({
                        "json": inline_data,
                        "file": "inline " + index_id,
                        "errors": [],
                    }, guess_unmatched=inline_schemas)
                    assert len(config_val_list) > 0
                    config_val_block = config_val_list[0]
                    config_val_block_errors = list(filter(lambda ve: (schema_url is None) or (ve.get("schema_id") == schema_url), config_val_block.get("errors", [])))
                    
                    if len(config_val_block_errors) > 0:
                        self.logger.error(f"Validation errors in inline data from {self.type} dataset {index_id} ({index_id_orig}) using {inline_schemas}. It should be rebuilt\n{json.dumps(config_val_block_errors, indent=4)}")
                        return None
        
        if is_aggregation:
            if not isinstance(o_datalink, dict):
                # Skip it, we cannot work with it
                self.logger.info(f"Skipping {self.type} dataset {index_id} indexing, as it does not have a datalink")
                return None
                
            # Is this dataset an inline one?
            if inline_data is None:
                # Skip it, we cannot work with it
                self.logger.info(f"Skipping {self.type} dataset {index_id} indexing, as it does not contain inline data")
                return None
        
            vis_hints = inline_data.get("visualization", {})
            vis_type = vis_hints.get("type")
            if len(vis_hints) == 0:
                self.logger.warning(f"No visualization type for {self.type} dataset {index_id}. Is it missing or intentional??")
                # TODO: What should we do????
            elif vis_type == "2D-plot":
                x_axis_metric_label = vis_hints.get("x_axis")
                y_axis_metric_label = vis_hints.get("y_axis")
                if x_axis_metric_label is None:
                    self.logger.critical(f"{self.type.capitalize()} dataset {index_id} of visualization type {vis_type} did not define x_axis label. Fix it")
                
                if y_axis_metric_label is None:
                    self.logger.critical(f"{self.type.capitalize()} dataset {index_id} of visualization type {vis_type} did not define y_axis label. Fix it")
                
                if x_axis_metric_label is not None and y_axis_metric_label is not None:
                    # Check there is some matching metric
                    x_metric_id, x_found_tool_id, x_proposed_label = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=x_axis_metric_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if x_metric_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses for x axis unmatched metric {x_axis_metric_label}. Fix it")
                        
                    y_metric_id, y_found_tool_id, y_proposed_label = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=y_axis_metric_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if y_metric_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses for y axis unmatched metric {y_axis_metric_label}. Fix it")
                    
                    # Check the suffix
                    suffix = f"_{x_axis_metric_label}+{y_axis_metric_label}"
                    proposed_suffix = f"_{x_proposed_label}+{y_proposed_label}"
                    if index_id_orig is None or not (index_id_orig.endswith(suffix) or index_id_orig.endswith(proposed_suffix)):
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} orig id {index_id_orig} does not end with either computed metrics suffix {suffix} or proposed computed metrics suffix {proposed_suffix}. Fix it")
                        
            elif vis_type == "bar-plot":
                metrics_label = vis_hints.get("metric")
                if metrics_label is None:
                    self.logger.critical(f"{self.type.capitalize()} dataset {index_id} of visualization type {vis_type} did not define metric label. Fix it")
                
                else:
                    # Check there is some matching metric
                    metric_id, found_tool_id, proposed_label = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=metrics_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if metric_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses unmatched metric {metrics_label}. Fix it")
                    
                    # Check the suffix
                    suffix = f"_{metrics_label}"
                    proposed_suffix = f"_{proposed_label}"
                    if index_id_orig is None or not (index_id_orig.endswith(suffix) or index_id_orig.endswith(proposed_suffix)):
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} orig id {index_id_orig} does not end with computed metrics suffix {suffix} or proposed computed metrics suffix {proposed_suffix}. Fix it")
            else:
                self.logger.warning(f"Unhandled visualization type {vis_type} for {self.type} dataset {index_id}. Is it a new visualization or a typo??")
            
        # New dataset to be tracked
        if d_pos is None:
            d_pos = len(self.d_list)
            
            if index_id_orig is not None:
                self.d_dict[index_id_orig] = d_pos
            self.d_dict[index_id] = d_pos
            
            # Only happens to assessment datasets
            if d_on_metrics_id is not None:
                self.d_m_dict.setdefault(d_on_metrics_id, []).append(d_pos)
            
            # And at last, store the dataset in the list
            self.d_list.append(raw_dataset)
        else:
            # Overwrite tracked dataset with future version
            self.d_list[d_pos] = raw_dataset
        
        return self
    
    def get(self, dataset_id: "str") -> "Optional[Mapping[str, Any]]":
        the_id = self.d_dict.get(dataset_id)
        
        return self.d_list[the_id] if the_id is not None else None

    def keys(self) -> "Iterator[str]":
        return self.d_dict.keys()

@dataclasses.dataclass
class DatasetsCatalog:
    # Which logger to use
    logger: "Any" = logging
    # Level2 min validator
    level2_min_validator: "Any" = None
    metrics_graphql: "Sequence[Mapping[str, Any]]" = dataclasses.field(default_factory=list)
    community_acronym: "str" = ""
    challenge: "Mapping[str, Any]" = dataclasses.field(default_factory=dict)
    catalogs: "MutableMapping[str, IndexedDatasets]" = dataclasses.field(default_factory=dict)
    
    def merge_datasets(self, raw_datasets: "Iterator[Mapping[str, Any]]", d_categories = None) -> "int":
        num_indexed = 0
        for raw_dataset in raw_datasets:
            d_type = raw_dataset["type"]
            
            idat = self.catalogs.get(d_type)
            if idat is None:
                # Only needed for aggregation dataset checks
                if d_type == "aggregation":
                    cam_d = gen_challenge_assessment_metrics_dict(self.challenge)
                else:
                    cam_d = None
                
                idat = IndexedDatasets(
                    type=d_type,
                    logger=self.logger,
                    metrics_graphql=self.metrics_graphql,
                    level2_min_validator=self.level2_min_validator,
                    community_acronym=self.community_acronym,
                    challenge=self.challenge,
                    cam_d=cam_d,
                    d_categories=d_categories,
                )
                self.catalogs[d_type] = idat
            
            if idat.index_dataset(raw_dataset) is not None:
                # Count only the indexed ones
                num_indexed += 1
                self.check_dataset_depends_on(raw_dataset)
        
        return num_indexed
    
    def check_dataset_depends_on(self, raw_dataset: "Mapping[str, Any]") -> "bool":
            d_type = raw_dataset["type"]
            
            idat = self.catalogs.get(d_type)
            # We are not going to validate uningested datasets
            if idat is None:
                self.logger.error(f"Revoked check of datasets of type {d_type}, as they have not been indexed yet")
                return False
            
            if idat.get(raw_dataset["_id"]) is None:
                self.logger.error(f"Revoked check of dataset {raw_dataset['_id']} of type {d_type}, as it either has not been indexed yet or some previous validation failed")
                return False
                
            failed = False
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
                        elif challenge_ids_set.intersection(d_on_datasets[0].get("challenge_ids", [])) == 0:
                            unmatching_dataset_ids.append(d_on_id)
                
                if len(ambiguous_dataset_ids) > 0:
                    self.logger.error(f"{raw_dataset['_id']} depends on these ambiguous datasets: {', '.join(ambiguous_dataset_ids)}")
                    failed = True
                
                if len(missing_dataset_ids) > 0:
                    self.logger.warning(f"{raw_dataset['_id']} depends on these unindexed datasets: {', '.join(missing_dataset_ids)}")
                
                if len(unmatching_dataset_ids) > 0:
                    self.logger.error(f"{raw_dataset['_id']} does not share challenges with these datasets: {', '.join(unmatching_dataset_ids)}")
                    failed = True
            
            return failed
    
    def get(self, dataset_type: "str") -> "Optional[IndexedDatasets]":
        return self.catalogs.get(dataset_type)
    
    def get_dataset(self, dataset_id: "str") -> "Sequence[Mapping[str, Any]]":
        retvals = []
        for idat in self.catalogs.values():
            retval = idat.get(dataset_id)
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
    # Which logger to use
    logger: "Any" = logging
    
    # Let's index the test actions
    # by _id and orig_id
    a_list: "MutableSequence[TestActionRel]" = dataclasses.field(default_factory=list)
    # Maps the test action ids and orig_ids to their
    # place in the previous list
    a_dict: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    
    # These catalogs allow checking other kind of input datasets
    other_d_catalogs: "Mapping[str, IndexedDatasets]" = dataclasses.field(default_factory=dict)
    
    def index_test_action(self, raw_test_action: "Mapping[str, Any]") -> "Optional[IndexedTestActions]":
        a_type = raw_test_action["action_type"]
        
        if a_type != self.action_type:
            self.logger.error(f"This instance is focused on test actions of type {self.action_type}, not of type {a_type}")
            return None
        
        # Now, time to record the position where the assessment
        # dataset is going to be in the list of assessment datasets
        index_id_orig = raw_test_action.get("orig_id")
        index_id = raw_test_action["_id"]
        ter_pos = self.a_dict.get(index_id_orig)
        if ter_pos is None:
            ter_pos = self.a_dict.get(index_id)
        
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
                        self.logger.debug(f"Unmatched {d_role} dataset {candidate_d_id} as {'(none)' if self.in_d_catalog is None else self.in_d_catalog.type} or others like {', '.join(self.other_d_catalogs.keys())}")
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
                else:
                    unmatched_out_dataset_ids.append(candidate_d_id)
                    should_fail = True
                    self.logger.debug(f"Unmatched {d_role} dataset {candidate_d_id} as {'(none)' if self.out_d_catalog is None else self.out_d_catalog.type}")
            else:
                self.logger.critical(f"Unexpected {d_role} dataset {candidate_d_id} in {index_id}. This program does not know how to handle it.")
        
        if len(unmatched_in_dataset_ids) > 0 or len(unmatched_out_dataset_ids) > 0:
            if len(unmatched_in_dataset_ids) > 0:
                self.logger.error(f"In {self.action_type} entry {raw_test_action['_id']} from challenge {raw_test_action['challenge_id']}, {len(unmatched_in_dataset_ids)} unmatched {'(none)' if self.in_d_catalog is None else self.in_d_catalog.type} input datasets: {', '.join(unmatched_in_dataset_ids)}")
                #if self.in_d_catalog:
                #    self.logger.error('\n'.join(self.in_d_catalog.keys()))
                #sys.exit(18)
            if len(unmatched_out_dataset_ids) > 0:
                self.logger.error(f"In {self.action_type} entry {raw_test_action['_id']} from challenge {raw_test_action['challenge_id']}, {len(unmatched_out_dataset_ids)} unmatched {'(none)' if self.out_d_catalog is None else self.out_d_catalog.type} output datasets: {', '.join(unmatched_out_dataset_ids)}")
        
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
        
        return self
            
ActionType2InOutDatasetTypes = {
    # "SetupEvent": (None, ),
    "TestEvent": ("input", ["public_reference"], "participant"),
    "MetricsEvent": ("participant", ["metrics_reference"], "assessment"),
    "AggregationEvent": ("assessment", ['public_reference', 'metrics_reference'], "aggregation"),
    # "StatisticsEvent": ("aggregation", "aggregation"),
}

@dataclasses.dataclass
class TestActionsCatalog:
    d_catalog: "DatasetsCatalog" = dataclasses.field(default_factory=DatasetsCatalog)
    catalogs: "MutableMapping[str, IndexedTestActions]" = dataclasses.field(default_factory=dict)
    # Which logger to use
    logger: "Any" = logging
    
    def merge_test_actions(self, raw_test_actions: "Iterator[Mapping[str, Any]]") -> "int":
        num_indexed = 0
        for raw_test_action in raw_test_actions:
            a_type = raw_test_action["action_type"]
            
            ita = self.catalogs.get(a_type)
            if ita is None:
                # Derive the in and out dataset types
                in_d_type, other_d_types, out_d_type = ActionType2InOutDatasetTypes.get(a_type, (None,None))
                
                if in_d_type is None:
                    self.logger.critical(f"Test action {raw_test_action['_id']} is of unhandable action type {a_type}. Either this program or the database have serious problems")
                    continue
                
                # Get the in and out dataset catalogs
                in_d_catalog = self.d_catalog.get(in_d_type)
                out_d_catalog = self.d_catalog.get(out_d_type)
                other_d_catalogs = dict(map(lambda d_c_t: (d_c_t, self.d_catalog.get(d_c_t)), other_d_types))
                ita = IndexedTestActions(
                    action_type=a_type,
                    in_d_catalog=in_d_catalog,
                    out_d_catalog=out_d_catalog,
                    other_d_catalogs=other_d_catalogs,
                    logger=self.logger
                )
                self.catalogs[a_type] = ita
            
            if ita.index_test_action(raw_test_action) is not None:
                # Count only the indexed ones
                num_indexed += 1
        
        return num_indexed

class Aggregation():

    def __init__(self, schemaMappings, migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.level2_min_validator = migration_utils.level2_min_validator
    
    def build_aggregation_datasets(
        self,
        community_id: "str",
        community_acronym: "str",
        min_aggregation_datasets,
        challenges_agg_graphql: "Sequence[Mapping[str, Any]]",
        metrics_agg_graphql: "Sequence[Mapping[str, Any]]",
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        valid_test_events: "Sequence[Tuple[str, Any]]",
        valid_metrics_events: "Sequence[Tuple[str, Any]]",
    ) -> "Sequence[AggregationTuple]":
    
        self.logger.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")
        
        # Grouping future assessment dataset tuples by challenge
        ass_c_dict = {}
        for ass_t in valid_assessment_tuples:
            ass_d = ass_t.assessment_dataset
            for challenge_id in ass_d.get("challenge_ids", []):
                ass_c_dict.setdefault(challenge_id, []).append(ass_t)
        
        valid_aggregation_datasets = []
        valid_agg_d_dict = {}
        
        # First, let's analyze all existing aggregation datasets
        # nested in the challenges
        agg_challenges = { }
        agg_datasets = { }
        should_exit_ch = False
        for agg_ch in challenges_agg_graphql:
            # Garrayo's school label
            challenge_id = agg_ch["_id"]
            challenge_label = agg_ch.get("_metadata", {}).get("level_2:challenge_id")
            if challenge_label is None:
                challenge_label = agg_ch.get("orig_id")
                # Very old school label
                if challenge_label is not None:
                    if challenge_label.startswith(community_acronym + ':'):
                        challenge_label = challenge_label[len(community_acronym) + 1]
                else:
                    challenge_label = challenge_id
            
            coll_ch = agg_challenges.get(challenge_label)
            if coll_ch is not None:
                self.logger.critical(f"Challenge collision: label {challenge_label}, challenges {coll_ch['_id']} and {challenge_id}. Please contact OpenEBench support to report this inconsistency")
                should_exit_ch = True
                continue
            
            # Pre-filter challenge categories of this challenge
            # Assessment ones
            ass_cat = []
            # Aggregation ones
            agg_cat = []
            for m_cat in agg_ch.get("metrics_categories", []):
                d_category = m_cat.get("category")
                if d_category == "assessment":
                    ass_cat.append(m_cat)
                elif d_category == "aggregation":
                    agg_cat.append(m_cat)
            
            d_catalog = DatasetsCatalog(
                logger=self.logger,
                level2_min_validator=self.level2_min_validator,
                metrics_graphql=metrics_agg_graphql,
                community_acronym=community_acronym,
                challenge=agg_ch,
            )
            
            # Start indexing input datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("input_datasets", []),
                d_categories=ass_cat,
            )
            # public reference datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("public_reference_datasets", []),
                d_categories=ass_cat,
            )
            # Metrics reference datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("metrics_reference_datasets", []),
                d_categories=ass_cat,
            )
            
            # Index stored participant datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("participant_datasets", []),
                d_categories=ass_cat,
            )
            
            # And also index the future participant datasets involved in this challenge
            d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.pt.participant_dataset, ass_c_dict.get(challenge_id, [])),
                d_categories=ass_cat,
            )
            
            # Then the assessment datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("assessment_datasets", []),
                d_categories=ass_cat,
            )
            
            # And also index the future assessment datasets involved in this challenge
            d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.assessment_dataset, ass_c_dict.get(challenge_id, [])),
                d_categories=ass_cat,
            )
            
            # The test actions catalog, which depends on the dataset ones
            ta_catalog = TestActionsCatalog(
                logger=self.logger,
                d_catalog=d_catalog,
            )
            
            # Let's index the TestEvent actions, as they can provide
            # a very trustable way to rescue the labels needed to
            # regenerate an aggregation dataset from their assessment
            # components
            
            # Merging the TestEvent test actions
            ta_catalog.merge_test_actions(agg_ch.get("event_test_actions", []))
            
            # newly added TestEvent involved in this challenge
            ta_catalog.merge_test_actions(filter(lambda te: te['challenge_id'] == challenge_id, valid_test_events))
            
            # Let's index the MetricsEvent actions, as they can provide
            # the way to link to the TestEvent needed to rescue the labels
            # needed to regenerate an aggregation dataset from their assessment
            # components
            
            # Merging the MetricsEvent test actions
            ta_catalog.merge_test_actions(agg_ch.get("metrics_test_actions", []))
            
            # newly added MetricsEvent
            ta_catalog.merge_test_actions(filter(lambda me: me['challenge_id'] == challenge_id, valid_metrics_events))
            
            # Now, let's index the existing aggregation datasets, and
            # their relationship. And check them, BTW
            
            # Then the assessment datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("aggregation_datasets", []),
                d_categories=agg_cat,
            )
            
            # Last, but not the least important
            agg_challenges[challenge_label] = (agg_ch, d_catalog, ta_catalog)
        
        if should_exit_ch:
            self.logger.critical("Some challenges have collisions at their label level. Please ask the team to fix the mess")
            sys.exit(4)

        # Now it is time to process all the new or updated aggregation datasets
        valid_aggregation_tuples = []
        for dataset in min_aggregation_datasets:
            # TODO
            self.logger.critical("Are you ready to implement minimal aggregation dataset (re)generation?")
            sys.exit(1)
        
        return valid_aggregation_tuples

    def build_aggregation_datasets_orig(
        self,
        challenges_graphql,
        metrics_graphql,
        stagedAggregationDatasets,
        min_aggregation_datasets,
        tool_name,
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        community_id,
        community_acronym,
        tool_id,
        version: "str",
        workflow__tool_id
    ):

        self.logger.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")

        valid_aggregation_datasets = []
        agg_by_id = dict()

        # This is needed to properly compare ids later
        oeb_challenges = {}
        for challenge_graphql in challenges_graphql:
            _metadata = challenge_graphql.get("_metadata")
            # TODO: detect collisions???
            if (_metadata is not None):
                oeb_challenges[_metadata["level_2:challenge_id"]] = challenge_graphql["_id"]
            else:
                oeb_challenges[challenge_graphql["acronym"]] = challenge_graphql["_id"]
        dataset_schema_uri = self.schemaMappings['Dataset']
        
        # The same, at the level of metrics
        oeb_metrics = {}
        for metric_graphql in metrics_graphql:
            metric_metadata = metric_graphql.get('_metadata')
            # BEWARE! There could be collisions!!!!!!!!
            if metric_metadata != None:
                
                oeb_metrics[metric_graphql["_id"]] = metric_metadata["level_2:metric_id"]
            else:
                metric_orig_id = metric_graphql["orig_id"]
                if not metric_orig_id.startswith(community_acronym + ":") and (":" in metric_orig_id):
                    self.logger.warning(f"Metrics {metric_graphql['_id']} has as original id {metric_orig_id}, community has as acronym {community_acronym}")
                # BEWARE! We could be taking into account other communities!?!?!?!
                oeb_metrics[metric_graphql["_id"]] = metric_orig_id[metric_orig_id.find(":") + 1 :]

        # get orig_id datasets
        orig_id_aggr = []
        for i in challenges_graphql:
            for elem in i['datasets']:
                orig_id_aggr.append(elem)
        
        for dataset in min_aggregation_datasets:
            orig_future_id = build_new_aggregation_id(dataset)
            agg_key = ""
            future_id = ""
            for d in orig_id_aggr:
                #check challenge
                if dataset['challenge_ids'][0] in d['orig_id']:
                    #check metrics
                    if (dataset['datalink']['inline_data']['visualization']['type'] == "2D-plot"):
                    	if(d['datalink']['inline_data']['visualization']['type'] == "2D-plot"):
                            if (dataset['datalink']['inline_data']['visualization']['x_axis'] in d['orig_id']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                sys.stdout.write(
                        'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                                break
                            elif (dataset['datalink']['inline_data']['visualization']['x_axis'] == d['datalink']['inline_data']['visualization']['x_axis']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                sys.stdout.write(
                        'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                                break
                            else:
                                agg_key = "orig_id"
                                future_id = build_new_aggregation_id(dataset)
                    #bar plot
                    else:
                        if (d['datalink']['inline_data']['visualization']['type'] == "bar-plot"):
                            if (dataset['datalink']['inline_data']['visualization']['metric'] in d['orig_id']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                break
                            else:
                                agg_key = "orig_id"
                                future_id = orig_future_id
                        

            # check if assessment datasets already exist in OEB for the provided bench event id
            # in that case, there is no need to generate new datasets, just adding the new metrics to the existing one
            
            '''orig_future_id = build_new_aggregation_id(dataset)
            
            if orig_future_id in orig_id_aggr:
                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                agg_key = "_id"
                future_id = dataset["_id"]
            else:
                agg_key = "orig_id"
                future_id = orig_future_id
            '''
            # This cache is very useful when assembling a bunch of new data from several participants
            valid_data = agg_by_id.get(future_id)
            if (valid_data is None) and future_id!=orig_future_id:
                valid_data = agg_by_id.get(orig_future_id)
            if valid_data is None:
                # Now, the aggregation datasets in the staging area
                for agg_data in stagedAggregationDatasets:
                    if future_id == agg_data[agg_key]:
                        valid_data = agg_data
                        break
                    
            if valid_data is None:
                # Last, the datasets associated to each challenge
                #dataset_challenge_ids = set(map(lambda challenge_id: oeb_challenges[challenge_id],dataset["challenge_ids"]))
                dataset_challenge_ids = set(oeb_challenges.values())
                for challenge in challenges_graphql:
                    if challenge["_id"] in dataset_challenge_ids:
                        for agg_data in challenge["datasets"]:
                            if (agg_key == ''): agg_key = "orig_id"
                            if future_id == agg_data[agg_key]:
                                valid_data = agg_data
                                break
                        if valid_data is not None:
                            break
            
            # If dataset could not be found, then store new one
            if valid_data is None:
                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is not registered in OpenEBench... Building new object\n')
                valid_data = self.new_aggregation(
                    oeb_challenges,
                    oeb_metrics,
                    dataset,
                    valid_assessment_datasets,
                    community_id,
                    version,
                    workflow_tool_id
                )
            else:

                # insPos is the position within the array when it is inserted something
                insPos = None
                challenge_participants = valid_data["datalink"]["inline_data"]["challenge_participants"]
                for iPart, cPart in enumerate(challenge_participants):
                    if cPart["tool_id"] == tool_name:
                        insPos = iPart
                        break
                
                for participant in dataset["datalink"]["inline_data"]["challenge_participants"]:
                    participant_id = participant.get("participant_id")
                    if participant_id == tool_name:
                        participant["tool_id"] = participant.pop("participant_id")
                        
                        if insPos is not None:
                            challenge_participants[insPos] = participant
                        else:
                            challenge_participants.append(participant)
                        break

                # update modification date
                valid_data["dates"]["modification"] = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

                # add referenced assessment datasets ids
                visualitzation_metric = ""
                visualization_x_axis =""
                visualization_y_axis = ""
                vis = valid_data['datalink']['inline_data']['visualization']
                if (vis.get("x_axis") is not None):
                    visualization_x_axis = valid_data["datalink"]["inline_data"]["visualization"]["x_axis"]
                    visualization_y_axis = valid_data["datalink"]["inline_data"]["visualization"]["y_axis"]
                else:
                    visualitzation_metric = valid_data["datalink"]["inline_data"]["visualization"]["metric"]
                    
                rel_dataset_ids = valid_data["depends_on"]["rel_dataset_ids"]
                rel_dataset_ids_set = set(map(lambda d: d['dataset_id'], rel_dataset_ids))
                
                for assess_element in valid_assessment_datasets:
                    assess_element_id = assess_element["_id"]
                    # Fail early in case of repetition
                    if assess_element_id in rel_dataset_ids_set:
                        continue
                    
                    if assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                        to_be_appended = False
                        
                        if oeb_metrics[assess_element["depends_on"]["metrics_id"]] in (visualization_x_axis,visualization_y_axis, visualitzation_metric):
                            to_be_appended = True
                        # also check for official oeb metrics, in case the aggregation dataset contains them
                        elif assess_element["depends_on"]["metrics_id"] in (visualization_x_axis, visualization_y_axis, visualitzation_metric):
                            to_be_appended = True
                        
                        if to_be_appended:
                            rel_dataset_ids_set.add(assess_element_id)
                            rel_dataset_ids.append({ "dataset_id": assess_element_id })

            valid_aggregation_datasets.append(valid_data)
            agg_by_id[valid_data["_id"]] = valid_data


        return valid_aggregation_datasets

    def build_aggregation_events_orig(self, challenges_graphql, all_events, min_aggregation_datasets, workflow_tool_id):

        self.logger.info(
            "\n\t==================================\n\t6. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of events
        aggregation_events = []

        for dataset in min_aggregation_datasets:

            # if the aggregation dataset is already in OpenEBench, it should also have an associated aggregation event
            event = None
            if dataset["_id"].startswith("OEB"):

                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench...\n')
                for action in all_events:

                    if action["action_type"] == "AggregationEvent" and action["challenge_id"] == dataset["challenge_ids"][0]:
                        #check if aggregation dataset id is an outgoing in the event involved data
                        for related_data in action["involved_datasets"]:
                            if related_data["role"] == "outgoing" and related_data["dataset_id"] == dataset["_id"]:

                                event = action
                                sys.stdout.write(
                                    'Adding new metadata to TestAction "' + str(event["_id"]) + '"\n')
                                break
                    # break loop if event is already found
                    if event:
                        break       

                # update the event modification date
                event["dates"]["modification"] = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

                # add referenced assessment datasets ids
                for agg_dataset_id in (item for item in dataset["depends_on"]["rel_dataset_ids"] if not item["dataset_id"].startswith('OEB')):
                    event["involved_datasets"].append(
                        {"dataset_id": agg_dataset_id["dataset_id"], "role": "incoming"})

                aggregation_events.append(event)

            else:  # if datset is not in oeb a  new event object will be created
                event = {
                    "_id": dataset["_id"] + "_Event",
                    "_schema": self.schemaMappings['TestAction'],
                    "action_type": "AggregationEvent",
                }

                sys.stdout.write(
                    'Building Event object for aggregation "' + str(dataset["_id"]) + '"...\n')

                # add id of workflow for the test event
                event["tool_id"] = workflow_tool_id

                # add the oeb official id for the challenge (which is already in the aggregation dataset)
                event["challenge_id"] = dataset["challenge_ids"][0]

                # append incoming and outgoing datasets
                involved_data = []

                # include the incomning datasets related to the event
                for data_id in dataset["depends_on"]["rel_dataset_ids"]:
                    involved_data.append({
                        "dataset_id": data_id["dataset_id"],
                        "role": "incoming"
                    })

                # ad the outgoing assessment data
                involved_data.append({
                    "dataset_id": dataset["_id"],
                    "role": "outgoing"
                })

                event["involved_datasets"] = involved_data

                # add data registration dates
                event["dates"] = {
                    "creation": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                    "reception": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
                }

                # add challenge managers as aggregation dataset contacts ids
                data_contacts = []
                for challenge in challenges_graphql:
                    if challenge["_id"] in dataset["challenge_ids"]:
                        data_contacts.extend(
                            challenge["challenge_contact_ids"])

                event["test_contact_ids"] = data_contacts
                
                aggregation_events.append(event)

        return aggregation_events

    def build_aggregation_events(
        self,
        valid_aggregation_tuples: "Sequence[AggregationTuple]",
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        workflow_tool_id: "str"
    ) -> "Sequence[Mapping[str, Any]]":

        self.logger.info(
            "\n\t==================================\n\t4. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of test events
        aggregation_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for agt in valid_aggregation_tuples:
            dataset = agt.aggregation_dataset
            
            orig_id = dataset.get("orig_id",dataset["_id"])
            event_id = orig_id + "_Event"
            event = {
                "_id": event_id,
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "AggregationEvent",
            }

            self.logger.info(
                'Building Event object for aggregation "' + str(dataset["_id"]) + '"...')

            # add id of tool for the test event
            event["tool_id"] = workflow_tool_id

            # add the oeb official id for the challenge (which is already in the dataset)
            event["challenge_id"] = dataset["challenge_ids"][0]

            # append incoming and outgoing datasets
            involved_data = []

            # include the incoming datasets related to the event
            for data_id in dataset["depends_on"]["rel_dataset_ids"]:
                involved_data.append({
                    "dataset_id": data_id["dataset_id"],
                    "role": "incoming"
                })
            # add the outgoing assessment data
            involved_data.append({
                "dataset_id": dataset["_id"],
                "role": "outgoing"
            })

            event["involved_datasets"] = involved_data
            
            # add data registration dates
            event["dates"] = {
                "creation": str(datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()),
                "reception": str(datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat())
            }

            # add challenge managers as aggregation dataset contacts ids
            data_contacts = []
            for challenge in challenges_graphql:
                if challenge["_id"] in dataset["challenge_ids"]:
                    data_contacts.extend(
                        challenge["challenge_contact_ids"])
            event["test_contact_ids"] = data_contacts

            aggregation_events.append(event)

        return aggregation_events


    def new_aggregation(
        self,
        oeb_challenges,
        challenge_contacts,
        oeb_metrics,
        min_dataset,
        valid_assessment_datasets,
        community_id,
        version: "str",
        workflow_tool_id
    ):

        # initialize new dataset object
        d = min_dataset["datalink"]["inline_data"]["visualization"]
        metrics_label = d.get("metric")
        if metrics_label is None:
            metrics_label = d["x_axis"]+ " - " + d["y_axis"]
            # TODO: Should we warn about this???
            
        valid_data = {
            "_id": build_new_aggregation_id(min_dataset),
            "type": "aggregation"
        }

        # add dataset visibility. AGGREGATION DATASETS ARE ALWAYS EXPECTED TO BE PUBLIC
        valid_data["visibility"] = "public"

        # add name and description, if workflow did not provide them
        valid_data_name = min_dataset.get("name")
        if valid_data_name is None:
            valid_data_name = "Summary dataset for challenge: " + \
                min_dataset["challenge_ids"][0] + ". Metrics: " + metrics_label
        valid_data["name"] = valid_data_name
        
        valid_data_description = min_dataset.get("description")
        if valid_data_description is None:
            valid_data_description = "Summary dataset that aggregates all results from participants in challenge: " + \
                min_dataset["challenge_ids"][0] + ". Metrics: " + metrics_label
        valid_data["description"] = valid_data_description

        # replace dataset related challenges with oeb challenge ids
        execution_challenges = []
        for challenge_id in min_dataset["challenge_ids"]:
            try:
                if challenge_id.startswith("OEB"):
                    execution_challenges.append(challenge_id)
                else:
                    execution_challenges.append(oeb_challenges[challenge_id])
            except:
                self.logger.info("No challenges associated to " + challenge_id +
                             " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.info(min_dataset["_id"] + " not processed")
                sys.exit()

        valid_data["challenge_ids"] = execution_challenges
        # add challenge managers as aggregation dataset contacts ids
        challenge_contacts = []
        for challenge_graphql in challenges_graphql:
            if challenge_graphql["_id"] in execution_challenges:
                challenge_contacts.extend(challenge_graphql["challenge_contact_ids"])

        rel_data = []
        for assess_element in valid_assessment_datasets:
            
            try:
                vis = min_dataset["datalink"]["inline_data"]["visualization"]

                if (vis.get("metric") is None):
                    if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    #check for not 'oeb' challenges ids, in case the datasets is still not uploaded
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    # also check for official oeb metrics, in case the aggregation dataset contains them
                    elif assess_element["depends_on"]["metrics_id"] == vis["x_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif assess_element["depends_on"]["metrics_id"] == vis["y_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif vis["metric"].upper() in assess_element["_id"].upper() and min_dataset["challenge_ids"][0] in assess_element["_id"]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                else:
                    if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif assess_element["depends_on"]["metrics_id"] == vis["metric"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif vis["metric"].upper() in assess_element["_id"].upper() and min_dataset["challenge_ids"][0] in assess_element["_id"]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
            except:
                continue

        # add data registration dates
        valid_data["dates"] = {
            "creation": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
            "modification": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        }

        # add assessment metrics values, as inline data
        datalink = min_dataset["datalink"]
        for participant in datalink["inline_data"]["challenge_participants"]:
            participant["tool_id"] = participant.pop("participant_id")

        datalink["schema_url"] = "https://raw.githubusercontent.com/inab/OpenEBench_scientific_visualizer/master/benchmarking_data_model/aggregation_dataset_json_schema.json"

        valid_data["datalink"] = datalink

        # add Benchmarking Data Model Schema Location
        valid_data["_schema"] = self.schemaMappings['Dataset']

        # add OEB id for the community
        valid_data["community_ids"] = [community_id]

        # add dataset dependencies: metric id, tool and reference datasets
        valid_data["depends_on"] = {
            "tool_id": workflow_tool_id,
            "rel_dataset_ids": rel_data,
        }

        # add data version
        valid_data["version"] = version

        valid_data["dataset_contact_ids"] = challenge_contacts

        sys.stdout.write('Processed "' + str(min_dataset["_id"]) + '"...\n')

        return valid_data


def build_new_aggregation_id(aggregation_dataset) -> "str":
    d = aggregation_dataset["datalink"]["inline_data"]["visualization"]
    if (d.get("metric") is not None):
        return aggregation_dataset["_id"] + "_" + d["metric"]
    
    else :
        metrics = [d["x_axis"], d["y_axis"]]
    
        return aggregation_dataset["_id"] + "_" + metrics[0] + "+" + metrics[1]
