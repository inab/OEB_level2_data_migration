#!/usr/bin/env python

import dataclasses
import json
import logging
import re
import sys

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
        Iterator,
        Mapping,
        Sequence,
        MutableMapping,
        MutableSequence,
        Tuple,
    )

def gen_challenge_assessment_metrics_dict(the_challenge: "Mapping[str, Any]") -> "Mapping[str, Mapping[str, str]]":
    for metrics_category in the_challenge.get("metrics_categories",[]):
        if metrics_category.get("category") == "assessment":
            challenge_assessment_metrics = metrics_category.get("metrics", [])
            cam_d = {
                cam["metrics_id"]: cam
                for cam in challenge_assessment_metrics
            }
            
            return cam_d
    
    return {}

def gen_inline_data_label(met_dataset: "Mapping[str, Any]", par_datasets: "Sequence[Mapping[str, Any]]"):
    met_metadata = met_dataset.get("_metadata",{})
    met_label = None if met_metadata is None else met_metadata.get("level_2:participant_id")
    for par_dataset in par_datasets:
        # First, look for the label in the participant dataset
        par_metadata = par_dataset.get("_metadata",{})
        par_label = None if par_metadata is None else par_metadata.get("level_2:participant_id")
        # Then, look for it in the assessment dataset
        if par_label is None:
            par_label = met_label
        
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
        
        par_dataset_id = par_dataset["_id"]
        inline_data_label = {
            "label": par_label,
            "dataset_orig_id": par_dataset.get("orig_id", par_dataset_id),
        }
        return inline_data_label , par_dataset_id
    
    return None, None

class MetricsTrio(NamedTuple):
    metrics_id: "str"
    tool_id: "Optional[str]"
    proposed_label: "str"

def match_metric_from_label(logger, metrics_graphql, community_acronym: "str", metrics_label: "str", challenge_id: "str", challenge_acronym: "str", challenge_assessment_metrics_d: "Mapping[str, Mapping[str, str]]", dataset_id: "Optional[str]" = None) -> "Union[Tuple[None, None, None], MetricsTrio]":
    # Select the metrics just "guessing"
    guessed_metrics = []
    community_prefix = community_acronym + ':'
    dataset_metrics_id_u = metrics_label.upper()
    for metric in metrics_graphql:
        # Could the metrics label match the metrics id?
        if metric['_id'] == metrics_label:
            guessed_metrics = [ metric ]
            break
        if metric['orig_id'].startswith(community_prefix):
            # First guess
            if metric["orig_id"][len(community_prefix):].upper().startswith(dataset_metrics_id_u):
                guessed_metrics.append(metric)
            
            # Second guess (it can introduce false crosses)
            metric_metadata = metric.get("_metadata")
            if isinstance(metric_metadata, dict) and 'level_2:metric_id' in metric_metadata:
                if metric_metadata['level_2:metric_id'].upper() == dataset_metrics_id_u:
                    guessed_metrics.append(metric)
    
    
    if len(guessed_metrics) == 0:
        logger.critical(f"For {dataset_id}, unable to match in OEB a metric to label {metrics_label} . Please contact OpenEBench support for information about how to register your own metrics and link them to the challenge {challenge_id} (acronym {challenge_acronym})")
        return None, None, None
        #should_end.append((the_challenge['_id'], the_challenge['acronym']))
        #continue
    
    matched_metrics = []
    for guessed_metric in guessed_metrics:
        cam = challenge_assessment_metrics_d.get(guessed_metric["_id"])
        if cam is not None:
            matched_metrics.append((cam, guessed_metric))
    
    metric_id = None
    tool_id = None
    proposed_label = None
    mmi = None
    if len(matched_metrics) == 0:
        if len(guessed_metrics) == 1:
            mmi = guessed_metrics[0]
            metric_id = mmi["_id"]
            logger.warning(f"Metric {metric_id} (guessed from {metrics_label} at dataset {dataset_id}) is not registered as an assessment metric at challenge {challenge_id} (acronym {challenge_acronym}). Consider register it")
        else:
            logger.critical(f"Several metrics {', '.join(map(lambda gm: gm['_id'], guessed_metrics))} were guessed from {metrics_label} at dataset {dataset_id} . No clever heuristic can be applied. Please properly register some of them as an assessment metric at challenge {challenge_id} (acronym {challenge_acronym}).")
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
    
    # Getting a proposed label
    if mmi is not None:
        mmi_metadata = mmi.get("_metadata")
        if isinstance(mmi_metadata, dict) and ('level_2:metric_id' in mmi_metadata):
            proposed_label = mmi_metadata['level_2:metric_id']
        elif mmi["orig_id"].startswith(community_prefix):
            proposed_label = mmi["orig_id"][len(community_prefix):]
        else:
            proposed_label = mmi["orig_id"]
    
    return MetricsTrio(
        metrics_id=metric_id,
        tool_id=tool_id,
        proposed_label=proposed_label,
    )

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
    # (assessment and aggregation)
    # Groups the assessment datasets by metrics
    d_m_dict: "MutableMapping[str, MutableSequence[int]]" = dataclasses.field(default_factory=dict)
    # (aggregation)
    # Maps datasets to lists of metric mappings
    metrics_by_d: "MutableMapping[str, Sequence[MetricsTrio]]" = dataclasses.field(default_factory=dict)
    
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
                    x_trio = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=x_axis_metric_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if x_trio.metrics_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses for x axis unmatched metric {x_axis_metric_label}. Fix it")
                        
                    y_trio = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=y_axis_metric_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if y_trio.metrics_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses for y axis unmatched metric {y_axis_metric_label}. Fix it")
                    
                    # Check the suffix
                    suffix = f"_{x_axis_metric_label}+{y_axis_metric_label}"
                    proposed_suffix = f"_{x_trio.proposed_label}+{y_trio.proposed_label}"
                    if index_id_orig is None or not (index_id_orig.endswith(suffix) or index_id_orig.endswith(proposed_suffix)):
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} orig id {index_id_orig} does not end with either computed metrics suffix {suffix} or proposed computed metrics suffix {proposed_suffix}. Fix it")
                    
                    # Saving it for later usage
                    self.metrics_by_d[index_id] = [
                        x_trio,
                        y_trio,
                    ]
                        
            elif vis_type == "bar-plot":
                metrics_label = vis_hints.get("metric")
                if metrics_label is None:
                    self.logger.critical(f"{self.type.capitalize()} dataset {index_id} of visualization type {vis_type} did not define metric label. Fix it")
                
                else:
                    # Check there is some matching metric
                    trio = match_metric_from_label(
                        logger=self.logger,
                        metrics_graphql=self.metrics_graphql,
                        community_acronym=self.community_acronym,
                        metrics_label=metrics_label,
                        challenge_id=self.challenge['_id'],
                        challenge_acronym=self.challenge['acronym'],
                        challenge_assessment_metrics_d=self.cam_d,
                        dataset_id=index_id,
                    )
                    if trio.metrics_id is None:
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} uses unmatched metric {metrics_label}. Fix it")
                    
                    # Check the suffix
                    suffix = f"_{metrics_label}"
                    proposed_suffix = f"_{trio.proposed_label}"
                    if index_id_orig is None or not (index_id_orig.endswith(suffix) or index_id_orig.endswith(proposed_suffix)):
                        self.logger.critical(f"{self.type.capitalize()} dataset {index_id} orig id {index_id_orig} does not end with computed metrics suffix {suffix} or proposed computed metrics suffix {proposed_suffix}. Fix it")
                    
                    # Saving it for later usage
                    self.metrics_by_d[index_id] = [
                        trio,
                    ]
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

    def get_metrics_trio(self, dataset_id: "str") -> "Optional[MetricsTrio]":
        return self.metrics_by_d.get(dataset_id)

    def datasets_from_metric(self, metrics_id) -> "Iterator[Mapping[str, Any]]":
        return map(lambda d_index: self.d_list[d_index], self.d_m_dict.get(metrics_id, []))

    def keys(self) -> "Iterator[str]":
        return self.d_dict.keys()
    
    def datasets(self) -> "Sequence[Mapping[str, Any]]":
        return self.d_list

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
    
    od_to_a: "MutableMapping[str, int]" = dataclasses.field(default_factory=dict)
    
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
        
        # Epilogue: index all the outgoing datasets
        for out_dataset in out_datasets:
            self.od_to_a[out_dataset["_id"]] = ter_pos
        
        return self
    
    def get_by_outgoing_dataset(self, dataset_id: "str") -> "Optional[TestActionRel]":
        ter_pos = self.od_to_a.get(dataset_id)
        return self.a_list[ter_pos]  if ter_pos is not None else None
            
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
    
    def get(self, action_type: "str") -> "Optional[IndexedTestActions]":
        return self.catalogs.get(action_type)
