#!/usr/bin/env python

import copy
import inspect
import logging
import math
import sys
import os
import datetime
import json

from ..schemas import TYPE2SCHEMA_ID
from ..utils.catalogs import (
    DatasetsCatalog,
    gen_inline_data_label,
    IndexedChallenge,
    TestActionsCatalog,
)

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        Optional,
        Sequence,
        MutableMapping,
        MutableSequence,
        Tuple,
        Union,
    )
    
    from typing_extensions import (
        NotRequired,
        Required,
        TypedDict,
    )
    
    from .assessment import AssessmentTuple
    
    from ..utils.catalogs import (
        IndexedDatasets,
        InlineDataLabel,
    )
    
    from ..utils.migration_utils import BenchmarkingEventPrefixEtAl
    
    from numbers import Real
    
    class RelDataset(TypedDict):
        dataset_id: "Required[str]"
    
    class BarData(TypedDict, total=False):
        tool_id: "str"
        metric_value: "Real"
        stderr: "Real"
    
    class ScatterData(TypedDict, total=False):
        tool_id: "str"
        metric_x: "Real"
        stderr_x: "Real"
        metric_y: "Real"
        stderr_y: "Real"
    
    class SeriesVal(TypedDict):
        v: "Required[Real]"
        e: "NotRequired[Real]"
    
    class SeriesData(TypedDict, total=False):
        label: "str"
        metric_id: "str"
        values: "Union[Sequence[Real], Sequence[SeriesVal]]"
    
    class DataLabel(TypedDict):
        label: "Required[str]"
        dataset_orig_id: "Required[str]"

from oebtools.fetch import FetchedInlineData

from ..utils.migration_utils import OpenEBenchUtils

class AggregationTuple(NamedTuple):
    aggregation_dataset: "Mapping[str, Any]"
    idx_challenge: "Optional[IndexedChallenge]"
    at_l: "Sequence[AssessmentTuple]"


class AggregationValidator():

    def __init__(self, schemaMappings: "Mapping[str, str]", migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.level2_min_validator = migration_utils.level2_min_validator
        self.admin_tools = migration_utils.admin_tools
    
    def check_and_index_challenges(
        self,
        community_prefix: "str",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        challenges_agg_graphql: "Sequence[Mapping[str, Any]]",
        metrics_agg_graphql: "Sequence[Mapping[str, Any]]",
    ) -> "Mapping[str, IndexedChallenge]":
    
        self.logger.info(
            "\n\t==================================\n\t5. Validating stored challenges\n\t==================================\n")
        
        # First, let's analyze all existing aggregation datasets
        # nested in the challenges
        agg_challenges: "MutableMapping[str, IndexedChallenge]" = { }
        should_exit_ch = False
        for agg_ch in challenges_agg_graphql:
            # Garrayo's school label
            challenge_id = agg_ch["_id"]
            
            challenge_label_and_sep = OpenEBenchUtils.get_challenge_label_from_challenge(
                agg_ch,
                bench_event_prefix_et_al,
                community_prefix,
            )
            
            self.logger.info(f"Validating challenge {challenge_id} ({challenge_label_and_sep.label})")
            
            challenge_orig_id = agg_ch.get("orig_id")
            if challenge_orig_id is None or not challenge_orig_id.startswith(community_prefix):
                self.logger.warning(f"Challenge {challenge_id} has as original id {challenge_orig_id}. It must start with {community_prefix}. Fix it.")
            if challenge_orig_id is None or not challenge_orig_id.startswith(bench_event_prefix_et_al.prefix):
                self.logger.warning(f"Challenge {challenge_id} has as original id {challenge_orig_id}. It must start with {bench_event_prefix_et_al.prefix}. Fix it.")

            expected_ch_orig_id = bench_event_prefix_et_al.prefix + challenge_label_and_sep.label
            if challenge_orig_id != expected_ch_orig_id:
                self.logger.warning(f"Challenge {challenge_id} has as original id {challenge_orig_id}. It is suggested to be {expected_ch_orig_id}.")
            
            # The existing original id prevails over the suggested one
            challenge_prefix = ("" if challenge_orig_id is None else challenge_orig_id) + challenge_label_and_sep.sep
            
            coll_ch = agg_challenges.get(challenge_label_and_sep.label)
            if coll_ch is not None:
                self.logger.critical(f"Challenge collision: label {challenge_label_and_sep.label}, challenges {coll_ch.challenge_id} and {challenge_id}. Please contact OpenEBench support to report this inconsistency")
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
                admin_tools=self.admin_tools,
                level2_min_validator=self.level2_min_validator,
                metrics_graphql=metrics_agg_graphql,
                community_prefix=community_prefix,
                bench_event_prefix_et_al=bench_event_prefix_et_al,
                challenge_prefix=challenge_prefix,
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
            
            # Then the assessment datasets
            ass_d_schema_pairs = d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("assessment_datasets", []),
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
            
            # Let's index the MetricsEvent actions, as they can provide
            # the way to link to the TestEvent needed to rescue the labels
            # needed to regenerate an aggregation dataset from their assessment
            # components
            
            # Merging the MetricsEvent test actions
            ta_catalog.merge_test_actions(agg_ch.get("metrics_test_actions", []))
            
            # Now, let's index the existing aggregation datasets, and
            # their relationship. And check them, BTW
            
            # Then the assessment datasets
            agg_d_schema_pairs = d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("aggregation_datasets", []),
                d_categories=agg_cat,
            )
            
            agg_d_schema_dict = dict(agg_d_schema_pairs)
            
            # Complement this with the aggregation_test_actions
            ta_catalog.merge_test_actions(agg_ch.get("aggregation_test_actions", []))
            
            # Let's rebuild the aggregation datasets, from the minimal information
            idat_agg = d_catalog.get("aggregation")
            # A new community has no aggregation dataset
            if idat_agg is not None:
                
                idat_ass = d_catalog.get("assessment")
                assert idat_ass is not None
                idat_part = d_catalog.get("participant")
                assert idat_part is not None
                
                ita_m_events = ta_catalog.get("MetricsEvent")
                assert ita_m_events is not None
                
                failed_agg_dataset = False
                for raw_dataset in idat_agg.datasets():
                    agg_dataset_id = raw_dataset["_id"]
                    found_schema_url = agg_d_schema_dict.get(agg_dataset_id)
                    metrics_trios = idat_agg.get_metrics_trio(agg_dataset_id)
                    if metrics_trios:
                        # Make an almost shallow copy of the entry
                        # removing what it is going to be populated from ground
                        r_dataset = cast("MutableMapping[str, Any]", copy.copy(raw_dataset))
                        
                        # depends_on is going to be rebuilt
                        d_on = copy.deepcopy(raw_dataset["depends_on"])
                        rel_dataset_ids: "MutableSequence[RelDataset]" = []
                        d_on["rel_dataset_ids"] = rel_dataset_ids
                        r_dataset["depends_on"] = d_on
                        
                        # datalink contents are going to be rebuilt, also
                        d_link = copy.deepcopy(raw_dataset["datalink"])
                        r_dataset["datalink"] = d_link
                        
                        # Fetch inline (or not so inline) content
                        fetched_inline_data = idat_agg.get_payload(agg_dataset_id)
                        
                        if ("_metadata" in r_dataset) and r_dataset["_metadata"] is None:
                            del r_dataset["_metadata"]
                        
                        # Challenge ids of this dataset (to check other dataset validness)
                        ch_ids_set = set(r_dataset["challenge_ids"])
                        if isinstance(fetched_inline_data, FetchedInlineData):
                            inline_data = copy.copy(fetched_inline_data.data)
                            r_dataset["inline_data"] = inline_data
                            
                            # Entry of each challenge participant, by participant label
                            # The right type should be "Union[MutableMapping[str, BarData], MutableMapping[str, ScatterData], MutableMapping[str, SeriesData]]"
                            # but the validation gets more complicated
                            cha_par_by_id = cast("MutableMapping[str, Union[BarData, ScatterData, SeriesData]]", {})
                            # To provide meaningful error messages
                            ass_par_by_id: "MutableMapping[str, str]" = {}
                            # The right type should be "Union[MutableSequence[BarData], MutableSequence[ScatterData], MutableSequence[SeriesData]]"
                            # but the validation gets more complicated
                            challenge_participants = cast("MutableSequence[Union[BarData, ScatterData, SeriesData]]", [])
                            inline_data["challenge_participants"] = challenge_participants
                            
                            # Visualization type determines later the labels
                            # to use in each entry of challenge_participants
                            vis_type = inline_data.get("visualization",{}).get("type")
                            series_type = inline_data.get("series_type")
                            if series_type == "aggregation-data-series":
                                inline_data["series_type"] = "aggregation-data-series"
                            raw_available_metrics_labels = inline_data.get("visualization",{}).get("available_metrics", [])
                            changed_metrics_labels = False
                            found_metrics = []
                            
                            # This set is used to detect mismatches between
                            # registered and gathered assessment datasets
                            rel_ids_set = set(map(lambda r: cast("str", r["dataset_id"]), filter(lambda r: r.get("role", "dependency") == "dependency", raw_dataset["depends_on"]["rel_dataset_ids"])))
                            
                            # Processing and validating already registered labels
                            potential_inline_data_labels: "Sequence[DataLabel]" = inline_data.get("labels", [])
                            inline_data_labels: "MutableSequence[InlineDataLabel]" = []
                            inline_data["labels"] = inline_data_labels
                            
                            # Inline data labels by participant dataset id
                            idl_by_d_id = {}
                            
                            changed_labels = False
                            for potential_inline_data_label in potential_inline_data_labels:
                                part_d_label = potential_inline_data_label['label']
                                part_d_orig_id = potential_inline_data_label['dataset_orig_id']
                                
                                discarded_label = True
                                # Let's obtain the raw entry of the participant
                                part_raw_dataset = idat_part.get(part_d_orig_id)
                                if part_raw_dataset is not None:
                                    # Checking its availability
                                    part_raw_metadata = part_raw_dataset.get("_metadata")
                                    if part_raw_metadata is None:
                                        part_raw_metadata = {}
                                    part_raw_label = part_raw_metadata.get("level_2:participant_id", part_d_label)
                                    if len(ch_ids_set.intersection(part_raw_dataset["challenge_ids"])) > 0 and part_raw_label == part_d_label:
                                        inline_data_labels.append(potential_inline_data_label)
                                        idl_by_d_id[part_raw_dataset["_id"]] = potential_inline_data_label
                                        discarded_label = False
                                    else:
                                        self.logger.warning(f"Discarded previous label {part_d_label} associated to {part_d_orig_id} in dataset {agg_dataset_id} due mismatches with {part_raw_dataset['_id']}")
                                else:
                                    self.logger.warning(f"Discarded previous label {part_d_label} associated to {part_d_orig_id} in dataset {agg_dataset_id} as no dataset was matched")
                                
                                changed_labels = changed_labels or discarded_label
                            
                            # Time to fetch
                            rebuild_agg = False
                            regen_rel_ids_set = set()
                            # Iterating over the different metrics
                            for i_trio, metrics_trio in enumerate(metrics_trios):
                                if metrics_trio is None:
                                    self.logger.fatal(f"Unable to map metric {i_trio} related to {agg_dataset_id}. It will be discarded in the best case")
                                    failed_agg_dataset = True
                                    continue
                                # To gather the assessment datasets for each metric
                                met_datasets = list(idat_ass.datasets_from_metric(metrics_trio.metrics_id))
                                
                                met_set = set(map(lambda m: cast("str", m["_id"]), met_datasets))
                                if not(rel_ids_set >= met_set):
                                    new_set = met_set - rel_ids_set
                                    self.logger.error(f"Aggregation dataset {agg_dataset_id} should also depend on {len(new_set)} datasets: {', '.join(new_set)}")
                                    rebuild_agg = True
                                regen_rel_ids_set.update(met_set)
                                rel_dataset_ids.extend(map(lambda m: {"dataset_id": m["_id"]}, met_datasets))
                                
                                if len(met_datasets) > 0:
                                    found_metrics.append(metrics_trio)
                                
                                for met_dataset in met_datasets:
                                    # Fetch the TestActionRel MetricsEvent entry
                                    tar = ita_m_events.get_by_outgoing_dataset(met_dataset["_id"])
                                    if tar is None:
                                        self.logger.error(f"Unindexed MetricsEvent TestAction for dataset {met_dataset['_id']}")
                                        #for m_k, m_p in ita_m_events.a_dict.items():
                                        #    self.logger.error(m_k)
                                        #    self.logger.error(ita_m_events.a_list[m_p])
                                        rebuild_agg = True
                                        continue
                                    
                                    # Now, the participant datasets can be rescued
                                    # to get or guess its label
                                    inline_data_label = None
                                    par_dataset_id: "Optional[str]" = None
                                    par_label = None
                                    for par_dataset in tar.in_d:
                                        inline_data_label = idl_by_d_id.get(par_dataset["_id"])
                                        if isinstance(inline_data_label, dict):
                                            par_dataset_id = par_dataset["_id"]
                                            par_label = inline_data_label["label"]
                                            break
                                    
                                    # Bad luck, time to create a new entry
                                    if par_label is None:
                                        inline_data_label , par_dataset_id = gen_inline_data_label(met_dataset, tar.in_d)
                                        if inline_data_label is None:
                                            self.logger.error(f"Unable to generate inline data label for {met_dataset['_id']}")
                                            
                                        # Now we should have the participant label
                                        assert inline_data_label is not None
                                        
                                        # Last, store it
                                        inline_data_labels.append(inline_data_label)
                                        idl_by_d_id[par_dataset_id] = inline_data_label
                                        par_label = inline_data_label["label"]
                                    
                                    assert par_dataset_id is not None
                                    
                                    mini_entry = cha_par_by_id.get(par_dataset_id)
                                    mini_entry_2d: "Optional[ScatterData]" = None
                                    mini_entry_b: "Optional[BarData]" = None
                                    mini_entry_s: "Optional[SeriesData]" = None
                                    do_processing = True
                                    if mini_entry is None:
                                        if vis_type == "2D-plot":
                                            mini_entry_2d = {
                                                "tool_id": par_label,
                                            }
                                            mini_entry = mini_entry_2d
                                        elif vis_type == "bar-plot":
                                            mini_entry_b = {
                                                "tool_id": par_label,
                                            }
                                            mini_entry = mini_entry_b
                                        else:
                                            mini_entry_s = {
                                                "label": par_label,
                                                "metric_id": metrics_trio.proposed_label,
                                            }
                                            mini_entry = mini_entry_s
                                        
                                        challenge_participants.append(mini_entry)
                                        cha_par_by_id[par_dataset_id] = mini_entry
                                        ass_par_by_id[par_dataset_id] = met_dataset['_id']
                                    elif i_trio == 0:
                                        self.logger.error(f"Assessment datasets {met_dataset['_id']} and {ass_par_by_id[par_dataset_id]} (both needed by {agg_dataset_id}) has metrics {met_dataset['depends_on']['metrics_id']}, but one is mislabelled (wrong?). Fix the wrong one")
                                        rebuild_agg = True
                                        do_processing = False
                                    elif vis_type == "2D-plot":
                                        mini_entry_2d = cast("ScatterData", mini_entry)
                                    elif vis_type == "bar-plot":
                                        mini_entry_b = cast("BarData", mini_entry)
                                    elif series_type == "aggregation-data-series":
                                        if vis_type == "box-plot":
                                            mini_entry_s = cast("SeriesData", mini_entry)
                                        else:
                                            self.logger.critical(f"Unimplemented aggregation for {series_type} visualization type {vis_type} in dataset {agg_dataset_id}")
                                            do_processing = False
                                    else:
                                        self.logger.critical(f"Unimplemented aggregation for visualization type {vis_type} in dataset {agg_dataset_id}")
                                        do_processing = False
                                        
                                    fetched_ass_inline_data = idat_ass.get_payload(met_dataset["_id"])
                                    if do_processing and isinstance(fetched_ass_inline_data, FetchedInlineData):
                                        ass_inline_data = fetched_ass_inline_data.data
                                        if mini_entry_2d is not None:
                                            mini_entry_values: "ScatterData"
                                            if i_trio == 0:
                                                mini_entry_values = {
                                                    "metric_x": ass_inline_data["value"],
                                                    "stderr_x": ass_inline_data.get("error", 0),
                                                }
                                            else:
                                                mini_entry_values = {
                                                    "metric_y": ass_inline_data["value"],
                                                    "stderr_y": ass_inline_data.get("error", 0),
                                                }
                                            
                                            mini_entry_2d.update(mini_entry_values)
                                        elif mini_entry_b:
                                            mini_entry_b.update({
                                                "metric_value": ass_inline_data["value"],
                                                "stderr": ass_inline_data.get("error", 0),
                                            })
                                        elif mini_entry_s:
                                            mini_entry_s.update({
                                                "values": ass_inline_data["values"],
                                            })
                            
                            if not rebuild_agg:
                                if series_type == "aggregation-data-series":
                                    found_metrics_labels = list(map(lambda mt: mt.proposed_label, found_metrics))
                                    if len(found_metrics) != len(raw_available_metrics_labels):
                                        self.logger.error(f"Mismatch in {agg_dataset_id} length of available_metrics\n\n{json.dumps(found_metrics_labels, indent=4, sort_keys=True)}\n\n{json.dumps(raw_available_metrics_labels, indent=4, sort_keys=True)}")
                                        inline_data["visualization"]["available_metrics"] = found_metrics_labels
                                        changed_metrics_labels = True
                                    else:
                                        # Now it is time to validate each metrics label, to check it is the canonical one
                                        proposed_metrics_labels = []
                                        for r_av_label in raw_available_metrics_labels:
                                            for found_metric in found_metrics:
                                                if r_av_label in found_metric.all_labels:
                                                    proposed_metrics_labels.append(found_metric.proposed_label)
                                                    break
                                            else:
                                                self.logger.error(f"Ill-formed available_metrics in {agg_dataset_id}, as {r_av_label} label cannot be mapped to a metric")
                                                changed_metrics_labels = True
                                        
                                        if len(proposed_metrics_labels) != len(raw_available_metrics_labels) or proposed_metrics_labels != raw_available_metrics_labels:
                                            self.logger.error(f"Mismatch in {agg_dataset_id} available_metrics\n\n{json.dumps(proposed_metrics_labels, indent=4, sort_keys=True)}\n\n{json.dumps(raw_available_metrics_labels, indent=4, sort_keys=True)}")
                                            inline_data["visualization"]["available_metrics"] = proposed_metrics_labels
                                            changed_metrics_labels = True
                                                
                                
                                raw_challenge_participants = cast("Union[Sequence[BarData], Sequence[ScatterData], Sequence[SeriesData]]", fetched_inline_data.data["challenge_participants"])
                                if len(challenge_participants) != len(raw_challenge_participants):
                                    self.logger.error(f"Mismatch in {agg_dataset_id} length of challenge_participants\n\n{json.dumps(challenge_participants, indent=4, sort_keys=True)}\n\n{json.dumps(raw_challenge_participants, indent=4, sort_keys=True)}")
                                    rebuild_agg = True
                                else:
                                    s_new_challenge_participants: "Union[Sequence[BarData], Sequence[ScatterData], Sequence[SeriesData]]"
                                    s_raw_challenge_participants: "Union[Sequence[BarData], Sequence[ScatterData], Sequence[SeriesData]]"
                                    if vis_type == "box-plot":
                                        s_new_challenge_participants = sorted(cast("Sequence[SeriesData]", challenge_participants), key=lambda cp: cp["label"])
                                        s_raw_challenge_participants = sorted(cast("Sequence[SeriesData]", raw_challenge_participants), key=lambda cp: cp["label"])
                                    elif vis_type == "2D-plot":
                                        s_new_challenge_participants = sorted(cast("Sequence[ScatterData]", challenge_participants), key=lambda cp: cp["tool_id"])
                                        s_raw_challenge_participants = sorted(cast("Sequence[ScatterData]", raw_challenge_participants), key=lambda cp: cp["tool_id"])
                                    elif vis_type == "bar-plot":
                                        s_new_challenge_participants = sorted(cast("Sequence[BarData]", challenge_participants), key=lambda cp: cp["tool_id"])
                                        s_raw_challenge_participants = sorted(cast("Sequence[BarData]", raw_challenge_participants), key=lambda cp: cp["tool_id"])
                                    else:
                                        # This should not happen
                                        raise Exception("This should not happen. Contact some OpenEBench developer")
                                    
                                    if s_new_challenge_participants != s_raw_challenge_participants:
                                        for s_new , s_raw in zip(s_new_challenge_participants, s_raw_challenge_participants):
                                            ts_new = cast("Mapping[str, Union[str, Real, Sequence[Real]]]", s_new)
                                            ts_raw = cast("Mapping[str, Union[str, Real, Sequence[Real]]]", s_raw)
                                            do_log_error = False
                                            if set(s_new.keys()) != set(s_raw.keys()):
                                                do_log_error = True
                                            else:
                                                for k in ts_new.keys():
                                                    v_new = ts_new[k]
                                                    v_raw = ts_raw[k]
                                                    
                                                    if isinstance(v_new, str) or isinstance(v_raw, str):
                                                        if v_new != v_raw:
                                                            do_log_error = True
                                                            break
                                                    elif isinstance(v_new, list) or isinstance(v_raw, list):
                                                        lv_new = cast("Sequence[Real]", v_new)
                                                        lv_raw = cast("Sequence[Real]", v_raw)
                                                        if len(lv_new) == len(lv_raw):
                                                            for lv_n, lv_r in zip(lv_new, lv_raw):
                                                                if not math.isclose(lv_n, lv_r):
                                                                    do_log_error = True
                                                                    break
                                                            if do_log_error:
                                                                break
                                                        else:
                                                            do_log_error = True
                                                            break
                                                    else:
                                                        sv_new = cast("Real", v_new)
                                                        sv_raw = cast("Real", v_raw)
                                                        if not math.isclose(sv_new, sv_raw):
                                                            do_log_error = True
                                                            break
                                            
                                            if do_log_error:
                                                rebuild_agg = True
                                                self.logger.error(f"Mismatch in {agg_dataset_id} challenge_participants\n\n{json.dumps(s_new, indent=4, sort_keys=True)}\n\n{json.dumps(s_raw, indent=4, sort_keys=True)}")
                                        
                                        if not rebuild_agg:
                                            self.logger.info(f"False positive rounding mismatch in aggregation dataset {agg_dataset_id}.")

                            if changed_labels:
                                if len(inline_data_labels) != len(potential_inline_data_labels):
                                    self.logger.error(f"Mismatch in {agg_dataset_id} length of labels\n\n{json.dumps(inline_data_labels, indent=4, sort_keys=True)}\n\n{json.dumps(potential_inline_data_labels, indent=4, sort_keys=True)}")
                                else:
                                    s_inline_data_labels = sorted(inline_data_labels, key=lambda dl: dl["label"])
                                    s_potential_inline_data_labels = sorted(potential_inline_data_labels, key=lambda dl: dl["label"])
                                    
                                    if s_inline_data_labels != s_potential_inline_data_labels:
                                        is_false_pos_label = True
                                        for zs_new , zs_raw in zip(s_inline_data_labels, s_potential_inline_data_labels):
                                            ts_new = cast("Mapping[str, Union[str, Real]]", zs_new)
                                            ts_raw = cast("Mapping[str, Union[str, Real]]", zs_raw)
                                            do_log_error = False
                                            if set(zs_new.keys()) != set(zs_raw.keys()):
                                                do_log_error = True
                                            else:
                                                for k in ts_new.keys():
                                                    v_new = ts_new[k]
                                                    v_raw = ts_raw[k]
                                                    
                                                    if v_new != v_raw:
                                                        do_log_error = True
                                                        break
                                            
                                            if do_log_error:
                                                self.logger.error(f"Mismatch in {agg_dataset_id} label\n\n{json.dumps(zs_new, indent=4, sort_keys=True)}\n\n{json.dumps(zs_raw, indent=4, sort_keys=True)}")
                                                
                                                is_false_pos_label = False
                                        
                                        if is_false_pos_label:
                                            self.logger.info(f"False positive label mismatch in aggregation dataset {agg_dataset_id}.")
                            
                            # Time to compare
                            if rebuild_agg or changed_labels or changed_metrics_labels:
                                if rebuild_agg:
                                    self.logger.error(f"Aggregation dataset {agg_dataset_id} from challenges {', '.join(raw_dataset['challenge_ids'])} has to be rebuilt: elegible assessments {len(regen_rel_ids_set)} vs {len(rel_ids_set)} used in the aggregation dataset")
                                    set_diff = rel_ids_set - regen_rel_ids_set
                                    if len(set_diff) > 0:
                                        self.logger.error(f"Also, {len(set_diff)} datasets do not appear in proposed rebuilt entry: {', '.join(set_diff)}")
                                
                                if changed_labels:
                                    self.logger.error(f"Aggregation dataset {agg_dataset_id} from challenges {', '.join(raw_dataset['challenge_ids'])} has to be rebuilt because the explicit mapping of labels and original ids has changed")
                                
                                # Last, explicit schema_id
                                guessed_schema_id = TYPE2SCHEMA_ID.get(vis_type)
                                if found_schema_url is not None:
                                    d_link["schema_url"] = found_schema_url
                                elif guessed_schema_id is not None:
                                    d_link["schema_url"] = guessed_schema_id
                                
                                self.logger.error(f"Proposed rebuilt entry {agg_dataset_id} (keep an eye in previous errors, it could be incomplete):\n" + json.dumps(r_dataset, indent=4))
                                failed_agg_dataset = True
                
                if failed_agg_dataset:
                    self.logger.critical("As some aggregation datasets seem corrupted, fix them to continue")
                    sys.exit(5)
            
            # Last, but not the least important
            agg_challenges[challenge_id] = agg_challenges[challenge_label_and_sep.label] = IndexedChallenge(
                challenge=agg_ch,
                challenge_id=challenge_id,
                challenge_label_and_sep=challenge_label_and_sep,
                d_catalog=d_catalog,
                ta_catalog=ta_catalog,
                ass_cat=ass_cat,
            )
        
        if should_exit_ch:
            self.logger.critical("Some challenges have collisions at their label level. Please ask the team to fix the mess")
            sys.exit(4)
        
        return agg_challenges
    
        

class AggregationBuilder():

    def __init__(self, schemaMappings: "Mapping[str, str]", migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.migration_utils = migration_utils
    
    def build_aggregation_datasets(
        self,
        community_id: "str",
        min_aggregation_datasets: "Sequence[Mapping[str, Any]]",
        agg_challenges: "Mapping[str, IndexedChallenge]",
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        valid_test_events: "Sequence[Mapping[str, Any]]",
        valid_metrics_events: "Sequence[Mapping[str, Any]]",
        putative_workflow_tool_id: "str",
    ) -> "Sequence[AggregationTuple]":
        
        self.logger.info(
            "\n\t==================================\n\t6. Validating stored aggregation datasets\n\t==================================\n")
        
        # Grouping future assessment dataset tuples by challenge
        ass_c_dict: "MutableMapping[str, MutableSequence[AssessmentTuple]]" = {}
        for ass_t in valid_assessment_tuples:
            ass_d = ass_t.assessment_dataset
            for challenge_id in ass_d.get("challenge_ids", []):
                ass_c_dict.setdefault(challenge_id, []).append(ass_t)
        
        # For each indexed challenge
        for idx_agg_v in agg_challenges.values():
            # index the future participant datasets involved in this challenge
            idx_agg_v.d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.pt.participant_dataset, ass_c_dict.get(idx_agg_v.challenge_id, [])),
                d_categories=idx_agg_v.ass_cat,
            )
            
            # newly added TestEvent involved in this challenge
            idx_agg_v.ta_catalog.merge_test_actions(filter(lambda te: te['challenge_id'] == idx_agg_v.challenge_id, valid_test_events))
            
            # And also index the future assessment datasets involved in this challenge
            idx_agg_v.d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.assessment_dataset, ass_c_dict.get(idx_agg_v.challenge_id, [])),
                d_categories=idx_agg_v.ass_cat,
            )
            
            # newly added MetricsEvent
            idx_agg_v.ta_catalog.merge_test_actions(filter(lambda me: me['challenge_id'] == idx_agg_v.challenge_id, valid_metrics_events))

        # Now it is time to process all the new or updated aggregation datasets
        valid_aggregation_tuples = []
        failed_min_agg = False
        for min_dataset in min_aggregation_datasets:
            the_id = min_dataset['_id']
            the_orig_id = None
            the_agg_metric_label = min_dataset.get("aggregation_metric_id")
            min_datalink = min_dataset.get("datalink")
            if not isinstance(min_datalink, dict):
                self.logger.critical(f"Minimal aggregation dataset {the_id} does not contain datalink!!!! Talk to the data providers")
                failed_min_agg = True
                continue
            
            the_vis_type = min_datalink.get("inline_data", {}).get("visualization", {}).get("type")
            if the_vis_type is None:
                self.logger.critical(f"Minimal aggregation dataset {the_id} does not contain a visualization type!!!! Talk to the data providers")
                failed_min_agg = True
                continue
            
            community_ids = [ community_id ]
            # Mapping challenges
            challenge_ids = []
            idx_challenges = []
            failed_ch_mapping = False
            
            # This is needed to be initialized here
            the_challenge_contacts = []
            workflow_tool_id = putative_workflow_tool_id
            workflow_metrics_id = None
            # This is used in the returned dataset
            the_rel_dataset_ids: "MutableSequence[RelDataset]" = []
            
            if len(min_dataset["challenge_ids"]) > 1:
                self.logger.warning(f"Minimal aggregation dataset {the_id} is associated to more than one challenge! Using only the first one due limitations of this software")
            idx_agg = None
            idat_ass = None
            for challenge_label in min_dataset["challenge_ids"]:
                idx_agg = agg_challenges.get(challenge_label)
                if idx_agg is None:
                    self.logger.critical(f"Unable to find challenge {challenge_label}, needed by minimal aggregation dataset {the_id}. Is this a typo? Fix it.")
                    failed_ch_mapping = True
                    break
                
                challenge_ids.append(idx_agg.challenge["_id"])
                idx_challenges.append(idx_agg)
                # Gathering the challenge contacts
                the_challenge_contacts.extend(idx_agg.challenge["challenge_contact_ids"])
                
                # Setting the appropriate workflow_metrics_id
                # and the related dataset ids
                wmi_was_set = False
                idat_ass = idx_agg.d_catalog.get("assessment")
                assert idat_ass is not None
                
                for metrics_category in idx_agg.challenge.get("metrics_categories",[]):
                    m_cat = metrics_category.get("category")
                    # This is how we deal with cases where more
                    #  than one aggregation metrics happens
                    if m_cat == "aggregation":
                        if not wmi_was_set:
                            workflow_metrics = []
                            for metric_decl in metrics_category.get("metrics", []):
                                metric_tool_id = metric_decl.get("tool_id")
                                mmi = self.migration_utils.fetchStagedEntry('Metrics', metric_decl["metrics_id"])
                                metrics_trio = self.migration_utils.getMetricsTrioFromMetric(mmi, idx_agg.d_catalog.community_prefix, metric_tool_id)
                                if the_agg_metric_label is not None:
                                    if metrics_trio.proposed_label != the_agg_metric_label:
                                        # Do not match
                                        continue
                                    workflow_metrics_ids = [ (metrics_trio, mmi) ]
                                    break
                                elif metric_tool_id is None or metric_tool_id == putative_workflow_tool_id:
                                    workflow_metrics.append((metrics_trio, mmi))

                            if len(workflow_metrics) > 1:
                                # Now looking for a best match
                                possible_trios = []
                                matched_trios = []
                                for metrics_trio, mmi in workflow_metrics:
                                    m_vis_type = mmi.get("representation_hints", {}).get("visualization")
                                    if m_vis_type is None:
                                        possible_trios.append(metrics_trio)
                                    elif m_vis_type == the_vis_type:
                                        matched_trios.append(metrics_trio)
                                if len(matched_trios) >= 1:
                                    if len(matched_trios) > 1:
                                        self.logger.warning(f"Guessed metrics {matched_trios[0].metrics_id} for {the_id}")
                                    workflow_metrics = [ (matched_trios[0], {}) ]
                                elif len(matched_trios) == 0:
                                    if len(possible_trios) >= 1:
                                        if len(possible_trios) > 1:
                                            self.logger.warning(f"Guessed metrics {possible_trios[0].metrics_id} for {the_id}")
                                        workflow_metrics = [ (possible_trios[0], {}) ]

                            if len(workflow_metrics) > 0:
                                metrics_trio = workflow_metrics[0][0]
                                workflow_metrics_id = metrics_trio.metrics_id
                                if metrics_trio.tool_id is not None and metrics_trio.tool_id != putative_workflow_tool_id:
                                    self.logger.warning(f"Proposed tool {putative_workflow_tool_id} does not match in the metrics declaration at the challenge. Using instead {metrics_trio.tool_id}")
                                    workflow_tool_id = metrics_trio.tool_id
                                wmi_was_set = True
                                        
                            
                        
                    
                if not wmi_was_set:
                    self.logger.critical(f"In challenge {idx_agg.challenge['_id']}, unable to set/guess the metrics id from workflow tool id {workflow_tool_id}")
                
                # Only the first challenge please
                break
            else:
                challenge_label = "(no challenge???)"
            
            if failed_ch_mapping:
                failed_min_agg = True
                continue
            
            if idx_agg is None:
                failed_min_agg = True
                continue
            
            assert idat_ass is not None
            
            # Dealing with the inline data, where 
            min_inline_data = min_datalink.get("inline_data")
            datalink = None
            metrics_str = "(unknown)"
            the_vis_optim = None
            if isinstance(min_inline_data, dict):
                vis = min_inline_data.get("visualization")
                series_type =  min_inline_data.get("series_type")
                manage_datalink = False
                if isinstance(vis, dict):
                    vis_type = vis.get("type")
                    
                    involved_metrics = []
                    
                    agg_postfix = idx_agg.challenge_label_and_sep.sep + idx_agg.challenge_label_and_sep.aggregation_sep
                    the_id_postfix = idx_agg.challenge_label_and_sep.sep
                    if vis_type == "2D-plot":
                        involved_metrics.append(vis['x_axis'])
                        involved_metrics.append(vis['y_axis'])
                        the_id_postfix += f"{vis['x_axis']}{idx_agg.challenge_label_and_sep.metrics_label_sep}{vis['y_axis']}"
                        metrics_str = f"{vis['x_axis']} - {vis['y_axis']}"
                        manage_datalink = True
                    elif vis_type == "bar-plot":
                        involved_metrics.append(vis['metric'])
                        the_id_postfix += vis['metric']
                        metrics_str = vis['metric']
                        manage_datalink = True
                    elif series_type == "aggregation-data-series":
                        involved_metrics.extend(vis['available_metrics'])
                        the_id_postfix += idx_agg.challenge_label_and_sep.metrics_label_sep.join(vis['available_metrics'])
                        metrics_str = ' - '.join(vis['available_metrics'])
                        manage_datalink = True
                        if vis_type != "box-plot":
                            self.logger.critical(f"Unimplemented aggregation for series type {series_type} and visualization type {vis_type} in minimal dataset {the_id}")
                    else:
                        self.logger.critical(f"Unimplemented aggregation for series type {series_type} and visualization type {vis_type} in minimal dataset {the_id}")
                    
                    if the_id.endswith(agg_postfix):
                        the_id += the_id_postfix
                    if not the_id.endswith(the_id_postfix):
                        the_id += agg_postfix + the_id_postfix
                    
                    # Initialize
                    the_vis_optim = vis.get("optimization")
                    
                    # Preparing the handling
                    if manage_datalink:
                        datalink = copy.copy(min_datalink)
                        inline_data = copy.deepcopy(min_inline_data)
                        datalink["inline_data"] = inline_data
                        available_metrics: "MutableSequence[str]" = []
                        if series_type == "aggregation-data-series":
                            inline_data["series_type"] = "aggregation-data-series"
                            inline_data.setdefault("visualization", {})["available_metrics"] = available_metrics
                        inline_data_labels: "MutableSequence[DataLabel]" = []
                        inline_data["labels"] = inline_data_labels
                        # Inline data labels by participant dataset id
                        idl_by_d_id: "MutableMapping[str, DataLabel]" = {}
                        challenge_participants: "MutableSequence[Union[BarData, ScatterData, SeriesData]]" = []
                        inline_data["challenge_participants"] = challenge_participants
                        cha_par_by_id: "MutableMapping[str, Union[BarData, ScatterData, SeriesData]]" = {}
                        ass_par_by_id: "MutableMapping[str, str]" = {}
                        
                        ita_m_events = idx_agg.ta_catalog.get("MetricsEvent")
                        assert ita_m_events is not None
                        
                        # Now to rebuild the metrics
                        for i_met, involved_metric in enumerate(involved_metrics):
                            # Select the metrics just "guessing"
                            mm_trio = idx_agg.match_metric_from_metrics_label(involved_metric, the_id)
                            assert mm_trio is not None
                            
                            met_datasets = list(idat_ass.datasets_from_metric(mm_trio.metrics_id))
                            if len(met_datasets) > 0:
                                available_metrics.append(mm_trio.proposed_label)
                            for met_dataset in met_datasets:
                                tar = ita_m_events.get_by_outgoing_dataset(met_dataset["_id"])
                                if tar is None:
                                    self.logger.error(f"Unindexed MetricsEvent TestAction for minimal dataset {the_id}")
                                    continue
                                
                                the_rel_dataset_ids.append({"dataset_id": cast("str", met_dataset["_id"])})
                                # Now, the participant datasets can be rescued
                                # to get or guess its label
                                inline_data_label = None
                                par_dataset_id: "Optional[str]" = None
                                par_label: "Optional[str]" = None
                                for par_dataset in tar.in_d:
                                    inline_data_label = idl_by_d_id.get(par_dataset["_id"])
                                    if isinstance(inline_data_label, dict):
                                        par_dataset_id = par_dataset["_id"]
                                        par_label = inline_data_label["label"]
                                        break
                                
                                # Bad luck, time to create a new entry
                                if par_dataset_id is None:
                                    inline_data_label , par_dataset_id = gen_inline_data_label(met_dataset, tar.in_d)
                                    if inline_data_label is None:
                                        self.logger.error(f"Unable to generate inline data label for {met_dataset['_id']}")
                                        
                                    # Now we should have the participant label
                                    assert inline_data_label is not None
                                    assert par_dataset_id is not None
                                    
                                    # Last, store it
                                    inline_data_labels.append(inline_data_label)
                                    idl_by_d_id[par_dataset_id] = inline_data_label
                                    par_label = inline_data_label["label"]
                                
                                assert par_label is not None
                                mini_entry = cha_par_by_id.get(par_dataset_id)
                                mini_entry_2d: "Optional[ScatterData]" = None
                                mini_entry_b: "Optional[BarData]" = None
                                mini_entry_s: "Optional[SeriesData]" = None
                                if mini_entry is None:
                                    if vis_type == "2D-plot":
                                        mini_entry_2d = {
                                            "tool_id": par_label,
                                        }
                                        mini_entry = mini_entry_2d
                                    elif vis_type == "bar-plot":
                                        mini_entry_b = {
                                            "tool_id": par_label,
                                        }
                                        mini_entry = mini_entry_b
                                    else:
                                        m_depends_on = met_dataset["depends_on"]
                                        d_on_metrics_id = m_depends_on["metrics_id"]
                                        d_on_tool_id = m_depends_on.get("tool_id")
                                        ass_trio = self.migration_utils.getMetricsTrioFromMetricsId(d_on_metrics_id, idat_ass.community_prefix, d_on_tool_id)
                                        mini_entry_s = {
                                            "label": par_label,
                                            "metric_id": ass_trio.proposed_label,
                                        }
                                        mini_entry = mini_entry_s
                                    
                                    challenge_participants.append(mini_entry)
                                    cha_par_by_id[par_dataset_id] = mini_entry
                                    ass_par_by_id[par_dataset_id] = met_dataset['_id']
                                elif i_met == 0:
                                    self.logger.error(f"Assessment datasets {met_dataset['_id']} and {ass_par_by_id[par_dataset_id]} (both needed by minimal {the_id}) has metrics {met_dataset['depends_on']['metrics_id']}, but one is mislabelled (wrong?). Fix the wrong one")
                                    failed_min_agg = True
                                elif vis_type == "2D-plot":
                                    mini_entry_2d = cast("ScatterData", mini_entry)
                                elif vis_type == "bar-plot":
                                    mini_entry_b = cast("BarData", mini_entry)
                                elif vis_type == "box-plot":
                                    mini_entry_s = cast("SeriesData", mini_entry)
                                else:
                                    self.logger.critical(f"Unimplemented aggregation for visualization type {vis_type} in minimal dataset {the_id}")
                                    failed_min_agg = True
                                
                                fetched_ass_inline_data = idat_ass.get_payload(met_dataset["_id"])

                                if fetched_ass_inline_data is None:
                                    self.logger.critical(f"Assessment dataset {met_dataset['_id']} data contents could not be obtained")
                                    failed_min_agg = True
                                
                                elif not failed_min_agg:
                                    ass_inline_data = fetched_ass_inline_data.data
                                    
                                    if mini_entry_2d is not None:
                                        mini_entry_values: "ScatterData"
                                        if i_met == 0:
                                            mini_entry_values = {
                                                "metric_x": ass_inline_data["value"],
                                                "stderr_x": ass_inline_data.get("error", 0),
                                            }
                                        else:
                                            mini_entry_values = {
                                                "metric_y": ass_inline_data["value"],
                                                "stderr_y": ass_inline_data.get("error", 0),
                                            }
                                        
                                        mini_entry_2d.update(mini_entry_values)
                                    elif mini_entry_b:
                                        mini_entry_b.update({
                                            "metric_value": ass_inline_data["value"],
                                            "stderr": ass_inline_data.get("error", 0),
                                        })
                                    elif mini_entry_s:
                                        mini_entry_s.update({
                                            "values": ass_inline_data["values"],
                                        })


                
            
            if datalink is None:
                # Unaltered datalink (as we do not know how to deal with it)
                datalink = min_datalink
            
            # Mapping the id to an existing entry, and update challenges list
            the_visibility = None
            the_name = None
            the_description = None
            the_dataset_contact_ids = None
            the_version = None
            the_metadata = None
            the_dates = {
                "creation": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                "modification": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
            }
            
            idx_challenge = None
            for idx_challenge in idx_challenges:
                orig_datasets = idx_challenge.d_catalog.get_dataset(the_id)
                if len(orig_datasets) > 0:
                    if len(orig_datasets) > 1:
                        self.logger.warning(f"Found more than one dataset with id {the_id}. Problems in the horizon???")
                    
                    for orig_dataset in orig_datasets:
                        if orig_dataset["type"] != "aggregation":
                            # Discard incompatible entries
                            continue
                        
                        the_id = orig_dataset["_id"]
                        the_orig_id = orig_dataset.get("orig_id")
                        the_name = orig_dataset.get("name")
                        the_description = orig_dataset.get("description")
                        the_version = orig_dataset.get("version")
                        the_visibility = orig_dataset.get("visibility")
                        the_dataset_contact_ids = orig_dataset.get("dataset_contact_ids")
                        the_local_creation = orig_dataset.get("dates",{}).get("creation")
                        if the_local_creation is not None:
                            the_dates["creation"] = the_local_creation
                        
                        the_metadata = orig_dataset.get("_metadata")
                        
                        # Is there an inherited visualization optimization?
                        if the_vis_optim is None:
                            orig_payloads = idx_challenge.d_catalog.get_dataset_payload(the_id)
                            if len(orig_payloads) > 0:
                                if len(orig_payloads) > 1:
                                    self.logger.warning(f"Found more than one dataset payload with id {the_id}. Problems in the horizon???")
                                inline_data = orig_payloads[0].data
                                the_vis_optim = inline_data.get("visualization", {}).get("optimization")
                        
                        # Widening the list (maybe it is wrong?????)
                        orig_ch_set = set(orig_dataset["challenge_ids"])
                        orig_co_set = set(orig_dataset["community_ids"])
                        if not orig_ch_set.issubset(challenge_ids):
                            self.logger.warning(f"For aggregation dataset {the_id} ({the_orig_id}) overlapping challenges between the stored entry and the new one")
                        challenge_ids = list(set(orig_ch_set.union(challenge_ids)))
                        community_ids = list(set(orig_co_set.union(community_ids)))
                        break
                    
                    break
            
            if the_vis_optim is not None and datalink != min_datalink:
                datalink["inline_data"]["visualization"]["optimization"] = the_vis_optim
            
            if the_visibility is None:
                the_visibility = "public"
            
            if the_version is None:
                the_version = "1"
            
            if the_name is None:
                the_name = f"Summary dataset for challenge: {challenge_label}. Metrics: {metrics_str}"
            
            if the_description is None:
                the_description = f"Summary dataset that aggregates all results from participants in challenge: {challenge_label}. Metrics: {metrics_str}"
            
            valid_data = {
                "_id": the_id,
                "_schema": self.schemaMappings["Dataset"],
                "type": "aggregation",
                "community_ids": community_ids,
                "challenge_ids": challenge_ids,
                "datalink": datalink,
                "name": the_name,
                "description": the_description,
                "version": the_version,
                "visibility": the_visibility,
                "dataset_contact_ids": the_challenge_contacts,
                "dates": the_dates,
                "depends_on": {
                    "tool_id": workflow_tool_id,
                    "metrics_id": workflow_metrics_id,
                    "rel_dataset_ids": the_rel_dataset_ids,
                }
            }
            if the_orig_id is not None:
                valid_data["orig_id"] = the_orig_id
            
            if the_metadata is not None:
                valid_data["_metadata"] = the_metadata
            
            valid_aggregation_tuples.append(
                AggregationTuple(
                    aggregation_dataset=valid_data,
                    idx_challenge=idx_challenge,
                    # TODO
                    #at_l=at_l,
                    at_l=[]
                )
            )
        
        if failed_min_agg:
            self.logger.critical("The generation of some aggregation dataset failed.")
            sys.exit(6)
        
        return valid_aggregation_tuples

    def build_aggregation_events(
        self,
        valid_aggregation_tuples: "Sequence[AggregationTuple]",
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        workflow_tool_id: "str"
    ) -> "Sequence[Mapping[str, Any]]":

        self.logger.info(
            "\n\t==================================\n\t7. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of test events
        aggregation_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for agt in valid_aggregation_tuples:
            dataset = agt.aggregation_dataset
            indexed_challenge = agt.idx_challenge
            
            assert indexed_challenge is not None
            
            orig_id = dataset.get("orig_id",dataset["_id"])
            event_id = orig_id + "_Event"
            
            ita = indexed_challenge.ta_catalog.get("AggregationEvent")
            if ita is not None:
                ta_event = ita.get_by_original_id(event_id)
            else:
                # Corner case of newly created community
                ta_event = None
            
            if ta_event is None:
                event = {
                    "_id": event_id,
                    "_schema": self.schemaMappings["TestAction"],
                    "action_type": "AggregationEvent",
                }
            else:
                event = {
                    "_id": ta_event["_id"],
                    "orig_id": event_id,
                    "_schema": self.schemaMappings["TestAction"],
                    "action_type": "AggregationEvent",
                }
                staged_dates = ta_event.get("dates")
                if staged_dates is not None:
                    event["dates"] = copy.copy(staged_dates)
                staged_metadata = ta_event.get("_metadata")
                if staged_metadata is not None:
                    event["_metadata"] = staged_metadata

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
            modtime = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
            event.setdefault("dates", {"creation": modtime})["reception"] = modtime

            # add challenge managers as aggregation dataset contacts ids
            data_contacts = []
            for challenge in challenges_graphql:
                if challenge["_id"] in dataset["challenge_ids"]:
                    data_contacts.extend(
                        challenge["challenge_contact_ids"])
            event["test_contact_ids"] = data_contacts

            aggregation_events.append(event)

        return aggregation_events
