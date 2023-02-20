#!/usr/bin/env python

import copy
import inspect
import logging
import sys
import os
import datetime
import json

from .benchmarking_dataset import BenchmarkingDataset
from ..utils.catalogs import (
    DatasetsCatalog,
    gen_inline_data_label,
    match_metric_from_label,
    TestActionsCatalog,
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

class IndexedAggregation(NamedTuple):
    challenge: "Mapping[str, Any]"
    challenge_id: "str"
    challenge_label: "str"
    d_catalog: "DatasetsCatalog"
    ta_catalog: "TestActionsCatalog"
    # Assessment metrics categories catalog
    ass_cat: "Sequence[Mapping[str, Any]]"

class AggregationTuple(NamedTuple):
    aggregation_dataset: "Mapping[str, Any]"
    idx_challenge: "Optional[IndexedAggregation]"
    at_l: "Sequence[AssessmentTuple]"


class Aggregation():

    def __init__(self, schemaMappings, migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.level2_min_validator = migration_utils.level2_min_validator
    
    def check_and_index_challenges(
        self,
        community_acronym: "str",
        challenges_agg_graphql: "Sequence[Mapping[str, Any]]",
        metrics_agg_graphql: "Sequence[Mapping[str, Any]]",
    ) -> "Sequence[AggregationTuple]":
    
        self.logger.info(
            "\n\t==================================\n\t5. Validating stored challenges\n\t==================================\n")
        
        # First, let's analyze all existing aggregation datasets
        # nested in the challenges
        agg_challenges = { }
        should_exit_ch = False
        for agg_ch in challenges_agg_graphql:
            # Garrayo's school label
            challenge_id = agg_ch["_id"]
            
            agg_metadata = agg_ch.get("_metadata")
            if agg_metadata is None:
                agg_metadata = {}
            challenge_label = agg_metadata.get("level_2:challenge_id")
            if challenge_label is None:
                challenge_label = agg_ch.get("acronym")
            
            if challenge_label is None:
                challenge_label = agg_ch.get("orig_id")
                # Very old school label
                if challenge_label is not None:
                    if challenge_label.startswith(community_acronym + ':'):
                        challenge_label = challenge_label[len(community_acronym) + 1]
                else:
                    challenge_label = challenge_id
            
            self.logger.info(f"Validating challenge {challenge_id} ({challenge_label})")
            
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
            
            # Then the assessment datasets
            d_catalog.merge_datasets(
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
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("aggregation_datasets", []),
                d_categories=agg_cat,
            )
            
            # Complement this with the aggregation_test_actions
            ta_catalog.merge_test_actions(agg_ch.get("aggregation_test_actions", []))
            
            # Let's rebuild the aggregation datasets, from the minimal information
            idat_agg = d_catalog.get("aggregation")
            assert idat_agg is not None
            
            idat_ass = d_catalog.get("assessment")
            assert idat_ass is not None
            idat_part = d_catalog.get("participant")
            assert idat_ass is not None
            
            ita_m_events = ta_catalog.get("MetricsEvent")
            assert ita_m_events is not None
            
            failed_agg_dataset = False
            for raw_dataset in idat_agg.datasets():
                agg_dataset_id = raw_dataset["_id"]
                metrics_trios = idat_agg.get_metrics_trio(agg_dataset_id)
                if metrics_trios:
                    # Make an almost shallow copy of the entry
                    # removing what it is going to be populated from ground
                    r_dataset = copy.copy(raw_dataset)
                    
                    # depends_on is going to be rebuilt
                    d_on = copy.deepcopy(raw_dataset["depends_on"])
                    rel_dataset_ids = []
                    d_on["rel_dataset_ids"] = rel_dataset_ids
                    r_dataset["depends_on"] = d_on
                    
                    # datalink contents are going to be rebuilt, also
                    d_link = copy.deepcopy(raw_dataset["datalink"])
                    r_dataset["datalink"] = d_link
                    inline_data = d_link.get("inline_data")
                    
                    if ("_metadata" in r_dataset) and r_dataset["_metadata"] is None:
                        del r_dataset["_metadata"]
                    
                    # Challenge ids of this dataset (to check other dataset validness)
                    ch_ids_set = set(r_dataset["challenge_ids"])
                    if isinstance(inline_data, dict):
                        # Entry of each challenge participant, by participant label
                        cha_par_by_id = {}
                        # To provide meaningful error messages
                        ass_par_by_id = {}
                        challenge_participants = []
                        inline_data["challenge_participants"] = challenge_participants
                        
                        # Visualization type determines later the labels
                        # to use in each entry of challenge_participants
                        vis_type = inline_data.get("visualization",{}).get("type")
                        
                        # This set is used to detect mismatches between
                        # registered and gathered assessment datasets
                        rel_ids_set = set(map(lambda r: r["dataset_id"], filter(lambda r: r.get("role", "dependency") == "dependency", raw_dataset["depends_on"]["rel_dataset_ids"])))
                        
                        # Processing and validating already registered labels
                        potential_inline_data_labels = inline_data.get("labels", [])
                        inline_data_labels = []
                        inline_data["labels"] = inline_data_labels
                        
                        # Inline data labels by participant dataset id
                        idl_by_d_id = {}
                        
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
                        
                        # Time to fetch
                        rebuild_agg = False
                        regen_rel_ids_set = set()
                        # Iterating over the different metrics
                        for i_trio, metrics_trio in enumerate(metrics_trios):
                            # To gather the assessment datasets for each metric
                            met_datasets = list(idat_ass.datasets_from_metric(metrics_trio.metrics_id))
                            
                            met_set = set(map(lambda m: m["_id"], met_datasets))
                            if not(rel_ids_set > met_set):
                                new_set = met_set - rel_ids_set
                                self.logger.error(f"Aggregation dataset {agg_dataset_id} should also depend on {len(new_set)} datasets: {', '.join(new_set)}")
                                rebuild_agg = True
                            regen_rel_ids_set.update(met_set)
                            rel_dataset_ids.extend(map(lambda m: {"dataset_id": m["_id"]}, met_datasets))
                            
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
                                par_dataset_id = None
                                par_label = None
                                for par_dataset in tar.in_d:
                                    inline_data_label = idl_by_d_id.get(par_dataset["_id"])
                                    if isinstance(inline_data_label, dict):
                                        par_dataset_id = par_dataset["_id"]
                                        par_label = inline_data_label["label"]
                                        break
                                
                                # Bad luck, time to create a new entry
                                if inline_data_label is None:
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
                                
                                mini_entry = cha_par_by_id.get(par_dataset_id)
                                if mini_entry is None:
                                    mini_entry = {
                                        "tool_id": par_label,
                                    }
                                    challenge_participants.append(mini_entry)
                                    cha_par_by_id[par_dataset_id] = mini_entry
                                    ass_par_by_id[par_dataset_id] = met_dataset['_id']
                                elif i_trio == 0:
                                    self.logger.error(f"Assessment datasets {met_dataset['_id']} and {ass_par_by_id[par_dataset_id]} (both needed by {agg_dataset_id}) has metrics {met_dataset['depends_on']['metrics_id']}, but one is mislabelled (wrong?). Fix the wrong one")
                                    rebuild_agg = True
                                
                                ass_inline_data = met_dataset["datalink"]["inline_data"]
                                if vis_type == "2D-plot":
                                    if i_trio == 0:
                                        value_label = "metric_x"
                                        stderr_label = "stderr_x"
                                    else:
                                        value_label = "metric_y"
                                        stderr_label = "stderr_y"
                                    
                                    mini_entry.update({
                                        value_label: ass_inline_data["value"],
                                        stderr_label: ass_inline_data.get("error", 0),
                                    })
                                elif vis_type == "bar-plot":
                                    mini_entry.update({
                                        "metric_value": ass_inline_data["value"],
                                        "stderr": ass_inline_data.get("error", 0),
                                    })
                                else:
                                    self.logger.critical(f"Unimplemented aggregation for visualization type {vis_type} in dataset {agg_dataset_id}")
                        
                        if not rebuild_agg:
                            raw_challenge_participants = raw_dataset["datalink"]["inline_data"]["challenge_participants"]
                            if len(challenge_participants) != len(raw_challenge_participants):
                                self.logger.error(f"Mismatch in {agg_dataset_id} length of challenge_participants\n\n{json.dumps(challenge_participants, indent=4, sort_keys=True)}\n\n{json.dumps(raw_challenge_participants, indent=4, sort_keys=True)}")
                                rebuild_agg = True
                            else:
                                s_new_challenge_participants = sorted(challenge_participants, key=lambda cp: cp["tool_id"])
                                s_raw_challenge_participants = sorted(raw_challenge_participants, key=lambda cp: cp["tool_id"])
                                
                                if s_new_challenge_participants != s_raw_challenge_participants:
                                    rebuild_agg = True
                                    for s_new , s_raw in zip(s_new_challenge_participants, s_raw_challenge_participants):
                                        if s_new != s_raw:
                                            self.logger.error(f"Mismatch in {agg_dataset_id} challenge_participants\n\n{json.dumps(s_new, indent=4, sort_keys=True)}\n\n{json.dumps(s_raw, indent=4, sort_keys=True)}")
                        
                        # Time to compare
                        if rebuild_agg:
                            self.logger.error(f"Aggregation dataset {agg_dataset_id} from challenges {', '.join(raw_dataset['challenge_ids'])} has to be rebuilt: elegible assessments {len(regen_rel_ids_set)} vs {len(rel_ids_set)} used in the aggregation dataset")
                            set_diff = rel_ids_set - regen_rel_ids_set
                            if len(set_diff) > 0:
                                self.logger.error(f"Also, {len(set_diff)} datasets do not appear in proposed rebuilt entry: {', '.join(set_diff)}")
                            self.logger.error(f"Proposed rebuilt entry {agg_dataset_id} (keep an eye in previous errors, it could be incomplete):\n" + json.dumps(r_dataset, indent=4))
                            failed_agg_dataset = True
            
            if failed_agg_dataset:
                self.logger.critical("As some aggregation datasets seem corrupted, fix them to continue")
                sys.exit(5)
            
            # Last, but not the least important
            agg_challenges[challenge_id] = agg_challenges[challenge_label] = IndexedAggregation(
                challenge=agg_ch,
                challenge_id=challenge_id,
                challenge_label=challenge_label,
                d_catalog=d_catalog,
                ta_catalog=ta_catalog,
                ass_cat=ass_cat,
            )
        
        if should_exit_ch:
            self.logger.critical("Some challenges have collisions at their label level. Please ask the team to fix the mess")
            sys.exit(4)
        
        return agg_challenges
    
    def build_aggregation_datasets(
        self,
        community_id: "str",
        min_aggregation_datasets: "Sequence[Mapping[str, Any]]",
        agg_challenges: "Mapping[str, IndexedAggregation]",
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        valid_test_events: "Sequence[Tuple[str, Any]]",
        valid_metrics_events: "Sequence[Tuple[str, Any]]",
        workflow_tool_id: "str",
    ) -> "Sequence[AggregationTuple]":
        
        self.logger.info(
            "\n\t==================================\n\t6. Validating stored aggregation datasets\n\t==================================\n")
        
        # Grouping future assessment dataset tuples by challenge
        ass_c_dict = {}
        for ass_t in valid_assessment_tuples:
            ass_d = ass_t.assessment_dataset
            for challenge_id in ass_d.get("challenge_ids", []):
                ass_c_dict.setdefault(challenge_id, []).append(ass_t)
        
        for idx_agg in agg_challenges.values():
            # Now index the future participant datasets involved in this challenge
            idx_agg.d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.pt.participant_dataset, ass_c_dict.get(idx_agg.challenge_id, [])),
                d_categories=idx_agg.ass_cat,
            )
            
            # newly added TestEvent involved in this challenge
            idx_agg.ta_catalog.merge_test_actions(filter(lambda te: te['challenge_id'] == idx_agg.challenge_id, valid_test_events))
            
            # And also index the future assessment datasets involved in this challenge
            idx_agg.d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.assessment_dataset, ass_c_dict.get(idx_agg.challenge_id, [])),
                d_categories=idx_agg.ass_cat,
            )
            
            # newly added MetricsEvent
            idx_agg.ta_catalog.merge_test_actions(filter(lambda me: me['challenge_id'] == idx_agg.challenge_id, valid_metrics_events))

        # Now it is time to process all the new or updated aggregation datasets
        valid_aggregation_tuples = []
        failed_min_agg = False
        for min_dataset in min_aggregation_datasets:
            the_id = min_dataset['_id']
            the_orig_id = None
            min_datalink = min_dataset.get("datalink")
            if not isinstance(min_datalink, dict):
                self.logger.critical(f"Minimal aggregation dataset {the_id} does not contain datalink!!!! Talk to the data providers")
                failed_min_agg = True
                continue
            
            community_ids = [ community_id ]
            # Mapping challenges
            challenge_ids = []
            idx_challenges = []
            failed_ch_mapping = False
            the_challenge_contacts = []
            workflow_metrics_id = None
            # This is used in the returned dataset
            the_rel_dataset_ids = []
            # This is used to later compute the contents
            met_dataset_groups = []
            
            idx_agg = None
            idat_ass = None
            ita_m_events = None
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
                    if m_cat == "aggregation":
                        if not wmi_was_set:
                            for metric_decl in metrics_category.get("metrics", []):
                                if metric_decl["tool_id"] == workflow_tool_id:
                                    workflow_metrics_id = metric_decl.get("metrics_id")
                                    wmi_was_set = True
                                    break
                    elif m_cat == "assessment":
                        # Now time to milk the structures
                        for metric_decl in metrics_category.get("metrics", []):
                            met_datasets = list(idat_ass.datasets_from_metric(metric_decl["metrics_id"]))
                            met_dataset_groups.append(met_datasets)
                            the_rel_dataset_ids.extend(map(lambda m: {"dataset_id": m["_id"]}, met_datasets))
                        
                    
                if not wmi_was_set:
                    self.logger.critical(f"In challenge {idx_agg.challenge['_id']}, unable to set the metrics id from workflow tool id {workflow_tool_id}")
                
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
            
            # Dealing with the inline data, where 
            min_inline_data = min_datalink.get("inline_data")
            datalink = None
            metrics_str = "(unknown)"
            the_vis_optim = None
            if isinstance(min_inline_data, dict):
                vis = min_inline_data.get("visualization")
                manage_datalink = False
                if isinstance(vis, dict):
                    vis_type = vis.get("type")
                    
                    if vis_type == "2D-plot":
                        the_id += f"_{vis['x_axis']}+{vis['y_axis']}"
                        metrics_str = f"{vis['x_axis']} - {vis['y_axis']}"
                        manage_datalink = True
                    elif vis_type == "box-plot":
                        the_id += f"_{vis['metric']}"
                        metrics_str = vis['metric']
                        manage_datalink = True
                    else:
                        self.logger.critical(f"Unimplemented aggregation for visualization type {vis_type} in minimal dataset {the_id}")
                    
                    # Initialize
                    the_vis_optim = vis.get("optimization")
                    
                    # Preparing the handling
                    if manage_datalink:
                        datalink = copy.copy(min_datalink)
                        inline_data = copy.deepcopy(min_inline_data)
                        datalink["inline_data"] = inline_data
                        inline_data_labels = []
                        inline_data["labels"] = inline_data_labels
                        # Inline data labels by participant dataset id
                        idl_by_d_id = {}
                        challenge_participants = []
                        inline_data["challenge_participants"] = challenge_participants
                        cha_par_by_id = {}
                        ass_par_by_id = {}
                        
                        ita_m_events = idx_agg.ta_catalog.get("MetricsEvent")
                        assert ita_m_events is not None
                        
                        # Now to rebuild the metrics
                        for i_met, met_datasets in enumerate(met_dataset_groups):
                            for met_dataset in met_datasets:
                                tar = ita_m_events.get_by_outgoing_dataset(met_dataset["_id"])
                                if tar is None:
                                    self.logger.error(f"Unindexed MetricsEvent TestAction for minimal dataset {the_id}")
                                    continue

                                # Now, the participant datasets can be rescued
                                # to get or guess its label
                                inline_data_label = None
                                par_dataset_id = None
                                par_label = None
                                for par_dataset in tar.in_d:
                                    inline_data_label = idl_by_d_id.get(par_dataset["_id"])
                                    if isinstance(inline_data_label, dict):
                                        par_dataset_id = par_dataset["_id"]
                                        par_label = inline_data_label["label"]
                                        break
                                
                                # Bad luck, time to create a new entry
                                if inline_data_label is None:
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
                                
                                
                                mini_entry = cha_par_by_id.get(par_dataset_id)
                                if mini_entry is None:
                                    mini_entry = {
                                        "tool_id": par_label,
                                    }
                                    challenge_participants.append(mini_entry)
                                    cha_par_by_id[par_dataset_id] = mini_entry
                                    ass_par_by_id[par_dataset_id] = met_dataset['_id']
                                elif i_met == 0:
                                    self.logger.error(f"Assessment datasets {met_dataset['_id']} and {ass_par_by_id[par_dataset_id]} (both needed by minimal {the_id}) has metrics {met_dataset['depends_on']['metrics_id']}, but one is mislabelled (wrong?). Fix the wrong one")
                                    failed_min_agg = True
                                
                                ass_inline_data = met_dataset["datalink"]["inline_data"]
                                if vis_type == "2D-plot":
                                    if i_met == 0:
                                        value_label = "metric_x"
                                        stderr_label = "stderr_x"
                                    else:
                                        value_label = "metric_y"
                                        stderr_label = "stderr_y"
                                    
                                    mini_entry.update({
                                        value_label: ass_inline_data["value"],
                                        stderr_label: ass_inline_data.get("error", 0),
                                    })
                                elif vis_type == "bar-plot":
                                    mini_entry.update({
                                        "metric_value": ass_inline_data["value"],
                                        "stderr": ass_inline_data.get("error", 0),
                                    })
                                else:
                                    self.logger.critical(f"Unimplemented aggregation for visualization type {vis_type} in minimal dataset {the_id}")
                                    failed_min_agg = True


                
            
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
                            the_vis_optim = orig_dataset.get("datalink", {}).get("inline_data", {}).get("visualization", {}).get("optimization")
                        
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
