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

import copy
import inspect
import logging
import sys
import os
from datetime import datetime, timezone
import json
import re

from ..utils.catalogs import (
    gen_challenge_assessment_metrics_dict,
    match_metric_from_label,
)

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        MutableMapping,
        MutableSequence,
        Sequence,
        Tuple,
    )
    from .participant import ParticipantTuple
    from ..utils.catalogs import IndexedChallenge
    from ..utils.migration_utils import BenchmarkingEventPrefixEtAl

from ..utils.migration_utils import OpenEBenchUtils
from ..schemas import (
    SERIES_METRIC_SCHEMA_ID,
    SINGLE_METRIC_SCHEMA_ID,
)

class AssessmentTuple(NamedTuple):
    assessment_dataset: "Mapping[str, Any]"
    pt: "ParticipantTuple"

class AssessmentBuilder():

    def __init__(self, schemaMappings: "Mapping[str, str]", migration_utils: "OpenEBenchUtils"):
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.migration_utils = migration_utils

    def build_assessment_datasets(
        self,
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        metrics_graphql: "Sequence[Mapping[str, Any]]",
        staged_assessment_datasets: "Sequence[Mapping[str, Any]]",
        min_assessment_datasets: "Sequence[Mapping[str, Any]]",
        data_visibility: "str",
        valid_participant_tuples: "Sequence[ParticipantTuple]",
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        community_prefix: "str",
        do_fix_orig_ids: "bool",
    ) -> "Sequence[AssessmentTuple]":
        
        valid_participants: "MutableMapping[str, MutableSequence[ParticipantTuple]]" = {}
        for valid_pvc in valid_participant_tuples:
            valid_participants.setdefault(valid_pvc.p_config.participant_label,[]).append(valid_pvc)
            
        self.logger.info(
            "\n\t==================================\n\t3. Processing assessment datasets\n\t==================================\n")
        
        stagedMap = dict()
        for staged_assessment_dataset in staged_assessment_datasets:
            stagedMap[staged_assessment_dataset['orig_id']] = staged_assessment_dataset
        
        # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.
        oeb_challenges = {}
        for challenge in challenges_graphql:
            oeb_challenges[
                OpenEBenchUtils.get_challenge_label_from_challenge(
                    challenge,
                    bench_event_prefix_et_al,
                    community_prefix
                ).label
            ] = challenge

        valid_assessment_tuples = []
        should_end = []
        for min_dataset in min_assessment_datasets:
            assessment_participant_label = min_dataset.get("participant_id")
            pvcs = valid_participants.get(assessment_participant_label) if assessment_participant_label is not None else None
            # If not found, next!!!!!!
            if pvcs is None:
                self.logger.warning(f"Assessment dataset {min_dataset['_id']} was not processed because participant {assessment_participant_label} was not declared, skipping to next assessment element...")
                continue
            
            # replace dataset related challenges with oeb challenge ids
            execution_challenges = []
            execution_challenge_ids = []
            try:
                the_challenge = oeb_challenges[min_dataset["challenge_id"]]
                execution_challenges.append(the_challenge)
                execution_challenge_ids.append(the_challenge["_id"])
                cam_d = gen_challenge_assessment_metrics_dict(the_challenge)
            except:
                self.logger.warning("No challenges associated to " +
                             min_dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.warning(
                    min_dataset["_id"] + " not processed, skipping to next assessment element...")
                continue
                # sys.exit()

            if do_fix_orig_ids:
                min_id = self.migration_utils.gen_assessment_original_id_from_min_dataset(
                    min_dataset,
                    community_prefix,
                    bench_event_prefix_et_al,
                    challenge_ids=execution_challenge_ids,
                )
            else:
                min_id = min_dataset["_id"]
            
            # Matching the correct challenge pair
            min_challenge_id = min_dataset["challenge_id"]
            for pvc in pvcs:
                got_challenge = False
                for challenge_pair in pvc.challenge_pairs:
                    if challenge_pair.label == min_challenge_id:
                        got_challenge = True
                        break
                if got_challenge:
                    break
            else:
                self.logger.warning(f"Assessment dataset {min_id} was not processed because participant {assessment_participant_label} with challenge {min_challenge_id} was not matched, skipping to next assessment element...")
                continue
            
            min_d_metrics = min_dataset["metrics"]
            metrics_label = min_d_metrics["metric_id"]
            participant_label = pvc.p_config.participant_label
            valid_participant_data = pvc.participant_dataset
            challenge_pairs = pvc.challenge_pairs
            
            tool_id = valid_participant_data["depends_on"]["tool_id"]
            community_ids = valid_participant_data["community_ids"]
            
            self.logger.info('Building object "' +
                             str(min_id) + '"...')
            # initialize new dataset object
            stagedEntry = stagedMap.get(min_id)
            valid_data: "MutableMapping[str, Any]"
            if stagedEntry is None:
                valid_data = {
                    "_id": min_id,
                    "type": "assessment"
                }
            else:
                valid_data = {
                    "_id": stagedEntry["_id"],
                    "type": "assessment",
                    "orig_id": min_id,
                    "dates": stagedEntry["dates"]
                }
            

            # add dataset visibility
            valid_data["visibility"] = data_visibility

            # add name and description, if workflow did not provide them
            dataset_name = valid_data.get("name")
            if dataset_name is None:
                dataset_name = "Metric '" + metrics_label + \
                    "' in challenge '" + min_dataset["challenge_id"] + "' applied to participant '" + min_dataset["participant_id"] + "'"
            valid_data["name"] = dataset_name
            
            dataset_description = min_dataset.get("description")
            if dataset_description is None:
                dataset_description = "Assessment dataset of applying metric '" + \
                    metrics_label + "' in challenge '" + min_dataset["challenge_id"] + "' to '"\
                     + min_dataset["participant_id"] + "' participant"
            valid_data["description"] = dataset_description

            challenge_labels_set = set(map(lambda cp: cp.label, challenge_pairs))
            if min_dataset["challenge_id"] not in challenge_labels_set:
                self.logger.warning("No challenges associated to " +
                             min_dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.warning(
                    min_id + " not processed, skipping to next assessment element...")
                continue
            
            valid_data["challenge_ids"] = execution_challenge_ids

            # select metrics_reference datasets used in the challenges
            rel_oeb_datasets = set()
            for challenge in execution_challenges:
                for ref_data in challenge["datasets"]:
                    rel_oeb_datasets.add(ref_data["_id"])

            # add the participant data that corresponds to this assessment
            rel_oeb_datasets.add(valid_participant_data["_id"])

            # add data registration dates
            
            modtime = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            valid_data.setdefault("dates", {"creation": modtime})["modification"] = modtime

            # add assessment metrics values, as inline data
            if "value" in min_d_metrics:
                metric_value = min_d_metrics["value"]
                error_value = min_d_metrics.get("stderr")
                
                inline_data = {
                    "value": metric_value,
                }
                if error_value is not None:
                    inline_data["error"] = error_value
                
                datalink_payload = {
                    "schema_url": SINGLE_METRIC_SCHEMA_ID,
                    "inline_data": inline_data,
                }
                        
            elif "values" in min_d_metrics:
                datalink_payload = {
                    "schema_url": SERIES_METRIC_SCHEMA_ID,
                    "inline_data": {
                        "values": min_d_metrics["values"],
                    }
                }
            else:
                self.logger.critical("FIXME: unexpected assessment metric (talk to the developers)")

            valid_data["datalink"] = datalink_payload
            
            # Breadcrumbs about the participant label and metrics label to ease the discovery
            new_metadata = {
                "level_2:participant_id": participant_label,
                "level_2:metric_id": metrics_label,
            }
            if stagedEntry is not None:
                staged_metadata = stagedEntry.get("_metadata")
                if isinstance(staged_metadata, dict):
                    updated_metadata = copy.copy(staged_metadata)
                    updated_metadata.update(new_metadata)
                    new_metadata = updated_metadata
            valid_data["_metadata"] = new_metadata

            # add Benchmarking Data Model Schema Location
            valid_data["_schema"] = self.schemaMappings["Dataset"]

            # add OEB id for the community
            valid_data["community_ids"] = community_ids

            # add dataset dependencies: metric id, tool and reference datasets
            list_oeb_datasets = []
            for dataset_id in rel_oeb_datasets:
                list_oeb_datasets.append({
                    "dataset_id": dataset_id
                })
            
            # Select the metrics just "guessing"
            matched_metric = match_metric_from_label(
                logger=self.logger,
                metrics_graphql=metrics_graphql,
                community_prefix=community_prefix,
                metrics_label=metrics_label,
                challenge_id=the_challenge['_id'],
                challenge_acronym=the_challenge['acronym'],
                challenge_assessment_metrics_d=cam_d,
                dataset_id=min_id,
            )
            if matched_metric is None:
                should_end.append((the_challenge['_id'], the_challenge['acronym']))
                continue
            
            # Declaring the assessment dependency
            valid_data["depends_on"] = {
                "rel_dataset_ids": list_oeb_datasets,
                "metrics_id": matched_metric.metrics_id
            }
            # There could be some corner case where no tool_id was declared
            # when the assessment metrics categories were set
            if matched_metric.tool_id is not None:
                valid_data["depends_on"]["tool_id"] = matched_metric.tool_id

            # add data version
            valid_data["version"] = pvc.p_config.data_version
            
            # add dataset contacts ids, based on already processed data
            valid_data["dataset_contact_ids"] = valid_participant_data["dataset_contact_ids"]

            self.logger.info(f'Processed "{str(min_id)}" (was {min_dataset["_id"]}) ...')

            # It is really a check through comparison of what was generated
            fixed_entry = self.migration_utils.fix_assessment_original_id(
                valid_data,
                community_prefix,
                bench_event_prefix_et_al,
                participant_label,
                metrics_label,
                do_fix_orig_ids,
            )
            
            valid_assessment_tuples.append(
                AssessmentTuple(
                    assessment_dataset=valid_data if fixed_entry is None else fixed_entry,
                    pt=pvc,
                )
            )

        if len(should_end) > 0:
            self.logger.critical(f"Several {len(should_end)} issues found related to metrics associated to the challenges {should_end}. Please fix all of them")
            sys.exit()
        
        return valid_assessment_tuples

    def build_metrics_events(self, valid_assessment_tuples: "Sequence[AssessmentTuple]", indexed_challenges: "Mapping[str, IndexedChallenge]") -> "Sequence[Mapping[str, Any]]":

        self.logger.info(
            "\n\t==================================\n\t4. Generating Metrics Events\n\t==================================\n")

        # initialize the array of test events
        metrics_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for at in valid_assessment_tuples:
            dataset = at.assessment_dataset
            pvc  = at.pt
            valid_participant_data = pvc.participant_dataset
            tool_id = dataset["depends_on"]["tool_id"]
        
            event_id = OpenEBenchUtils.gen_metrics_event_original_id(dataset)
            
            indexed_challenge = indexed_challenges[dataset["challenge_ids"][0]]
            ita = indexed_challenge.ta_catalog.get("MetricsEvent")
            if ita is not None:
                ta_event = ita.get_by_original_id(event_id)
            else:
                # Corner case of newly created community
                ta_event = None
            
            event: "MutableMapping[str, Any]"
            if ta_event is None:
                event = {
                    "_id": event_id,
                    "_schema": self.schemaMappings["TestAction"],
                    "action_type": "MetricsEvent",
                }
            else:
                event = {
                    "_id": ta_event["_id"],
                    "orig_id": event_id,
                    "_schema": self.schemaMappings["TestAction"],
                    "action_type": "MetricsEvent",
                }
                staged_dates = ta_event.get("dates")
                if staged_dates is not None:
                    event["dates"] = copy.copy(staged_dates)
                staged_metadata = ta_event.get("_metadata")
                if staged_metadata is not None:
                    event["_metadata"] = staged_metadata

            self.logger.info(
                'Building Event object for assessment "' + str(event["_id"]) + '"...')

            # add id of tool for the test event
            event["tool_id"] = tool_id

            # add the oeb official id for the challenge (which is already in the assessment dataset)
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
            modtime = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            event.setdefault("dates", {"creation": modtime})["reception"] = modtime

            # add dataset contacts ids, based on already processed data
            event["test_contact_ids"] = valid_participant_data["dataset_contact_ids"]

            metrics_events.append(event)

        return metrics_events
