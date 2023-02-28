#!/usr/bin/env python

import inspect
import logging
import sys
import os
from datetime import datetime, timezone
import json
import re

from .benchmarking_dataset import BenchmarkingDataset
from ..utils.catalogs import (
    gen_challenge_assessment_metrics_dict,
    get_challenge_label_from_challenge,
    match_metric_from_label,
)

from typing import (
    NamedTuple,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        Sequence,
        Tuple,
    )
    from .participant import ParticipantTuple
    from ..utils.migration_utils import OpenEBenchUtils

class AssessmentTuple(NamedTuple):
    assessment_dataset: "Mapping[str, Any]"
    pt: "ParticipantTuple"

class Assessment():

    def __init__(self, schemaMappings: "Mapping[str, str]"):
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings

    def build_assessment_datasets(
        self,
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        metrics_graphql,
        staged_assessment_datasets: "Sequence[Mapping[str, Any]]",
        min_assessment_datasets: "Sequence[Mapping[str, Any]]",
        data_visibility: "str",
        valid_participant_tuples: "Sequence[ParticipantTuple]",
        benchmarking_event_prefix: "str",
        community_prefix: "str"
    ) -> "Sequence[AssessmentTuple]":
        
        valid_participants = {}
        for pvc in valid_participant_tuples:
            valid_participants[pvc.p_config.participant_id] = pvc
            
        self.logger.info(
            "\n\t==================================\n\t3. Processing assessment datasets\n\t==================================\n")
        
        
        
        
        stagedMap = dict()
        for staged_assessment_dataset in staged_assessment_datasets:
            stagedMap[staged_assessment_dataset['orig_id']] = staged_assessment_dataset
        
        # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.
        oeb_challenges = {}
        for challenge in challenges_graphql:
            oeb_challenges[get_challenge_label_from_challenge(challenge, benchmarking_event_prefix, community_prefix)] = challenge

        valid_assessment_tuples = []
        should_end = []
        for dataset in min_assessment_datasets:
            assessment_participant_id = dataset.get("participant_id")
            pvc = valid_participants.get(assessment_participant_id)
            # If not found, next!!!!!!
            if pvc is None:
                self.logger.warning(f"Assessment dataset {dataset['_id']} was not processed because participant {assessment_participant_id} was not declared, skipping to next assessment element...")
                continue
            
            participant_id = pvc.p_config.participant_id
            valid_participant_data = pvc.participant_dataset
            challenge_pairs = pvc.challenge_pairs
            
            tool_id = valid_participant_data["depends_on"]["tool_id"]
            community_ids = valid_participant_data["community_ids"]
            
            self.logger.info('Building object "' +
                             str(dataset["_id"]) + '"...')
            # initialize new dataset object
            stagedEntry = stagedMap.get(dataset["_id"])
            if stagedEntry is None:
                valid_data = {
                    "_id": dataset["_id"],
                    "type": "assessment"
                }
            else:
                valid_data = {
                    "_id": stagedEntry["_id"],
                    "type": "assessment",
                    "orig_id": dataset["_id"],
                    "dates": stagedEntry["dates"]
                }
            

            # add dataset visibility
            valid_data["visibility"] = data_visibility

            # add name and description, if workflow did not provide them
            dataset_name = valid_data.get("name")
            if dataset_name is None:
                dataset_name = "Metric '" + dataset["metrics"]["metric_id"] + \
                    "' in challenge '" + dataset["challenge_id"] + "' applied to participant '" + dataset["participant_id"] + "'"
            valid_data["name"] = dataset_name
            
            dataset_description = dataset.get("description")
            if dataset_description is None:
                dataset_description = "Assessment dataset of applying metric '" + \
                    dataset["metrics"]["metric_id"] + "' in challenge '" + dataset["challenge_id"] + "' to '"\
                     + dataset["participant_id"] + "' participant"
            valid_data["description"] = dataset_description

            challenge_labels_set = set(map(lambda cp: cp[0], challenge_pairs))
            if dataset["challenge_id"] not in challenge_labels_set:
                self.logger.warning("No challenges associated to " +
                             dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.warning(
                    dataset["_id"] + " not processed, skipping to next assessment element...")
                continue
            
            # replace dataset related challenges with oeb challenge ids
            execution_challenges = []
            try:
                the_challenge = oeb_challenges[dataset["challenge_id"]]
                execution_challenges.append(the_challenge)
                cam_d = gen_challenge_assessment_metrics_dict(the_challenge)
            except:
                self.logger.warning("No challenges associated to " +
                             dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.warning(
                    dataset["_id"] + " not processed, skipping to next assessment element...")
                continue
                # sys.exit()

            valid_data["challenge_ids"] = list(map(lambda ex: ex["_id"] , execution_challenges))

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
            metric_value = dataset["metrics"]["value"]
            error_value = dataset["metrics"]["stderr"]

            valid_data["datalink"] = {
                "schema_url": "https://github.com/inab/OEB_level2_data_migration/single-metric",
                "inline_data": {
                    "value": metric_value,
                    "error": error_value
                }
            }
            
            # Breadcrumbs about the participant id to ease the discovery
            valid_data.setdefault("_metadata", {})["level_2:participant_id"] = participant_id

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
            metric_id, found_tool_id, _ = match_metric_from_label(
                logger=self.logger,
                metrics_graphql=metrics_graphql,
                community_prefix=community_prefix,
                metrics_label=dataset["metrics"]["metric_id"],
                challenge_id=the_challenge['_id'],
                challenge_acronym=the_challenge['acronym'],
                challenge_assessment_metrics_d=cam_d,
                dataset_id=dataset['_id'],
            )
            if metric_id is None:
                should_end.append((the_challenge['_id'], the_challenge['acronym']))
                continue
            
            # Declaring the assessment dependency
            valid_data["depends_on"] = {
                "rel_dataset_ids": list_oeb_datasets,
                "metrics_id": metric_id
            }
            # There could be some corner case where no tool_id was declared
            # when the assessment metrics categories were set
            if found_tool_id is not None:
                valid_data["depends_on"]["tool_id"] = found_tool_id

            # add data version
            valid_data["version"] = pvc.p_config.data_version
            
            # add dataset contacts ids, based on already processed data
            valid_data["dataset_contact_ids"] = valid_participant_data["dataset_contact_ids"]

            self.logger.info('Processed "' + str(dataset["_id"]) + '"...')

            valid_assessment_tuples.append(
                AssessmentTuple(
                    assessment_dataset=valid_data,
                    pt=pvc,
                )
            )

        if len(should_end) > 0:
            self.logger.critical(f"Several {len(should_end)} issues found related to metrics associated to the challenges {should_end}. Please fix all of them")
            sys.exit()
        
        return valid_assessment_tuples

    def build_metrics_events(self, valid_assessment_tuples: "Sequence[AssessmentTuple]") -> "Sequence[Mapping[str, Any]]":

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
        
            orig_id = dataset.get("orig_id",dataset["_id"])
            event_id = rchop(orig_id, "_A") + "_MetricsEvent"
            event = {
                "_id": event_id,
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "MetricsEvent",
            }

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
            event["dates"] = {
                "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
                "reception": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
            }

            # add dataset contacts ids, based on already processed data
            event["test_contact_ids"] = valid_participant_data["dataset_contact_ids"]

            metrics_events.append(event)

        return metrics_events


def rchop(s: "str", sub: "str") -> "str":
    return s[:-len(sub)] if s.endswith(sub) else s
