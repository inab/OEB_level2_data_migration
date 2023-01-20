#!/usr/bin/env python

import logging
import sys
import os
from datetime import datetime, timezone
import json
import re

from .benchmarking_dataset import BenchmarkingDataset

class Assessment():

    def __init__(self, schemaMappings):

        logging.basicConfig(level=logging.INFO)
        self.schemaMappings = schemaMappings

    def build_assessment_datasets(self, challenges_graphql, metrics_graphql, stagedAssessmentDatasets, assessment_datasets, data_visibility, version: "str", valid_participant_data):

        logging.info(
            "\n\t==================================\n\t3. Processing assessment datasets\n\t==================================\n")
        
        default_tool_id = valid_participant_data["depends_on"]["tool_id"]
        community_ids = valid_participant_data["community_ids"]
        
        stagedMap = dict()
        for stagedAssessmentDataset in stagedAssessmentDatasets:
            stagedMap[stagedAssessmentDataset['orig_id']] = stagedAssessmentDataset
        
        # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.
        oeb_challenges = {}
        for challenge in challenges_graphql:
            _metadata = challenge.get("_metadata")
            if (_metadata is None):
                oeb_challenges[challenge["acronym"]] = challenge
            else:
                oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge

        valid_assessment_datasets = []
        should_end = []
        for dataset in assessment_datasets:
            tool_id = default_tool_id

            logging.info('Building object "' +
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
            if "name" not in dataset:
                valid_data["name"] = "Metric '" + dataset["metrics"]["metric_id"] + \
                    "' in challenge '" + dataset["challenge_id"] + "' applied to participant '" + dataset["participant_id"] + "'"
            else:
                valid_data["name"] = dataset["name"]

            if "description" not in dataset:
                valid_data["description"] = "Assessment dataset of applying metric '" + \
                    dataset["metrics"]["metric_id"] + "' in challenge '" + dataset["challenge_id"] + "' to '"\
                     + dataset["participant_id"] + "' participant"
            else:
                valid_data["description"] = dataset["description"]

            # replace dataset related challenges with oeb challenge ids
            execution_challenges = []
            challenge_assessment_metrics = []
            cam_d = {}
            try:
                the_challenge = oeb_challenges[dataset["challenge_id"]]
                execution_challenges.append(the_challenge)
                for metrics_category in the_challenge.get("metrics_categories",[]):
                    if metrics_category.get("category") == "assessment":
                        challenge_assessment_metrics = metrics_category.get("metrics", [])
                        cam_d = {
                            cam["metrics_id"]: cam
                            for cam in challenge_assessment_metrics
                        }
                        break
            except:
                logging.warning("No challenges associated to " +
                             dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                logging.warning(
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
                "inline_data": {
                    "value": metric_value,
                    "error": error_value
                }
            }

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
            guessed_metrics_ids = []
            community_prefix = dataset['community_id'] + ':'
            dataset_metrics_id_u = dataset["metrics"]["metric_id"].upper()
            for metric in metrics_graphql:
                if metric['orig_id'].startswith(community_prefix):
                    # First guess
                    if metric["orig_id"][len(community_prefix):].upper().startswith(dataset_metrics_id_u):
                        guessed_metrics_ids.append(metric["_id"])
                    
                    # Second guess (it can introduce false crosses)
                    metric_metadata = metric.get("_metadata")
                    if isinstance(metric_metadata, dict) and 'level_2:metric_id' in metric_metadata:
                        if metric['_metadata']['level_2:metric_id'].upper() == dataset_metrics_id_u:
                            guessed_metrics_ids.append(metric["_id"])
            
            
            if len(guessed_metrics_ids) == 0:
                logging.fatal(f"Unable to match in OEB a metric to label {dataset['metrics']['metric_id']} . Please contact OpenEBench support for information about how to register your own metrics and link them to the challenge {the_challenge['_id']} (acronym {the_challenge['acronym']})")
                should_end.append((the_challenge['_id'], the_challenge['acronym']))
                continue
            
            matched_metrics_ids = []
            for guessed_metric_id in guessed_metrics_ids:
                cam = cam_d.get(guessed_metric_id)
                if cam is not None:
                    matched_metrics_ids.append(cam)
            
            if len(matched_metrics_ids) == 0:
                if len(guessed_metrics_ids) == 1:
                    metric_id = guessed_metrics_ids[0]
                    logging.warning(f"Metric {metric_id} (guessed from {dataset['metrics']['metric_id']} at dataset {dataset['_id']}) is not registered as an assessment metric at challenge {the_challenge['_id']} (acronym {the_challenge['acronym']}). Consider register it")
                else:
                    logging.fatal(f"Several metrics {guessed_metrics_ids} were guessed from {dataset['metrics']['metric_id']} at dataset {dataset['_id']} . No clever heuristic can be applied. Please properly register some of them as an assessment metric at challenge {the_challenge['_id']} (acronym {the_challenge['acronym']}).")
                    should_end.append((the_challenge['_id'], the_challenge['acronym']))
                    continue
            elif len(matched_metrics_ids) == 1:
                mmi = matched_metrics_ids[0]
                metric_id = mmi['metrics_id']
                if mmi['tool_id'] is not None:
                    tool_id = mmi['tool_id']
            else:
                logging.fatal(f"Several metrics registered at challenge {the_challenge['_id']} (acronym {the_challenge['acronym']}) matched from {dataset['metrics']['metric_id']} at dataset {dataset['_id']} . Fix the challenge declaration.")
                should_end.append((the_challenge['_id'], the_challenge['acronym']))
                continue
                

            valid_data["depends_on"] = {
                "tool_id": tool_id,
                "rel_dataset_ids": list_oeb_datasets,
                "metrics_id": metric_id
            }

            # add data version
            valid_data["version"] = version
            
            # add dataset contacts ids, based on already processed data
            valid_data["dataset_contact_ids"] = valid_participant_data["dataset_contact_ids"]

            logging.info('Processed "' + str(dataset["_id"]) + '"...')

            valid_assessment_datasets.append(valid_data)

        if len(should_end) > 0:
            logging.fatal(f"Several issues found related to metrics associated to the challenges {should_end}. Please fix all of them")
            sys.exit()
        
        return valid_assessment_datasets

    def build_metrics_events(self, assessment_datasets, valid_participant_data):

        logging.info(
            "\n\t==================================\n\t4. Generating Metrics Events\n\t==================================\n")

        tool_id = valid_participant_data["depends_on"]["tool_id"]
        
        # initialize the array of test events
        metrics_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for dataset in assessment_datasets:
            orig_id = dataset.get("orig_id",dataset["_id"])
            event_id = rchop(orig_id, "_A") + "_MetricsEvent"
            event = {
                "_id": event_id,
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "MetricsEvent",
            }

            logging.info(
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


def rchop(s, sub):
    return s[:-len(sub)] if s.endswith(sub) else s
