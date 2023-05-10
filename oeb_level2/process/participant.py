#!/usr/bin/env python

import copy
import inspect
import logging
import sys
import os
from datetime import datetime, timezone
import json
import re

from dataclasses import dataclass

from typing import (
    NamedTuple,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        MutableMapping,
        Optional,
        Sequence,
    )
    
    from ..schemas.typed_schemas.submission_form_schema import DatasetsVisibility
    from ..utils.catalogs import IndexedChallenge
   
from ..utils.migration_utils import OpenEBenchUtils

class ChallengePair(NamedTuple):
    label: "str"
    entry: "Mapping[str, Any]"

@dataclass
class ParticipantConfig:
    tool_id: "str"
    data_version: "str"
    data_contacts: "Sequence[str]"
    participant_label: "str"
    
    def process_contact_ids(self, contacts_graphql: "Sequence[Mapping[str, Any]]") -> "Sequence[str]":
        # add dataset contacts ids
        # CHECK IF EMAIL IS GIVEN
        # Make a regular expression
        # for validating an Email
        
        if not(hasattr(self, "data_contact_ids")):
            regex_email = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'
            contacts_ids = []
            should_exit = False
            for contact in self.data_contacts:
                if(re.search(regex_email, contact)):
                    for x in contacts_graphql:
                        if x["email"][0] == contact:
                            contacts_ids.append(x["_id"])
                            break
                    else:
                        logging.fatal(f"Contact {contact} (e-mail) could not be found in database")
                        should_exit = True
                        
                else:
                    for x in contacts_graphql:
                        if x["_id"] == contact:
                            contacts_ids.append(contact)
                            break
                    else:
                        logging.fatal(f"Contact {contact} (_id) could not be found in database")
                        should_exit = True
            
            # This is going to be reported later            
            if should_exit:
                logging.fatal("Several contacts were not found. Please either fix their names or register them in OpenEBench")
                raise KeyError("Some contacts did not match")
            
            self.data_contact_ids = contacts_ids
        
        return self.data_contact_ids
        
class ParticipantTuple(NamedTuple):
    p_config: "ParticipantConfig"
    participant_dataset: "Mapping[str, Any]"
    challenge_pairs: "Sequence[ChallengePair]"
    community_acronym: "str"


class ParticipantBuilder():

    def __init__(self, schemaMappings: "Mapping[str, str]", migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.migration_utils = migration_utils

    def build_participant_dataset(
        self,
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        staged_participant_datasets: "Sequence[Mapping[str, Any]]",
        min_participant_dataset: "Sequence[Mapping[str, Any]]",
        data_visibility: "DatasetsVisibility", 
        file_location: "str",
        community_id: "str",
        benchmarking_event_prefix: "str",
        bench_event_orig_id_separator: "str",
        community_prefix: "str",
        tool_mapping: "Mapping[Optional[str], ParticipantConfig]",
    ) -> "Sequence[ParticipantTuple]":
       
        self.logger.info(
            "\n\t==================================\n\t1. Processing participant dataset\n\t==================================\n")
        
        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.
        oeb_challenges = {}
        
        for challenge_graphql in challenges_graphql:
            oeb_challenges[OpenEBenchUtils.get_challenge_label_from_challenge(challenge_graphql, benchmarking_event_prefix, bench_event_orig_id_separator, community_prefix).label] = challenge_graphql

        stagedMap = dict()
        for staged_participant_dataset in staged_participant_datasets:
            stagedMap[staged_participant_dataset['orig_id']] = staged_participant_dataset
        
        valid_participant_tuples = []
        should_exit_challenge = False
        for min_participant_data in min_participant_dataset:
            the_id = min_participant_data["_id"]
            
            stagedEntry = stagedMap.get(the_id)
            # initialize new dataset object
            if stagedEntry is None:
                valid_participant_data = {
                    "_id": the_id,
                    "type": "participant"
                }
            else:
                valid_participant_data = {
                    "_id": stagedEntry["_id"],
                    "type": "participant",
                    "orig_id": the_id,
                }
                staged_dates = stagedEntry.get("dates")
                if staged_dates is not None:
                    valid_participant_data["dates"] = copy.copy(staged_dates)
            
            # add dataset visibility
            valid_participant_data["visibility"] = data_visibility

            # add name and description, if workflow did not provide them
            min_participant_data_name = min_participant_data.get("name")
            if min_participant_data_name is None:
                min_participant_data_name = "Predictions made by " + \
                    min_participant_data["participant_id"] + " participant"
            valid_participant_data["name"] = min_participant_data_name
            
            # Get the participant_config
            p_config = tool_mapping.get(min_participant_data["participant_id"])
            # Default case
            if p_config is None:
                if len(tool_mapping) > 1 or (None not in tool_mapping):
                    self.logger.critical(f"Trying to use old school default mapping to map {min_participant_data['participant_id']} in new school scenario. Fix it")
                    sys.exit(4)
                p_config = tool_mapping[None]
                
            min_participant_data_description = min_participant_data.get("description")
            if min_participant_data_description is None:
                min_participant_data_description = "Predictions made by " + \
                    min_participant_data["participant_id"] + " participant"
            valid_participant_data["description"] = min_participant_data_description

            # replace dataset related challenges with oeb challenge ids
            execution_challenges = []
            min_challenge_labels = min_participant_data["challenge_id"]
            if not isinstance(min_challenge_labels, list):
                min_challenge_labels = [ min_challenge_labels ]
            
            challenge_pairs = []
            should_exit = False
            for challenge_label in min_challenge_labels:
                try:
                    execution_challenges.append(oeb_challenges[challenge_label]["_id"])
                    challenge_pairs.append(
                        ChallengePair(
                            label=challenge_label,
                            entry=oeb_challenges[challenge_label]
                        )
                    )
                except:
                    self.logger.critical("No challenges associated to " + challenge_label +
                                  " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                    should_exit = True
                    # Process next one
            if should_exit:
                should_exit_challenge = True
                continue
            
            if stagedEntry is not None:
                cis = set(stagedEntry["challenge_ids"])
                cis.update(execution_challenges)
                execution_challenges = sorted(cis)
            else:
                execution_challenges.sort()
            valid_participant_data["challenge_ids"] = execution_challenges

            # select input datasets related to the challenges
            rel_oeb_datasets = set()
            for challenge_graphql in map(lambda cp: cp.entry, challenge_pairs):
                for input_data in challenge_graphql["datasets"]:
                    rel_oeb_datasets.add(input_data["_id"])

            # add data registration dates
            modtime = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            valid_participant_data.setdefault("dates", {"creation": modtime})["modification"] = modtime

            # add participant's file permanent location
            valid_participant_data["datalink"] = min_participant_data["datalink"]
            valid_participant_data["datalink"]["uri"] = file_location
            
            # check validation date is iso format, otherwise fix it
            if not min_participant_data["datalink"]["validation_date"].endswith("Z") and not (re.search("[+-]\d{2}:\d{2}$", min_participant_data["datalink"]["validation_date"])):
                valid_participant_data["datalink"]["validation_date"] = min_participant_data["datalink"]["validation_date"]+"+00:00"
           
            # add Benchmarking Data Model Schema Location
            valid_participant_data["_schema"] = self.schemaMappings["Dataset"]

            # ignore custom workflow community id and add OEB id for the community
            valid_participant_data["community_ids"] = [community_id]

            # add dataset dependencies: tool and reference datasets
            list_oeb_datasets = []
            for dataset in rel_oeb_datasets:
                list_oeb_datasets.append({
                    "dataset_id": dataset
                })

            valid_participant_data["depends_on"] = {
                "tool_id": p_config.tool_id,
                "rel_dataset_ids": list_oeb_datasets
            }

            # add data version
            valid_participant_data["version"] = p_config.data_version
            
            valid_participant_data["dataset_contact_ids"] = p_config.data_contact_ids
            
            # Breadcrumbs about the participant id to ease the discovery
            new_metadata = {
                "level_2:participant_id": min_participant_data["participant_id"],
            }
            if stagedEntry is not None:
                staged_metadata = stagedEntry.get("_metadata")
                if isinstance(staged_metadata, dict):
                    updated_metadata = copy.copy(staged_metadata)
                    updated_metadata.update(new_metadata)
                    new_metadata = updated_metadata
            valid_participant_data["_metadata"] = new_metadata

            self.logger.info(
                'Processed "' + str(min_participant_data["_id"]) + '"...')
            
            # It is really a check through comparison of what was generated
            self.migration_utils.gen_expected_participant_original_id(
                valid_participant_data,
                community_prefix,
                benchmarking_event_prefix,
                bench_event_orig_id_separator,
                p_config.participant_label,
            )
            
            valid_participant_tuples.append(
                ParticipantTuple(
                    p_config=p_config,
                    participant_dataset=valid_participant_data,
                    challenge_pairs=challenge_pairs,
                    community_acronym=min_participant_data["community_id"]
                )
            )

        if should_exit_challenge:
            self.logger.critical("Some challenges where not located. Please either fix their labels or register them in OpenEBench")
            sys.exit()
        
        return valid_participant_tuples

    def build_test_events(self, valid_participant_tuples: "Sequence[ParticipantTuple]", indexed_challenges: "Mapping[str, IndexedChallenge]") -> "Sequence[Mapping[str, Any]]":

        self.logger.info(
            "\n\t==================================\n\t2. Generating Test Events\n\t==================================\n")

        # initialize the array of test events
        test_events = []

        # an  new event object will be created for each of the challenge where the participants has taken part in
        for pt in valid_participant_tuples:
            participant_label = pt.p_config.participant_label
            for challenge_pair in pt.challenge_pairs:
                indexed_challenge = indexed_challenges[challenge_pair.entry["_id"]]
                the_id = OpenEBenchUtils.gen_test_event_original_id(indexed_challenge.challenge, participant_label)
                self.logger.info(f'Building TestEvent "{the_id}"...')

                ita = indexed_challenge.ta_catalog.get("TestEvent")
                if ita is not None:
                    ta_event = ita.get_by_original_id(the_id)
                else:
                    # Corner case of newly created community
                    ta_event = None
                
                event: "MutableMapping[str, Any]"
                if ta_event is None:
                    event = {
                        "_id": the_id,
                        "_schema": self.schemaMappings["TestAction"],
                        "action_type": "TestEvent",
                    }
                else:
                    event = {
                        "_id": ta_event["_id"],
                        "orig_id": the_id,
                        "_schema": self.schemaMappings["TestAction"],
                        "action_type": "TestEvent",
                    }
                    staged_dates = ta_event.get("dates")
                    if staged_dates is not None:
                        event["dates"] = copy.copy(staged_dates)
                    staged_metadata = ta_event.get("_metadata")
                    if staged_metadata is not None:
                        event["_metadata"] = staged_metadata

                # add id of tool for the test event
                event["tool_id"] = pt.participant_dataset["depends_on"]["tool_id"]

                # add the oeb official id for the challenge
                event["challenge_id"] = challenge_pair.entry["_id"]

                # append incoming and outgoing datasets
                involved_data = []

                # select input datasets related to the challenge
                rel_oeb_datasets = set()
                for input_data in challenge_pair.entry["datasets"]:
                    rel_oeb_datasets.add(input_data["_id"])

                for dataset in rel_oeb_datasets:
                    involved_data.append({
                        "dataset_id": dataset,
                        "role": "incoming"
                    })

                involved_data.append({
                    "dataset_id": pt.participant_dataset["_id"],
                    "role": "outgoing"
                })

                event["involved_datasets"] = involved_data
                # add data registration dates
                modtime = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                event.setdefault("dates", {"creation": modtime})["reception"] = modtime

                # add dataset contacts ids, based on already processed data
                event["test_contact_ids"] = pt.participant_dataset["dataset_contact_ids"]

                test_events.append(event)

        return test_events
