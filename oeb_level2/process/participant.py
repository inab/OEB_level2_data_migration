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
        MutableSequence,
        Optional,
        Sequence,
    )
    
    from ..schemas.typed_schemas.submission_form_schema import DatasetsVisibility
    from ..utils.catalogs import IndexedChallenge
    from ..utils.migration_utils import BenchmarkingEventPrefixEtAl

from ..utils.catalogs import (
    gen_inline_data_label_from_participant_dataset,
)

from ..utils.migration_utils import (
    EXCLUDE_PARTICIPANT_KEY,
    OEBDatasetType,
    OpenEBenchUtils,
    PARTICIPANT_ID_KEY,
)

class ChallengePair(NamedTuple):
    label: "str"
    entry: "Mapping[str, Any]"
        
    @classmethod
    def FromChallengeEtAl(cls, challenge: "Mapping[str, Any]", bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl", community_prefix: "str") -> "ChallengePair":
        return cls(
            label=OpenEBenchUtils.get_challenge_label_from_challenge(
                challenge,
                bench_event_prefix_et_al,
                community_prefix
            ).label,
            entry=challenge,
        )

@dataclass
class ParticipantConfig:
    tool_id: "str"
    data_version: "str"
    data_contacts: "Sequence[str]"
    participant_label: "str"
    exclude: "bool" = False
    
    @classmethod
    def FromDataset(cls, participant_ql: "Mapping[str, Any]") -> "ParticipantConfig":
        return cls(
            tool_id=participant_ql["depends_on"]["tool_id"],
            data_version=participant_ql["version"],
            data_contacts=participant_ql["dataset_contact_ids"],
            participant_label=gen_inline_data_label_from_participant_dataset(participant_ql).label["label"],
            exclude=participant_ql.get("_metadata", {}).get(EXCLUDE_PARTICIPANT_KEY, False),
        )

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
        
@dataclass
class ParticipantTuple:
    p_config: "ParticipantConfig"
    participant_dataset: "Mapping[str, Any]"
    challenge_pairs: "MutableSequence[ChallengePair]"
    community_acronym: "str"

    @classmethod
    def FromDataset(cls, participant_ql: "Mapping[str, Any]", community_label: "str") -> "ParticipantTuple":
        return cls(
            p_config=ParticipantConfig.FromDataset(participant_ql),
            participant_dataset=participant_ql,
            # Initially empty one
            challenge_pairs=[],
            community_acronym=community_label,
        )


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
        bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
        community_prefix: "str",
        tool_mapping: "Mapping[Optional[str], ParticipantConfig]",
        agg_challenges: "Mapping[str, IndexedChallenge]",
        do_fix_orig_ids: "bool",
    ) -> "Sequence[ParticipantTuple]":
       
        self.logger.info(
            "\n\t==================================\n\t1. Processing participant dataset\n\t==================================\n")
        
        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.
        oeb_challenges = {}
        
        for challenge_graphql in challenges_graphql:
            oeb_challenges[
                OpenEBenchUtils.get_challenge_label_from_challenge(
                    challenge_graphql,
                    bench_event_prefix_et_al,
                    community_prefix
                ).label
            ] = challenge_graphql

        stagedMap = dict()
        for staged_participant_dataset in staged_participant_datasets:
            stagedMap[staged_participant_dataset['orig_id']] = staged_participant_dataset
        
        valid_participant_tuples = []
        should_exit_challenge = False
        should_exit_input_dataset = set()
        for min_participant_data in min_participant_dataset:
            # Get the participant_config
            p_config = tool_mapping.get(min_participant_data["participant_id"])
            # Default case
            if p_config is None:
                if len(tool_mapping) > 1 or (None not in tool_mapping):
                    self.logger.critical(f"Trying to use old school default mapping to map {min_participant_data['participant_id']} in new school scenario. Fix it")
                    sys.exit(4)
                p_config = tool_mapping[None]
                
            ## This dataset has the minimum data needed for the generation
            #mock_dataset = {
            #    "challenge_ids":,
            #    "community_ids":,
            #}
            #expected_orig_id = self.migration_utils.gen_participant_original_id_from_dataset(
            #    mock_dataset,
            #    community_prefix,
            #    bench_event_prefix_et_al,
            #    p_config.participant_label,
            #)
            
            # replace dataset related challenges with oeb challenge ids
            execution_challenge_ids = []
            min_challenge_labels = min_participant_data["challenge_id"]
            if not isinstance(min_challenge_labels, list):
                min_challenge_labels = [ min_challenge_labels ]
            
            challenge_pairs = []
            should_exit = False
            min_id: "Optional[str]" = None
            for challenge_label in min_challenge_labels:
                try:
                    execution_challenge_id = oeb_challenges[challenge_label]["_id"]
                    # Checking participant label on each challenge
                    idx_cha_p = agg_challenges.get(execution_challenge_id)
                    if idx_cha_p is None:
                        self.logger.fatal(f"Unable to fetch indexed challenge {execution_challenge_id}")
                        should_exit = True
                        continue
                    participant_data_labels_pairs = idx_cha_p.d_catalog.get_participant_labels()
                    existing_min_id: "Optional[str]" = None
                    for participant_data_label, p_dataset_id in participant_data_labels_pairs:
                        if participant_data_label["label"] == p_config.participant_label:
                            existing_min_id = participant_data_label["dataset_orig_id"]

                    if existing_min_id is not None:
                        if min_id is None:
                            min_id = existing_min_id
                        elif min_id != existing_min_id:
                            self.logger.fatal(f"Different challenges for participant dataset label {p_config.participant_label} have different original dataset ids {min_id} and {existing_min_id}")
                            should_exit = True

                    execution_challenge_ids.append(execution_challenge_id)
                    challenge_pairs.append(
                        ChallengePair(
                            label=challenge_label,
                            entry=oeb_challenges[challenge_label]
                        )
                    )
                except:
                    self.logger.exception("No challenges associated to " + challenge_label +
                                  " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                    should_exit = True
                    # Process next one
            if should_exit:
                should_exit_challenge = True
                continue

            # No previous original id
            if min_id is None:
                if do_fix_orig_ids:
                    min_id = self.migration_utils.gen_participant_original_id_from_min_dataset(
                        min_participant_data,
                        community_prefix,
                        bench_event_prefix_et_al,
                        challenge_ids=execution_challenge_ids,
                    )
                else:
                    min_id = min_participant_data["_id"]
            
            stagedEntry = stagedMap.get(min_id)
            # initialize new dataset object
            valid_participant_data: "MutableMapping[str, Any]"
            if stagedEntry is None:
                valid_participant_data = {
                    "_id": min_id,
                    "type": OEBDatasetType.Participant.value,
                }
            else:
                valid_participant_data = {
                    "_id": stagedEntry["_id"],
                    "type": OEBDatasetType.Participant.value,
                    "orig_id": min_id,
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
            
            min_participant_data_description = min_participant_data.get("description")
            if min_participant_data_description is None:
                min_participant_data_description = "Predictions made by " + \
                    min_participant_data["participant_id"] + " participant"
            valid_participant_data["description"] = min_participant_data_description
            
            if stagedEntry is not None:
                cis = set(stagedEntry["challenge_ids"])
                cis.update(execution_challenge_ids)
                execution_challenge_ids = sorted(cis)
            else:
                execution_challenge_ids.sort()
            valid_participant_data["challenge_ids"] = execution_challenge_ids

            # select input datasets related to the challenges
            rel_oeb_datasets = set()
            for challenge_graphql in map(lambda cp: cp.entry, challenge_pairs):
                for input_data in challenge_graphql["datasets"]:
                    rel_oeb_datasets.add(input_data["_id"])
                
            if len(rel_oeb_datasets) == 0:
                self.logger.critical(f"No input dataset is associated to any of these challenges: {', '.join(min_challenge_labels)}. This is needed for correct building of participant dataset {valid_participant_data['_id']}. Skipped.")
                should_exit_input_dataset.update(execution_challenge_ids)
                continue

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
                PARTICIPANT_ID_KEY: min_participant_data["participant_id"],
                EXCLUDE_PARTICIPANT_KEY: p_config.exclude,
            }
            if stagedEntry is not None:
                staged_metadata = stagedEntry.get("_metadata")
                if isinstance(staged_metadata, dict):
                    updated_metadata = copy.copy(staged_metadata)
                    updated_metadata.update(new_metadata)
                    new_metadata = updated_metadata
            valid_participant_data["_metadata"] = new_metadata

            self.logger.info(f'Processed "{min_id}" (was {str(min_participant_data["_id"])})...')
            
            # It is really a check through comparison of what was generated
            fixed_entry = self.migration_utils.fix_participant_original_id(
                valid_participant_data,
                community_prefix,
                bench_event_prefix_et_al,
                p_config.participant_label,
                do_fix_orig_ids,
            )
            
            valid_participant_tuples.append(
                ParticipantTuple(
                    p_config=p_config,
                    participant_dataset=valid_participant_data if fixed_entry is None else fixed_entry,
                    challenge_pairs=challenge_pairs,
                    community_acronym=min_participant_data["community_id"]
                )
            )

        if should_exit_challenge or len(should_exit_input_dataset) > 0:
            if should_exit_challenge:
                self.logger.critical("Some challenges where not located. Please either fix their labels or register them in OpenEBench")
            else:
                self.logger.critical(f"No input dataset is associated to any of these challenges: {', '.join(should_exit_input_dataset)}. This is needed for correct building of several participant datasets. Fix it.")
            sys.exit(5)
        
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
