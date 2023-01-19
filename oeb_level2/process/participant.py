#!/usr/bin/env python

import logging
import sys
import os
from datetime import datetime, timezone
import json
import re




class Participant():

    def __init__(self, schemaMappings):

        logging.basicConfig(level=logging.INFO)
        self.schemaMappings = schemaMappings

    def build_participant_dataset(self, challenges_graphql, contacts_graphql, min_participant_data, data_visibility, 
                                  file_location, community_id, tool_id, version: "str", contacts):
       
        logging.info(
            "\n\t==================================\n\t1. Processing participant dataset\n\t==================================\n")

        # initialize new dataset object
        valid_participant_data = {
            "_id": min_participant_data["_id"],
            "type": "participant"
        }

        # add dataset visibility
        valid_participant_data["visibility"] = data_visibility

        # add name and description, if workflow did not provide them
        if "name" not in min_participant_data:
            valid_participant_data["name"] = "Predictions made by " + \
                min_participant_data["participant_id"] + " participant"
        else:
            valid_participant_data["name"] = min_participant_data["name"]
        if "description" not in min_participant_data:
            valid_participant_data["description"] = "Predictions made by " + \
                min_participant_data["participant_id"] + " participant"
        else:
            valid_participant_data["description"] = min_participant_data["description"]

        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.

        oeb_challenges = {}
        for challenge_graphql in challenges_graphql:
            _metadata = challenge_graphql.get("_metadata")
            if (_metadata is None):
                oeb_challenges[challenge_graphql["acronym"]] = challenge_graphql
            else:
                oeb_challenges[challenge_graphql["_metadata"]["level_2:challenge_id"]] = challenge_graphql

        # replace dataset related challenges with oeb challenge ids
        execution_challenges = []
        min_challenge_labels = min_participant_data["challenge_id"]
        if not isinstance(min_challenge_labels, list):
            min_challenge_labels = [ min_challenge_labels ]
        
        challenge_pairs = []
        for challenge_label in min_challenge_labels:
            try:
                execution_challenges.append(oeb_challenges[challenge_label]["_id"])
                challenge_pairs.append((challenge_label, oeb_challenges[challenge_label]))
            except:
                logging.fatal("No challenges associated to " + challenge_label +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()

        valid_participant_data["challenge_ids"] = execution_challenges

        # select input datasets related to the challenges
        rel_oeb_datasets = set()
        for challenge_graphql in map(lambda cp: cp[1], challenge_pairs):
            for input_data in challenge_graphql["datasets"]:
                rel_oeb_datasets.add(input_data["_id"])

        # add data registration dates
        valid_participant_data["dates"] = {
            "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            "modification": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
        }

        # add participant's file permanent location
        valid_participant_data["datalink"] = min_participant_data["datalink"]
        valid_participant_data["datalink"]["uri"] = file_location
        
        # check validation date is iso format, otherwise fix it
        if not min_participant_data["datalink"]["validation_date"].endswith("Z") and not (re.search("[+-]\d{2}:\d{2}$", min_participant_data["datalink"]["validation_date"])):
            valid_participant_data["datalink"]["validation_date"] = min_participant_data["datalink"]["validation_date"]+"+00:00"
       
        # add Benchmarking Data Model Schema Location
        valid_participant_data["_schema"] = self.schemaMappings["Dataset"]

        # remove custom workflow community id and add OEB id for the community
        valid_participant_data["community_ids"] = [community_id]

        # add dataset dependencies: tool and reference datasets
        list_oeb_datasets = []
        for dataset in rel_oeb_datasets:
            list_oeb_datasets.append({
                "dataset_id": dataset
            })

        valid_participant_data["depends_on"] = {
            "tool_id": tool_id,
            "rel_dataset_ids": list_oeb_datasets
        }

        # add data version
        valid_participant_data["version"] = version
        
        # add dataset contacts ids
        # CHECK IF EMAIL IS GIVEN
        # Make a regular expression
        # for validating an Email
        regex_email = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'
        contacts_ids = []
        for contact in contacts:
            if(re.search(regex_email, contact)):
                for x in contacts_graphql:
                    if x["email"][0] == contact:
                        contacts_ids.append(x["_id"])
                        break
                else:
                    logging.error(f"Contact {contact} (e-mail) could not be found in database")
                    sys.exit(1)
                    
            else:
                for x in contacts_graphql:
                    if x["_id"] == contact:
                        contacts_ids.append(contact)
                        break
                else:
                    logging.error(f"Contact {contact} (_id) could not be found in database")
                    sys.exit(1)
                        
        valid_participant_data["dataset_contact_ids"] = contacts_ids

        sys.stdout.write(
            'Processed "' + str(min_participant_data["_id"]) + '"...\n')

        return valid_participant_data, challenge_pairs

    def build_test_events(self, participant_id, valid_participant_data, challenge_pairs):

        logging.info(
            "\n\t==================================\n\t2. Generating Test Events\n\t==================================\n")

        # initialize the array of test events
        test_events = []

        # an  new event object will be created for each of the challenge where the participants has taken part in
        for challenge_label, challenge_graphql in challenge_pairs:

            sys.stdout.write('Building object "' + str(challenge_label +
                                                       "_testEvent_" + participant_id) + '"...\n')

            event = {
                "_id": challenge_label + "_testEvent_" + participant_id,
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "TestEvent",
            }

            # add id of tool for the test event
            event["tool_id"] = valid_participant_data["depends_on"]["tool_id"]

            # add the oeb official id for the challenge
            event["challenge_id"] = challenge_graphql["_id"]

            # append incoming and outgoing datasets
            involved_data = []

            # select input datasets related to the challenge
            rel_oeb_datasets = set()
            for input_data in challenge_graphql["datasets"]:
                rel_oeb_datasets.add(input_data["_id"])

            for dataset in rel_oeb_datasets:
                involved_data.append({
                    "dataset_id": dataset,
                    "role": "incoming"
                })

            involved_data.append({
                "dataset_id": valid_participant_data["_id"],
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

            test_events.append(event)

        return test_events
