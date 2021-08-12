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

    def build_participant_dataset(self, response, participant_data, data_visibility, file_location, community_id, tool_id, version, contacts):
       
        logging.info(
            "\n\t==================================\n\t1. Processing participant dataset\n\t==================================\n")

        # initialize new dataset object
        valid_participant_data = {
            "_id": participant_data["_id"],
            "type": "participant"
        }

        # add dataset visibility
        valid_participant_data["visibility"] = data_visibility

        # add name and description, if workflow did not provide them
        if "name" not in participant_data:
            valid_participant_data["name"] = "Predictions made by " + \
                participant_data["participant_id"] + " participant"
        else:
            valid_participant_data["name"] = participant_data["name"]
        if "description" not in participant_data:
            valid_participant_data["description"] = "Predictions made by " + \
                participant_data["participant_id"] + " participant"
        else:
            valid_participant_data["description"] = participant_data["description"]

        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.

        data = response["data"]["getChallenges"]

        oeb_challenges = {}
        for challenge in data:
            _metadata = challenge.get("_metadata")
            if (_metadata is None):
                oeb_challenges[challenge["acronym"]] = challenge["_id"]
            else: oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge["_id"]
            #oeb_challenges[_metadata["level_2:challenge_id"]] = challenge["_id"]

        # replace dataset related challenges with oeb challenge ids
        execution_challenges = []
        for challenge in participant_data["challenge_id"]:

            try:
                execution_challenges.append(oeb_challenges[challenge])
            except:
                logging.error("No challenges associated to " + challenge +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit(1)

        valid_participant_data["challenge_ids"] = execution_challenges

        # select input datasets related to the challenges
        rel_oeb_datasets = set()
        for dataset in [item for item in data if item["_id"] in valid_participant_data["challenge_ids"]]:
            for input_data in dataset["datasets"]:
                rel_oeb_datasets.add(input_data["_id"])

        # add data registration dates
        valid_participant_data["dates"] = {
            "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            "modification": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
        }

        # add participant's file permanent location
        datalink = { "uri": file_location,
                    "attrs": ["archive"],
                    "kind": "mirror",
                    "status": "ok"}
        valid_participant_data["datalinks"] = [datalink]

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
        valid_participant_data["version"] = str(version)
        
        # add dataset contacts ids
        # CHECK IF EMAIL IS GIVEN
        # Make a regular expression
        # for validating an Email
        regex = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'
        contacts_ids = []
        for contact in contacts:
            if(re.search(regex, contact)):
                for x in response["data"]["getContacts"]:
                    if x["email"][0] in contacts:
                        contacts_ids.append(x["_id"]) 
            else:
                for x in response["data"]["getContacts"]:
                    if x["_id"] in contacts:
                        contacts_ids.append(contact) 
                        
        if not contacts_ids:
            logging.error("Contacts {}, does not exist in database".format(contacts))
            sys.exit(1)
        else:
            valid_participant_data["dataset_contact_ids"] = contacts_ids

        sys.stdout.write(
            'Processed "' + str(participant_data["_id"]) + '"...\n')

        return valid_participant_data

    def build_test_events(self, response, participant_data, tool_id, contacts):

        logging.info(
            "\n\t==================================\n\t2. Generating Test Events\n\t==================================\n")

        # initialize the array of test events
        test_events = []

        # an  new event object will be created for each of the challenge where the participants has taken part in
        for challenge in participant_data["challenge_id"]:

            sys.stdout.write('Building object "' + str(challenge +
                                                       "_testEvent_" + participant_data["participant_id"]) + '"...\n')

            event = {
                "_id": challenge + "_testEvent_" + participant_data["participant_id"],
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "TestEvent",
            }

            # add id of tool for the test event
            event["tool_id"] = tool_id

            # add the oeb official id for the challenge
            data = response["data"]["getChallenges"]
            oeb_challenges = {}
            for oeb_challenge in data:
                _metadata = oeb_challenge.get("_metadata")
                if (_metadata is None):
                    oeb_challenges[oeb_challenge["acronym"]] = oeb_challenge["_id"]
                else: oeb_challenges[oeb_challenge["_metadata"]["level_2:challenge_id"]] = oeb_challenge["_id"]


            try:
                event["challenge_id"] = oeb_challenges[challenge]
            except:
                logging.fatal("No challenge associated to " + challenge +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()

            # append incoming and outgoing datasets
            involved_data = []

            # select input datasets related to the challenge
            rel_oeb_datasets = set()
            for dataset in [item for item in data if item["_id"] == event["challenge_id"]]:
                for input_data in dataset["datasets"]:
                    rel_oeb_datasets.add(input_data["_id"])

            for dataset in rel_oeb_datasets:
                involved_data.append({
                    "dataset_id": dataset,
                    "role": "incoming"
                })

            involved_data.append({
                "dataset_id": participant_data["_id"],
                "role": "outgoing"
            })

            event["involved_datasets"] = involved_data
            # add data registration dates
            event["dates"] = {
                "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
                "reception": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
            }

            # add dataset contacts ids
            # CHECK IF EMAIL IS GIVEN
            # Make a regular expression
            # for validating an Email
            regex = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'
            contacts_ids = []
            for contact in contacts:
                if(re.search(regex, contact)):
                    for x in response["data"]["getContacts"]:
                        if x["email"][0] in contacts:
                            contacts_ids.append(x["_id"]) 
                else:
                    for x in response["data"]["getContacts"]:
                        if x["_id"] in contacts:
                            contacts_ids.append(contact) 
                            
            if not contacts_ids:
                logging.error("Contacts {}, does not exist in database".format(contacts))
                sys.exit(1)
            else:
                event["test_contact_ids"] = contacts_ids

            test_events.append(event)

        return test_events
