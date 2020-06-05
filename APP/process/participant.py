import logging
import sys, os
from datetime import datetime
from schema_validators.python import jsonValidate
import json
import tempfile

class participant():

    logging.basicConfig(level=logging.INFO)

    def build_participant_dataset(self, response, participant_data, data_visibility, bench_event_id, file_location,community_id, tool_id, version, contacts, data_model_dir):

        logging.info("\n==================================\n\t1. Processing participant dataset\n==================================\n")

        ## initialize new dataset object
        valid_participant_data = {
            "_id": participant_data["_id"],
            "type": "participant"
        }
        
        # add dataset visibility 
        valid_participant_data["visibility"] = data_visibility

        # add name and description, if workflow did not provide them
        if "name" not in participant_data:
            valid_participant_data["name"] = "Predictions made by " + participant_data["participant_id"] + " participant"
        if "description" not in participant_data:
            valid_participant_data["description"] = "Predictions made by " + participant_data["participant_id"]  + " participant"

        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.

        data = response["data"]["getChallenges"]

        oeb_challenges = {}
        for challenge in data:
            oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge["_id"]

        ## replace dataset related challenges with oeb challenge ids
        execution_challenges = []
        for challenge in participant_data["challenge_id"]:
            
            try:
                execution_challenges.append(oeb_challenges[challenge])
            except:
                logging.fatal("No challenges associated to " + challenge + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()

        valid_participant_data["challenge_ids"] = execution_challenges
        
        ## select input datasets related to the challenges
        rel_oeb_datasets = set()
        for dataset in [item for item in data if item["_id"] in valid_participant_data["challenge_ids"] ]:
            for input_data in dataset["datasets"]:
                rel_oeb_datasets.add( input_data["_id"] )

        # add data registration dates
        valid_participant_data["dates"] = {
                "creation": str(datetime.now().replace(microsecond=0).isoformat()),
                "modification": str(datetime.now().replace(microsecond=0).isoformat())
            }
        
        # add participant's file permanent location
        valid_participant_data["datalink"] = participant_data["datalink"]
        valid_participant_data["datalink"]["uri"] = file_location
        
        ## add Benchmarking Data Model Schema Location
        valid_participant_data["_schema"] = "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset"

        ## remove custom workflow community id and add OEB id for the community
        valid_participant_data["community_ids"] = [community_id]
          
        ## add dataset dependencies: tool and reference datasets
        list_oeb_datasets = []
        for dataset in rel_oeb_datasets:
            list_oeb_datasets.append({
                "dataset_id": dataset
            })

        valid_participant_data["depends_on"] = {
            "tool_id": tool_id,
            "rel_dataset_ids": list_oeb_datasets
        }

        ## add data version
        valid_participant_data["version"] = str(version)

        ## add dataset contacts ids
        valid_participant_data["dataset_contact_ids"] = [contact["_id"] for contact in response["data"]["getContacts"] if contact["email"][0] in contacts]

        sys.stdout.write('Processed "' + str(participant_data["_id"]) + '"...\n')

        ## validate the newly annotated dataset against https://github.com/inab/benchmarking-data-model

        ## TODO: now, only local object is validated, as the validator does not have capability to check for remote foreign keys
        ## thus, FK errors are expected and allowed
        logging.info("\n==================================\n\t2. Validating participant dataset\n==================================\n")
        with suppress_stdout_stderr(): ## avoid printing to stdout the logs for checking the schemas
            uriLoad = jsonValidate.cacheJSONSchemas(os.path.join(data_model_dir, "json-schemas", "1.0.x"))
            schemaHash = {}
            jsonValidate.loadJSONSchemas(schemaHash,uriLoad)

        tmp = tempfile.NamedTemporaryFile()

        with open(tmp.name, 'w') as fp:
            json.dump(valid_participant_data, fp)
        jsonValidate.jsonValidate(schemaHash,uriLoad, tmp.name)
        tmp.close()
        
        logging.info("\n==================================\n\t Participant dataset OK\n==================================\n")
        
        return valid_participant_data
        

    def build_test_events(self, response, participant_data, tool_id, contacts, data_model_dir):
        
        logging.info("\n==================================\n\t3. Generating Test Events\n==================================\n")

        # initialize the array of test events
        test_events = []

        # an  new event object will be created for each of the challenge where the participants has taken part in 
        for challenge in participant_data["challenge_id"]:

            sys.stdout.write('Building object "' + str(challenge + "_testEvent_" + participant_data["participant_id"]) + '"...\n')

            event = {
                "_id": challenge + "_testEvent_" + participant_data["participant_id"],
                "_schema":"https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
                "action_type":"TestEvent",
            }

            ## add id of tool for the test event
            event["tool_id"] = tool_id

            ## add the oeb official id for the challenge
            data = response["data"]["getChallenges"]
            oeb_challenges = {}
            for oeb_challenge in data:
                oeb_challenges[oeb_challenge["_metadata"]["level_2:challenge_id"]] = oeb_challenge["_id"]

            try:
                event["challenge_id"] = oeb_challenges[challenge]
            except:
                logging.fatal("No challenge associated to " + challenge + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()   
            
            ## append incoming and outgoing datasets
            involved_data = []

            ## select input datasets related to the challenge
            rel_oeb_datasets = set()
            for dataset in [item for item in data if item["_id"] == event["challenge_id"] ]:
                for input_data in dataset["datasets"]:
                    rel_oeb_datasets.add( input_data["_id"] )

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
                "creation": str(datetime.now().replace(microsecond=0).isoformat()),
                "reception": str(datetime.now().replace(microsecond=0).isoformat())
            }

            ## add dataset contacts ids
            event["test_contact_ids"] = [contact["_id"] for contact in response["data"]["getContacts"] if contact["email"][0] in contacts]

            test_events.append(event)

        ## validate the new objects against https://github.com/inab/benchmarking-data-model

        ## TODO: now, only local object is validated, as the validator does not have capability to check for remote foreign keys
        ## thus, FK errors are expected and allowed
        logging.info("\n==================================\n\t4. Validating Test Events\n==================================\n")
        with suppress_stdout_stderr(): ## avoid printing to stdout the logs for checking the schemas
            uriLoad = jsonValidate.cacheJSONSchemas(os.path.join(data_model_dir, "json-schemas", "1.0.x"))
            schemaHash = {}
            jsonValidate.loadJSONSchemas(schemaHash,uriLoad)

        for element in test_events:

            tmp = tempfile.NamedTemporaryFile()

            with open(tmp.name, 'w') as fp:
                json.dump(element, fp)
            
            with suppress_stdout_stderr(): ## avoid printing to stdout the logs for checking, as it would get too verbose
                jsonValidate.jsonValidate(schemaHash,uriLoad, tmp.name)
            tmp.close()

            sys.stdout.write('Validated object "' + str(event["_id"]) + '"...\n')
            #  VALIDATEEE and add loggings !!
        logging.info("\n==================================\n\t Test Events OK\n==================================\n")
        
        return test_events




# Define a context manager to suppress stdout
class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in 
    Python, i.e. will suppress all print, even if the print originates in a 
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).      

    '''
    def __init__(self):
        # Open a pair of null files
        self.null_fds =  [os.open(os.devnull,os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1)  file descriptors.
        self.save_fds = [os.dup(1)]

    def __enter__(self):
        # Assign the null pointers to stdout 
        os.dup2(self.null_fds[0],1)

    def __exit__(self, *_):
        # Re-assign the real stdout back to (1)
        os.dup2(self.save_fds[0],1)
        # Close all file descriptors
        for fd in self.null_fds + self.save_fds:
            os.close(fd)
