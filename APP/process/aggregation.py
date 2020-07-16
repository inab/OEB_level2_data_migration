import logging
import sys, os
from datetime import datetime, timezone
import json
from process.benchmarking_dataset import benchmarking_dataset

class aggregation():

    def __init__(self):

        logging.basicConfig(level=logging.INFO)


    def build_aggregation_datasets(self, response, aggregation_datasets, participant_data, assessment_datasets, community_id, tool_id, version, workflow_id):

        logging.info("\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")

        valid_aggregation_datasets = []
        data = response["data"]["getChallenges"]
        for dataset in aggregation_datasets:

            # check if assessment datasets already exist in OEB for the provided bench event id
            # in that case, there is no need to generate new datasets, just adding the new metrics to the existing one
            if dataset["_id"].startswith("OEB"):
                sys.stdout.write('Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')

                for challenge in [item for item in data if item["_id"] in dataset["challenge_ids"] ]:
                    for agg_data in challenge["datasets"]:
                        if agg_data["_id"] == dataset["_id"]:
                            valid_data = agg_data
                            break
                    break
                
                if valid_data == None:
                    sys.stdout.write('Dataset "' + str(dataset["_id"]) + '" is not registered in OpenEBench... Building new object\n')
                    new_aggregation(response, dataset, assessment_datasets, community_id, version, workflow_id)
                    continue

                ## add new participant metrics to OEB aggregation dataset
                tool_name = participant_data["participant_id"]
                for participant in dataset["datalink"]["inline_data"]["challenge_participants"]:

                    if participant["participant_id"] == tool_name:
                        participant["tool_id"] = participant.pop("participant_id")
                        valid_data["datalink"]["inline_data"]["challenge_participants"].append(participant)
                        break

                ## update modification date
                valid_data["dates"]["modification"] = str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())

                ## add referenced assessment datasets ids
                oeb_metrics={}
                for metric in response["data"]["getMetrics"]:
                    if metric["_metadata"] != None:
                        oeb_metrics[metric["_id"]] = metric["_metadata"]["level_2:metric_id"]

                for assess_element in assessment_datasets:
                    
                    try:
                        if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == valid_data["datalink"]["inline_data"]["visualization"]["x_axis"] and assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                            valid_data["depends_on"]["rel_dataset_ids"].append({"dataset_id": assess_element["_id"]})
                        elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == valid_data["datalink"]["inline_data"]["visualization"]["y_axis"] and assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                            valid_data["depends_on"]["rel_dataset_ids"].append({"dataset_id": assess_element["_id"]})
                        #also check for official oeb metrics, in case the aggregation dataset contains them
                        elif assess_element["depends_on"]["metrics_id"] == valid_data["datalink"]["inline_data"]["visualization"]["x_axis"] and assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                            valid_data["depends_on"]["rel_dataset_ids"].append({"dataset_id": assess_element["_id"]})
                        elif assess_element["depends_on"]["metrics_id"] == valid_data["datalink"]["inline_data"]["visualization"]["y_axis"] and assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                            valid_data["depends_on"]["rel_dataset_ids"].append({"dataset_id": assess_element["_id"]})
                    except:
                        continue

                valid_aggregation_datasets.append(valid_data)

            else: # if dataset does not have oeb id, build a new one

                sys.stdout.write('Building object "' + str(dataset["_id"]) + '"...\n')
                valid_data = new_aggregation(response, dataset, assessment_datasets, community_id, version, workflow_id)

                valid_aggregation_datasets.append(valid_data)
               
        return valid_aggregation_datasets
    
    def build_aggregation_events(self, response, aggregation_datasets, workflow_id):
        
        logging.info("\n\t==================================\n\t6. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of events
        aggregation_events = []

        data = response["data"]["getTestActions"]

        for dataset in aggregation_datasets:
            
            # if the aggregation dataset is already in OpenEBench, it should also have an associated aggregation event
            if dataset["_id"].startswith("OEB"):

                sys.stdout.write('Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench...\n')
                for action in data:

                    if action["action_type"] == "AggregationEvent" and action["challenge_id"] == dataset["challenge_ids"][0]:
                        event = action
                        sys.stdout.write('Adding new metadata to TestAction "' + str(event["_id"]) + '"\n')
                        break
                
                #update the event modification date
                event["dates"]["modification"] = str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())

                ## add referenced assessment datasets ids
                for agg_dataset_id in (item for item in dataset["depends_on"]["rel_dataset_ids"] if not item["dataset_id"].startswith('OEB')):
                    event["involved_datasets"].append( { "dataset_id": agg_dataset_id["dataset_id"], "role": "incoming"} )
                
                aggregation_events.append(event)
                
            else: # if datset is not in oeb a  new event object will be created
                event = {
                    "_id": dataset["_id"] + "_Event",
                    "_schema":"https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
                    "action_type":"AggregationEvent",
                }

                sys.stdout.write('Building Event object for aggregation "' + str(dataset["_id"]) + '"...\n')

                ## add id of workflow for the test event
                event["tool_id"] = workflow_id

                ## add the oeb official id for the challenge (which is already in the aggregation dataset)
                event["challenge_id"] = dataset["challenge_ids"][0]
                
                ## append incoming and outgoing datasets
                involved_data = []

                ## include the incomning datasets related to the event
                for data_id in dataset["depends_on"]["rel_dataset_ids"]:
                    data_id["role"] = "incoming"
                    involved_data.append(data_id)
                
                ## ad the outgoing assessment data
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

                ## add challenge managers as aggregation dataset contacts ids
                data_contacts = []
                for challenge in response["data"]["getChallenges"]:
                    if challenge["_id"] in dataset["challenge_ids"]:
                        data_contacts.extend(challenge["challenge_contact_ids"])

                event["test_contact_ids"] = data_contacts

                aggregation_events.append(event)
                
        return aggregation_events


def new_aggregation(response, dataset, assessment_datasets, community_id, version, workflow_id):

    ## initialize new dataset object
    valid_data = {
        "_id": dataset["_id"],
        "type": "aggregation"
    }
    
    # add dataset visibility. AGGREGATION DATASETS ARE ALWAYS EXPECTED TO BE PUBLIC
    valid_data["visibility"] = "public"

    # add name and description, if workflow did not provide them
    if "name" not in dataset:
        valid_data["name"] = "Summary dataset for challenge: " + dataset["challenge_ids"][0] 
    else:
        valid_data["name"] = dataset["name"]
    if "description" not in dataset:
        valid_data["description"] = "Summary dataset that aggregates all results from participants in challenge: " + dataset["challenge_ids"][0]
    else:
        valid_data["description"] = dataset["description"]

    # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.

    data = response["data"]["getChallenges"]

    oeb_challenges = {}
    for challenge in data:
        oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge["_id"]

    ## replace dataset related challenges with oeb challenge ids
    execution_challenges = []  
    for id in dataset["challenge_ids"]:
        try:
            if id.startswith("OEB"):
                execution_challenges.append(id)
            else:
                execution_challenges.append(oeb_challenges[id])
        except:
            logging.info("No challenges associated to " + id + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
            logging.info(dataset["_id"] +  " not processed")
            sys.exit()

    valid_data["challenge_ids"] = execution_challenges
    
    ## add referenced assessment datasets ids
    rel_data = []
    oeb_metrics={}
    for metric in response["data"]["getMetrics"]:
        if metric["_metadata"] != None:
            oeb_metrics[metric["_id"]] = metric["_metadata"]["level_2:metric_id"]

    for assess_element in assessment_datasets:
        
        try:
            if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == dataset["datalink"]["inline_data"]["visualization"]["x_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                rel_data.append({"dataset_id": assess_element["_id"]})
            elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == dataset["datalink"]["inline_data"]["visualization"]["y_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                rel_data.append({"dataset_id": assess_element["_id"]})
            #also check for official oeb metrics, in case the aggregation dataset contains them
            elif assess_element["depends_on"]["metrics_id"] == dataset["datalink"]["inline_data"]["visualization"]["x_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                rel_data.append({"dataset_id": assess_element["_id"]})
            elif assess_element["depends_on"]["metrics_id"] == dataset["datalink"]["inline_data"]["visualization"]["y_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                rel_data.append({"dataset_id": assess_element["_id"]})
        except:
            continue

    # add data registration dates
    valid_data["dates"] = {
            "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            "modification": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
        }
    
    # add assessment metrics values, as inline data
    datalink = dataset["datalink"]
    for participant in datalink["inline_data"]["challenge_participants"]:
        participant["tool_id"] = participant.pop("participant_id")
    
    datalink["schema_url"] = "https://raw.githubusercontent.com/inab/OpenEBench_scientific_visualizer/master/benchmarking_data_model/aggregation_dataset_json_schema.json"

    valid_data["datalink"] =  datalink

    ## add Benchmarking Data Model Schema Location
    valid_data["_schema"] = "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset"

    ## add OEB id for the community
    valid_data["community_ids"] = [community_id]
    
    ## add dataset dependencies: metric id, tool and reference datasets
    valid_data["depends_on"] = {
        "tool_id": workflow_id,
        "rel_dataset_ids": rel_data,
    }

    ## add data version
    valid_data["version"] = str(version)

    ## add challenge managers as aggregation dataset contacts ids
    data_contacts = []
    for challenge in data:
        if challenge["_id"] in valid_data["challenge_ids"]:
            data_contacts.extend(challenge["challenge_contact_ids"])

    valid_data["dataset_contact_ids"] = data_contacts

    sys.stdout.write('Processed "' + str(dataset["_id"]) + '"...\n')
    
    return valid_data
