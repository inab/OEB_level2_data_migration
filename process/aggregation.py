#!/usr/bin/env python

import logging
import sys
import os
from datetime import datetime, timezone
import datetime
import json
from .benchmarking_dataset import BenchmarkingDataset


class Aggregation():

    def __init__(self, schemaMappings):

        logging.basicConfig(level=logging.INFO)
        self.schemaMappings = schemaMappings

    def build_aggregation_datasets(self, response, stagedAggregationDatasets, aggregation_datasets, participant_data, assessment_datasets, community_id, tool_id, version, workflow_id):

        logging.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")

        valid_aggregation_datasets = []
        agg_by_id = dict()
        challenges = response["data"]["getChallenges"]

        # This is needed to properly compare ids later
        oeb_challenges = {}
        for challenge in challenges:
            _metadata = challenge.get("_metadata")
            if (_metadata is not None):
                oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge["_id"]
            else:
                oeb_challenges[challenge["acronym"]] = challenge["_id"]
        dataset_schema_uri = self.schemaMappings['Dataset']
        
        # get orig_id datasets
        orig_id_aggr = []
        for i in challenges:
            for elem in i['datasets']:
                orig_id_aggr.append(elem)
        
        for dataset in aggregation_datasets:
            orig_future_id = build_new_aggregation_id(dataset)
            agg_key = ""
            future_id = ""
            for d in orig_id_aggr:
                #check challenge
                if dataset['challenge_ids'][0] in d['orig_id']:
                    #check metrics
                    if (dataset['datalink']['inline_data']['visualization']['type'] == "2D-plot"):
                    	if(d['datalink']['inline_data']['visualization']['type'] == "2D-plot"):
                            if (dataset['datalink']['inline_data']['visualization']['x_axis'] in d['orig_id']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                sys.stdout.write(
                        'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                                break
                            elif (dataset['datalink']['inline_data']['visualization']['x_axis'] == d['datalink']['inline_data']['visualization']['x_axis']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                sys.stdout.write(
                        'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                                break
                            else:
                                agg_key = "orig_id"
                                future_id = build_new_aggregation_id(dataset)
                    #bar plot
                    else:
                        if (d['datalink']['inline_data']['visualization']['type'] == "bar-plot"):
                            if (dataset['datalink']['inline_data']['visualization']['metric'] in d['orig_id']):
                                agg_key = "_id"
                                future_id = d["_id"]
                                break
                            else:
                                agg_key = "orig_id"
                                future_id = orig_future_id
                        

            # check if assessment datasets already exist in OEB for the provided bench event id
            # in that case, there is no need to generate new datasets, just adding the new metrics to the existing one
            
            '''orig_future_id = build_new_aggregation_id(dataset)
            
            if orig_future_id in orig_id_aggr:
                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench... Adding new participant data\n')
                agg_key = "_id"
                future_id = dataset["_id"]
            else:
                agg_key = "orig_id"
                future_id = orig_future_id
            '''
            # This cache is very useful when assembling a bunch of new data from several participants
            valid_data = agg_by_id.get(future_id)
            if (valid_data is None) and future_id!=orig_future_id:
                valid_data = agg_by_id.get(orig_future_id)
            if valid_data is None:
                # Now, the aggregation datasets in the staging area
                for agg_data in stagedAggregationDatasets:
                    if future_id == agg_data[agg_key]:
                        valid_data = agg_data
                        break
                    
            if valid_data is None:
                # Last, the datasets associated to each challenge
                #dataset_challenge_ids = set(map(lambda challenge_id: oeb_challenges[challenge_id],dataset["challenge_ids"]))
                dataset_challenge_ids = set(oeb_challenges.values())
                for challenge in challenges:
                    if challenge["_id"] in dataset_challenge_ids:
                        for agg_data in challenge["datasets"]:
                            if (agg_key == ''): agg_key = "orig_id"
                            if future_id == agg_data[agg_key]:
                                valid_data = agg_data
                                break
                        if valid_data is not None:
                            break
            
            # If dataset could not be found, then store new one
            if valid_data is None:
                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is not registered in OpenEBench... Building new object\n')
                valid_data = new_aggregation(self,
                    response, dataset, assessment_datasets, community_id, version, workflow_id, dataset_schema_uri)
            else:

                # add new participant metrics to OEB aggregation dataset
                tool_name = participant_data["participant_id"]
                # insPos is the position within the array when it is inserted something
                insPos = None
                challenge_participants = valid_data["datalink"]["inline_data"]["challenge_participants"]
                for iPart, cPart in enumerate(challenge_participants):
                    if cPart["tool_id"] == tool_name:
                        insPos = iPart
                        break
                
                for participant in dataset["datalink"]["inline_data"]["challenge_participants"]:
                    participant_id = participant.get("participant_id")
                    if participant_id == tool_name:
                        participant["tool_id"] = participant.pop("participant_id")
                        
                        if insPos is not None:
                            challenge_participants[insPos] = participant
                        else:
                            challenge_participants.append(participant)
                        break

                # update modification date
                valid_data["dates"]["modification"] = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

                # add referenced assessment datasets ids
                oeb_metrics = {}
                for metric in response["data"]["getMetrics"]:
                    if metric["_metadata"] != None:
                        oeb_metrics[metric["_id"]
                                    ] = metric["_metadata"]["level_2:metric_id"]
                visualitzation_metric = ""
                visualization_x_axis =""
                visualization_y_axis = ""
                vis = valid_data['datalink']['inline_data']['visualization']
                if (vis.get("x_axis") is not None):
                    visualization_x_axis = valid_data["datalink"]["inline_data"]["visualization"]["x_axis"]
                    visualization_y_axis = valid_data["datalink"]["inline_data"]["visualization"]["y_axis"]
                else:
                    visualitzation_metric = valid_data["datalink"]["inline_data"]["visualization"]["metric"]
                    
                rel_dataset_ids = valid_data["depends_on"]["rel_dataset_ids"]
                rel_dataset_ids_set = set(map(lambda d: d['dataset_id'], rel_dataset_ids))
                
                for assess_element in assessment_datasets:
                    assess_element_id = assess_element["_id"]
                    # Fail early in case of repetition
                    if assess_element_id in rel_dataset_ids_set:
                        continue
                    
                    if assess_element["challenge_ids"][0] == valid_data["challenge_ids"][0]:
                        to_be_appended = False
                        
                        if oeb_metrics[assess_element["depends_on"]["metrics_id"]] in (visualization_x_axis,visualization_y_axis, visualitzation_metric):
                            to_be_appended = True
                        # also check for official oeb metrics, in case the aggregation dataset contains them
                        elif assess_element["depends_on"]["metrics_id"] in (visualization_x_axis, visualization_y_axis, visualitzation_metric):
                            to_be_appended = True
                        
                        if to_be_appended:
                            rel_dataset_ids_set.add(assess_element_id)
                            rel_dataset_ids.append({ "dataset_id": assess_element_id })

            valid_aggregation_datasets.append(valid_data)
            agg_by_id[valid_data["_id"]] = valid_data


        return valid_aggregation_datasets

    def build_aggregation_events(self, response, stagedEvents, aggregation_datasets, workflow_id):

        logging.info(
            "\n\t==================================\n\t6. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of events
        aggregation_events = []

        data = stagedEvents + response["data"]["getTestActions"]

        for dataset in aggregation_datasets:

            # if the aggregation dataset is already in OpenEBench, it should also have an associated aggregation event
            event = None
            if dataset["_id"].startswith("OEB"):

                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench...\n')
                for action in data:

                    if action["action_type"] == "AggregationEvent" and action["challenge_id"] == dataset["challenge_ids"][0]:
                        #check if aggregation dataset id is an outgoing in the event involved data
                        for related_data in action["involved_datasets"]:
                            if related_data["role"] == "outgoing" and related_data["dataset_id"] == dataset["_id"]:

                                event = action
                                sys.stdout.write(
                                    'Adding new metadata to TestAction "' + str(event["_id"]) + '"\n')
                                break
                    # break loop if event is already found
                    if event:
                        break       

                # update the event modification date
                event["dates"]["modification"] = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

                # add referenced assessment datasets ids
                for agg_dataset_id in (item for item in dataset["depends_on"]["rel_dataset_ids"] if not item["dataset_id"].startswith('OEB')):
                    event["involved_datasets"].append(
                        {"dataset_id": agg_dataset_id["dataset_id"], "role": "incoming"})

                aggregation_events.append(event)

            else:  # if datset is not in oeb a  new event object will be created
                event = {
                    "_id": dataset["_id"] + "_Event",
                    "_schema": self.schemaMappings['TestAction'],
                    "action_type": "AggregationEvent",
                }

                sys.stdout.write(
                    'Building Event object for aggregation "' + str(dataset["_id"]) + '"...\n')

                # add id of workflow for the test event
                event["tool_id"] = workflow_id

                # add the oeb official id for the challenge (which is already in the aggregation dataset)
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
                    "creation": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                    "reception": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
                }

                # add challenge managers as aggregation dataset contacts ids
                data_contacts = []
                for challenge in response["data"]["getChallenges"]:
                    if challenge["_id"] in dataset["challenge_ids"]:
                        data_contacts.extend(
                            challenge["challenge_contact_ids"])

                event["test_contact_ids"] = data_contacts
                
                aggregation_events.append(event)

        return aggregation_events


def build_new_aggregation_id(dataset):
    d = dataset["datalink"]["inline_data"]["visualization"]
    if (d.get("metric") is not None):
        return dataset["_id"] + "_" + dataset["datalink"]["inline_data"]["visualization"]["metric"]
    
    else :
        metrics = [dataset["datalink"]["inline_data"]["visualization"]["x_axis"], 
                   dataset["datalink"]["inline_data"]["visualization"]["y_axis"]]
    
        return dataset["_id"] + "_" + metrics[0] + "+" + metrics[1]

def new_aggregation(self, response, dataset, assessment_datasets, community_id, version, workflow_id, dataset_schema_uri):

    # initialize new dataset object
    metrics = ""
    d = dataset["datalink"]["inline_data"]["visualization"]
    if (d.get("metric") is not None):
        metrics = dataset["datalink"]["inline_data"]["visualization"]['metric']
    else:
        metrics = dataset["datalink"]["inline_data"]["visualization"]["x_axis"]+ " - " + dataset["datalink"]["inline_data"]["visualization"]["y_axis"]
        
    valid_data = {
        "_id": build_new_aggregation_id(dataset),
        "type": "aggregation"
    }

    # add dataset visibility. AGGREGATION DATASETS ARE ALWAYS EXPECTED TO BE PUBLIC
    valid_data["visibility"] = "public"

    # add name and description, if workflow did not provide them
    if "name" not in dataset:
        valid_data["name"] = "Summary dataset for challenge: " + \
            dataset["challenge_ids"][0] + ". Metrics: " + metrics
    else:
        valid_data["name"] = dataset["name"]
    if "description" not in dataset:
        valid_data["description"] = "Summary dataset that aggregates all results from participants in challenge: " + \
            dataset["challenge_ids"][0] + ". Metrics: " + metrics
    else:
        valid_data["description"] = dataset["description"]

    # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.

    challenges = response["data"]["getChallenges"]

    oeb_challenges = {}
    for challenge in challenges:
        _metadata = challenge.get("_metadata")
        if (_metadata is not None):
            oeb_challenges[challenge["_metadata"]["level_2:challenge_id"]] = challenge["_id"]
        else:
            oeb_challenges[challenge["acronym"]] = challenge["_id"]

    # replace dataset related challenges with oeb challenge ids
    execution_challenges = []
    for challenge_id in dataset["challenge_ids"]:
        try:
            if challenge_id.startswith("OEB"):
                execution_challenges.append(challenge_id)
            else:
                execution_challenges.append(oeb_challenges[challenge_id])
        except:
            logging.info("No challenges associated to " + challenge_id +
                         " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
            logging.info(dataset["_id"] + " not processed")
            sys.exit()

    valid_data["challenge_ids"] = execution_challenges

    # add referenced assessment datasets ids
    rel_data = []
    oeb_metrics = {}
    for metric in response["data"]["getMetrics"]:
        if metric["_metadata"] != None:
            oeb_metrics[metric["_id"]] = metric["_metadata"]["level_2:metric_id"]
        else:
            oeb_challenges[challenge["acronym"]] = challenge["_id"]

    for assess_element in assessment_datasets:
        
        try:
            vis = dataset["datalink"]["inline_data"]["visualization"]

            if (vis.get("metric") is None):
                if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                #check for not 'oeb' challenges ids, in case the datasets is still not uploaded
                elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[dataset["challenge_ids"][0]]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[dataset["challenge_ids"][0]]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                # also check for official oeb metrics, in case the aggregation dataset contains them
                elif assess_element["depends_on"]["metrics_id"] == vis["x_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif assess_element["depends_on"]["metrics_id"] == vis["y_axis"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif vis["metric"].upper() in assess_element["_id"].upper() and dataset["challenge_ids"][0] in assess_element["_id"]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
            else:
                if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == oeb_challenges[dataset["challenge_ids"][0]]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif assess_element["depends_on"]["metrics_id"] == vis["metric"] and assess_element["challenge_ids"][0] == dataset["challenge_ids"][0]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
                elif vis["metric"].upper() in assess_element["_id"].upper() and dataset["challenge_ids"][0] in assess_element["_id"]:
                    rel_data.append({"dataset_id": assess_element["_id"]})
        except:
            continue

    # add data registration dates
    valid_data["dates"] = {
        "creation":datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "modification":datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
    }

    # add assessment metrics values, as inline data
    datalink = dataset["datalink"]
    for participant in datalink["inline_data"]["challenge_participants"]:
        participant["tool_id"] = participant.pop("participant_id")

    datalink["schema_url"] = "https://raw.githubusercontent.com/inab/OpenEBench_scientific_visualizer/master/benchmarking_data_model/aggregation_dataset_json_schema.json"

    valid_data["datalink"] = datalink

    # add Benchmarking Data Model Schema Location
    valid_data["_schema"] = self.schemaMappings["Dataset"]

    # add OEB id for the community
    valid_data["community_ids"] = [community_id]

    # add dataset dependencies: metric id, tool and reference datasets
    valid_data["depends_on"] = {
        "tool_id": workflow_id,
        "rel_dataset_ids": rel_data,
    }

    # add data version
    valid_data["version"] = str(version)

    # add challenge managers as aggregation dataset contacts ids
    data_contacts = []
    for challenge in challenges:
        if challenge["_id"] in valid_data["challenge_ids"]:
            data_contacts.extend(challenge["challenge_contact_ids"])

    valid_data["dataset_contact_ids"] = data_contacts

    sys.stdout.write('Processed "' + str(dataset["_id"]) + '"...\n')

    return valid_data

