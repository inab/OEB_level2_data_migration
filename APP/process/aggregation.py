#!/usr/bin/env python
import logging
import sys
import os
from datetime import datetime, timezone
import datetime
import json
from .benchmarking_dataset import BenchmarkingDataset
from itertools import combinations


class Aggregation():
    def __init__(self, schemaMappings):

        logging.basicConfig(level=logging.INFO)
        self.schemaMappings = schemaMappings
        
        
    def build_aggregation_datasets(self, response, valid_participant_data, 
                                   valid_assessments_datasets, community_id, tool_id, version, workflow_id):
        logging.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")

        valid_aggregation_datasets = []
        
        #Group assessments(metrics) for the same challenge:                  
        valid_results = []
        
        for i in valid_participant_data['challenge_ids']:
            challenge_results = dict()
            challenge_results['metrics'] = []
            challenge_results["challenge"] = i
            for j in valid_assessments_datasets:
                if (j['challenge_ids'][0] == i):
                    r = dict()
                    r['metrics_id'] = j['depends_on']['metrics_id']
                    r['metrics_name'] = j['_id']
                    r['assess_id'] = j['_id']
                    r.update(j['datalink']['inline_data'])
                    challenge_results['metrics'].append(r)
            valid_results.append(challenge_results)
        
        print(valid_results)
        
        aggregation_datasets_existed = []
        ##Check if aggregation datasets already exists in OEB for each challenge
        #Get list of aggregation datasets whose challenge belong to that benchmarking_event
        aggregation_datasets = response["data"]["getChallenges"]
        
        for assessment in valid_assessments_datasets:
            for i in aggregation_datasets:
                if i['_id'] == assessment['challenge_ids'][0]:
                    #exists an aggregation dataset for that challenge
                    if(i['datasets']):
                        for elem in i['datasets']:
                            if elem not in valid_aggregation_datasets:
                                aggregation_datasets_existed.append(elem)
                    break
                
              
        #Once we have the list of existed aggregation datasets, add assessments to them
        for elem in valid_results:
            for i in aggregation_datasets_existed:
               if (elem['challenge'] == i['challenge_ids'][0]):
                   participant_results_challenge = dict()
                       
                   metrics_id = i['datalink']['inline_data']['visualization']
                   
                   #As not always in visualitzation object, metrics_id is a real id. Sometimes is its metrics name
                   if not metrics_id['x_axis'].startswith("OEB"):
                           for metric in elem['metrics']:
                               if metrics_id['x_axis'] in metric['metrics_name']:
                                   metrics_id['x_axis'] = metric['metrics_id']
                                   
                   if not metrics_id['y_axis'].startswith("OEB"):
                           for metric in elem['metrics']:
                               if metrics_id['y_axis'] in metric['metrics_name']:
                                   metrics_id['y_axis'] = metric['metrics_id']
                       
                   
                   for metric in elem['metrics']:
                        if metrics_id['x_axis'] == metric['metrics_id']:
                           participant_results_challenge["metric_x"] = metric['value']
                           participant_results_challenge["stderr_x"] = metric['error']
                           
                           assess_dataset = {"dataset_id": metric['assess_id']}
                           i['depends_on']['rel_dataset_ids'].append(assess_dataset)
                       
                           if ("tool_id" not in participant_results_challenge.keys()):
                               participant_results_challenge['tool_id'] = valid_participant_data["_id"].split(":")[-1]
                       
                        elif  metrics_id['y_axis'] == metric['metrics_id']:
                           participant_results_challenge["metric_y"] = metric['value']
                           participant_results_challenge["stderr_y"] = metric['error']
                           
                           assess_dataset = {"dataset_id": metric['assess_id']}
                           i['depends_on']['rel_dataset_ids'].append(assess_dataset)
                           
                           if ("tool_id" not in participant_results_challenge.keys()):
                               participant_results_challenge['tool_id'] = valid_participant_data["_id"].split(":")[-1]
                  
                   #update also schema, as still some datasets refer to old schema
                   i['_schema'] =  self.schemaMappings["Dataset"]
                   #update modification date
                   i["dates"]["modification"] =  datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
                   
                   if participant_results_challenge not in i['datalink']['inline_data']['challenge_participants']:
                       i['datalink']['inline_data']['challenge_participants'].append(participant_results_challenge)
                   valid_aggregation_datasets.append(i)
                   break    
        
        
        #Check if all participant challenges have already an aggregation dataset
        challenges_done = []
        for j in valid_aggregation_datasets:
            challenges_done.append(j['challenge_ids'][0])
        
        for i in valid_participant_data['challenge_ids']:
            #Challege does not have an aggregation, then build it
            if not i in challenges_done:
                sys.stdout.write(
                    'Challenge "' + str(i) + '" has not an aggregation dataset in OpenEBench... Building new object\n')
                #Build as many aggregations datasets as combinations of metrics
                #Combinatorial: Cm,n = (m/n) = m!/n!(m-n)! where always n =2
                #Get all metrics results for that challenge
                for m in valid_results:
                    if (m['challenge'] == i): 
                       comb = combinations(m['metrics'], 2) 
                       for j in list(comb):
                           obj_results = dict()
                           obj_visualitzation = {
                               "type": "2D-plot",
                               "x_axis": j[0]['metrics_id'],
                               "y_axis": j[1]['metrics_id']
                            }
                           obj_results = { "metric_x" : j[0]['value'],
                                           "metric_y" : j[1]['value'],
                                           "stderr_x" : j[0]['error'],
                                           "stderr_y" : j[1]['error'],
                                           "tool_id" : valid_participant_data["_id"].split(":")[-1] }    
                           rel_dataset = [{"dataset_id":j[0]['assess_id']}, {"dataset_id":j[1]['assess_id']}]
                           # add challenge managers as aggregation dataset contacts ids
                           data_contacts = []
                           for challenge in aggregation_datasets:
                              if challenge["_id"] == i:
                                  data_contacts.extend(challenge["challenge_contact_ids"])
                
                           new_aggregation = {
                                    "_id": j[0]['metrics_name']+j[1]['metrics_name']+"_Aggregation",
                                    "_schema": self.schemaMappings["Dataset"],
                                    "challenge_ids":[i],
                                    "community_ids": community_id,
                                    "type": "aggregation",
                                    "datalink": {
                                            "inline_data":{
                                                    "challenge_participants":[obj_results],
                                                    "visualitzation": obj_visualitzation
                                            }
                                    },
                                    "dataset_contact_ids": data_contacts,
                                    'dates': {
                                        'creation': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                                        'modification': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
                                    }, 
                                    "depends_on": {
                                            "rel_dataset_ids": rel_dataset,
                                            "tool_id": workflow_id
                                            
                                    },
                                    "description": "Summary dataset with information about challenge "+ i+" (e.g. input/output datasets, metrics...)",
                                    "name" : "Summary dataset for challenge: "+i,
                                    "version" : str(version),
                                    "visibility" : "public"
                                }
                           valid_aggregation_datasets.append(new_aggregation)
                
              
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
                event['_schema'] = self.schemaMappings["TestAction"]
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

        
        
        
        
    