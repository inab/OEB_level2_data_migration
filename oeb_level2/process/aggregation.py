#!/usr/bin/env python

import inspect
import logging
import sys
import os
import datetime
import json

from .benchmarking_dataset import BenchmarkingDataset
from ..utils.catalogs import (
    DatasetsCatalog,
    gen_challenge_assessment_metrics_dict,
    match_metric_from_label,
    TestActionsCatalog,
)

from typing import (
    NamedTuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        Sequence,
        MutableSequence,
        Tuple,
    )
    from .assessment import AssessmentTuple
    from ..utils.migration_utils import OpenEBenchUtils

class AggregationTuple(NamedTuple):
    aggregation_dataset: "Mapping[str, Any]"
    at_l: "Sequence[AssessmentTuple]"


class Aggregation():

    def __init__(self, schemaMappings, migration_utils: "OpenEBenchUtils"):

        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )
        self.schemaMappings = schemaMappings
        self.level2_min_validator = migration_utils.level2_min_validator
    
    def build_aggregation_datasets(
        self,
        community_id: "str",
        community_acronym: "str",
        min_aggregation_datasets,
        challenges_agg_graphql: "Sequence[Mapping[str, Any]]",
        metrics_agg_graphql: "Sequence[Mapping[str, Any]]",
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        valid_test_events: "Sequence[Tuple[str, Any]]",
        valid_metrics_events: "Sequence[Tuple[str, Any]]",
    ) -> "Sequence[AggregationTuple]":
    
        self.logger.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")
        
        # Grouping future assessment dataset tuples by challenge
        ass_c_dict = {}
        for ass_t in valid_assessment_tuples:
            ass_d = ass_t.assessment_dataset
            for challenge_id in ass_d.get("challenge_ids", []):
                ass_c_dict.setdefault(challenge_id, []).append(ass_t)
        
        valid_aggregation_datasets = []
        valid_agg_d_dict = {}
        
        # First, let's analyze all existing aggregation datasets
        # nested in the challenges
        agg_challenges = { }
        agg_datasets = { }
        should_exit_ch = False
        for agg_ch in challenges_agg_graphql:
            # Garrayo's school label
            challenge_id = agg_ch["_id"]
            challenge_label = agg_ch.get("_metadata", {}).get("level_2:challenge_id")
            if challenge_label is None:
                challenge_label = agg_ch.get("orig_id")
                # Very old school label
                if challenge_label is not None:
                    if challenge_label.startswith(community_acronym + ':'):
                        challenge_label = challenge_label[len(community_acronym) + 1]
                else:
                    challenge_label = challenge_id
            
            coll_ch = agg_challenges.get(challenge_label)
            if coll_ch is not None:
                self.logger.critical(f"Challenge collision: label {challenge_label}, challenges {coll_ch['_id']} and {challenge_id}. Please contact OpenEBench support to report this inconsistency")
                should_exit_ch = True
                continue
            
            # Pre-filter challenge categories of this challenge
            # Assessment ones
            ass_cat = []
            # Aggregation ones
            agg_cat = []
            for m_cat in agg_ch.get("metrics_categories", []):
                d_category = m_cat.get("category")
                if d_category == "assessment":
                    ass_cat.append(m_cat)
                elif d_category == "aggregation":
                    agg_cat.append(m_cat)
            
            d_catalog = DatasetsCatalog(
                logger=self.logger,
                level2_min_validator=self.level2_min_validator,
                metrics_graphql=metrics_agg_graphql,
                community_acronym=community_acronym,
                challenge=agg_ch,
            )
            
            # Start indexing input datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("input_datasets", []),
                d_categories=ass_cat,
            )
            # public reference datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("public_reference_datasets", []),
                d_categories=ass_cat,
            )
            # Metrics reference datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("metrics_reference_datasets", []),
                d_categories=ass_cat,
            )
            
            # Index stored participant datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("participant_datasets", []),
                d_categories=ass_cat,
            )
            
            # And also index the future participant datasets involved in this challenge
            d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.pt.participant_dataset, ass_c_dict.get(challenge_id, [])),
                d_categories=ass_cat,
            )
            
            # Then the assessment datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("assessment_datasets", []),
                d_categories=ass_cat,
            )
            
            # And also index the future assessment datasets involved in this challenge
            d_catalog.merge_datasets(
                raw_datasets=map(lambda ass_t: ass_t.assessment_dataset, ass_c_dict.get(challenge_id, [])),
                d_categories=ass_cat,
            )
            
            # The test actions catalog, which depends on the dataset ones
            ta_catalog = TestActionsCatalog(
                logger=self.logger,
                d_catalog=d_catalog,
            )
            
            # Let's index the TestEvent actions, as they can provide
            # a very trustable way to rescue the labels needed to
            # regenerate an aggregation dataset from their assessment
            # components
            
            # Merging the TestEvent test actions
            ta_catalog.merge_test_actions(agg_ch.get("event_test_actions", []))
            
            # newly added TestEvent involved in this challenge
            ta_catalog.merge_test_actions(filter(lambda te: te['challenge_id'] == challenge_id, valid_test_events))
            
            # Let's index the MetricsEvent actions, as they can provide
            # the way to link to the TestEvent needed to rescue the labels
            # needed to regenerate an aggregation dataset from their assessment
            # components
            
            # Merging the MetricsEvent test actions
            ta_catalog.merge_test_actions(agg_ch.get("metrics_test_actions", []))
            
            # newly added MetricsEvent
            ta_catalog.merge_test_actions(filter(lambda me: me['challenge_id'] == challenge_id, valid_metrics_events))
            
            # Now, let's index the existing aggregation datasets, and
            # their relationship. And check them, BTW
            
            # Then the assessment datasets
            d_catalog.merge_datasets(
                raw_datasets=agg_ch.get("aggregation_datasets", []),
                d_categories=agg_cat,
            )
            
            # Last, but not the least important
            agg_challenges[challenge_label] = (agg_ch, d_catalog, ta_catalog)
        
        if should_exit_ch:
            self.logger.critical("Some challenges have collisions at their label level. Please ask the team to fix the mess")
            sys.exit(4)

        # Now it is time to process all the new or updated aggregation datasets
        valid_aggregation_tuples = []
        for dataset in min_aggregation_datasets:
            # TODO
            self.logger.critical("Are you ready to implement minimal aggregation dataset (re)generation?")
            sys.exit(1)
        
        return valid_aggregation_tuples

    def build_aggregation_datasets_orig(
        self,
        challenges_graphql,
        metrics_graphql,
        stagedAggregationDatasets,
        min_aggregation_datasets,
        tool_name,
        valid_assessment_tuples: "Sequence[AssessmentTuple]",
        community_id,
        community_acronym,
        tool_id,
        version: "str",
        workflow__tool_id
    ):

        self.logger.info(
            "\n\t==================================\n\t5. Processing aggregation datasets\n\t==================================\n")

        valid_aggregation_datasets = []
        agg_by_id = dict()

        # This is needed to properly compare ids later
        oeb_challenges = {}
        for challenge_graphql in challenges_graphql:
            _metadata = challenge_graphql.get("_metadata")
            # TODO: detect collisions???
            if (_metadata is not None):
                oeb_challenges[_metadata["level_2:challenge_id"]] = challenge_graphql["_id"]
            else:
                oeb_challenges[challenge_graphql["acronym"]] = challenge_graphql["_id"]
        dataset_schema_uri = self.schemaMappings['Dataset']
        
        # The same, at the level of metrics
        oeb_metrics = {}
        for metric_graphql in metrics_graphql:
            metric_metadata = metric_graphql.get('_metadata')
            # BEWARE! There could be collisions!!!!!!!!
            if metric_metadata != None:
                
                oeb_metrics[metric_graphql["_id"]] = metric_metadata["level_2:metric_id"]
            else:
                metric_orig_id = metric_graphql["orig_id"]
                if not metric_orig_id.startswith(community_acronym + ":") and (":" in metric_orig_id):
                    self.logger.warning(f"Metrics {metric_graphql['_id']} has as original id {metric_orig_id}, community has as acronym {community_acronym}")
                # BEWARE! We could be taking into account other communities!?!?!?!
                oeb_metrics[metric_graphql["_id"]] = metric_orig_id[metric_orig_id.find(":") + 1 :]

        # get orig_id datasets
        orig_id_aggr = []
        for i in challenges_graphql:
            for elem in i['datasets']:
                orig_id_aggr.append(elem)
        
        for dataset in min_aggregation_datasets:
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
                for challenge in challenges_graphql:
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
                valid_data = self.new_aggregation(
                    oeb_challenges,
                    oeb_metrics,
                    dataset,
                    valid_assessment_datasets,
                    community_id,
                    version,
                    workflow_tool_id
                )
            else:

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
                
                for assess_element in valid_assessment_datasets:
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

    def build_aggregation_events_orig(self, challenges_graphql, all_events, min_aggregation_datasets, workflow_tool_id):

        self.logger.info(
            "\n\t==================================\n\t6. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of events
        aggregation_events = []

        for dataset in min_aggregation_datasets:

            # if the aggregation dataset is already in OpenEBench, it should also have an associated aggregation event
            event = None
            if dataset["_id"].startswith("OEB"):

                sys.stdout.write(
                    'Dataset "' + str(dataset["_id"]) + '" is already in OpenEBench...\n')
                for action in all_events:

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
                event["tool_id"] = workflow_tool_id

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
                for challenge in challenges_graphql:
                    if challenge["_id"] in dataset["challenge_ids"]:
                        data_contacts.extend(
                            challenge["challenge_contact_ids"])

                event["test_contact_ids"] = data_contacts
                
                aggregation_events.append(event)

        return aggregation_events

    def build_aggregation_events(
        self,
        valid_aggregation_tuples: "Sequence[AggregationTuple]",
        challenges_graphql: "Sequence[Mapping[str, Any]]",
        workflow_tool_id: "str"
    ) -> "Sequence[Mapping[str, Any]]":

        self.logger.info(
            "\n\t==================================\n\t4. Generating Aggregation Events\n\t==================================\n")

        # initialize the array of test events
        aggregation_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for agt in valid_aggregation_tuples:
            dataset = agt.aggregation_dataset
            
            orig_id = dataset.get("orig_id",dataset["_id"])
            event_id = orig_id + "_Event"
            event = {
                "_id": event_id,
                "_schema": self.schemaMappings["TestAction"],
                "action_type": "AggregationEvent",
            }

            self.logger.info(
                'Building Event object for aggregation "' + str(dataset["_id"]) + '"...')

            # add id of tool for the test event
            event["tool_id"] = workflow_tool_id

            # add the oeb official id for the challenge (which is already in the dataset)
            event["challenge_id"] = dataset["challenge_ids"][0]

            # append incoming and outgoing datasets
            involved_data = []

            # include the incoming datasets related to the event
            for data_id in dataset["depends_on"]["rel_dataset_ids"]:
                involved_data.append({
                    "dataset_id": data_id["dataset_id"],
                    "role": "incoming"
                })
            # add the outgoing assessment data
            involved_data.append({
                "dataset_id": dataset["_id"],
                "role": "outgoing"
            })

            event["involved_datasets"] = involved_data
            
            # add data registration dates
            event["dates"] = {
                "creation": str(datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()),
                "reception": str(datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat())
            }

            # add challenge managers as aggregation dataset contacts ids
            data_contacts = []
            for challenge in challenges_graphql:
                if challenge["_id"] in dataset["challenge_ids"]:
                    data_contacts.extend(
                        challenge["challenge_contact_ids"])
            event["test_contact_ids"] = data_contacts

            aggregation_events.append(event)

        return aggregation_events


    def new_aggregation(
        self,
        oeb_challenges,
        challenge_contacts,
        oeb_metrics,
        min_dataset,
        valid_assessment_datasets,
        community_id,
        version: "str",
        workflow_tool_id
    ):

        # initialize new dataset object
        d = min_dataset["datalink"]["inline_data"]["visualization"]
        metrics_label = d.get("metric")
        if metrics_label is None:
            metrics_label = d["x_axis"]+ " - " + d["y_axis"]
            # TODO: Should we warn about this???
            
        valid_data = {
            "_id": build_new_aggregation_id(min_dataset),
            "type": "aggregation"
        }

        # add dataset visibility. AGGREGATION DATASETS ARE ALWAYS EXPECTED TO BE PUBLIC
        valid_data["visibility"] = "public"

        # add name and description, if workflow did not provide them
        valid_data_name = min_dataset.get("name")
        if valid_data_name is None:
            valid_data_name = "Summary dataset for challenge: " + \
                min_dataset["challenge_ids"][0] + ". Metrics: " + metrics_label
        valid_data["name"] = valid_data_name
        
        valid_data_description = min_dataset.get("description")
        if valid_data_description is None:
            valid_data_description = "Summary dataset that aggregates all results from participants in challenge: " + \
                min_dataset["challenge_ids"][0] + ". Metrics: " + metrics_label
        valid_data["description"] = valid_data_description

        # replace dataset related challenges with oeb challenge ids
        execution_challenges = []
        for challenge_id in min_dataset["challenge_ids"]:
            try:
                if challenge_id.startswith("OEB"):
                    execution_challenges.append(challenge_id)
                else:
                    execution_challenges.append(oeb_challenges[challenge_id])
            except:
                self.logger.info("No challenges associated to " + challenge_id +
                             " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                self.logger.info(min_dataset["_id"] + " not processed")
                sys.exit()

        valid_data["challenge_ids"] = execution_challenges
        # add challenge managers as aggregation dataset contacts ids
        challenge_contacts = []
        for challenge_graphql in challenges_graphql:
            if challenge_graphql["_id"] in execution_challenges:
                challenge_contacts.extend(challenge_graphql["challenge_contact_ids"])

        rel_data = []
        for assess_element in valid_assessment_datasets:
            
            try:
                vis = min_dataset["datalink"]["inline_data"]["visualization"]

                if (vis.get("metric") is None):
                    if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    #check for not 'oeb' challenges ids, in case the datasets is still not uploaded
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["x_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["y_axis"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    # also check for official oeb metrics, in case the aggregation dataset contains them
                    elif assess_element["depends_on"]["metrics_id"] == vis["x_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif assess_element["depends_on"]["metrics_id"] == vis["y_axis"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif vis["metric"].upper() in assess_element["_id"].upper() and min_dataset["challenge_ids"][0] in assess_element["_id"]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                else:
                    if oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif oeb_metrics[assess_element["depends_on"]["metrics_id"]] == vis["metric"] and assess_element["challenge_ids"][0] == oeb_challenges[min_dataset["challenge_ids"][0]]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif assess_element["depends_on"]["metrics_id"] == vis["metric"] and assess_element["challenge_ids"][0] == min_dataset["challenge_ids"][0]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
                    elif vis["metric"].upper() in assess_element["_id"].upper() and min_dataset["challenge_ids"][0] in assess_element["_id"]:
                        rel_data.append({"dataset_id": assess_element["_id"]})
            except:
                continue

        # add data registration dates
        valid_data["dates"] = {
            "creation": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
            "modification": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        }

        # add assessment metrics values, as inline data
        datalink = min_dataset["datalink"]
        for participant in datalink["inline_data"]["challenge_participants"]:
            participant["tool_id"] = participant.pop("participant_id")

        datalink["schema_url"] = "https://raw.githubusercontent.com/inab/OpenEBench_scientific_visualizer/master/benchmarking_data_model/aggregation_dataset_json_schema.json"

        valid_data["datalink"] = datalink

        # add Benchmarking Data Model Schema Location
        valid_data["_schema"] = self.schemaMappings['Dataset']

        # add OEB id for the community
        valid_data["community_ids"] = [community_id]

        # add dataset dependencies: metric id, tool and reference datasets
        valid_data["depends_on"] = {
            "tool_id": workflow_tool_id,
            "rel_dataset_ids": rel_data,
        }

        # add data version
        valid_data["version"] = version

        valid_data["dataset_contact_ids"] = challenge_contacts

        sys.stdout.write('Processed "' + str(min_dataset["_id"]) + '"...\n')

        return valid_data


def build_new_aggregation_id(aggregation_dataset) -> "str":
    d = aggregation_dataset["datalink"]["inline_data"]["visualization"]
    if (d.get("metric") is not None):
        return aggregation_dataset["_id"] + "_" + d["metric"]
    
    else :
        metrics = [d["x_axis"], d["y_axis"]]
    
        return aggregation_dataset["_id"] + "_" + metrics[0] + "+" + metrics[1]
