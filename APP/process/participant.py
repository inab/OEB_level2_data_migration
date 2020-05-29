import logging
import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from datetime import datetime

class participant():

    DEFAULT_API="https://dev-openebench.bsc.es/sciapi/graphql"

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    logging.basicConfig(level=logging.INFO)

    def build_participant_dataset(self, participant_data, data_visibility, bench_event_id, file_location,community_id, tool_id):

        logging.info("\t1. Processing participant dataset")

        # add dataset visibility 
        participant_data["visibility"] = data_visibility

        # add name and description, if workflow did not provide them
        if "name" not in participant_data:
            participant_data["name"] = "Predictions made by " + participant_data["participant_id"] + " participant"
        if "description" not in participant_data:
            participant_data["description"] = "Predictions made by " + participant_data["participant_id"]  + " participant"

        # replace all workflow challenge identifiers with the official OEB ids, which should already be defined in the database.

        try:
            url = self.DEFAULT_API
            # get challenges and input datasets for provided benchmarking event
            json = { 'query' : '{\
                                    getChallenges(challengeFilters: {benchmarking_event_id: "'+ bench_event_id + '"}) {\
                                        _id\
                                        _metadata\
                                        datasets(datasetFilters: {type: "input"}) {\
                                            _id\
                                        }\
                                    }\
                                }' }

            r = requests.post(url=url, json=json, verify=False )
            response = r.json()
            if response["data"]["getChallenges"] == []:

                logging.fatal("No challenges associated to benchmarking event " + bench_event_id + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()
            else:
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

                del participant_data["challenge_id"]
                participant_data["challenge_ids"] = execution_challenges
                
                ## select input datasets related to the challenges
                rel_oeb_datasets = set()
                for dataset in [item for item in data if item["_id"] in participant_data["challenge_ids"] ]:
                    for input_data in dataset["datasets"]:
                        rel_oeb_datasets.add( input_data["_id"] )



        except Exception as e:

            logging.exception(e)

        # add data registration dates
        participant_data["dates"] = {
                "creation": str(datetime.now().replace(microsecond=0).isoformat()),
                "modification": str(datetime.now().replace(microsecond=0).isoformat())
            }
        
        # add participant's file permanent location
        participant_data["datalink"]["uri"] = file_location
        
        ## add Benchmarking Data Model Schema Location
        participant_data["_schema"] = "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset"

        ## remove custom workflow community id and add OEB id for the community
        del participant_data["community_id"]
        participant_data["community_ids"] = [community_id]

        ## add dataset dependencies: tool and reference datasets
        list_oeb_datasets = []
        for dataset in rel_oeb_datasets:
            list_oeb_datasets.append({
                "dataset_id": dataset
            })
        participant_data["depends_on"] = {
            "tool_id": tool_id,
            "rel_dataset_ids": list_oeb_datasets
        }

        #############
        # check if provided oeb tool actually exists
        #############
        print (participant_data)
        ## validate!!
        

        # info = {
        #     "_id": "QfO:2018-07-07_P_" + tool_name,
        #     "name": "Orthologs predicted by " + tool_name,
        #     "description": "List of orthologs pairs predicted by " + tool_name + " using the Quest for Orthologs reference proteome",
        #     "dates": {
        #         "creation": "2018-07-07T00:00:00Z",
        #         "modification": "2018-07-07T14:00:00Z"
        #     },
        #     "datalink": {
        #         "uri": link,
        #         "attrs": ["archive"],
        #         "validation_date": "2018-07-07T00:00:00Z",
        #         "status": "ok"
        #     },
        #     "type": "participant",
        #     "visibility": "public",
        #     "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset",
        #     "community_ids": ["OEBC002"],
        #     "challenge_ids": [
        #                         "OEBX002000000A",
        #                         "OEBX002000000B",
        #                     ],
        #     "depends_on": {
        #         "tool_id": tools_oeb_hash[tool_name],
        #         "rel_dataset_ids": [
        #             {
        #                 "dataset_id": "OEBD00200001FC",
        #             }
        #         ]
        #     },
        #     "version": "unknown",
        #     "dataset_contact_ids": [
        #         "Adrian.Altenhoff",
        #         "Christophe.Dessimoz"
        #     ]
        # }

    # def build_test_events:
