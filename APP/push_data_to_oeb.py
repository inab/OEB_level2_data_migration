"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Author: Javier Garrayo Ventas
		Barcelona Supercomputing Center. Spain. 2020
#########################################################
"""
from process.participant import participant
from process.assessment import assessment
from process.aggregation import aggregation
from utils.migration_utils import utils
import json
from argparse import ArgumentParser
import sys, os
import logging

def main(config_json, config_db):

    #check whether config file exists and has all the required fields
    try:
        with open(config_json, 'r') as f:
            config_params = json.load(f)

            input_file = config_params["consolidated_oeb_data"]
            data_visibility = config_params["data_visibility"]
            bench_event_id = config_params["benchmarking_event_id"]
            file_location = config_params["participant_file"]
            community_id = config_params["community_id"]
            tool_id = config_params["tool_id"]
            version = config_params["data_version"]
            contacts = config_params["data_contacts"]
            data_model_repo = config_params["data_model_repo"]
            data_model_tag = config_params["data_model_tag"]
            storage_server_endpoint = config_params["data_storage_endpoint"]

    except Exception as e:

        logging.fatal(e, "config file " + config_json + " is missing or has incorrect format")
        sys.exit()

    with open(input_file, 'r') as f:
        data = json.load(f)
    
    #sort out dataset depending on 'type' property
    min_assessment_datasets = []
    min_aggregation_datasets = []
    for dataset in data:

        if "type" in dataset and dataset["type"] == "participant":
            min_participant_data = dataset
        elif "type" in dataset and dataset["type"] == "assessment":
            min_assessment_datasets.append(dataset)
        elif "type" in dataset and dataset["type"] == "aggregation":
            min_aggregation_datasets.append(dataset)
    
    # get data model to validate against
    migration_utils = utils()
    data_model_dir = migration_utils.doMaterializeRepo(data_model_repo, data_model_tag)
    data_model_dir = os.path.join(os.getcwd(), data_model_dir)

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    query_response = migration_utils.query_OEB_DB(bench_event_id, tool_id, community_id, "input")

    # upload predicitions file to stable server and get permanent identifier
    data_doi = migration_utils.upload_to_storage_service(storage_server_endpoint, min_participant_data, file_location, contacts[0], version)

    # generate all required objects
    process_participant = participant(data_model_dir, config_db)
    valid_participant_data = process_participant.build_participant_dataset(query_response, min_participant_data, data_visibility, data_doi, community_id, tool_id, version, contacts)
    print(valid_participant_data)
    valid_test_events = process_participant.build_test_events(query_response, min_participant_data, tool_id, contacts)
    # print(valid_test_events)

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    query_response = migration_utils.query_OEB_DB(bench_event_id, tool_id, community_id, "metrics_reference")

    process_assessments = assessment(data_model_dir)
    valid_assessment_datasets = process_assessments.build_assessment_datasets(query_response, min_assessment_datasets, data_visibility, min_participant_data, community_id, tool_id, version, contacts)
    # print(valid_assessment_datasets)
    valid_metrics_events = process_assessments.build_metrics_events(query_response, valid_assessment_datasets, tool_id, contacts)
    # print(valid_metrics_events)


    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    query_response = migration_utils.query_OEB_DB(bench_event_id, tool_id, community_id, "aggregation")

    process_aggregations = aggregation(data_model_dir)
    process_aggregations.build_aggregation_datasets(query_response, min_aggregation_datasets, min_participant_data, valid_assessment_datasets, community_id, tool_id, version, contacts)
    ##VALIDATE!! JM validator


if __name__ == '__main__':
    
    parser = ArgumentParser()
    parser.add_argument("-i", "--config_json", help="json file which contains all parameters for migration", required=True)
    parser.add_argument("-db", "--config_db", help="yaml file with configuration for remote OEB db validation", required=True)
                                                                
    args = parser.parse_args()

    config_json = args.config_json
    config_db = args.config_db
    
    main(config_json, config_db)
