"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Author: Javier Garrayo Ventas
		Barcelona Supercomputing Center. Spain. 2020
#########################################################
"""
from process.participant import participant
from utils.migration_utils import utils
import json
from argparse import ArgumentParser
import sys, os
import logging

def main(config_json):

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

    except Exception as e:

        logging.fatal(e, "config file " + config_json + " is missing or has incorrect format")
        sys.exit()

    with open(input_file, 'r') as f:
        data = json.load(f)
    
    #sort out dataset depending on 'type' property
    for dataset in data:

        if "type" in dataset and dataset["type"] == "participant":
            participant_data = dataset
    
    # get data model to validate against
    migration_utils = utils()
    data_model_dir = migration_utils.doMaterializeRepo(data_model_repo, data_model_tag)
    data_model_dir = os.path.join(os.getcwd(), data_model_dir)

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    query_response = migration_utils.query_OEB_DB(bench_event_id, tool_id, community_id)

    # generate all required objects
    process_participant = participant()
    valid_participant_data = process_participant.build_participant_dataset(query_response, participant_data, data_visibility, bench_event_id, file_location, community_id, tool_id, version, contacts, data_model_dir)
    print (valid_participant_data)
    #build_test_events()

    ##VALIDATE!! JM validator


if __name__ == '__main__':
    
    parser = ArgumentParser()
    parser.add_argument("-i", "--config_json", help="json file which contains all parameters for migration)", required=True)
                                                                
    args = parser.parse_args()

    config_json = args.config_json
    
    main(config_json)
