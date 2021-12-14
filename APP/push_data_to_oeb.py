#!/usr/bin/env python

"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Author: Javier Garrayo Ventas
		Barcelona Supercomputing Center. Spain. 2020
#########################################################
"""
from process.participant import Participant
from process.assessment import Assessment
from process.aggregation import Aggregation
from utils.migration_utils import OpenEBenchUtils
import json
from argparse import ArgumentParser
import sys
import os
import urllib.parse
import urllib.request
import logging
import uuid
import validators


DEFAULT_DATA_MODEL_RELDIR = os.path.join("json-schemas","1.0.x")

# curl -v -d "client_id=THECLIENTID" -d "username=YOURUSER" -d "password=YOURPASSWORD" -d "grant_type=password" https://inb.bsc.es/auth/realms/openebench/protocol/openid-connect/token

def main(config_json, oeb_credentials, oeb_token=None, val_result_filename=None, output_filename=None):
    
    # check whether config file exists and has all the required fields
    try:
        config_json_dir = os.path.dirname(os.path.abspath(config_json))
        with open(config_json, 'r') as f:
            config_params = json.load(f)
        
        # When this path is relative, the reference is the directory
        # of the configuration file
        input_file = config_params["consolidated_oeb_data"]
        input_parsed = urllib.parse.urlparse(input_file)
        
        if len(input_parsed.scheme) > 0:
            input_url = input_file
        else:
            input_url = None
            if not os.path.isabs(input_file):
                input_file = os.path.normpath(os.path.join(config_json_dir,input_file))
            if not os.path.exists(input_file):
                logging.error("File {}, referenced from {}, does not exist".format(input_file, config_json))
                sys.exit(1)
        
        #Collect data
        data_visibility = config_params["data_visibility"]
        bench_event_id = config_params["benchmarking_event_id"]
        file_location = config_params["participant_file"]
        community_id = config_params["community_id"]
        tool_id = config_params["tool_id"]
        version = config_params["data_version"]
        contacts = config_params["data_contacts"]
        data_model_repo = config_params["data_model_repo"]
        data_model_tag = config_params["data_model_tag"]
        data_model_reldir = config_params.get("data_model_reldir", DEFAULT_DATA_MODEL_RELDIR)
        
        '''
        storageServer = oeb_credentials.get('storageServer', {})
        if storageServer['type'] == 'b2share':
            if 'endpoint' not in storageServer:
                storageServer['endpoint'] = config_params["data_storage_endpoint"]
        else:
            raise Exception('Unknown server type "{}"'.format(storageServer['type']))
        '''
        workflow_id = config_params["workflow_oeb_id"]
        
        dataset_submission_id = config_params.get("dataset_submission_id")
        if not dataset_submission_id:
            # This unique identifier depends on the machine, the current
            # timestamp and a random element
            ts = uuid.uuid1()
            dataset_submission_id = str(ts)
            
        #check partiicpant file location is a valid url
        valid = validators.url(file_location)
        if not valid:
            logging.fatal("Participant file location invalid: "+file_location)
            sys.exit(1)

    except Exception as e:

        logging.fatal(e, "config file " + config_json +
                      " is missing or has incorrect format")
        sys.exit(1)
    
    if input_url is not None:
        try:
            req = urllib.request.Request(input_url, method='GET')
            with urllib.request.urlopen(req) as iu:
                data = json.load(iu)
        except Exception as e:
            logging.fatal(e, "input url " + input_url +
                          " is missing or has incorrect format")
            sys.exit(1)
    else:
        try:
            with open(input_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            logging.fatal(e, "input file " + input_file +
                          " is missing or has incorrect format")
            sys.exit(1)

    # sort out dataset depending on 'type' property
    min_assessment_datasets = []
    min_aggregation_datasets = []
    for i_dataset, dataset in enumerate(data):
        dataset_type = dataset.get("type")
        if dataset_type == "participant":
            min_participant_data = dataset
        elif dataset_type == "assessment":
            min_assessment_datasets.append(dataset)
            
        #elif dataset_type == "aggregation":
            #min_aggregation_datasets.append(dataset)
        elif dataset_type is not None:
            logging.warning("Dataset {} is of unknown type {}. Skipping".format(i_dataset, dataset_type))
        #    sys.exit(2)

    # get data model to validate against
    migration_utils = OpenEBenchUtils(oeb_credentials, config_json_dir, oeb_token)
    data_model_repo_dir = migration_utils.doMaterializeRepo(
        data_model_repo, data_model_tag)
    data_model_dir = os.path.abspath(os.path.join(data_model_repo_dir, data_model_reldir))
    schemaMappings = migration_utils.load_schemas(data_model_dir)

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    input_query_response = migration_utils.query_OEB_DB(
        bench_event_id, tool_id, community_id, "input")
    
    '''
    # upload predicitions file to stable server and get permanent identifier
    data_doi = migration_utils.upload_to_storage_service(
        min_participant_data, file_location, contacts[0], version)
    '''
    
    ### GENENERATE ALL VALID DATASETS AND TEST ACTIONS
    #PARTICIPANT DATASETS
    process_participant = Participant(schemaMappings)
    valid_participant_data = process_participant.build_participant_dataset(
        input_query_response, min_participant_data, data_visibility, file_location, 
        community_id, tool_id, version, contacts)
    
    #TEST EVENT
    valid_test_events = process_participant.build_test_events(
        input_query_response, min_participant_data, tool_id, contacts)

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    metrics_reference_query_response = migration_utils.query_OEB_DB(
        bench_event_id, tool_id, community_id, "metrics_reference")

    #ASSESSMENT DATASETS & METRICS EVENT
    stagedEvents = migration_utils.fetchStagedData('TestAction')
    stagedDatasets = migration_utils.fetchStagedData('Dataset')

    # Needed to better consolidate
    stagedAssessmentDatasets = list(filter(lambda d: d.get('type') == "assessment", stagedDatasets))
    
    process_assessments = Assessment(schemaMappings)
    valid_assessment_datasets = process_assessments.build_assessment_datasets(
        metrics_reference_query_response, stagedAssessmentDatasets, min_assessment_datasets, 
        data_visibility, min_participant_data, community_id, tool_id, version, contacts)

    valid_metrics_events = process_assessments.build_metrics_events(
        metrics_reference_query_response, stagedEvents, valid_assessment_datasets, tool_id, contacts)
    
    
    #AGGREGATION DATASETS & AGGREGATION EVENT
    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    aggregation_query_response = migration_utils.query_OEB_DB(
        bench_event_id, tool_id, community_id, "aggregation")
    
    process_aggregations = Aggregation(schemaMappings)
    valid_aggregation_datasets = process_aggregations.build_aggregation_datasets(
        aggregation_query_response, valid_participant_data, valid_assessment_datasets, 
        community_id, tool_id, version, workflow_id)
    
    valid_aggregation_events = process_aggregations.build_aggregation_events(
        aggregation_query_response, stagedEvents, valid_aggregation_datasets, workflow_id)

    # join all elements in a single list, validate, and push them to OEB tmp database
    final_data = [valid_participant_data] + valid_test_events + valid_assessment_datasets + \
        valid_metrics_events + valid_aggregation_datasets + valid_aggregation_events
    
    # Generate the umbrella dataset
    umbrella = migration_utils.generate_manifest_dataset(dataset_submission_id, 
                                                         community_id, bench_event_id, 
                                                         version, data_visibility, final_data)
    final_data.append(umbrella)
    
    if output_filename is not None:
        logging.info("Storing output before validation at {}".format(output_filename))
        with open(output_filename, mode="w", encoding="utf-8") as wb:
            json.dump(final_data, wb)
    
    migration_utils.schemas_validation(final_data, val_result_filename)
    
    if output_filename is None:
        logging.info("Submitting...")
        migration_utils.submit_oeb_buffer(final_data, community_id)
    else:
        logging.info("Data was stored at {} (submission was skipped)".format(output_filename))


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument("-i", "--config_json",
                        help="json file which contains all parameters for migration", required=True)
    parser.add_argument("-cr", "--oeb_submit_api_creds",
                        help="Credentials and endpoints used to obtain a token for submission to oeb buffer DB", required=True)
    parser.add_argument("-tk", "--oeb_submit_api_token",
                        help="Token used for submission to oeb buffer DB. If it is not set, the credentials file provided with -cr must have defined 'clientId', 'grantType', 'user' and 'pass'")
    parser.add_argument("--val_output",
                        help="Save the JSON Schema validation output to a file")
    parser.add_argument("-o", "--output",
                        help="Save what it was going to be submitted in this file, instead of sending them (like a dry-run)")

    args = parser.parse_args()

    config_json = args.config_json
    with open(args.oeb_submit_api_creds, mode='r', encoding='utf-8') as ac:
        oeb_credentials = json.load(ac)

    logging.basicConfig(level=logging.INFO)
    main(config_json, oeb_credentials, args.oeb_submit_api_token, args.val_output, args.output)
