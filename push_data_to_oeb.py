#!/usr/bin/env python

"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Authors:
            Javier Garrayo Ventas (2020)
            Meritxell Ferret (2020-2022)
            José Mª Fernández (2020-2022)
		Barcelona Supercomputing Center. Spain. 2022
#########################################################
"""
import json
from argparse import ArgumentParser
import sys
import os
import os.path
import urllib.parse
import urllib.request
import logging
import uuid
import requests

# Just to get the directory through __file__
import oeb_level2.schemas

from oeb_level2.process.participant import Participant
from oeb_level2.process.assessment import Assessment
from oeb_level2.process.aggregation import Aggregation
from oeb_level2.utils.migration_utils import (
    OpenEBenchUtils,
)


DEFAULT_DATA_MODEL_RELDIR = os.path.join("json-schemas","1.0.x")

def validate_url(the_url: "str") -> "bool":
    """
    Inspired in https://stackoverflow.com/a/38020041
    """
    try:
        result = urllib.parse.urlparse(the_url)
        return all([result.scheme in ('http' ,'https', 'ftp'), result.netloc != "", result.path]) or all([result.scheme in ('file' ,'data'), result.netloc == "", result.path])
    except:
        return False    

# curl -v -d "client_id=THECLIENTID" -d "username=YOURUSER" -d "password=YOURPASSWORD" -d "grant_type=password" https://inb.bsc.es/auth/realms/openebench/protocol/openid-connect/token

def main(config_json_filename: "str", oeb_credentials_filename: "str", oeb_token: "Optional[str]" = None, val_result_filename: "Optional[str]" = None, output_filename: "Optional[str]" = None):
    
    # check whether config file exists and has all the required fields
    oeb_validator, num_level2_schemas = oeb_level2.schemas.create_validator_for_oeb_level2()
    if num_level2_schemas < 2:
        logging.error("OEB level2 operational JSON Schemas not found")
        sys.exit(1)
    
    try:
        config_json_dir = os.path.dirname(os.path.abspath(config_json_filename))
        # with open(config_json_filename, mode='r', encoding="utf-8") as f:
        #     config_params = json.load(f)
        
        # Loading and checking the dataset configuration file
        config_val_list = oeb_validator.jsonValidate(config_json_filename, guess_unmatched=True)
        assert len(config_val_list) > 0
        config_val_block = config_val_list[0]
        if len(config_val_block.get("errors", [])) > 0:
            logging.error(f"Errors in configuration file {config_json_filename}\n{config_val_block['errors']}")
            sys.exit(2)
        
        if config_val_block["schema_id"] != oeb_level2.schemas.SUBMISSION_FORM_SCHEMA_ID:
            logging.error(f"Wrong configuration file {config_json_filename}, as it matches schema {config_val_block['schema_id']}")
            sys.exit(2)
        
        config_params = config_val_block["json"]
        
        # Loading and checking the authentication and endpoints file
        oeb_credentials_val_list = oeb_validator.jsonValidate(oeb_credentials_filename, guess_unmatched=True)
        assert len(oeb_credentials_val_list) > 0
        oeb_credentials_val_block = oeb_credentials_val_list[0]
        if len(oeb_credentials_val_block.get("errors", [])) > 0:
            logging.error(f"Errors in configuration file {oeb_credentials_filename}\n{oeb_credentials_val_block['errors']}")
            sys.exit(2)
        
        if oeb_credentials_val_block["schema_id"] != oeb_level2.schemas.AUTH_CONFIG_SCHEMA_ID:
            logging.error(f"Wrong configuration file {oeb_credentials_filename}, as it matches schema {oeb_credentials_val_block['schema_id']}")
            sys.exit(2)
        
        oeb_credentials = oeb_credentials_val_block["json"]
        
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
                logging.error("File {}, referenced from {}, does not exist".format(input_file, config_json_filename))
                sys.exit(1)
        
        #Collect data
        data_visibility = config_params["data_visibility"]
        bench_event_id = config_params["benchmarking_event_id"]
        file_location = config_params["participant_file"]
        community_id = config_params["community_id"]
        tool_id = config_params["tool_id"]
        version = config_params["data_version"]
        version_str = str(version)
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
        valid = validate_url(file_location)
        if not valid:
            logging.fatal("Participant file location invalid: "+file_location)
            sys.exit(1)

    except Exception as e:

        logging.fatal(e, "config file " + config_json_filename +
                      " is missing or has incorrect format")
        sys.exit(1)
    
    if input_url is not None:
        
        response = requests.request("GET", input_url)
        if (response.status_code == 200):
            data = json.loads(response.text)
        else:
            logging.fatal(str(response.status_code) +" input url " + input_url + " is missing or has incorrect format")
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
            
        elif dataset_type == "aggregation":
            min_aggregation_datasets.append(dataset)
        elif dataset_type is not None:
            logging.warning("Dataset {} is of unknown type {}. Skipping".format(i_dataset, dataset_type))
            sys.exit(2)

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
        min_participant_data, file_location, contacts[0], version_str)
    '''
    
    ### GENENERATE ALL VALID DATASETS AND TEST ACTIONS
    #PARTICIPANT DATASETS
    process_participant = Participant(schemaMappings)
    valid_participant_data = process_participant.build_participant_dataset(
        input_query_response, min_participant_data, data_visibility, file_location, 
        community_id, tool_id, version_str, contacts)
    
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
        data_visibility, min_participant_data, community_id, tool_id, version_str, contacts)

    valid_metrics_events = process_assessments.build_metrics_events(
        metrics_reference_query_response, stagedEvents, valid_assessment_datasets, tool_id, contacts)
    
    
    #AGGREGATION DATASETS & AGGREGATION EVENT
    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    aggregation_query_response = migration_utils.query_OEB_DB(
        bench_event_id, tool_id, community_id, "aggregation")
    
    # Needed to better consolidate
    stagedAggregationDatasets = list(filter(lambda d: d.get('type') == "aggregation", stagedDatasets))
    
    process_aggregations = Aggregation(schemaMappings)
    valid_aggregation_datasets = process_aggregations.build_aggregation_datasets(
        aggregation_query_response, stagedAggregationDatasets, min_aggregation_datasets, min_participant_data, valid_assessment_datasets, community_id, tool_id, version_str, workflow_id)
    
    valid_aggregation_events = process_aggregations.build_aggregation_events(
        aggregation_query_response, stagedEvents, valid_aggregation_datasets, workflow_id)

    # join all elements in a single list, validate, and push them to OEB tmp database
    final_data = [valid_participant_data] + valid_test_events + valid_assessment_datasets + \
        valid_metrics_events + valid_aggregation_datasets + valid_aggregation_events
    
    # Generate the umbrella dataset
    umbrella = migration_utils.generate_manifest_dataset(dataset_submission_id, 
                                                         community_id, bench_event_id, 
                                                         version_str, data_visibility, final_data)
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

    parser = ArgumentParser(description='OEB Level 2 push_data_to_oeb', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--dataset_config_json",
                        help="json file which contains all parameters for dataset consolidation and migration", required=True)
    parser.add_argument("-cr", "--oeb_submit_api_creds",
                        help="Credentials and endpoints used to obtain a token for submission \
                        to oeb sandbox DB", required=True)
    parser.add_argument("-tk", "--oeb_submit_api_token",
                        help="Token used for submission to oeb buffer DB. If it is not set, the \
                        credentials file provided with -cr must have defined 'clientId', 'grantType', 'user' and 'pass'")
    parser.add_argument("--val_output",
                        help="Save the JSON Schema validation output to a file")
    parser.add_argument("-o", "--dry-run",
                        help="Save what it was going to be submitted in this file, instead of \
                        sending them (like a dry-run)")

    args = parser.parse_args()

    config_json_filename = args.dataset_config_json

    logging.basicConfig(level=logging.INFO)
    main(config_json_filename, args.oeb_submit_api_creds, args.oeb_submit_api_token, args.val_output, args.dry_run)
