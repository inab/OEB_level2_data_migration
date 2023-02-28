#!/usr/bin/env python

"""
#########################################################
	    VRE Level 2 to OpenEBench migration tool 
		Authors:
            Javier Garrayo Ventas (2020)
            Meritxell Ferret (2020-2022)
            José Mª Fernández (2020-2023)
		Barcelona Supercomputing Center. Spain. 2023
#########################################################
"""
import json
import argparse
import datetime
import sys
import os
import os.path
import urllib.parse
import urllib.request
import logging
import uuid

import coloredlogs
import requests
from rfc3339_validator import validate_rfc3339

# Just to get the directory through __file__
from .. import schemas as level2_schemas

from ..process.participant import (
    Participant,
    ParticipantConfig,
)
from ..process.assessment import Assessment
from ..process.aggregation import Aggregation
from ..utils.migration_utils import (
    gen_ch_id_to_label,
    OpenEBenchUtils,
)

from . import (
    COLORED_LOGS_FMT,
    COLORED_LOGS_FMT_BRIEF,
    COLORED_LOGS_LEVEL_STYLES,
    LOGFORMAT,
    VERBOSE_LOGFORMAT,
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

def validate_transform_and_push(
    config_json_filename: "str",
    oeb_credentials_filename: "str",
    oeb_token: "Optional[str]" = None,
    val_result_filename: "Optional[str]" = None,
    output_filename: "Optional[str]" = None,
    dry_run: "bool" = False,
    use_server_schemas: "bool" = False,
    log_filename: "Optional[str]" = None,
    log_level: "int" = logging.INFO,
):
    loggingConfig = {
        "level": log_level,
#        "format": LOGFORMAT,
    }
    # check whether config file exists and has all the required fields
    if log_filename is not None:
            loggingConfig["filename"] = log_filename
    
    logging.basicConfig(**loggingConfig)
    coloredlogs.install(
        fmt=COLORED_LOGS_FMT,
        level_styles=COLORED_LOGS_LEVEL_STYLES,
    )
    
    level2_min_validator, num_level2_schemas = level2_schemas.create_validator_for_oeb_level2()
    if num_level2_schemas < 6:
        logging.error("OEB level2 operational JSON Schemas not found")
        sys.exit(1)
    
    # This is to avoid too much verbosity
    logging.getLogger(level2_min_validator.__class__.__name__).setLevel(logging.CRITICAL)
    
    try:
        config_json_dir = os.path.dirname(os.path.abspath(config_json_filename))
        # with open(config_json_filename, mode='r', encoding="utf-8") as f:
        #     config_params = json.load(f)
        
        # Loading and checking the dataset configuration file
        config_val_list = level2_min_validator.jsonValidate(config_json_filename, guess_unmatched=[level2_schemas.SUBMISSION_FORM_SCHEMA_ID])
        assert len(config_val_list) > 0
        config_val_block = config_val_list[0]
        config_val_block_errors = list(filter(lambda ve: ve.get("schema_id") == level2_schemas.SUBMISSION_FORM_SCHEMA_ID, config_val_block.get("errors", [])))
        if len(config_val_block_errors) > 0:
            logging.error(f"Errors in configuration file {config_json_filename}\n{config_val_block_errors}")
            sys.exit(2)
        
        config_params = config_val_block["json"]
        
        # Loading and checking the authentication and endpoints file
        oeb_credentials_val_list = level2_min_validator.jsonValidate(oeb_credentials_filename, guess_unmatched=[level2_schemas.AUTH_CONFIG_SCHEMA_ID])
        assert len(oeb_credentials_val_list) > 0
        oeb_credentials_val_block = oeb_credentials_val_list[0]
        oeb_credentials_val_block_errors = list(filter(lambda ve: ve.get("schema_id") == level2_schemas.AUTH_CONFIG_SCHEMA_ID, oeb_credentials_val_block.get("errors", [])))
        if len(oeb_credentials_val_block_errors) > 0:
            logging.error(f"Errors in configuration file {oeb_credentials_filename}\n{oeb_credentials_val_block_errors}")
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
        data_model_repo = config_params["data_model_repo"]
        data_model_tag = config_params["data_model_tag"]
        data_model_reldir = config_params.get("data_model_reldir", DEFAULT_DATA_MODEL_RELDIR)
        
        # Tool mapping contains the correspondence from
        # participant label to official OpenEBench local id
        tool_mapping: "Mapping[Optional[str], ParticipantConfig]" = {}
        tool_mapping_list = config_params.get("tool_mapping")
        if tool_mapping_list is None:
            # Old school assumed there were only a tool_id declaration
            # so a "blind mapping" is only possible
            tool_id = config_params["tool_id"]
            
            tool_mapping[None] = ParticipantConfig(
                tool_id=tool_id,
                data_version=str(config_params["data_version"]),
                data_contacts=config_params["data_contacts"],
                participant_id=None,
            )
        else:
            tool_id = None
            # This data structure is fed all the time
            for tool_mapping_e in tool_mapping_list:
                participant_id = tool_mapping_e["participant_id"]
                assert participant_id is not None
                
                tool_mapping[participant_id] = ParticipantConfig(
                    tool_id=tool_mapping_e["tool_id"],
                    data_version=str(tool_mapping_e["data_version"]),
                    data_contacts=tool_mapping_e["data_contacts"],
                    participant_id=participant_id,
                )
            
        
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
        logging.info(f"-> Fetching and reading minimal dataset to be processed from {input_url}")
        
        response = requests.request("GET", input_url)
        if (response.status_code == 200):
            data = json.loads(response.text)
        else:
            logging.fatal(str(response.status_code) +" input url " + input_url + " is missing or has incorrect format")
            sys.exit(1)
    else:
        logging.info(f"-> Opening and reading minimal dataset file {input_file}")
        try:
            with open(input_file, 'r') as f:
                data = json.load(f)
        except Exception as e:
            logging.fatal(e, "input file " + input_file +
                          " is missing or has incorrect format")
            sys.exit(1)

    logging.info("-> Validating minimal dataset to be processed")
    # Some data have incomplete timestamps (hence, incorrect)
    for data_i, data_entry in enumerate(data):
        if data_entry.get("type") == "participant":
            val_date_b = data_entry.get("datalink", {})
            if val_date_b:
                val_date = val_date_b.get("validation_date")
                if val_date is not None and not validate_rfc3339(val_date):
                    # Guessing the problem is incomplete timestamp
                    new_val_date = val_date + 'Z'
                    if validate_rfc3339(new_val_date):
                        logging.warning(f"Patching date in entry {data_i} from {input_file}")
                        val_date_b["validation_date"] = new_val_date
                    
    # Now, it is time to validate the fetched data
    # in inline mode
    validable_data = {
        "file": input_file if input_url is None else input_url,
        "json": data,
        "errors": []
    }
    validated_data = level2_min_validator.jsonValidate(validable_data, guess_unmatched=[level2_schemas.MINIMAL_DATA_BLOCK_SCHEMA_ID])
    assert len(validated_data) > 0
    validated_data_block = validated_data[0]
    validated_data_block_errors = list(filter(lambda ve: ve.get("schema_id") == level2_schemas.MINIMAL_DATA_BLOCK_SCHEMA_ID, validated_data_block.get("errors", [])))
    if len(validated_data_block_errors) > 0:
        logging.error(f"Errors in data file {validated_data_block['file']}\n{json.dumps(validated_data_block_errors, indent=4)}")
        sys.exit(2)
    
    # sort out dataset depending on 'type' property
    logging.info("-> Sorting out minimal datasets to be processed based on their type")
    min_participant_dataset = []
    min_assessment_datasets = []
    min_aggregation_datasets = []
    # A two level dictionary to account for the
    # number of participant datasets in a given challenge
    participants_per_challenge = {}
    participants_set = set()
    not_datasets = 0
    discarded_datasets = 0
    for i_dataset, dataset in enumerate(data):
        dataset_type = dataset.get("type")
        if dataset_type == "participant":
            min_participant_dataset.append(dataset)
            
            # Detecting 'old school' inconsistencies
            participant_id = dataset["participant_id"]
            participants_set.add(participant_id)
            if tool_id is not None:
                if len(participants_set) > 1:
                    logging.warning(f"'Old school' configuration file and {len(participants_set)} participant datasets")
                elif None in tool_mapping:
                    # Fixed "old" default tool mapping
                    tool_mapping[None].participant_id = participant_id
                    tool_mapping[participant_id] = tool_mapping[None]
                    del tool_mapping[None]
            
            # Detecting 'old school' inconsistencies in a finer grain
            challenges = dataset["challenge_id"]
            if not isinstance(challenges, list):
                challenges = [ challenges ]
            for challenge in challenges:
                p_in_chall = participants_per_challenge.setdefault(challenge, {})
                
                if participant_id in p_in_chall:
                    p_in_chall[participant_id] += 1
                else:
                    p_in_chall[participant_id] = 1
                
                if tool_id is not None and p_in_chall[participant_id] > 1:
                    logging.warning(f"'Old school' configuration file and {p_in_chall[participant_id]} participant datasets in challenge {challenge}")
        elif dataset_type == "assessment":
            min_assessment_datasets.append(dataset)
            
        elif dataset_type == "aggregation":
            min_aggregation_datasets.append(dataset)
        elif dataset_type is not None:
            logging.warning(f"Dataset {i_dataset} is of unknown type {dataset_type}. Skipping")
            discarded_datasets += 1
        else:
            not_datasets += 1
    
    logging.info(f"   Stats: {len(participants_per_challenge)} challenges, {len(min_participant_dataset)} participant, {len(min_assessment_datasets)} assessment, {len(min_aggregation_datasets)} aggregation, {discarded_datasets} unknown, {not_datasets} not datasets")
    
    if discarded_datasets > 0:
        logging.warning(f"{discarded_datasets} minimal datasets of unknown type. Please fix it")
        sys.exit(2)
    
    if len(min_participant_dataset) == 0:
        logging.error(f"No dataset of type participant was available. There must be at least one. Fix it")
        sys.exit(2)
        
    
    # get data model to validate against
    migration_utils = OpenEBenchUtils(oeb_credentials, config_json_dir, oeb_token, level2_min_validator=level2_min_validator)

    # Early check over the tools
    logging.info("-> Fetching the list of recorded tools")
    allTools = list(migration_utils.fetchStagedData('Tool'))
    logging.info("-> Early checking in the participant tools")
    unique_participant_tool_ids = set(map(lambda pc: pc.tool_id, tool_mapping.values()))
    participant_tools = list(migration_utils.filter_by(allTools, {"_id": unique_participant_tool_ids}))
    participant_tool_ids = set(map(lambda pt: pt["_id"], participant_tools))
    if participant_tool_ids < unique_participant_tool_ids:
        logging.fatal(
            f"Tool ids {', '.join(unique_participant_tool_ids.difference(participant_tool_ids))} could not be found in OEB. Maybe you are using the wrong instance (development instead of production) or the tool ids have a typo. Otherwise, please contact OpenEBench support for information about how to register these tools")
        sys.exit(2)

    # Early checks over the minimal input datasets
    logging.info("-> Fetching the list of datasets")
    allDatasets = list(migration_utils.fetchStagedData('Dataset'))
    
    # Prefixes about communities
    stagedCommunities = list(migration_utils.fetchStagedData("Community", {"_id": [community_id]}))
    community_prefix = migration_utils.gen_community_prefix(stagedCommunities[0])
    
    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    logging.info(f"-> Query challenges related to benchmarking event {bench_event_id}")
    input_query_response = migration_utils.graphql_query_OEB_DB(
        "input",
        bench_event_id,
    )
    
    bench_event = input_query_response["data"]["getBenchmarkingEvents"][0]
    benchmarking_event_prefix = migration_utils.gen_benchmarking_event_prefix(bench_event, community_prefix)

    ch_id_to_label = gen_ch_id_to_label(input_query_response["data"]["getChallenges"], benchmarking_event_prefix, community_prefix)
    
    logging.info("-> Early minimal participant datasets check")
    m_p_collisions = migration_utils.check_min_dataset_collisions(allDatasets, min_participant_dataset, ch_id_to_label)
    if len(m_p_collisions) > 0:
        sys.exit(5)
    logging.info("-> Early minimal assessment datasets check")
    m_a_collisions = migration_utils.check_min_dataset_collisions(allDatasets, min_assessment_datasets, ch_id_to_label)
    if len(m_a_collisions) > 0:
        sys.exit(5)
    
    if use_server_schemas:
        logging.info(f"-> Fetching and using schemas from the server {migration_utils.oeb_api_base}")
        schemaMappings = migration_utils.load_schemas_from_server()
    else:
        logging.info("-> Fetching and using schemas from the repository")
        data_model_repo_dir = migration_utils.doMaterializeRepo(
            data_model_repo, data_model_tag)
        data_model_dir = os.path.abspath(os.path.join(data_model_repo_dir, data_model_reldir))
        schemaMappings = migration_utils.load_schemas_from_repo(data_model_dir)
    
    # Get Benchmarking Event entry (in case it has to be updated)
    # benchmarking_event = migration_utils.fetchStagedEntry("BenchmarkingEvent", bench_event_id)
    
    # Update the list of contact_ids
    for p_config in tool_mapping.values():
        p_config.process_contact_ids(input_query_response["data"]["getContacts"])
    
    '''
    # upload predicitions file to stable server and get permanent identifier
    data_doi = migration_utils.upload_to_storage_service(
        min_participant_data, file_location, contacts[0], version_str)
    '''
    
    ### GENENERATE ALL VALID DATASETS AND TEST ACTIONS
    #PARTICIPANT DATASETS
    logging.info(f"-> Processing {len(min_participant_dataset)} minimal participant datasets")
    process_participant = Participant(schemaMappings)
    community_id = bench_event["community_id"]
    stagedParticipantDatasets = list(migration_utils.filter_by(allDatasets, {"community_ids": [ community_id ], "type": [ "participant" ]}))
    valid_participant_tuples = process_participant.build_participant_dataset(
        input_query_response["data"]["getChallenges"],
        stagedParticipantDatasets,
        min_participant_dataset,
        data_visibility,
        file_location, 
        community_id,
        benchmarking_event_prefix,
        community_prefix,
        tool_mapping
    )
    
    # Now it is time to check anomalous collisions
    logging.info(f"-> Check collisions on {len(valid_participant_tuples)} generated participant datasets")
    p_d_collisions = migration_utils.check_dataset_collisions(
        allDatasets,
        list(map(lambda pvc: pvc.participant_dataset, valid_participant_tuples)),
        default_schema_url=False,
    )
    if len(p_d_collisions) > 0:
        sys.exit(5)
    
    #TEST EVENT
    logging.info(f"-> Generating {len(valid_participant_tuples)} TestEvent test actions")
    valid_test_events = process_participant.build_test_events(
        valid_participant_tuples
    )

    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    logging.info("-> Querying graphql about metrics reference and assessments")
    metrics_reference_query_response = migration_utils.graphql_query_OEB_DB(
        "metrics_reference",
        bench_event_id,
    )

    #ASSESSMENT DATASETS & METRICS EVENT
    challenge_ids_set = set()
    community_ids_set = set()
    for pvc in valid_participant_tuples:
        
        community_ids_set.update(pvc.participant_dataset["community_ids"])
        challenge_ids_set.update(map(lambda chp: chp.entry["_id"], pvc.challenge_pairs))
        
    stagedEvents = list(migration_utils.fetchStagedData('TestAction', {"challenge_id": list(challenge_ids_set)}))
    stagedDatasets = list(migration_utils.filter_by(allDatasets, {"community_ids": list(community_ids_set), "type": [ "assessment", "aggregation"]}))

    # Needed to better consolidate
    stagedAssessmentDatasets = list(filter(lambda d: d.get('type') == "assessment", stagedDatasets))
    
    logging.info(f"-> Processing {len(min_assessment_datasets)} minimal assessment datasets")
    process_assessments = Assessment(schemaMappings)
    valid_assessment_tuples = process_assessments.build_assessment_datasets(
        metrics_reference_query_response["data"]["getChallenges"],
        metrics_reference_query_response["data"]["getMetrics"],
        stagedAssessmentDatasets,
        min_assessment_datasets, 
        data_visibility,
        valid_participant_tuples,
        benchmarking_event_prefix,
        community_prefix,
    )
    
    logging.info(f"-> Check collisions on {len(valid_assessment_tuples)} generated assessment datasets")
    a_d_collisions = migration_utils.check_dataset_collisions(
        allDatasets,
        list(map(lambda at: at.assessment_dataset, valid_assessment_tuples)),
        default_schema_url=[
            level2_schemas.SINGLE_METRIC_SCHEMA_ID
        ]
    )
    if len(a_d_collisions) > 0:
        sys.exit(5)

    logging.info(f"-> Generating {len(valid_assessment_tuples)} MetricsEvent test actions")
    valid_metrics_events = process_assessments.build_metrics_events(
        valid_assessment_tuples
    )
    
    
    #AGGREGATION DATASETS & AGGREGATION EVENT
    # query remote OEB database to get offical ids from associated challenges, tools and contacts
    logging.info("-> Querying graphql about aggregations")
    aggregation_query_response = migration_utils.graphql_query_OEB_DB(
        "aggregation",
        bench_event_id,
    )
    
    # Needed to better consolidate
    stagedAggregationDatasets = list(filter(lambda d: d.get('type') == "aggregation", stagedDatasets))
    
    logging.info(f"-> Processing {len(min_aggregation_datasets)} minimal aggregation datasets")
    process_aggregations = Aggregation(schemaMappings, migration_utils)
    
    # Check and index challenges and their main components
    agg_challenges = process_aggregations.check_and_index_challenges(
        community_prefix,
        benchmarking_event_prefix,
        aggregation_query_response["data"]["getChallenges"],
        aggregation_query_response["data"]["getMetrics"],
    )
    
    # Now, build the aggregation datasets
    valid_aggregation_tuples = process_aggregations.build_aggregation_datasets(
        community_id,
        min_aggregation_datasets,
        agg_challenges,
        valid_assessment_tuples,
        valid_test_events,
        valid_metrics_events,
        workflow_id,
    )
    
    logging.info(f"-> Check collisions on {len(valid_aggregation_tuples)} generated aggregation datasets")
    agg_d_collisions = migration_utils.check_dataset_collisions(
        allDatasets,
        list(map(lambda agt: agt.aggregation_dataset, valid_aggregation_tuples)),
        default_schema_url=[
            level2_schemas.AGGREGATION_2D_PLOT_SCHEMA_ID,
            level2_schemas.AGGREGATION_BAR_PLOT_SCHEMA_ID,
        ]
    )
    if len(agg_d_collisions) > 0:
        sys.exit(5)
    
    valid_aggregation_events = process_aggregations.build_aggregation_events(
        valid_aggregation_tuples,
        aggregation_query_response["data"]["getChallenges"],
#        stagedEvents + aggregation_query_response["data"]["getTestActions"],
        workflow_id
    )

    # join all elements in a single list, validate, and push them to OEB tmp database
    final_data = list(map(lambda pt: pt.participant_dataset, valid_participant_tuples)) + \
        valid_test_events + \
        list(map(lambda at: at.assessment_dataset, valid_assessment_tuples)) + \
        valid_metrics_events + \
        list(map(lambda ag: ag.aggregation_dataset, valid_aggregation_tuples)) + \
        valid_aggregation_events
    
    # Generate the umbrella dataset
    version_str = datetime.datetime.now(datetime.timezone.utc).astimezone().replace(microsecond=0).isoformat()
    umbrella = migration_utils.generate_manifest_dataset(dataset_submission_id, 
                                                         community_id, bench_event_id, 
                                                         version_str, data_visibility, final_data)
    final_data.append(umbrella)
    
    if output_filename is not None:
        logging.info(f"Storing output before validation at {output_filename}")
        with open(output_filename, mode="w", encoding="utf-8") as wb:
            json.dump(final_data, wb)
    
    migration_utils.schemas_validation(final_data, val_result_filename)
    
    if dry_run:
        logging.info("Data was stored at {} (submission was skipped)".format(output_filename))
    else:
        logging.info("Submitting...")
        migration_utils.submit_oeb_buffer(final_data, community_id)

def main():
    parser = argparse.ArgumentParser(description='OEB Level 2 push_data_to_oeb', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
    parser.add_argument("-o",
                        dest="submit_output_file",
                        help="Save what it was going to be submitted in this file")
    parser.add_argument("--dry-run",
                        help="Only validate, do not submit (dry-run)",
                        action="store_true")
    parser.add_argument('--trust-rest-bdm',
        dest='trustREST',
        help="Trust on the copy of Benchmarking data model referred by server, fetching from it instead from GitHub.",
        action="store_true"
    )
    parser.add_argument(
        "--log-file",
        dest="logFilename",
        help="Store logging messages in a file instead of using standard error and standard output",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        dest="logLevel",
        action="store_const",
        const=logging.WARNING,
        help="Only show engine warnings and errors",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="logLevel",
        action="store_const",
        const=logging.INFO,
        help="Show verbose (informational) messages",
    )
    parser.add_argument(
        "-d",
        "--debug",
        dest="logLevel",
        action="store_const",
        const=logging.DEBUG,
        help="Show debug messages (use with care, as it could potentially disclose sensitive contents)",
    )

    args = parser.parse_args()

    config_json_filename = args.dataset_config_json

    validate_transform_and_push(
        config_json_filename,
        args.oeb_submit_api_creds,
        args.oeb_submit_api_token,
        args.val_output,
        args.submit_output_file,
        args.dry_run,
        args.trustREST,
        args.logFilename,
        args.logLevel
    )

if __name__ == '__main__':
    main()
