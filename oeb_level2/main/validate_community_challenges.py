#!/usr/bin/env python

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

from .. import schemas as level2_schemas

from ..process.participant import (
    Participant,
    ParticipantConfig,
)
from ..process.aggregation import AggregationValidator
from ..utils.migration_utils import (
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


def validate_challenges(
    bench_event_id: "str",
    challenge_ids: "Sequence[str]",
    oeb_credentials_filename: "str",
    oeb_token: "Optional[str]" = None,
    log_filename: "Optional[str]" = None,
):
    loggingConfig = {
        "level": logging.INFO,
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
        # Loading and checking the authentication and endpoints file
        oeb_credentials_val_list = level2_min_validator.jsonValidate(oeb_credentials_filename, guess_unmatched=[level2_schemas.AUTH_CONFIG_SCHEMA_ID])
        assert len(oeb_credentials_val_list) > 0
        oeb_credentials_val_block = oeb_credentials_val_list[0]
        oeb_credentials_val_block_errors = list(filter(lambda ve: ve.get("schema_id") == level2_schemas.AUTH_CONFIG_SCHEMA_ID, oeb_credentials_val_block.get("errors", [])))
        if len(oeb_credentials_val_block_errors) > 0:
            logging.error(f"Errors in configuration file {oeb_credentials_filename}\n{oeb_credentials_val_block_errors}")
            sys.exit(2)
        
        oeb_credentials = oeb_credentials_val_block["json"]

    except Exception as e:
        logging.fatal(e, "config file " + oeb_credentials_filename +
                      " is missing or has incorrect format")
        sys.exit(1)
    
    config_json_dir = os.path.dirname(oeb_credentials_filename)
    
    # get data model to validate against
    migration_utils = OpenEBenchUtils(oeb_credentials, config_json_dir, oeb_token, level2_min_validator=level2_min_validator)
    
    logging.info(f"-> Fetching and using schemas from the server {migration_utils.oeb_api_base}")
    schemaMappings = migration_utils.load_schemas_from_server()

    logging.info("-> Querying graphql about aggregations")
    aggregation_query_response = migration_utils.graphql_query_OEB_DB(
        "aggregation",
        bench_event_id,
    )
    
    # Work needed to learn about the communities
    bench_event = aggregation_query_response["data"]["getBenchmarkingEvents"][0]
    community_id = bench_event["community_id"]
    
    # Prefixes about communities
    stagedCommunities = list(migration_utils.fetchStagedData("Community", {"_id": [community_id]}))
    
    community_prefix = migration_utils.gen_community_prefix(stagedCommunities[0])
    benchmarking_event_prefix = migration_utils.gen_benchmarking_event_prefix(bench_event, community_prefix)
    
    logging.info(f"-> Validating Benchmarking Event {bench_event_id}")
    process_aggregations = AggregationValidator(schemaMappings, migration_utils)
    
    if len(challenge_ids) == 0:
        challenges_graphql = aggregation_query_response["data"]["getChallenges"]
    else:
        challenge_ids_set = set(challenge_ids)
        challenges_graphql = []
        included_challenge_ids = []
        for challenge_graphql in aggregation_query_response["data"]["getChallenges"]:
            if challenge_graphql["_id"] in challenge_ids_set:
                challenges_graphql.append(challenge_graphql)
        
        included_challenge_ids = set(map(lambda ch: ch['_id'] , challenges_graphql))
        if len(challenges_graphql) > 0:
            logging.info(f"   Restricting validation to challenges {', '.join(included_challenge_ids)}")
            not_included = challenge_ids_set - included_challenge_ids
            if len(not_included) > 0:
                logging.warning(f"   Next challenges were not found in the benchmarking event: {', '.join(not_included)}")
        else:
            logging.critical(f"Benchmarking event {bench_event_id} does not contain challenges {', '.join(challenge_ids)}. Is it the right benchmarking event or community???")
            sys.exit(1)
    
    # Check and index challenges and their main components
    agg_challenges = process_aggregations.check_and_index_challenges(
        community_prefix,
        benchmarking_event_prefix,
        challenges_graphql,
        aggregation_query_response["data"]["getMetrics"],
    )    

def main():
    parser = argparse.ArgumentParser(description='OEB Scientific Challenge validator', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-cr",
        "--oeb_submit_api_creds",
        help="Credentials and endpoints used to obtain a token for submission to oeb sandbox DB",
        required=True
    )
    parser.add_argument(
        "-tk",
        "--oeb_submit_api_token",
        help="Token used for submission to oeb buffer DB. If it is not set, the credentials file provided with -cr must have defined 'clientId', 'grantType', 'user' and 'pass'"
    )
    parser.add_argument(
        "--log-file",
        dest="logFilename",
        help="Store logging messages in a file instead of using standard error and standard output",
    )
    parser.add_argument(
        "bench_event_id",
        help="Benchmarking event id whose challenges are going to be validated",
    )
    parser.add_argument(
        "challenge_id",
        nargs="*",
        help="Restrict the validation to these challenges (they must belong to the benchmarking event)",
    )

    args = parser.parse_args()

    validate_challenges(args.bench_event_id, args.challenge_id, args.oeb_submit_api_creds, args.oeb_submit_api_token, args.logFilename)
    
if __name__ == '__main__':
    main()
