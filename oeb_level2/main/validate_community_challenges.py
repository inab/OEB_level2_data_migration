#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: GPL-3.0-only
# Copyright (C) 2020 Barcelona Supercomputing Center, Javier Garrayo Ventas
# Copyright (C) 2020-2022 Barcelona Supercomputing Center, Meritxell Ferret
# Copyright (C) 2020-2023 Barcelona Supercomputing Center, José M. Fernández
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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

from typing import (
    cast,
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from typing import (
        Optional,
        Sequence,
    )
    
    from typing_extensions import (
        NotRequired,
        TypedDict,
    )
    
    class BasicLoggingConfigDict(TypedDict):
        filename: NotRequired[str]
        format: NotRequired[str]
        level: int

import coloredlogs  # type: ignore[import]
import requests
from rfc3339_validator import validate_rfc3339  # type: ignore[import]

from .. import schemas as level2_schemas
from .. import version as oeb_level2_version

from ..process.participant import (
    ParticipantConfig,
)
from ..process.aggregation import AggregationValidator
from ..utils.migration_utils import (
    OpenEBenchUtils,
)

from . import (
    COLORED_LOGS_FMT,
    COLORED_LOGS_FMT_BRIEF,
    COLORED_LOGS_FMT_DEBUG,
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
    log_level: "int" = logging.INFO,
    proposed_entries_dir: "Optional[str]" = None,
) -> "None":
    loggingConfig: "BasicLoggingConfigDict" = {
        "level": log_level,
        "format": VERBOSE_LOGFORMAT if log_level < logging.INFO else LOGFORMAT,
    }
    # check whether config file exists and has all the required fields
    if log_filename is not None:
            loggingConfig["filename"] = log_filename
    
    logging.basicConfig(**loggingConfig)
    coloredlogs.install(
        level=log_level,
        fmt=COLORED_LOGS_FMT_DEBUG if log_level < logging.INFO else COLORED_LOGS_FMT,
        level_styles=COLORED_LOGS_LEVEL_STYLES,
    )
    logging.debug(f"Logging level set to {log_level}")
    
    level2_min_validator, num_level2_schemas = level2_schemas.create_validator_for_oeb_level2()
    if num_level2_schemas < 6:
        logging.error("OEB level2 operational JSON Schemas not found")
        sys.exit(1)
    
    # This is to avoid too much verbosity
    if log_level >= logging.INFO:
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
    stagedCommunity = migration_utils.fetchStagedEntry("Community", community_id)
    
    community_prefix = OpenEBenchUtils.gen_community_prefix(stagedCommunity)
    bench_event_prefix_et_al = migration_utils.gen_benchmarking_event_prefix(bench_event, community_prefix)
    
    logging.info(f"-> Validating Benchmarking Event {bench_event_id}")
    process_aggregations = AggregationValidator(schemaMappings, migration_utils)
    
    if len(challenge_ids) == 0:
        challenges_graphql = aggregation_query_response["data"]["getChallenges"]
    else:
        challenge_ids_set = set(challenge_ids)
        challenges_graphql = []
        for challenge_graphql in aggregation_query_response["data"]["getChallenges"]:
            if challenge_graphql["_id"] in challenge_ids_set:
                challenges_graphql.append(challenge_graphql)
        
        included_challenge_ids = set(map(lambda ch: cast("str", ch['_id']), challenges_graphql))
        if len(challenges_graphql) > 0:
            logging.info(f"   Restricting validation to challenges {', '.join(included_challenge_ids)}")
            not_included = challenge_ids_set - included_challenge_ids
            if len(not_included) > 0:
                logging.warning(f"   Next challenges were not found in the benchmarking event: {', '.join(not_included)}")
        else:
            logging.critical(f"Benchmarking event {bench_event_id} does not contain challenges {', '.join(challenge_ids)}. Is it the right benchmarking event or community???")
            sys.exit(1)
    
    # Check and index challenges and their main components
    if proposed_entries_dir is not None:
        os.makedirs(proposed_entries_dir, exist_ok=True)
    agg_challenges = process_aggregations.check_and_index_challenges(
        community_prefix,
        bench_event_prefix_et_al,
        challenges_graphql,
        aggregation_query_response["data"]["getMetrics"],
        fix_dir=proposed_entries_dir,
    )    

def main() -> "None":
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
    parser.add_argument(
        "-p",
        "--proposed-entries-dir",
        help="Directory where proposed entries are going to be saved, easing the work",
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
    parser.add_argument(
        '-V',
        '--version',
        action="version",
        version="%(prog)s version " + oeb_level2_version,
    )

    args = parser.parse_args()

    validate_challenges(
        args.bench_event_id,
        args.challenge_id,
        args.oeb_submit_api_creds,
        args.oeb_submit_api_token,
        args.logFilename,
        log_level=logging.INFO if args.logLevel is None else args.logLevel,
        proposed_entries_dir=args.proposed_entries_dir,
    )
    
if __name__ == '__main__':
    main()
