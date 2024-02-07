#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: GPL-3.0-only
# Copyright (C) 2024 José M. Fernández
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

import argparse
import copy
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile

from typing import (
    cast,
    NamedTuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing import (
        Any,
        Mapping,
        MutableMapping,
        MutableSequence,
        Optional,
        Sequence,
        Set,
        Tuple,
        Union,
    )
    
    from . import (
        BasicLoggingConfigDict,
    )
    
    from ..utils.migration_utils import (
        BenchmarkingEventPrefixEtAl,
    )

import urllib.parse

import coloredlogs  # type: ignore[import]

from oebtools.fetch import (
    OEBFetcher,
)

from ..process.participant import (
    ChallengePair,
    ParticipantTuple,
)

from ..utils.migration_utils import (
    AGGREGATION_CATEGORY_LABEL,
    GraphQLQueryLabel,
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

from .. import schemas as level2_schemas
from .. import version as oeb_level2_version

DEFAULT_PD_CACHE_DIR = "PD_CACHE"
FETCHED_DIR = "FETCHED"
JOBS_DIR = "JOBS"
VRE_EXECUTABLE = "VRE_NF_RUNNER"


DOI_HOST = "doi.org"
DOI_HANDLE_REST = "https://" + DOI_HOST +  "/api/handles/"
B2SHARE_HOST = "b2share.eudat.eu"
B2SHARE_RECORD_REST = "https://" + B2SHARE_HOST + "/api/records/"
ZENODO_HOST = "zenodo.org"
ZENODO_RECORD_REST = "https://" + ZENODO_HOST + "/api/records/"

def resolve_uri(the_uri: "str") -> "Sequence[Tuple[str, Optional[str]]]":
    parsed_uri = urllib.parse.urlparse(the_uri)
    is_http = parsed_uri.scheme in ("http", "https")
    splitted_path = parsed_uri.path.split("/")
    if len(splitted_path) > 0 and splitted_path[0] == "":
        splitted_path = splitted_path[1:]

    urls: "MutableSequence[Tuple[str, Optional[str]]]"
    if is_http and parsed_uri.netloc == DOI_HOST:
        with urllib.request.urlopen(DOI_HANDLE_REST + "/".join(splitted_path)) as meta_doi:
            meta_doi_answer = json.load(meta_doi)
            
            new_url = None
            if isinstance(meta_doi_answer, dict) and meta_doi_answer.get("responseCode") == 1:
                for rec in meta_doi_answer.get("values", []):
                    if isinstance(rec, dict) and rec.get("type") == "URL":
                        rec_data = rec.get("data", {})
                        if rec_data.get("format") == "string":
                            new_url = rec_data.get("value")
                            break
            
            if isinstance(new_url, str):
                return resolve_uri(new_url)

    elif is_http and parsed_uri.netloc == B2SHARE_HOST:
        if len(splitted_path) >= 2 and splitted_path[0] == "records":
            b2share_meta_url = urllib.parse.urljoin(B2SHARE_RECORD_REST, splitted_path[1])
            with urllib.request.urlopen(b2share_meta_url) as b2share_meta_h:
                b2share_meta = json.load(b2share_meta_h)
                if isinstance(b2share_meta, dict):
                    urls = []
                    for file in b2share_meta.get("files", []):
                        if not isinstance(file, dict):
                            continue
                        new_url = file.get("ePIC_PID")
                        assert new_url is not None
                        relname = file.get("key")
                        urls.append( (new_url, relname) )
                    if len(urls) > 0:
                        return urls

    elif is_http and parsed_uri.netloc == ZENODO_HOST:
        if len(splitted_path) >= 2 and splitted_path[0] == "records":
            zenodo_meta_url = urllib.parse.urljoin(ZENODO_RECORD_REST, splitted_path[1])
            with urllib.request.urlopen(zenodo_meta_url) as zenodo_meta_h:
                zenodo_meta = json.load(zenodo_meta_h)
                if isinstance(zenodo_meta, dict):
                    urls = []
                    for file in zenodo_meta.get("files", []):
                        if not isinstance(file, dict):
                            continue
                        new_url = file.get("links", {}).get("self")
                        assert new_url is not None
                        relname = file.get("key")
                        urls.append( (new_url, relname) )
                    if len(urls) > 0:
                        return urls
        
        
    return [ (the_uri, None) ]
            

def process_challenges(
    community_label: "str",
    community_prefix: "str",
    bench_event_id: "str",
    bench_event_prefix_et_al: "BenchmarkingEventPrefixEtAl",
    challenges_ql: "Sequence[Mapping[str, Any]]",
    include_new_from: "Optional[str]",
) -> "Tuple[Mapping[str, ChallengePair], Mapping[str, ParticipantTuple], Sequence[ChallengePair], Mapping[str, Sequence[ParticipantTuple]], Mapping[str, Set[str]]]":

    challenges: "MutableMapping[str, ChallengePair]" = dict()
    participants: "MutableMapping[str, ParticipantTuple]" = dict()
    empty_challenge_pairs: "MutableSequence[ChallengePair]" = list()
    challenge2participants: "MutableMapping[str, Sequence[ParticipantTuple]]" = dict()
    uri2participants: "MutableMapping[str, Set[str]]" = dict()

    for challenge_ql in challenges_ql:
        challenge_id = challenge_ql["_id"]
        trimmed_challenge = cast("MutableMapping[str, Any]", copy.copy(challenge_ql))
        del trimmed_challenge["participant_datasets"]
        challenge_pair = ChallengePair.FromChallengeEtAl(trimmed_challenge, bench_event_prefix_et_al, community_prefix)

        # Recording the challenge for further checks
        challenges[challenge_id] = challenge_pair
        challenges[challenge_pair.label] = challenge_pair
        
        logging.info(f"Challenge {challenge_pair.label} {challenge_id}")
        challenge_participants: "MutableSequence[ParticipantTuple]" = list()
        for participant_ql in challenge_ql["participant_datasets"]:
            the_uri = participant_ql["datalink"].get("uri")
            if not isinstance(the_uri, str):
                logging.warning(f"Skipping {participant_ql['_id']}, as it does not have a fetch URI")
                continue
            p_tuple = ParticipantTuple.FromDataset(participant_ql, community_label)
            # Skip excluded participants
            if not p_tuple.p_config.exclude:
                # Find already declared participant tuple
                p_tuple = participants.setdefault(participant_ql["_id"], p_tuple)
                challenge_participants.append(p_tuple)
                
                # Add challenge (in case it was not added yet)
                if all(map(lambda ch_p: ch_p.entry["_id"] != challenge_pair.entry["_id"], p_tuple.challenge_pairs)):
                    p_tuple.challenge_pairs.append(challenge_pair)
                
                # Add fetch URI <=> participant relationship
                uri2participants.setdefault(the_uri, set()).add(participant_ql["_id"])

        if len(challenge_participants) > 0:
            challenge2participants[challenge_id] = challenge_participants
            challenge2participants[challenge_pair.label] = challenge_participants
        else:
            logging.info(f"Challenge {challenge_pair.label} had no participants")
            empty_challenge_pairs.append(challenge_pair)
    
    if include_new_from is not None and len(empty_challenge_pairs) > 0:
        new_challenges_participants = challenge2participants.get(include_new_from)
        if new_challenges_participants is not None:
            logging.info(f"Using participants from challenge {include_new_from} to run the first analysis for {' '.join(map(lambda cp: cp.label, empty_challenge_pairs))}")
            
            # Now let's create separate, fake participant entries to reuse reanalysis machinery below
            for challenge_pair in empty_challenge_pairs:
                challenge_participants = list()
                for template_p_tuple in new_challenges_participants:
                    new_id = template_p_tuple.p_config.participant_label
                    the_uri = template_p_tuple.participant_dataset["datalink"].get("uri")
                    assert isinstance(the_uri, str)

                    # Creating the template tuple
                    p_tuple = ParticipantTuple(
                        p_config=template_p_tuple.p_config,
                        participant_dataset={
                            "_id": new_id,
                            "_schema": template_p_tuple.participant_dataset["_schema"],
                            "datalink": template_p_tuple.participant_dataset["datalink"],
                        },
                        challenge_pairs=[],
                        community_acronym=community_label,
                    )
                    # Recording it
                    p_tuple = participants.setdefault(template_p_tuple.p_config.participant_label, p_tuple)
                    # Recording the challenge on it
                    p_tuple.challenge_pairs.append(challenge_pair)
                    
                    # Recording at the challenge level
                    challenge_participants.append(p_tuple)

                    # Add fetch URI <=> participant relationship
                    uri2participants.setdefault(the_uri, set()).add(new_id)
                
                challenge2participants[challenge_pair.entry["_id"]] = challenge_participants
                challenge2participants[challenge_pair.label] = challenge_participants
        else:
            logging.fatal(f"Challenge {include_new_from} not found in benchmarking event {bench_event_id}")
            sys.exit(1)
    
    return challenges, participants, empty_challenge_pairs, challenge2participants, uri2participants

def fetch_datasets(
    filtering_challenges: "Sequence[ChallengePair]",
    participants: "Mapping[str, ParticipantTuple]",
    uri2participants: "Mapping[str, Set[str]]",
    workdir: "str",
    datasets_cache_dir: "Optional[str]",
) -> "Mapping[str, str]":
    if datasets_cache_dir is None:
        datasets_cache_dir = os.path.join(workdir, DEFAULT_PD_CACHE_DIR)

    logging.info(f"=> Fetched datasets are being cached at {datasets_cache_dir}")
    os.makedirs(datasets_cache_dir, exist_ok=True)
    fetcher = OEBFetcher("", cache_entry_expire=-1, raw_cache_dir=datasets_cache_dir, do_compress_raw=False)

    logging.info(f"=> Fetching  datasets are being cached at {datasets_cache_dir}")
    uri2path = {}
    for uri, p_ids in uri2participants.items():
        # Checking whether to skip this URI
        do_skip = True
        for p_id in p_ids:
            p_tuple = participants[p_id]
            for challenge_pair in p_tuple.challenge_pairs:
                if challenge_pair in filtering_challenges:
                    do_skip = False
                    break
            if not do_skip:
                break

        if do_skip:
            logging.info(f"\t-> Skipping {uri} as its participants' challenges are not going to be analyzed")
            continue

        rel_files_dir = "_".join(sorted(p_ids))
        files_dir = os.path.join(workdir, FETCHED_DIR, rel_files_dir)
        logging.info(f"=> Fetching {uri}")
        logging.info(f"\t-> Creating directory {rel_files_dir} in {workdir}")
        os.makedirs(files_dir, exist_ok=True)
        
        fetch_uris = resolve_uri(uri)
        abs_resolved_file = None
        for resolved_uri, relname in fetch_uris:
            if relname is not None:
                resolved_file = relname
            else:
                resolved_file = hashlib.sha1(resolved_uri.encode("utf-8")).hexdigest()
            
            # Local filename to use in the working directory
            abs_resolved_file = os.path.normpath(os.path.join(files_dir, resolved_file))
            if os.path.commonpath((files_dir, abs_resolved_file)) != files_dir:
                abs_resolved_file = os.path.join(files_dir, os.path.basename(resolved_file))
            parent_abs_resolved_file = os.path.dirname(abs_resolved_file)
            if parent_abs_resolved_file != files_dir:
                os.makedirs(parent_abs_resolved_file, exist_ok=True)

            logging.info(f"\t-> Fetching {resolved_uri} into {abs_resolved_file}")
            with fetcher.openurl(resolved_uri, headers={}) as fetH, open(abs_resolved_file, mode="wb") as resH:
                shutil.copyfileobj(fetH, resH)

        if len(fetch_uris) > 1:
            input_file = files_dir
        else:
            assert abs_resolved_file is not None
            input_file = abs_resolved_file

        uri2path[uri] = input_file

    return uri2path

class ParticipantReanalysisUnit(NamedTuple):
    job_dir: "str"
    p_tuple: "ParticipantTuple"
    vre_config_json_path: "str"
    vre_in_metadata_json_path: "str"
    challenge_pairs: "Sequence[ChallengePair]"
    workflow_tool_id: "str"

def prepare_participants_reanalysis(
    migration_utils: "OpenEBenchUtils",
    workdir: "str",
    uri2path: "Mapping[str, str]",
    uri2participants: "Mapping[str, Set[str]]",
    participants: "Mapping[str, ParticipantTuple]",
    filtering_challenges: "Sequence[ChallengePair]",
    template_input_files_dict: "Mapping[str, Mapping[str, str]]",
    template_arguments_dict: "Mapping[str, Mapping[str, str]]",
    template_output_files_dict: "Mapping[str, Mapping[str, Any]]",
    template_in_metadata_dict: "Mapping[str, Mapping[str, Any]]",
) -> "Sequence[ParticipantReanalysisUnit]":
    jobs_to_perform: "MutableSequence[ParticipantReanalysisUnit]" = []

    jobs_dir = os.path.join(workdir, JOBS_DIR)
    for uri, input_file in uri2path.items():
        p_ids = uri2participants[uri]
        # We need any of the participant tuples to rescue the common
        # participant label and challenge ones
        for p_id in p_ids:
            p_tuple = participants[p_id]
            # Checking whether to skip this participant
            challenge_pairs_to_run = []
            for challenge_pair in p_tuple.challenge_pairs:
                if challenge_pair in filtering_challenges:
                    challenge_pairs_to_run.append(challenge_pair)

            if len(challenge_pairs_to_run) == 0:
                continue

            job_dir = os.path.join(jobs_dir, p_id)
            # To avoid undesired mixings
            #if os.path.exists(job_dir):
            #    shutil.rmtree(job_dir)
            os.makedirs(job_dir, exist_ok=True)

            p_label = p_tuple.p_config.participant_label
            challenge_labels_spc = " ".join(map(lambda cp: cp.label, challenge_pairs_to_run))

            input_files_dict = cast("Mapping[str, MutableMapping[str, str]]", copy.deepcopy(template_input_files_dict))
            arguments_dict = cast("MutableMapping[str, MutableMapping[str, str]]", copy.deepcopy(template_arguments_dict))
            output_files_dict = cast("Mapping[str, MutableMapping[str, Any]]", copy.deepcopy(template_output_files_dict))
            
            arguments_dict["challenges_ids"]["value"] = challenge_labels_spc
            arguments_dict["participant_id"]["value"] = p_label
            arguments_dict["project"]["value"] = job_dir

            # Using first valid challenge to get the tool details
            # Should a check about all of them using the same tool be added?
            tool_link: "Optional[str]" = None
            workflow_tool_id: "Optional[str]" = None
            for challenge_pair in p_tuple.challenge_pairs:
                c_entry = p_tuple.challenge_pairs[0].entry
                c_label = p_tuple.challenge_pairs[0].label
                for m_cat in c_entry.get("metrics_categories", []):
                    if m_cat.get("category") == AGGREGATION_CATEGORY_LABEL:
                        for m_tool in m_cat.get("metrics", []):
                            workflow_tool_id = m_tool.get("tool_id")
                            if workflow_tool_id is None:
                                logging.fatal(f"Fix challenge {c_label}")
                                sys.exit(3)
                            
                            tool = migration_utils.fetchStagedEntry("Tool", workflow_tool_id)
                            for tool_access in tool.get("tool_access", []):
                                if tool_access.get("tool_access_type") == "other":
                                    tool_link = tool_access.get("link")
                                    break
                            
                            if tool_link is not None:
                                break
                        if tool_link is not None:
                            break
                if tool_link is not None:
                    break

            if tool_link is not None:
                # FIXME: be prepared for other kind of git repos
                parsed_tool_link = urllib.parse.urlparse(tool_link)
                splitted_path = parsed_tool_link.path.split("/")
                
                nextflow_repo_tag = "main"
                nextflow_repo_reldir = ""
                if len(splitted_path) > 4 and splitted_path[3] == "tree":
                    nextflow_repo_tag = splitted_path[4]
                    if len(splitted_path) > 5:
                        nextflow_repo_reldir = "/".join(splitted_path[5:])
                
                parsed_tool_link_trimmed = parsed_tool_link._replace(path="/".join(splitted_path[0:3]))
                nextflow_repo_uri = urllib.parse.urlunparse(parsed_tool_link_trimmed)

                arguments_dict["nextflow_repo_uri"]["value"] = nextflow_repo_uri
                arguments_dict["nextflow_repo_tag"]["value"] = nextflow_repo_tag
                # This argument does not always appear
                arguments_dict.setdefault("nextflow_repo_reldir", {"name": "nextflow_repo_reldir"})["value"] = nextflow_repo_reldir
            else:
                logging.fatal(f"FIXME: challenges {challenge_labels_spc} tool {workflow_tool_id}")
                sys.exit(3)
            
            assert workflow_tool_id is not None

            vre_config_json = {
                "input_files": list(input_files_dict.values()),
                "arguments": list(arguments_dict.values()),
                "output_files": list(output_files_dict.values()),
            }
            
            in_metadata_dict = cast("Mapping[str, MutableMapping[str, Any]]", copy.deepcopy(template_in_metadata_dict))
            
            # Setting the input file path
            in_metadata_dict[input_files_dict["input"]["value"]]["file_path"] = input_file
            
            vre_in_metadata = list(in_metadata_dict.values())

            vre_config_json_path = os.path.join(job_dir, "config.json")
            with open(vre_config_json_path, mode="w", encoding="utf-8") as vcH:
                json.dump(vre_config_json, vcH)
            
            vre_in_metadata_json_path = os.path.join(job_dir, "in_metadata.json")
            with open(vre_in_metadata_json_path, mode="w", encoding="utf-8") as vimH:
                json.dump(vre_in_metadata, vimH)

            jobs_to_perform.append(
                ParticipantReanalysisUnit(
                    job_dir=job_dir,
                    p_tuple=p_tuple,
                    vre_config_json_path=vre_config_json_path,
                    vre_in_metadata_json_path=vre_in_metadata_json_path,
                    challenge_pairs=challenge_pairs_to_run,
                    workflow_tool_id=workflow_tool_id,
                )
            )
            logging.info(f"{p_label} => {challenge_labels_spc} <= {uri}")

    return jobs_to_perform

def run_vre_runner_job(vre_runner_path: "str", job_to_perform: "ParticipantReanalysisUnit") -> "Tuple[Optional[int], str]":
    job_meta_dir = os.path.join(job_to_perform.job_dir, "_meta_")
    os.makedirs(job_meta_dir, exist_ok=True)

    vre_out_metadata_json_path = os.path.join(job_meta_dir, "out_metadata.json")
    cmdline = [
        vre_runner_path,
        "--config", job_to_perform.vre_config_json_path,
        "--in_metadata", job_to_perform.vre_in_metadata_json_path,
        "--out_metadata", vre_out_metadata_json_path,
        "--log_file", os.path.join(job_meta_dir, "runner_log.txt"),
    ]
    retcode = -1
    retcode_file = os.path.join(job_meta_dir, "exitcode.txt")
    if os.path.exists(retcode_file):
        try:
            with open(retcode_file, mode="r", encoding="utf-8") as rH:
                retcode = int(rH.read())
        except:
            pass
    
    if retcode != 0:
        job_log = os.path.join(job_meta_dir, "job_log.txt")
        with open(job_log, mode="w", encoding="utf-8") as logH:
            cproc = subprocess.run(
                cmdline,
                stdin=subprocess.DEVNULL,
                stdout=logH,
                stderr=subprocess.STDOUT,
                cwd=job_to_perform.job_dir,
                encoding="utf-8",
            )
            with open(retcode_file, mode="w", encoding="utf-8") as wH:
                wH.write(str(cproc.returncode))

            return cproc.returncode, vre_out_metadata_json_path
    else:
        return None, vre_out_metadata_json_path

def reanalyse_challenges(
    bench_event_id: "str",
    challenge_ids: "Sequence[str]",
    executor_dir: "str",
    template_base_dir: "Optional[str]",
    template_config_path: "str",
    template_in_metadata_path: "str",
    oeb_credentials_filename: "str",
    oeb_token: "Optional[str]" = None,
    cache_entry_expire: "Optional[Union[int, float]]" = None,
    override_cache: "bool" = False,
    log_filename: "Optional[str]" = None,
    log_level: "int" = logging.INFO,
    minimal_entries_dir: "str" = ".",
    workdir: "Optional[str]" = None,
    datasets_cache_dir: "Optional[str]" = None,
    include_new_from: "Optional[str]" = None,
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
    
    level2_min_validator, num_level2_schemas, expected_level2_schemas = level2_schemas.create_validator_for_oeb_level2()
    if num_level2_schemas < expected_level2_schemas:
        logging.error(f"OEB level2 operational JSON Schemas not found ({num_level2_schemas} vs {expected_level2_schemas})")
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

    vre_runner_path = os.path.join(executor_dir, VRE_EXECUTABLE)
    
    if not os.path.isfile(vre_runner_path) or not os.access(vre_runner_path, os.X_OK):
        logging.fatal(f"Unable to find or run {VRE_EXECUTABLE} in {executor_dir}, needed for the reanalysis")
        sys.exit(1)

    # Some checks on the VRE templates
    if template_base_dir is None:
        template_base_dir = executor_dir

    logging.info(f"Loading VRE template files (resolved against {template_base_dir})")

    if os.path.isabs(template_config_path):
        abs_template_config_path = template_config_path
    else:
        abs_template_config_path = os.path.normpath(os.path.join(template_base_dir, template_config_path))

    try:
        with open(abs_template_config_path, mode="r", encoding="utf-8") as atH:
            template_config = json.load(atH)
    except:
        logging.exception(f"Unable to read VRE template config.json from {abs_template_config_path}")
        raise
    
    if not isinstance(template_config, dict):
        errmsg = f"VRE template config.json file {abs_template_config_path} is not a dictionary"
        logging.fatal(errmsg)
        raise ValueError(errmsg)
        
    if os.path.isabs(template_in_metadata_path):
        abs_template_in_metadata_path = template_in_metadata_path
    else:
        abs_template_in_metadata_path = os.path.normpath(os.path.join(template_base_dir, template_in_metadata_path))
    
    try:
        with open(abs_template_in_metadata_path, mode="r", encoding="utf-8") as atH:
            template_in_metadata = json.load(atH)
    except:
        logging.exception(f"Unable to read VRE template in_metadata.json from {abs_template_in_metadata_path}")
        raise

    if not isinstance(template_in_metadata, list):
        errmsg = f"VRE template in_metadata.json file {abs_template_in_metadata_path} is not a list"
        logging.fatal(errmsg)
        raise ValueError(errmsg)

    input_files = template_config.get("input_files", [])
    if len(template_in_metadata) != len(input_files):
        errmsg = f"VRE template in_metadata.json refers {len(template_in_metadata)} files, but {len(input_files)} were declared in config.json"
        logging.fatal(errmsg)
        raise KeyError(errmsg)

    template_in_metadata_dict = {
        input_m["_id"]: input_m
        for input_m in template_in_metadata
    }
    
    template_input_files_dict = {}
    for input_file in input_files:
        input_metadata = template_in_metadata_dict[input_file.get("value")]
        if input_metadata is None:
            errmsg = f"VRE template config.json refers file {input_file.get('value')}, but it was missing in in_metadata.json"
            logging.fatal(errmsg)
            raise KeyError(errmsg)
        
        # Fixing paths
        if not os.path.isabs(input_metadata["file_path"]):
            input_metadata["file_path"] = os.path.normpath(os.path.join(template_base_dir, input_metadata["file_path"]))
        
        input_file_name = input_file.get("name")
        template_input_files_dict[input_file_name] = input_file
    
    template_arguments_dict = {
        argument["name"]: argument
        for argument in template_config.get("arguments", [])
    }
    
    template_output_files_dict: "MutableMapping[str, MutableMapping[str, Any]]" = {}
    for output_file in template_config.get("output_files", []):
        template_output_files_dict[output_file["name"]] = output_file
        output_file_path = output_file["file"].get("file_path")
        if output_file_path is not None:
            output_file["file"]["file_path"] = os.path.basename(output_file_path)

    # TODO
    # Minimal consistency checks

    # get data model to validate against
    migration_utils = OpenEBenchUtils(
        oeb_credentials,
        config_json_dir,
        oeb_token=oeb_token,
        cache_entry_expire=cache_entry_expire,
        override_cache=override_cache,
        level2_min_validator=level2_min_validator,
    )
    
    logging.info(f"-> Fetching and using schemas from the server {migration_utils.oeb_api_base}")
    schemaMappings = migration_utils.load_schemas_from_server()

    logging.info("-> Querying graphql about known participants")
    participant_query_response = migration_utils.graphql_query_OEB_DB(
        GraphQLQueryLabel.Participant,
        bench_event_id,
    )
    
    # Getting the benchmarking event prefix details
    # Work needed to learn about the communities
    bench_event = participant_query_response["data"]["getBenchmarkingEvents"][0]
    community_id = bench_event["community_id"]

    # Fixing several values
    template_arguments_dict["community_id"]["value"] = community_id

    
    # Prefixes about communities
    stagedCommunity = migration_utils.fetchStagedEntry("Community", community_id)
    
    community_label = OpenEBenchUtils.get_community_label(stagedCommunity)
    community_prefix = OpenEBenchUtils.gen_community_prefix_from_label(community_label)
    bench_event_prefix_et_al = migration_utils.gen_benchmarking_event_prefix(bench_event, community_prefix)

    # Now it is time to focus this on the participants
    challenges, participants, empty_challenge_pairs, challenge2participants, uri2participants = process_challenges(
        community_label,
        community_prefix,
        bench_event_id,
        bench_event_prefix_et_al,
        participant_query_response["data"]["getChallenges"],
        include_new_from,
    )
    
    logging.info(f"Mapped {len(uri2participants)} URLs from {len(participants)} participant datasets involved in {len(participant_query_response['data']['getChallenges'])} challenges")

    filtering_challenges: "MutableSequence[ChallengePair]" = []
    if len(challenge_ids) > 0:
        the_challenge_ids: "MutableSequence[str]" = list(challenge_ids)
        # if include_new_from is not None:
        #     the_challenge_ids.insert(0, include_new_from)
        for challenge_id in the_challenge_ids:
            if challenge_id in challenge2participants:
                filtered_challenge = challenges[challenge_id]
                if filtered_challenge not in filtering_challenges:
                    filtering_challenges.append(filtered_challenge)
            else:
                logging.fatal(f"Challenge {challenge_id} to be filtered in does not exist (or does not have participants)")
                sys.exit(2)
    else:
        filtering_challenges = list(challenges.values())

    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="OEB",suffix="rel2")
    elif not os.path.isabs(workdir):
        workdir = os.path.abspath(workdir)
    # At this point we should have always an absolute path

    logging.info(f"=> Working directory is now {workdir}")

    uri2path = fetch_datasets(filtering_challenges, participants, uri2participants, workdir, datasets_cache_dir)
        
    logging.info(f"* Reanalysis on {' '.join(map(lambda fc: fc.label, filtering_challenges))} is starting now")
    
    jobs_to_perform = prepare_participants_reanalysis(
        migration_utils,
        workdir,
        uri2path,
        uri2participants,
        participants,
        filtering_challenges,
        template_input_files_dict,
        template_arguments_dict,
        template_output_files_dict,
        template_in_metadata_dict,
    )
    
    logging.info(f"{len(jobs_to_perform)} jobs will be run (please be patient)")

    for job_i, job_to_perform in enumerate(jobs_to_perform, 1):
        logging.info(f"* Job {job_i} of {len(jobs_to_perform)}: {job_to_perform.p_tuple.p_config.participant_label} challenges {' '.join(map(lambda cp: cp.label, job_to_perform.challenge_pairs))}")
        retval , vre_out_metadata_json_path = run_vre_runner_job(vre_runner_path, job_to_perform)
        if retval is None:
            logging.info("\t\tAlready done. Skipping.")
        elif retval != 0:
            logging.error(f"\t\t-> Job at {job_to_perform.job_dir} failed (exit {retval})")
            continue
        else:
            logging.info("\t\tDone!")
        
        # Now it is time to transfer the minimal aggregation dataset to
        # the results place and generate its companion config file
        minimal_result_dir = os.path.join(minimal_entries_dir, os.path.basename(job_to_perform.job_dir))
        if os.path.exists(minimal_result_dir):
            shutil.rmtree(minimal_result_dir)
        os.makedirs(minimal_result_dir, exist_ok=True)

        with open(vre_out_metadata_json_path, mode="r", encoding="utf-8") as vH:
            vre_out_metadata = json.load(vH)
        
        minimal_datasets_path: "Optional[str]" = None
        for output_meta in vre_out_metadata.get("output_files", []):
            if output_meta.get("name") == "data_model_export_dir":
                minimal_datasets_path = output_meta.get("file_path")
        if minimal_datasets_path is None:
            logging.error(f"\t\tMissing data_model_export_dir in {vre_out_metadata_json_path}")
            continue
                
        rel_minimal_datasets_path = os.path.basename(minimal_datasets_path)
        dest_minimal_datasets_path = os.path.join(minimal_result_dir, rel_minimal_datasets_path)
        shutil.copyfile(minimal_datasets_path, dest_minimal_datasets_path)
        config_level2 = {
            "consolidated_oeb_data": rel_minimal_datasets_path,
            "data_visibility": "public",
            "benchmarking_event_id": bench_event_id,
            "community_id": community_id,
            "tool_mapping": [
                {
                    "participant_id": job_to_perform.p_tuple.p_config.participant_label,
                    "tool_id": job_to_perform.p_tuple.p_config.tool_id,
                    "data_version": 1,
                    "data_contacts": job_to_perform.p_tuple.p_config.data_contacts,
                    "participant_file": job_to_perform.p_tuple.p_config.participant_file,
                }
            ],
            "data_storage_endpoint": "https://trng-b2share.eudat.eu/api/",
            "fix_original_ids": True,
            "workflow_oeb_id": job_to_perform.workflow_tool_id,
        }
        with open(os.path.join(minimal_result_dir, "config_level2.json"), mode="w", encoding="utf-8") as clH:
            json.dump(config_level2, clH)
        
        logging.info(f"\t-> Minimal datasets available at {minimal_result_dir}")
    

def main() -> "None":
    parser = argparse.ArgumentParser(description='OEB Scientific Challenge Re-analyser', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

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
        "-m",
        "--minimal-entries-dir",
        help="Output directory where minimal entries computed by the metrics workflow are going to be saved, easing the work (by default, current directory)",
        required=True,
    )
    parser.add_argument(
        "--cache",
        dest="cache_entry_expire",
        help="If this parameter is used, OEB graphql and REST requests will be cached at most the seconds specified in the parameter, in order to speed up some code paths. When a value is provided, it sets the expiration in seconds for those cached values whose server did not provide or implement caching headers Last-Modified or ETag",
        nargs="?",
        const=-1,
        type=float,
    )
    parser.add_argument(
        "--invalidate-cache",
        dest="override_cache",
        help="When OEB cache is enabled, this flag teaches to invalidate previously cached OEB contents",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-w",
        "--work-dir",
        dest="workdir",
        help="Working directory where intermediate files are going to be stored",
    )
    parser.add_argument(
        "-C",
        "--cache-dir",
        dest="datasets_cache_dir",
        help="Directory where participant dataset files are cached",
    )
    inswitch = parser.add_mutually_exclusive_group()
    inswitch.add_argument(
        "--include-new",
        dest="include_new_from",
        type=str,
        help="Include all participants from this group in new challenges. You can use either the challenge label or the challenge id",
    )
    inswitch.add_argument(
        "--no-include-new",
        dest="include_new_from",
        const=None,
        action="store_const",
        help="Discard filling empty challenges with participants from other ones",
    )
    
    parser.add_argument(
        "--executor-dir",
        dest="executor_dir",
        required=True,
        help="Location of the directory of the VRE executor",
    )
    
    parser.add_argument(
        "--template-base-dir",
        dest="template_base_dir",
        help="Base directory of the VRE config and in_metadata templates. By default, the same as the executor.",
    )
    
    parser.add_argument(
        "--template-config",
        dest="template_config_path",
        required=True,
        help="Path to the template VRE config.json file to use for the reanalysis. When it is a relative path, the path is resolved using the template base directory.",
    )
    
    parser.add_argument(
        "--template-in-metadata",
        dest="template_in_metadata_path",
        required=True,
        help="Path to the template VRE in_metadata.json file to use for the reanalysis. When it is a relative path, the path is resolved using the template base directory.",
    )
    
    parser.add_argument(
        "bench_event_id",
        help="Benchmarking event id whose challenges are going to be reanalysed",
    )
    parser.add_argument(
        "challenge_id",
        nargs="*",
        help="Restrict the reanalysis to these challenges (they must belong to the benchmarking event)",
    )
    parser.add_argument(
        '-V',
        '--version',
        action="version",
        version="%(prog)s version " + oeb_level2_version,
    )

    args = parser.parse_args()

    reanalyse_challenges(
        args.bench_event_id,
        args.challenge_id,
        executor_dir=args.executor_dir,
        template_base_dir=args.template_base_dir,
        template_config_path=args.template_config_path,
        template_in_metadata_path=args.template_in_metadata_path,
        oeb_credentials_filename=args.oeb_submit_api_creds,
        oeb_token=args.oeb_submit_api_token,
        cache_entry_expire=args.cache_entry_expire,
        override_cache=args.override_cache,
        log_filename=args.logFilename,
        log_level=logging.INFO if args.logLevel is None else args.logLevel,
        minimal_entries_dir=args.minimal_entries_dir,
        workdir=args.workdir,
        datasets_cache_dir=args.datasets_cache_dir,
        include_new_from=args.include_new_from,
    )

if __name__ == '__main__':
    main()
