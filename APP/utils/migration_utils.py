#!/usr/bin/env python

import os
import sys
import hashlib
import tempfile
import subprocess
import logging
import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from fairtracks_validator.validator import FairGTracksValidator
import yaml
# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
    from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
    from yaml import Loader as YAMLLoader, Dumper as YAMLDumper


class utils():
    DEFAULT_OEB_API = "https://dev-openebench.bsc.es/sciapi/graphql"
    DEFAULT_OEB_SUBMISSION_API = "https://dev-openebench.bsc.es/api/scientific/submission/"
    DEFAULT_GIT_CMD = 'git'
    DEFAULT_DATA_MODEL_DIR = "benchmarking_data_model"

    def __init__(self, config_db, oeb_credentials, workdir):

        self.data_model_dir = os.path.join(workdir, self.DEFAULT_DATA_MODEL_DIR)
        self.git_cmd = self.DEFAULT_GIT_CMD
        self.oeb_api = oeb_credentials.get("graphqlURI", self.DEFAULT_OEB_API)
	
        storageServer = oeb_credentials.get('storageServer', {})
        self.b2share_api_token = storageServer['token']
        self.b2share_community = storageServer['community']
        self.b2share_endpoint = storageServer['endpoint']
        
        self.oeb_submission_api = oeb_credentials.get('submissionURI', self.DEFAULT_OEB_SUBMISSION_API)

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        logging.basicConfig(level=logging.INFO)

        # load the remote OEB DB ids and the validator

        with open(config_db, "r", encoding="utf-8") as cf:
            local_config = yaml.load(cf, Loader=YAMLLoader)

        self.schema_validators = FairGTracksValidator(config=local_config)

    # function to pull a github repo obtained from https://github.com/inab/vre-process_nextflow-executor/blob/master/tool/VRE_NF.py

    def doMaterializeRepo(self, git_uri, git_tag):

        repo_hashed_id = hashlib.sha1(git_uri.encode('utf-8')).hexdigest()
        repo_hashed_tag_id = hashlib.sha1(git_tag.encode('utf-8')).hexdigest()

        # Assure directory exists before next step
        repo_destdir = os.path.join(
            self.data_model_dir, repo_hashed_id)
        if not os.path.exists(repo_destdir):
            try:
                os.makedirs(repo_destdir)
            except IOError as error:
                errstr = "ERROR: Unable to create intermediate directories for repo {}. ".format(
                    git_uri,)
                raise Exception(errstr)

        repo_tag_destdir = os.path.join(repo_destdir, repo_hashed_tag_id)
        # We are assuming that, if the directory does exist, it contains the repo
        if not os.path.exists(repo_tag_destdir):
            # Try cloing the repository without initial checkout
            gitclone_params = [
                self.git_cmd, 'clone', '-n', '--recurse-submodules', git_uri, repo_tag_destdir
            ]

            # Now, checkout the specific commit
            gitcheckout_params = [
                self.git_cmd, 'checkout', git_tag
            ]

            # Last, initialize submodules
            gitsubmodule_params = [
                self.git_cmd, 'submodule', 'update', '--init'
            ]

            with tempfile.NamedTemporaryFile() as git_stdout:
                with tempfile.NamedTemporaryFile() as git_stderr:
                    # First, bare clone
                    retval = subprocess.call(
                        gitclone_params, stdout=git_stdout, stderr=git_stderr)
                    # Then, checkout
                    if retval == 0:
                        retval = subprocess.Popen(
                            gitcheckout_params, stdout=git_stdout, stderr=git_stderr, cwd=repo_tag_destdir).wait()
                    # Last, submodule preparation
                    if retval == 0:
                        retval = subprocess.Popen(
                            gitsubmodule_params, stdout=git_stdout, stderr=git_stderr, cwd=repo_tag_destdir).wait()

                    # Proper error handling
                    if retval != 0:
                        # Reading the output and error for the report
                        with open(git_stdout.name, "r") as c_stF:
                            git_stdout_v = c_stF.read()
                        with open(git_stderr.name, "r") as c_stF:
                            git_stderr_v = c_stF.read()

                        errstr = "ERROR:  could not pull '{}' (tag '{}'). Retval {}\n======\nSTDOUT\n======\n{}\n======\nSTDERR\n======\n{}".format(
                            git_uri, git_tag, retval, git_stdout_v, git_stderr_v)
                        raise Exception(errstr)

        return repo_tag_destdir

    # function that retrieves all the required metadata from OEB database
    def query_OEB_DB(self, bench_event_id, tool_id, community_id, data_type):

        if data_type == "input":
            json_query = {'query': '{\
                                    getChallenges(challengeFilters: {benchmarking_event_id: "' + bench_event_id + '"}) {\
                                        _id\
                                        _metadata\
                                        datasets(datasetFilters: {type: "input"}) {\
                                            _id\
                                        }\
                                    }\
                                    getTools(toolFilters: {id: "' + tool_id + '"}) {\
                                        _id\
                                    }\
                                    getContacts(contactFilters:{community_id:"' + community_id + '"}){\
                                        _id\
                                        email\
                                    }\
                                }'}
        elif data_type == "metrics_reference":
            json_query = {'query': '{\
                                    getChallenges(challengeFilters: {benchmarking_event_id: "' + bench_event_id + '"}) {\
                                        _id\
                                        _metadata\
                                        datasets(datasetFilters: {type: "metrics_reference"}) {\
                                            _id\
                                        }\
                                    }\
                                    getTools(toolFilters: {id: "' + tool_id + '"}) {\
                                        _id\
                                    }\
                                    getContacts(contactFilters:{community_id:"' + community_id + '"}){\
                                        _id\
                                        email\
                                    }\
                                    getMetrics {\
                                        _id\
                                        _metadata\
                                    }\
                                }'}
        elif data_type == "aggregation":
            json_query = {'query': '{\
                                    getChallenges(challengeFilters: {benchmarking_event_id: "' + bench_event_id + '"}) {\
                                        _id\
                                        _metadata\
                                        challenge_contact_ids\
                                        datasets(datasetFilters: {type: "aggregation"}) {\
                                                _id\
                                                _schema\
                                                orig_id\
                                                community_ids\
                                                challenge_ids\
                                                visibility\
                                                name\
                                                version\
                                                description\
                                                dates {\
                                                    creation\
                                                    modification\
                                                }\
                                                type\
                                                datalink {\
                                                    inline_data\
                                                }\
                                                dataset_contact_ids\
                                                depends_on {\
                                                    tool_id\
                                                    rel_dataset_ids {\
                                                    dataset_id\
                                                    }\
                                                }\
                                        }\
                                    }\
                                    getTools(toolFilters: {id: "' + tool_id + '"}) {\
                                        _id\
                                    }\
                                    getContacts(contactFilters:{community_id:"' + community_id + '"}){\
                                        _id\
                                        email\
                                    }\
                                    getMetrics {\
                                        _id\
                                        _metadata\
                                    }\
                                      getTestActions{\
                                        _id\
                                        _schema\
                                        orig_id\
                                        tool_id\
                                        action_type\
                                        involved_datasets{\
                                            dataset_id\
                                            role\
                                        }\
                                        challenge_id\
                                        test_contact_ids\
                                        dates{\
                                            creation\
                                            modification\
                                        }\
                                    }\
                                }'}
        try:
            url = self.oeb_api
            # get challenges and input datasets for provided benchmarking event
            r = requests.post(url=url, json=json_query, verify=False)
            response = r.json()
            if response["data"]["getChallenges"] == []:

                logging.fatal("No challenges associated to benchmarking event " + bench_event_id +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()
            # check if provided oeb tool actually exists
            elif response["data"]["getTools"] == []:

                logging.fatal(
                    "No tool '" + tool_id + "' was found in OEB. Please contact OpenEBench support for information about how to register your tool")
                sys.exit()
            else:
                return response
        except Exception as e:

            logging.exception(e)

    # function that uploads the predictions file to a remote server for it long-term storage, and produces a DOI
    def upload_to_storage_service(self, participant_data, file_location, contact_email, data_version):

        # check if file already has an assigned doi, if not, upload
        if "doi.org" in file_location:
            logging.info(
                "Participant's predictions file already has an assigned DOI: " + file_location)
            return file_location

        else:
            endpoint = self.b2share_endpoint
            # 1. create new record
            logging.info("Uploading participant's predictions file to " +
                         endpoint + " for permanent storage")
            header = {"Content-Type": "application/json"}
            params = {'access_token': self.b2share_api_token}
            metadata = {"titles": [{"title": "Predictions made by " + participant_data["participant_id"] + " participant in OpenEBench Virtual Research Environment"}],
                        "community": self.b2share_community,
                        "community_specific": {},
                        "contact_email": contact_email,
                        "version": str(data_version),
                        "open_access": True}
            r = requests.post(endpoint + "records/", params=params,
                              data=json.dumps(metadata), headers=header)

            result = json.loads(r.text)
            # check whether request was succesful
            if r.status_code != 201:
                logging.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            # 2. add file to new record
            filebucketid = result["links"]["files"].split('/')[-1]
            record_id = result["id"]

            try:
                upload_file = open(file_location, 'rb')
            except OSError as exc:
                logging.fatal("OS error: {0}".format(exc))
                sys.exit()

            url = endpoint + 'files/' + filebucketid
            header = {"Accept": "application/json",
                      "Content-Type": "application/octet-stream"}

            r = requests.put(url + '/' + os.path.basename(file_location),
                             data=upload_file, params=params, headers=header)

            # check whether request was succesful
            if r.status_code != 200:
                logging.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            # 3. publish the new record
            header = {'Content-Type': 'application/json-patch+json'}
            commit = '[{"op": "add", "path":"/publication_state", "value": "submitted"}]'

            url = endpoint + "records/" + record_id + "/draft"
            r = requests.patch(url, data=commit, params=params, headers=header)

            # check whether request was succesful
            if r.status_code != 200:
                logging.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            published_result = json.loads(r.text)

            data_doi = published_result["metadata"]["DOI"]
            # print(record_id) https://trng-b2share.eudat.eu/api/records/637a25e86dbf43729d30217613f1218b
            logging.info("File '" + file_location +
                         "' uploaded and permanent ID assigned: " + data_doi)
            return data_doi

    def schemas_validation(self, jsonSchemas_array, data_model_dir):

        # create the cached json schemas for validation
        numSchemas = self.schema_validators.loadJSONSchemas(
            os.path.join(data_model_dir, "json-schemas", "1.0.x"), verbose=False)

        if numSchemas == 0:
            print(
                "FATAL ERROR: No schema was successfuly loaded. Exiting...\n", file=sys.stderr)
            sys.exit(1)

        # validate the newly annotated dataset against https://github.com/inab/benchmarking-data-model

        logging.info(
            "\n\t==================================\n\t7. Validating datasets and TestActions\n\t==================================\n")

        cached_jsons = []
        for element in jsonSchemas_array:

            cached_jsons.append(
                {'json': element, 'file': "inline" + element["_id"], 'errors': []})

        val_res = self.schema_validators.jsonValidate(
            *cached_jsons, verbose=True)

        # check for errors in the validation results
        # TODO: if one of the objects to upload has a OEBXXXX primary key, the validator returns a 'Duplicated PK' error
        # For now, those objects are escaped...
        for val_obj in val_res:
            if val_obj["json"]["_id"].startswith("OEB"):
                continue
            elif val_obj["errors"]:
                logging.fatal("\nObjects validation Failed:\n " + str(val_obj))
                # logging.fatal("\nSee full validation logs:\n " + str(val_res))
                sys.exit()

        logging.info(
            "\n\t==================================\n\t Objects validated\n\t==================================\n")

    def submit_oeb_buffer(self, json_data, oeb_buffer_token, community_id):

        logging.info("\n\t==================================\n\t8. Uploading workflow results to https://dev-openebench.bsc.es/api/scientific/submission/\n\t==================================\n")

        header = {"Content-Type": "application/json"}
        params = {
                    'access_token': oeb_buffer_token,
                    'community_id': community_id
                    
                }
        r = requests.post(self.oeb_submission_api, params=params,
                          data=json.dumps(json_data), headers=header)

        if r.status_code != 200:
            logging.fatal("Error in uploading data to OpenEBench. Bad request: " +
                          str(r.status_code) + str(r.text))
            sys.exit()
        else:
            logging.info(
                "\n\tData uploaded correctly...finalizing migration\n\n")
