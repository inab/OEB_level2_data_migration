#!/usr/bin/env python

import os
import sys
import datetime
import hashlib
import tempfile
import subprocess
import logging
import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from fairtracks_validator.validator import FairGTracksValidator

import urllib.request
import urllib.parse

import yaml
# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
    from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
    from yaml import Loader as YAMLLoader, Dumper as YAMLDumper


DEFAULT_AUTH_URI = 'https://inb.bsc.es/auth/realms/openebench/protocol/openid-connect/token'
DEFAULT_CLIENT_ID = 'THECLIENTID'
DEFAULT_GRANT_TYPE = 'password'

def getAccessToken(oeb_credentials):
    authURI = oeb_credentials.get('authURI', DEFAULT_AUTH_URI)
    payload = {
        'client_id': oeb_credentials.get('clientId', DEFAULT_CLIENT_ID),
        'grant_type': oeb_credentials.get('grantType', DEFAULT_GRANT_TYPE),
        'username': oeb_credentials['user'],
        'password': oeb_credentials['pass'],
    }
    
    req = urllib.request.Request(authURI, data=urllib.parse.urlencode(payload).encode('UTF-8'), method='POST')
    with urllib.request.urlopen(req) as t:
        token = json.load(t)
        
        logging.info("Token {}".format(token['access_token']))
        
        return token['access_token']    

class OpenEBenchUtils():
    DEFAULT_OEB_API = "https://dev-openebench.bsc.es/api/scientific/graphql"
    DEFAULT_OEB_SUBMISSION_API = "https://dev-openebench.bsc.es/api/scientific/submission/"
    DEFAULT_GIT_CMD = 'git'
    DEFAULT_DATA_MODEL_DIR = "benchmarking_data_model"

    def __init__(self, oeb_credentials, workdir, oeb_token = None):

        self.data_model_repo_dir = os.path.join(workdir, self.DEFAULT_DATA_MODEL_DIR)
        self.git_cmd = self.DEFAULT_GIT_CMD
        self.oeb_api = oeb_credentials.get("graphqlURI", self.DEFAULT_OEB_API)
        
        self.oeb_submission_api = oeb_credentials.get('submissionURI', self.DEFAULT_OEB_SUBMISSION_API)

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        logging.basicConfig(level=logging.INFO)
        
        oebIdProviders = oeb_credentials['accessURI']
        if not isinstance(oebIdProviders, list):
            oebIdProviders = [ oebIdProviders ]
        
        local_config = {
            'primary_key': {
                'provider': [
                    *oebIdProviders,
                    oeb_credentials['submissionURI']
                ],
                # To be set on instantiation
                # 'schema_prefix': None,
                'accept': 'text/uri-list'
            }
        }
        
        
        self.oeb_token = getAccessToken(oeb_credentials)  if oeb_token is None  else  oeb_token
        
        self.schema_validators_local_config = local_config

        self.schema_validators = None
        
        self.schemaMappings = None

    # function to pull a github repo obtained from https://github.com/inab/vre-process_nextflow-executor/blob/master/tool/VRE_NF.py

    def doMaterializeRepo(self, git_uri, git_tag):

        repo_hashed_id = hashlib.sha1(git_uri.encode('utf-8')).hexdigest()
        repo_hashed_tag_id = hashlib.sha1(git_tag.encode('utf-8')).hexdigest()

        # Assure directory exists before next step
        repo_destdir = os.path.join(
            self.data_model_repo_dir, repo_hashed_id)
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
#            }
            json_query = {'query': """query InputQuery($bench_event_id: String, $tool_id: String, $community_id: String) {
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        datasets(datasetFilters: {type: "input"}) {
            _id
        }
    }
    getTools(toolFilters: {id: $tool_id}) {
        _id
    }
    getContacts(contactFilters: {community_id: $community_id}) {
        _id
        email
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                    'community_id': community_id,
                    'tool_id': tool_id
                }
            }
        elif data_type == "metrics_reference":
            json_query = {'query': """query MetricsReferenceQuery($bench_event_id: String, $tool_id: String, $community_id: String) {
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        datasets(datasetFilters: {type: "metrics_reference"}) {
            _id
        }
    }
    getTools(toolFilters: {id: $tool_id}) {
        _id
    }
    getContacts(contactFilters:{community_id: $community_id}) {
        _id
        email
    }
    getMetrics {
        _id
        _metadata
        orig_id
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                    'community_id': community_id,
                    'tool_id': tool_id
                }
            }
        elif data_type == "aggregation":
            json_query = {'query': """query AggregationQuery($bench_event_id: String, $tool_id: String, $community_id: String) {
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        _metadata
        challenge_contact_ids
        datasets(datasetFilters: {type: "aggregation"}) {
                _id
                _schema
                orig_id
                community_ids
                challenge_ids
                visibility
                name
                version
                description
                dates {
                    creation
                    modification
                }
                type
                datalink {
                    inline_data
                }
                dataset_contact_ids
                depends_on {
                    tool_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
        }
    }
    getTools(toolFilters: {id: $tool_id}) {
        _id
    }
    getContacts(contactFilters:{community_id: $community_id}) {
        _id
        email
    }
    getMetrics {
        _id
        _metadata
    }
    getTestActions {
        _id
        _schema
        orig_id
        tool_id
        action_type
        involved_datasets {
            dataset_id
            role
        }
        challenge_id
        test_contact_ids
        dates {
            creation
            modification
        }
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                    'community_id': community_id,
                    'tool_id': tool_id
                }
            }
        else:
            logging.fatal("Unable to generate graphQL query: Unknown datatype {}".format(data_type))
            sys.exit(2)
        try:
            url = self.oeb_api
            # get challenges and input datasets for provided benchmarking event
            r = requests.post(url=url, json=json_query, headers={'Authorization': 'Bearer {}'.format(self.oeb_token)}, verify=False)
            response = r.json()
            data = response.get("data")
            if data is None:
                logging.fatal("For {}, {}, {} got response error from graphql query: {}".format(community_id, bench_event_id, tool_id, r.text))
                sys.exit(6)
            if len(data["getChallenges"]) == 0:

                logging.fatal("No challenges associated to benchmarking event " + bench_event_id +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit()
            # check if provided oeb tool actually exists
            elif len(data["getTools"]) == 0:

                logging.fatal(
                    "No tool '" + tool_id + "' was found in OEB. Please contact OpenEBench support for information about how to register your tool")
                sys.exit(2)
            else:
                # Deserializing _metadata
                challenges = data.get('getChallenges')
                if challenges is not None:
                    for challenge in challenges:
                        metadata = challenge.get('_metadata')
                        # Deserialize the metadata
                        if isinstance(metadata,str):
                            challenge['_metadata'] = json.loads(metadata)
                        
                        # Deserializing inline_data
                        datasets = challenge.get('datasets',[])
                        for dataset in datasets:
                            datalink = dataset.get('datalink')
                            if datalink is not None:
                                inline_data = datalink.get('inline_data')
                                if isinstance(inline_data, str):
                                    datalink['inline_data'] = json.loads(inline_data)
                
                metrics = data.get('getMetrics')
                if metrics is not None:
                    for metric in metrics:
                        metadata = metric.get('_metadata')
                        # Deserialize the metadata
                        if isinstance(metadata,str):
                            metric['_metadata'] = json.loads(metadata)
                
                return response
        except Exception as e:

            logging.exception(e)

    # function that uploads the predictions file to a remote server for it long-term storage, and produces a DOI
    def upload_to_storage_service(self, participant_data, file_location, contact_email, data_version):
        # First, check whether file_location is an URL
        file_location_parsed = urllib.parse.urlparse(file_location)
        
        if len(file_location_parsed.scheme) > 0:
            logging.info(
                "Participant's predictions file already has an assigned URI: " + file_location)
            return file_location
        
        if self.storage_server_type == 'b2share':
            endpoint = self.storage_server_endpoint
            # 1. create new record
            logging.info("Uploading participant's predictions file to " +
                         endpoint + " for permanent storage")
            header = {"Content-Type": "application/json"}
            params = {'access_token': self.storage_server_token}
            metadata = {"titles": [{"title": "Predictions made by " + participant_data["participant_id"] + " participant in OpenEBench Virtual Research Environment"}],
                        "community": self.storage_server_community,
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
            commit_msg = [
                {
                    "op": "add",
                    "path": "/publication_state",
                    "value": "submitted"
                }
            ]
            commit_str = json.dumps(commit_msg)

            url = endpoint + "records/" + record_id + "/draft"
            r = requests.patch(url, data=commit_str, params=params, headers=header)

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
        else:
            logging.fatal('Unsupported storage server type {}'.format(self.storage_server_type))
            sys.exit(5)

    def load_schemas(self, data_model_dir):
        if self.schema_validators is None:
            local_config = self.schema_validators_local_config
            
            # Now, guessing the prefix
            schema_prefix = None
            with os.scandir(data_model_dir) as dmit:
                for entry in dmit:
                    if entry.name.endswith('.json') and entry.is_file():
                        with open(entry.path, mode="r", encoding="utf-8") as jschH:
                            jsch = json.load(jschH)
                            theId = jsch.get('$id')
                            if theId is not None:
                                schema_prefix = theId[0:theId.rindex('/')+1]
                                break
            
            local_config['primary_key']['schema_prefix'] = schema_prefix
            logging.debug(json.dumps(local_config))
            
            self.schema_validators = FairGTracksValidator(config=local_config)
        
        # create the cached json schemas for validation
        numSchemas = self.schema_validators.loadJSONSchemas(data_model_dir, verbose=False)

        if numSchemas == 0:
            print(
                "FATAL ERROR: No schema was successfully loaded. Exiting...\n", file=sys.stderr)
            sys.exit(1)
        
        schemaMappings = {}
        for key in self.schema_validators.getValidSchemas().keys():
            concept = key[key.rindex('/')+1:]
            if concept:
                schemaMappings[concept] = key
        
        self.schemaMappings = schemaMappings
        
        return schemaMappings

    def schemas_validation(self, jsonSchemas_array, val_result_filename):
        # validate the newly annotated dataset against https://github.com/inab/benchmarking-data-model

        logging.info(
            "\n\t==================================\n\t7. Validating datasets and TestActions\n\t==================================\n")

        cached_jsons = []
        for element in jsonSchemas_array:

            cached_jsons.append(
                {'json': element, 'file': "inline" + element["_id"], 'errors': []})

        val_res = self.schema_validators.jsonValidate(
            *cached_jsons, verbose=True)
        
        if val_result_filename is not None:
            logging.info("Saving validation result to {}".format(val_result_filename))
            with open(val_result_filename, mode="w", encoding="utf-8") as wb:
                json.dump(val_res, wb)
        
        # check for errors in the validation results
        # skipping the duplicate keys case
        to_warning = 0
        to_obj_warning = 0
        for val_obj in val_res:
            for error in val_obj['errors']:
                if error['reason'] not in ("dup_pk",):
                    logging.fatal("\nObjects validation Failed:\n " + str(val_obj))
                    # logging.fatal("\nSee full validation logs:\n " + str(val_res))
                    sys.exit(3)
                
                to_warning += 1
            if len(val_obj['errors']) > 0:
                to_obj_warning += 1
        
        logging.info(
            "\n\t==================================\n\t Objects validated\n\t==================================\n")
        
        logging.info("Report: {} duplicated keys in {} of {} documents".format(to_warning, to_obj_warning, len(val_res)))

    def fetchStagedData(self, dataType):
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.oeb_token)
        }
        
        req = urllib.request.Request(self.oeb_submission_api + '/' + urllib.parse.quote(dataType), headers=headers, method='GET')
        with urllib.request.urlopen(req) as t:
            datares = json.load(t)
            
            return datares
    
    def generate_manifest_dataset(self, dataset_submission_id, community_id, benchmarking_event_id, version, data_visibility, final_data):
        """
        This method receives both a dataset submission id and
        the array of data elements (datasets, testactions) to
        be stored in the database
        """
        
        dataset_schema = self.schemaMappings['Dataset']
        umbrella_assembling_timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        
        unique_contacts = set()
        unique_challenges = set()
        rel_dataset_ids = []
        for elem in final_data:
            if elem['_schema'] == dataset_schema:
                for dataset_contact_id in elem['dataset_contact_ids']:
                    unique_contacts.add(dataset_contact_id)
                for challenge_id in elem['challenge_ids']:
                    unique_challenges.add(challenge_id)
                rel_dataset_ids.append({
                    'dataset_id': elem['_id'],
                    'role': 'dependency'
                })
        
        umbrella = {
            '_id': dataset_submission_id,
            '_schema': dataset_schema,
            'community_ids': [ community_id ],
            'challenge_ids': list(unique_challenges),
            'visibility': data_visibility,
            'name': dataset_submission_id,
            'version': str(version),
            'description': 'Manifest dataset {} from consolidated data'.format(dataset_submission_id),
            'dates': {
                'creation': umbrella_assembling_timestamp,
                'modification': umbrella_assembling_timestamp,
            },
            'type': 'other',
            'datalink': {
                'uri': 'oeb:{}'.format(benchmarking_event_id),
                'attrs': [
                    'curie'
                ]
            },
            'dataset_contact_ids': list(unique_contacts),
            'depends_on': {
                'rel_dataset_ids': rel_dataset_ids,
            }
        }
        
        return umbrella
    
    def submit_oeb_buffer(self, json_data, community_id):

        logging.info("\n\t==================================\n\t8. Uploading workflow results to https://dev-openebench.bsc.es/api/scientific/submission/\n\t==================================\n")

        header = {"Content-Type": "application/json"}
        params = {
                    'access_token': self.oeb_token,
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
            logging.debug(r.text)
