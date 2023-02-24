#!/usr/bin/env python

import os
import sys
import datetime
import hashlib
import inspect
import itertools
import tempfile
import logging
import json
import urllib.request
import urllib.parse

import requests

import yaml
# We have preference for the C based loader and dumper, but the code
# should fallback to default implementations when C ones are not present
try:
    from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
except ImportError:
    from yaml import Loader as YAMLLoader, Dumper as YAMLDumper

from oebtools.fetch import (
    checkoutSchemas,
    DEFAULT_BDM_TAG,
)

from oebtools.uploader import (
    loadSchemas,
)

from oebtools.auth import getAccessToken

from ..schemas import (
    create_validator,
    create_validator_for_oeb_level2,
)

from .catalogs import (
    get_challenge_label_from_challenge,
)


def gen_ch_id_to_label(challenges: "Sequence[Mapping[str, Any]]", community_prefix: "str") -> "Mapping[str, str]":
    ch_id_to_label = {}
    for challenge in challenges:
        challenge_label = get_challenge_label_from_challenge(challenge, community_prefix)
        ch_id_to_label[challenge["_id"]] = challenge_label
        ch_orig_id = challenge.get("orig_id")
        if ch_orig_id is not None:
            ch_id_to_label[ch_orig_id] = challenge_label
    
    return ch_id_to_label


GRAPHQL_POSTFIX = "/graphql"

class OpenEBenchUtils():
    DEFAULT_OEB_API = "https://dev-openebench.bsc.es/api/scientific/graphql"
    DEFAULT_OEB_SUBMISSION_API = "https://dev-openebench.bsc.es/api/scientific/submission/"
    DEFAULT_DATA_MODEL_DIR = "benchmarking_data_model"

    def __init__(self, oeb_credentials: "Mapping[str, Any]", workdir: "str", oeb_token: "Optional[str]" = None, level2_min_validator: "Optional[Any]" = None):
        self.logger = logging.getLogger(
            dict(inspect.getmembers(self))["__module__"]
            + "::"
            + self.__class__.__name__
        )

        self.data_model_repo_dir = os.path.join(workdir, self.DEFAULT_DATA_MODEL_DIR)
        self.oeb_api = oeb_credentials.get("graphqlURI", self.DEFAULT_OEB_API)
        
        # Dealing with endpoints
        if self.oeb_api.endswith(GRAPHQL_POSTFIX):
            self.oeb_api_base = self.oeb_api[0:-len(GRAPHQL_POSTFIX)+1]
        else:
            self.oeb_api_base = self.oeb_api
            if self.oeb_api_base.endswith("/"):
                self.oeb_api = self.oeb_api_base[0:-1]
            else:
                self.oeb_api = self.oeb_api_base
            
            self.oeb_api += GRAPHQL_POSTFIX
            
        
        self.oeb_submission_api = oeb_credentials.get('submissionURI', self.DEFAULT_OEB_SUBMISSION_API)
        oebIdProviders = oeb_credentials['accessURI']
        if not isinstance(oebIdProviders, list):
            oebIdProviders = [ oebIdProviders ]
        self.oeb_access_api_points = oebIdProviders
        
        storage_server_block = oeb_credentials.get('storageServer', {})
        self.storage_server_type = storage_server_block.get("type")
        self.storage_server_endpoint = storage_server_block.get("endpoint")
        self.storage_server_community = storage_server_block.get("community")
        self.storage_server_token = storage_server_block.get("token")

        logging.basicConfig(level=logging.INFO)
        
        
        local_config = {
            'primary_key': {
                'provider': [
                    *oebIdProviders,
                    self.oeb_submission_api
                ],
                # To be set on instantiation
                # 'schema_prefix': None,
                "allow_provider_duplicates": True,
                'accept': 'text/uri-list'
            }
        }
        
        if level2_min_validator is None:
            level2_min_validator, num_level2_schemas = create_validator_for_oeb_level2()
            if num_level2_schemas < 6:
                self.logger.error("OEB level2 operational JSON Schemas not found")
                sys.exit(1)
            
            # This is to avoid too much verbosity
            logging.getLogger(level2_min_validator.__class__.__name__).setLevel(logging.CRITICAL)
        
        self.level2_min_validator = level2_min_validator
        
        self.oeb_token = getAccessToken(oeb_credentials, logger=self.logger)  if oeb_token is None  else  oeb_token
        
        self.schema_validators_local_config = local_config

        self.schema_validators = None
        self.num_schemas = 0
        
        self.schemaMappings = None

    # function to pull a github repo obtained from https://github.com/inab/vre-process_nextflow-executor/blob/master/tool/VRE_NF.py

    def doMaterializeRepo(self, git_uri, git_tag) -> "Union[str, Tuple[str, Sequence[SchemaHashEntry]]]":

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
        return checkoutSchemas(
            checkoutDir=repo_tag_destdir,
            git_repo=git_uri,
            tag=git_tag,
            logger=self.logger
        )

    # function that retrieves all the required metadata from OEB database
    def graphql_query_OEB_DB(self, data_type: "str", bench_event_id: "str") -> "Tuple[Mapping[str, Any], Mapping[str, Mapping[str, Any]]]":

        if data_type == "input":
#            }
            json_query = {'query': """query InputQuery($bench_event_id: String) {
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        datasets(datasetFilters: {type: "input"}) {
            _id
        }
    }
    getContacts {
        _id
        email
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                }
            }
        elif data_type == "metrics_reference":
            json_query = {'query': """query MetricsReferenceQuery($bench_event_id: String) {
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        orig_id
        challenge_contact_ids
        metrics_categories {
          category
          description
          metrics {
            metrics_id
            orig_id
            tool_id
          }
        }
        datasets(datasetFilters: {type: "metrics_reference"}) {
            _id
        }
    }
    getMetrics {
        _id
        _metadata
        orig_id
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                }
            }
        elif data_type == "aggregation":
            json_query = {'query': """query AggregationQuery($bench_event_id: String) {
    getBenchmarkingEvents(benchmarkingEventFilters: {id: $bench_event_id}) {
        _id
        orig_id
        community_id
    }
    getChallenges(challengeFilters: {benchmarking_event_id: $bench_event_id}) {
        _id
        acronym
        _metadata
        orig_id
        challenge_contact_ids
        metrics_categories {
          category
          description
          metrics {
            metrics_id
            orig_id
            tool_id
          }
        }
        event_test_actions: test_actions(testActionFilters: {action_type: "TestEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        metrics_test_actions: test_actions(testActionFilters: {action_type: "MetricsEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        aggregation_test_actions: test_actions(testActionFilters: {action_type: "AggregationEvent"}) {
          _id
          action_type
          challenge_id
          _metadata
          orig_id
          _schema
          status
          tool_id
          involved_datasets {
              dataset_id
              role
          }
        }
        public_reference_datasets: datasets(datasetFilters: {type: "public_reference"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        metrics_reference_datasets: datasets(datasetFilters: {type: "metrics_reference"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        input_datasets: datasets(datasetFilters: {type: "input"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        participant_datasets: datasets(datasetFilters: {type: "participant"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        assessment_datasets: datasets(datasetFilters: {type: "assessment"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
        aggregation_datasets: datasets(datasetFilters: {type: "aggregation"}) {
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
                    metrics_id
                    rel_dataset_ids {
                        dataset_id
                    }
                }
                _metadata
        }
    }
    getMetrics {
        _id
        _metadata
        orig_id
    }
}""",
                'variables': {
                    'bench_event_id': bench_event_id,
                }
            }
        else:
            self.logger.fatal("Unable to generate graphQL query: Unknown datatype {}".format(data_type))
            sys.exit(2)
        
        
        try:
            url = self.oeb_api
            # get challenges and input datasets for provided benchmarking event
            r = requests.post(url=url, json=json_query, headers={'Authorization': 'Bearer {}'.format(self.oeb_token)})
            response = r.json()
            data = response.get("data")
            if data is None:
                self.logger.fatal(f"For {bench_event_id} got response error from graphql query: {r.text}")
                sys.exit(6)
            if len(data["getBenchmarkingEvents"]) == 0:
                self.logger.fatal(f"Benchmarking event {bench_event_id} is not available in OEB. Please double check the id, or contact OpenEBench support for information about how to open a new benchmarking event")
                sys.exit(2)
            if len(data["getChallenges"]) == 0:

                self.logger.fatal("No challenges associated to benchmarking event " + bench_event_id +
                              " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                sys.exit(2)
            
            # Deserializing _metadata
            challenges = data.get('getChallenges')
            # mapping from challenge _id to challenge label
            if challenges is not None:
                for challenge in challenges:
                    metadata = challenge.get('_metadata')
                    # Deserialize the metadata
                    if isinstance(metadata,str):
                        challenge_metadata = json.loads(metadata)
                        challenge['_metadata'] = challenge_metadata
                    else:
                        challenge_metadata = None
                    
                    # Deserializing inline_data
                    datasets = challenge.get('datasets',[])
                    for dataset in datasets:
                        datalink = dataset.get('datalink')
                        if datalink is not None:
                            inline_data = datalink.get('inline_data')
                            if isinstance(inline_data, str):
                                datalink['inline_data'] = json.loads(inline_data)
                    
                    # And now, for the embedded datasets and test actions
                    for sub_k in ('event_test_actions', 'metrics_test_actions', 'public_reference_datasets', 'metrics_reference_datasets', 'input_datasets', 'participant_datasets', 'assessment_datasets', 'aggregation_datasets'):
                        sub_v_l = challenge.get(sub_k)
                        if isinstance(sub_v_l, list):
                            for sub_v in sub_v_l:
                                sub_inline_data = sub_v.get('datalink',{}).get('inline_data')
                                if isinstance(sub_inline_data, str):
                                    sub_v['datalink']['inline_data'] = json.loads(sub_inline_data)
                                
                                sub_metadata = sub_v.get('_metadata')
                                if isinstance(sub_metadata, str):
                                    sub_v['_metadata'] = json.loads(sub_metadata)
                            
            
            metrics = data.get('getMetrics')
            if metrics is not None:
                for metric in metrics:
                    metadata = metric.get('_metadata')
                    # Deserialize the metadata
                    if isinstance(metadata,str):
                        metric['_metadata'] = json.loads(metadata)
            
            return response
        except Exception as e:

            self.logger.exception(e)

    # function that uploads the predictions file to a remote server for it long-term storage, and produces a DOI
    def upload_to_storage_service(self, participant_data, file_location, contact_email, data_version: "str"):
        # First, check whether file_location is an URL
        file_location_parsed = urllib.parse.urlparse(file_location)
        
        if len(file_location_parsed.scheme) > 0:
            self.logger.info(
                "Participant's predictions file already has an assigned URI: " + file_location)
            return file_location
        
        if self.storage_server_type == 'b2share':
            endpoint = self.storage_server_endpoint
            # 1. create new record
            self.logger.info("Uploading participant's predictions file to " +
                         endpoint + " for permanent storage")
            header = {"Content-Type": "application/json"}
            params = {'access_token': self.storage_server_token}
            metadata = {"titles": [{"title": "Predictions made by " + participant_data["participant_id"] + " participant in OpenEBench Virtual Research Environment"}],
                        "community": self.storage_server_community,
                        "community_specific": {},
                        "contact_email": contact_email,
                        "version": data_version,
                        "open_access": True}
            r = requests.post(endpoint + "records/", params=params,
                              data=json.dumps(metadata), headers=header)

            result = json.loads(r.text)
            # check whether request was succesful
            if r.status_code != 201:
                self.logger.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            # 2. add file to new record
            filebucketid = result["links"]["files"].split('/')[-1]
            record_id = result["id"]

            try:
                upload_file = open(file_location, 'rb')
            except OSError as exc:
                self.logger.fatal("OS error: {0}".format(exc))
                sys.exit()

            url = endpoint + 'files/' + filebucketid
            header = {"Accept": "application/json",
                      "Content-Type": "application/octet-stream"}

            r = requests.put(url + '/' + os.path.basename(file_location),
                             data=upload_file, params=params, headers=header)

            # check whether request was succesful
            if r.status_code != 200:
                self.logger.fatal("Bad request: " +
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
                self.logger.fatal("Bad request: " +
                              str(r.status_code) + str(r.text))
                sys.exit()

            published_result = json.loads(r.text)

            data_doi = published_result["metadata"]["DOI"]
            # print(record_id) https://trng-b2share.eudat.eu/api/records/637a25e86dbf43729d30217613f1218b
            self.logger.info("File '" + file_location +
                         "' uploaded and permanent ID assigned: " + data_doi)
            return data_doi
        else:
            self.logger.fatal('Unsupported storage server type {}'.format(self.storage_server_type))
            sys.exit(5)

    def load_schemas_from_repo(self, data_model_dir: "str", tag: "str" = DEFAULT_BDM_TAG) -> "Mapping[str, str]":
        if self.schema_validators is None:
            local_config = self.schema_validators_local_config
            
            schema_prefix, schema_dir = loadSchemas(
                data_model_dir,
                tag=tag,
                logger=self.logger
            )
            
            local_config['primary_key']['schema_prefix'] = schema_prefix
            self.logger.debug(json.dumps(local_config))
            
            # create the cached json schemas for validation
            self.schema_validators, self.num_schemas = create_validator(schema_dir, config=local_config)

            if self.num_schemas == 0:
                print(
                    "FATAL ERROR: No schema was successfully loaded. Exiting...\n", file=sys.stderr)
                sys.exit(1)
            
            schemaMappings = {}
            for key in self.schema_validators.getValidSchemas().keys():
                concept = key[key.rindex('/')+1:]
                if concept:
                    schemaMappings[concept] = key
            
            self.schemaMappings = schemaMappings
        
        return self.schemaMappings

    def load_schemas_from_server(self) -> "Mapping[str, str]":
        if self.schema_validators is None:
            data_model_in_memory = checkoutSchemas(fetchFromREST=self.oeb_api_base, logger=self.logger)
            
            local_config = self.schema_validators_local_config
            
            schema_prefix, schema_dir = loadSchemas(
                data_model_in_memory,
                logger=self.logger
            )
            
            local_config['primary_key']['schema_prefix'] = schema_prefix
            self.logger.debug(json.dumps(local_config))
            
            # create the cached json schemas for validation
            self.schema_validators, self.num_schemas = create_validator(schema_dir, config=local_config)

            if self.num_schemas == 0:
                print(
                    "FATAL ERROR: No schema was successfully loaded. Exiting...\n", file=sys.stderr)
                sys.exit(1)
            
            schemaMappings = {}
            for key in self.schema_validators.getValidSchemas().keys():
                concept = key[key.rindex('/')+1:]
                if concept:
                    schemaMappings[concept] = key
            
            self.schemaMappings = schemaMappings
        
        return self.schemaMappings

    def schemas_validation(self, jsonSchemas_array, val_result_filename):
        # validate the newly annotated dataset against https://github.com/inab/benchmarking-data-model

        self.logger.info(
            "\n\t==================================\n\t8. Validating datasets and TestActions\n\t==================================\n")

        cached_jsons = []
        for element in jsonSchemas_array:

            cached_jsons.append(
                {'json': element, 'file': "inline" + element["_id"], 'errors': []})

        val_res = self.schema_validators.jsonValidate(
            *cached_jsons, verbose=True)
        
        if val_result_filename is not None:
            self.logger.info("Saving validation result to {}".format(val_result_filename))
            with open(val_result_filename, mode="w", encoding="utf-8") as wb:
                json.dump(val_res, wb)
        
        # check for errors in the validation results
        # skipping the duplicate keys case
        to_error = 0
        to_warning = 0
        to_obj_warning = 0
        to_obj_error = 0
        for val_obj in val_res:
            to_p_error = 0
            to_p_warning = 0
            for error in val_obj['errors']:
                if error['reason'] not in ("dup_pk",):
                    self.logger.fatal("\nObjects validation Failed:\n " + json.dumps(val_obj, indent=4))
                    # self.logger.fatal("\nSee full validation logs:\n " + str(val_res))
                    to_p_error += 1
                else:
                    to_p_warning += 1
            to_warning += to_p_warning
            to_error += to_p_error
            if to_p_warning > 0:
                to_obj_warning += 1
            if to_p_error > 0:
                to_obj_error += 1
        
        if to_error > 0:
            self.logger.error(
                "\n\t==================================\n\t Some objects did not validate\n\t==================================\n")
            
            self.logger.error("Report: {} errors in {} of {} documents".format(to_error, to_obj_error, len(val_res)))
            sys.exit(3)
        
        self.logger.info(
            "\n\t==================================\n\t Objects validated\n\t==================================\n")
        
        self.logger.info("Report: {} duplicated keys in {} of {} documents".format(to_warning, to_obj_warning, len(val_res)))

    def fetchStagedData(self, dataType: "str", filtering_keys: "Optional[Mapping[str, Union[Sequence[str], Set[str]]]]" = None) -> "Iterator[Mapping[str, Any]]":
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.oeb_token)
        }
        
        seen = set()
        # sandbox entries take precedence over other ones
        for api_endpoint in (self.oeb_submission_api, *self.oeb_access_api_points):
            if not api_endpoint.endswith('/'):
                api_endpoint += '/'
            data_endpoint = api_endpoint + urllib.parse.quote(dataType)
            req = urllib.request.Request(data_endpoint, headers=headers, method='GET')
            with urllib.request.urlopen(req) as t:
                try:
                    datares_raw = json.load(t)
                    
                    assert isinstance(datares_raw, list), "The answer is expected to be a list"
                    filtered_datares_raw = list(filter(lambda d: d["_id"] not in seen, datares_raw))
                    seen.update(map(lambda d: d["_id"], filtered_datares_raw))
                    if isinstance(filtering_keys, dict) and len(filtering_keys) > 0:
                        yield from self.filter_by(filtered_datares_raw, filtering_keys)
                    else:
                        yield from filtered_datares_raw
                    
                except:
                    self.logger.exception(f"Failed to fetch {dataType} data from {data_endpoint}")
    
    def fetchStagedEntry(self, dataType: "str", the_id: "str") -> "Mapping[str, Any]":
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.oeb_token)
        }
        
        req = urllib.request.Request(self.oeb_submission_api + '/' + urllib.parse.quote(dataType) + '/' + urllib.parse.quote(the_id), headers=headers, method='GET')
        with urllib.request.urlopen(req) as t:
            datares_raw = json.load(t)
            
            assert isinstance(datares_raw, dict), "The answer is expected to be a dictionary"
            
            return datares_raw
    
    def filter_by(self, datares_raw: "Iterator[Mapping[str, Any]]", filtering_keys: "Mapping[str, Union[Sequence[str], Set[str]]]") -> "Iterator[Mapping[str, Any]]":
            if len(filtering_keys) > 0:
                fk_set = {
                    fk_key: fk_values if isinstance(fk_values, set) else set(fk_values) 
                    for fk_key, fk_values in filtering_keys.items()
                }
                for dr in datares_raw:
                    for filt_key, filt_values in fk_set.items():
                        if filt_key in dr:
                            if isinstance(dr[filt_key], list):
                                if all(map(lambda dv: dv not in filt_values, dr[filt_key])):
                                    # Skip this entry
                                    continue
                            elif dr[filt_key] not in filt_values:
                                # Skip this entry
                                continue
                            
                            # Passed all tests
                            yield dr
            else:
                yield from datares_raw

    def check_min_dataset_collisions(self, db_datasets: "Sequence[Mapping[str, Any]]", input_min_datasets: "Sequence[Mapping[str, Any]]", ch_id_to_label: "Mapping[str, Any]") -> "Sequence[Tuple[str, str]]":
        # This method is only valid for minimal dataset of type participant and assessment
        # Those ones with original ids
        input_d_dict = dict(map(lambda i_d: (i_d["_id"],i_d), input_min_datasets))
        collisions = []
        i_keys = list(input_d_dict.keys())
        for db_dataset in itertools.chain(
            self.filter_by(db_datasets, {"orig_id": i_keys})
            ,
            self.filter_by(db_datasets, {"_id": i_keys})
        ):
            orig_id = db_dataset.get("orig_id")
            if orig_id is not None:
                input_min_dataset = input_d_dict.get(orig_id)
            else:
                input_min_dataset = None
            
            if input_min_dataset is None:
                input_min_dataset = input_d_dict.get(db_dataset["_id"])
            
            if input_min_dataset is None:
                self.logger.info(f"Nothing matched db dataset {db_dataset['_id']}")
                continue
            
            has_coll = False
            # The dataset must be of the same type
            if db_dataset["type"] != input_min_dataset["type"]:
                self.logger.error(f"Dataset type mismatch: orig id {input_min_dataset['_id']} has type {input_min_dataset['type']}, database entry ({db_dataset['_id']}) has {db_dataset['type']}")
                has_coll = True
            
            # The new challenge ids must be equal or a superset
            # of the dataset in the database
            if isinstance(input_min_dataset["challenge_id"], list):
                i_ch_set = set(input_min_dataset["challenge_id"])
            else:
                i_ch_set = set()
                i_ch_set.add(input_min_dataset["challenge_id"])
            
            # Translation from challenge id to challenge label
            
            db_ch_set = set(map(lambda d_id: ch_id_to_label.get(d_id), db_dataset["challenge_ids"]))
            if not db_ch_set.issubset(i_ch_set):
                self.logger.error(f"Challenges where new dataset {input_min_dataset['_id']} appears is not a superset of the new dataset challenges: {db_ch_set - i_ch_set} ({i_ch_set} vs {db_ch_set})")
                has_coll = True
            
            # 
            
            if has_coll:
                collisions.append((db_dataset['_id'], orig_id, input_min_dataset['_id']))
        
        return collisions
    
    def check_dataset_collisions(
        self,
        db_datasets: "Sequence[Mapping[str, Any]]",
        output_datasets: "Sequence[Mapping[str, Any]]",
        default_schema_url: "Optional[Union[Sequence[str], bool]]" = None
    ) -> "Sequence[Tuple[str, str]]":
        
        # Those ones with original ids
        output_d_list = list(map(lambda o_d: (o_d.get("orig_id"), o_d["_id"], o_d), output_datasets))
        output_d_dict = dict(map(lambda odl: (odl[0], odl[2]) , filter(lambda odl: odl[0] is not None, output_d_list)))
        output_d_dict.update(map(lambda odl: (odl[1], odl[2]) , filter(lambda odl: odl[0] is None, output_d_list)))
        
        collisions = []
        
        db_d_dict = dict(map(lambda db_d: (db_d["_id"],db_d), db_datasets))
        db_orig_d_dict = dict(filter(lambda db_t: db_t[0] is not None, map(lambda db_d: (db_d.get("orig_id"),db_d), db_datasets)))
        
        o_keys = list(output_d_dict.keys())
        for o_dataset in output_datasets:
            should_exit = False
            # Should we validate the inline data?
            o_datalink = o_dataset.get("datalink")
            o_type = o_dataset["type"]
            o_id = o_dataset["_id"]
            o_orig_id = o_dataset.get("orig_id")
            if (not isinstance(default_schema_url, bool) or default_schema_url) and isinstance(o_datalink, dict):
                inline_data = o_datalink.get("inline_data")
                # Is this dataset an inline one?
                if inline_data is not None:
                    schema_url = o_datalink.get("schema_url", default_schema_url)
                    
                    if schema_url is None:
                        guess_unmatched = True
                    else:
                        guess_unmatched = schema_url
                    
                    # Now, time to validate the dataset
                    config_val_list = self.level2_min_validator.jsonValidate({
                        "json": inline_data,
                        "file": "inline " + o_id,
                        "errors": [],
                    }, guess_unmatched=guess_unmatched)
                    assert len(config_val_list) > 0
                    config_val_block = config_val_list[0]
                    config_val_block_errors = list(filter(lambda ve: (schema_url is None) or (ve.get("schema_id") == schema_url), config_val_block.get("errors", [])))
                    if len(config_val_block_errors) > 0:
                        self.logger.error(f"Validation errors in inline data from dataset {o_id} ({o_orig_id}) using {schema_url}\n{config_val_block_errors}")
                        should_exit = True
            
            
            db_dataset = db_d_dict.get(o_id)
            db_o_dataset = db_orig_d_dict.get(o_id)
            
            # The different ambiguity corner cases (take 1)
            if db_dataset is not None and db_o_dataset is not None:
                self.logger.error(f"Database could be poisoned, as {o_id} output matched two entries, {o_id} and {db_o_dataset['_id']}")
                self.logger.error(json.dumps(o_dataset, sort_keys=True, indent=4))
                collisions.append((o_id, o_orig_id, db_o_dataset['_id']))
                continue
                
            d_dataset = db_dataset if db_dataset is not None else db_o_dataset
            
            # The different ambiguity corner cases (take 2)
            if o_orig_id is not None:
                db_i_orig_dataset = db_d_dict.get(o_orig_id)
                db_orig_dataset = db_orig_d_dict.get(o_orig_id)
                
                if db_i_orig_dataset is not None:
                    if db_orig_dataset is not None:
                        self.logger.error(f"Database could be poisoned, as {o_orig_id} orig output matched two entries, {o_orig_id} and {db_orig_dataset['_id']}")
                        self.logger.error(json.dumps(db_orig_dataset, sort_keys=True, indent=4))
                    else:
                        self.logger.error(f"Database could be poisoned, as {o_orig_id} orig output matched entry with {o_orig_id} id")
                    
                    self.logger.error(json.dumps(db_i_orig_dataset, sort_keys=True, indent=4))
                    self.logger.error(json.dumps(o_dataset, sort_keys=True, indent=4))
                    collisions.append((o_id, o_orig_id, db_orig_dataset['_id']))
                    continue
                elif db_orig_dataset is not None:
                    if d_dataset is None:
                        d_dataset = db_orig_dataset
                    elif db_orig_dataset != d_dataset:
                        self.logger.error(f"Output dataset {o_id} (orig {o_orig_id}) matches two different database datasets:")
                        self.logger.error(json.dumps(d_dataset, sort_keys=True, indent=4))
                        self.logger.error(json.dumps(db_orig_dataset, sort_keys=True, indent=4))
                        collisions.append((o_id, o_orig_id, db_orig_dataset['_id']))
                        continue
            else:
                db_i_orig_dataset = None
                db_orig_dataset = None
            
            # No dataset, no fun!
            if d_dataset is None:
                self.logger.info(f"Nothing matched output dataset {o_id} (orig {o_orig_id})")
                continue
            
            d_orig_id = d_dataset.get("orig_id")
            d_id = d_dataset["_id"]
            
            # Now, corner cases
            # Case 1 o_id matches d_orig_id => good
            # Case 2 o_id matches d_id
            # Case 2.a o_orig_id matches d_orig_id => good
            # Case 2.b o_orig_id does not match d_orig_id => bad
            
            has_coll = False
            if d_orig_id is not None and d_orig_id == o_id:
                pass
            elif d_id == o_id and o_orig_id is not None and d_orig_id is not None and o_orig_id != d_orig_id:
                self.logger.error(f"Mismatches in the pairs of (_id, orig_id): ({o_id}, {o_orig_id}) vs ({d_id}, {d_orig_id})")
                has_coll = True
            
            # The dataset must be of the same type
            if d_dataset["type"] != o_dataset["type"]:
                self.logger.error(f"Dataset type mismatch: id {o_id} has type {o_dataset['type']}, database entry ({d_id}) has {d_dataset['type']}")
                has_coll = True
            
            # The new challenge ids must be equal or a superset
            # of the dataset in the database
            o_ch_set = set(o_dataset["challenge_ids"])
            d_ch_set = set(d_dataset["challenge_ids"])
            if not d_ch_set.issubset(o_ch_set):
                self.logger.error(f"New dataset {o_id} challenges are not a superset of the dataset challenges: {d_ch_set} vs {o_ch_set}. (Tip: ill management of original ids on community side?)")
                has_coll = True
            
            # 
            
            if has_coll or should_exit:
                collisions.append((d_id, d_orig_id, o_id))
        
        return collisions
    
    def generate_manifest_dataset(self, dataset_submission_id, community_id, benchmarking_event_id, version: "str", data_visibility, final_data):
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
            'version': version,
            'description': f'Manifest dataset {dataset_submission_id} from consolidated data',
            'dates': {
                'creation': umbrella_assembling_timestamp,
                'modification': umbrella_assembling_timestamp,
            },
            'type': 'other',
            'datalink': {
                'uri': f'oeb:{benchmarking_event_id}',
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

        self.logger.info(f"\n\t==================================\n\t8. Uploading workflow results to {self.oeb_submission_api}\n\t==================================\n")

        header = {"Content-Type": "application/json"}
        params = {
                    'access_token': self.oeb_token,
                    'community_id': community_id
                    
                }
        r = requests.post(self.oeb_submission_api, params=params,
                          data=json.dumps(json_data), headers=header)

        if r.status_code != 200:
            self.logger.fatal("Error in uploading data to OpenEBench. Bad request: " +
                          str(r.status_code) + str(r.text))
            sys.exit()
        else:
            self.logger.info(
                "\n\tData uploaded correctly...finalizing migration\n\n")
            self.logger.debug(r.text)
