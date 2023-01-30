# OEB_workflows_data_migration (BETA version)
## Description
Application used by community managers to migrate results of a benchmarking workflow (for instance, from [Virtual Research Environment](https://openebench.bsc.es/vre)) to [OpenEBench](https://openebench.bsc.es) scientific benchmarking database. It takes the minimal datasets from the 'consolidated results' from the workflow, adds the rest of metadata to validate against the [Benchmarking Data Model](https://github.com/inab/benchmarking-data-model), and the required OEB keys, builds the necessary TestActions, and finally pushes them to OpenEBench temporary database.

## Prerequisites for moving workflow results to OEB
In order to use the migration tool, some requirements need to be fulfilled:
* The benchmarking event, challenges, metrics, and input/reference datasets that the results refer to should already be registered in OpenEBench and have official OEB identifiers.
* IDs of challenges and metrics used in the workflow should be annotated in the correspondent OEB objects (in the *_metadata:level_2* field) so that the can be mapped to the registered OEB elements.
* The tool that computed the input file' predictions should also be registered in OpenEBench.
* The 'consolidated results' file should come from a pipeline that follows the OpenEBench Benchmarking Workflows Standards.
(If any of these requirements is not satisfied, a form should be provided so that the manager or developer can 'inaugurate' the required object in OEB)
* **NOTE:** this tool just uplifts and upload benchmarking workflow generated minimal datasets to OEB database, it does NOT update the reference aggregation dataset referenced by the workflow (for instance, the one used by VRE to instantiate the workflow). In order to update official OEB aggregation datasets to a VRE workflow, please contact OEB team, so they can copy them manually to the corresponding reference directory *(/gpfs/VRE/public/aggreggation/<workflow_name>)* 

## Parameters

```
usage: push_data_to_oeb.py [-h] -i CONFIG_JSON -cr OEB_SUBMIT_API_CREDS
                           [-tk OEB_SUBMIT_API_TOKEN]
                           [--val_output VAL_OUTPUT] [-o OUTPUT]

optional arguments:
  -h, --help            show this help message and exit
  -i CONFIG_JSON, --config_json CONFIG_JSON
                        json file which contains all parameters for migration
  -cr OEB_SUBMIT_API_CREDS, --oeb_submit_api_creds OEB_SUBMIT_API_CREDS
                        Credentials and endpoints used to obtain a token for
                        submission to oeb buffer DB
  -tk OEB_SUBMIT_API_TOKEN, --oeb_submit_api_token OEB_SUBMIT_API_TOKEN
                        Token used for submission to oeb buffer DB. If it is
                        not set, the credentials file provided with -cr must
                        have defined 'clientId', 'grantType', 'user' and
                        'pass'
  --val_output VAL_OUTPUT
                        Save the JSON Schema validation output to a file
  -o OUTPUT, --output OUTPUT
                        Save what it was going to be submitted in this file,
                        instead of sending them (like a dry-run)
```

## Usage

First, install the Python dependencies in a virtual environment:

```bash
python3 -m venv .py3env
source .py3env/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt
```

The minimal/partial dataset to be uplifted to the [OpenEBench benchmarking data model](https://github.com/inab/benchmarking_data_model) should validate against the schema [minimal_bdm_oeb_level2.yaml available here](oeb_level2/schemas/minimal_bdm_oeb_level2.yaml) using [ext-json-validate](https://pypi.org/project/extended-json-schema-validator/), with a command-line similar to:

```bash
ext-json-validate --guess-schema oeb_level2/schemas/minimal_bdm_oeb_level2.yaml minimal_dataset_examples/results_example.json
```

An example of the dataset is [available here](minimal_dataset_examples/results_example.json).That dataset should be declared through a `config.json` file declaring the URL or relative path where it is (it should follow JSON Schema [submission_form_schema.json available here](oeb_level2/schemas/submission_form_schema.json), you have an [example here](minimal_dataset_examples/config_example.json)), and set up an `auth_config.json` with the different credentials ([template here](oebdev_api_auth.json.template) and JSON Schema [auth_config_schema.json available here](oeb_level2/schemas/auth_config_schema.json)).

```bash
# The command must be run with the virtual environment enabled
python push_data_to_oeb.py -i config.json -cr auth_config.json
```
