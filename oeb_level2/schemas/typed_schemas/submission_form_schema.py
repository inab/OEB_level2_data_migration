# Automatically generated file from a JSON schema


from typing import Literal, Required, TypedDict


class ConfigParams(TypedDict, total=False):
    r"""
    Schema that defines the set of fields to be filled for a submission to OpenEBench database.

    dependentSchemas:
      tool_id:
        $ref: '#/$defs/ParticipantElements'
    oneOf:
      - not:
          required:
          - tool_mapping
        required:
        - tool_id
        - participant_file
      - not:
          required:
          - tool_id
        required:
        - tool_mapping
    """

    consolidated_oeb_data: Required[str]
    r"""
    Consolidated dataset path or URL.

    Path or publicly reachable URL to the consolidated dataset, coming from an OpenEBench standardized benchmarking workflow (e.g.https://github.com/inab/TCGA_benchmarking_workflow

    readonly: True
    options:
      hidden: true

    Required property
    """

    data_visibility: Required["DatasetsVisibility"]
    r"""
    Datasets visibility.

    The desired visibility of the submitted datasets, which must be acknowledged by the APIs

    default: public
    readonly: True

    Required property
    """

    benchmarking_event_id: Required[str]
    r"""
    Benchmarking event.

    The unique id of the benchmarking event the dataset belongs to, as is returned by the API

    pattern: ^OEBE[0-9]{3}[A-Z0-9]{7}$
    readonly: True

    Required property
    """

    participant_file: "ParticipantFileAssociated"
    r"""
    Participant file associated.

    Path or URI (e.g. DOI) to the participant file associated with the consolidated results

    readonly: True
    """

    community_id: str
    r"""
    OEB community.

    The unique id of the community where the data should be uploaded. Should come from VRE workflow. (This parameter is not needed any more)

    pattern: ^OEBC[0-9]{3}$
    options:
      hidden: true
    """

    tool_id: "ParticipantTool"
    r"""
    Participant Tool.

    The id of the tool used to generate the dataset. Should be selected by uploader, using API

    pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
    options:
      hidden: true
    """

    tool_mapping: list["_ParticipantElements"]
    r"""
    List of tools.

    minItems: 1
    unevaluatedProperties: False
    """

    tool_selection: str
    r"""
    Participant Tool.

    The id of the tool used to generate the dataset. Should be selected by uploader, using API

    format: selectize
    options:
      selectize:
        labelField: name
        load: load
        options: []
        placeholder: Name of your tool
        render: render
        searchField: name
        valueField: _id
    """

    dataset_submission_id: str
    r"""
    Dataset submission id.

    The unique identifier of the dataset which will hold the identifiers of all the loaded/updated datasets. Beware, this id should be unique! If it is not set, a UUIDv1 is used

    options:
      hidden: true
    """

    workflow_oeb_id: str
    r"""
    OEB Workflow.

    The id of the workflow (as a tool) used to compute the assessment metrics. Should be associated to VRE tool

    pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
    options:
      hidden: true
    """

    data_model_repo: str
    r"""
    OpenEBench benchmarking data model repository.

    The git repository URI, needed to check out the data model

    format: uri
    minLength: 1
    default: https://github.com/inab/benchmarking-data-model.git
    options:
      hidden: true
    """

    data_model_tag: str
    r"""
    Data model tag.

    Either the tag, branch or checkout hash needed to fetch the right version of the data model

    minLength: 1
    default: 6495b7317f830ad739591be2de1e279ea6c4c0d8
    options:
      hidden: true
    """

    data_model_reldir: str
    r"""
    Data model relative path.

    Within a checked-out copy of the benchmarking data model repository, the relative path to the data model

    format: uri-reference
    default: json-schemas/1.0.x
    options:
      hidden: true
    """

    fix_original_ids: bool
    r"""
    Fix non-conformant original ids.

    Fix original ids provided by the minimal datasets, so they conform the original id naming conventions

    default: True
    """

    data_storage_endpoint: str
    r"""
    Storage endpoint.

    Storage endpoint used to submit the assessment dataset to a perdurable storage which is able to provide a persistent id (e.g. a DOI)

    $comment: This key is only allowed for backward compatibility
    format: uri
    minLength: 1
    """



Contacts = list["_ContactsItem"]
r"""
Contacts.

Emails of the dataset contact(s). Should be registered in Mongo and OIDC, as they are going to be used to do the proper internal mappings

minItems: 1
uniqueItems: True
"""



DATASETS_VISIBILITY_DEFAULT = 'public'
r""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_visibility' """



DATA_MODEL_RELATIVE_PATH_DEFAULT = 'json-schemas/1.0.x'
r""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_reldir' """



DATA_MODEL_TAG_DEFAULT = '6495b7317f830ad739591be2de1e279ea6c4c0d8'
r""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_tag' """



DatasetsVisibility = Literal['public'] | Literal['community'] | Literal['challenge'] | Literal['participant']
r"""
Datasets visibility.

The desired visibility of the submitted datasets, which must be acknowledged by the APIs

default: public
readonly: True
"""
DATASETSVISIBILITY_PUBLIC: Literal['public'] = "public"
r"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_COMMUNITY: Literal['community'] = "community"
r"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_CHALLENGE: Literal['challenge'] = "challenge"
r"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_PARTICIPANT: Literal['participant'] = "participant"
r"""The values for the 'Datasets visibility' enum"""



FIX_NON_CONFORMANT_ORIGINAL_IDS_DEFAULT = True
r""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database fix_original_ids' """



OPENEBENCH_BENCHMARKING_DATA_MODEL_REPOSITORY_DEFAULT = 'https://github.com/inab/benchmarking-data-model.git'
r""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_repo' """



ParticipantFileAssociated = str
r"""
Participant file associated.

Path or URI (e.g. DOI) to the participant file associated with the consolidated results

readonly: True
"""



ParticipantTool = str
r"""
Participant Tool.

The id of the tool used to generate the dataset. Should be selected by uploader, using API

pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
options:
  hidden: true
"""



SHOULD_PARTICIPANT_BE_EXCLUDED_QUESTION_MARK__DEFAULT = False
r""" Default value of the field path 'participant elements exclude' """



Version = str | int | float
r"""
Version.

Version (or release date) of the dataset

minLength: 1
options:
  hidden: true
"""



_ContactsItem = str
r"""
format: autocomplete
options:
  autocomplete:
    autoSelect: true
    getResultValue: getResultValue_zza
    renderResult: renderResult_zza
    search: search_zza
"""



class _ParticipantElements(TypedDict, total=False):
    tool_id: Required["ParticipantTool"]
    r"""
    Participant Tool.

    The id of the tool used to generate the dataset. Should be selected by uploader, using API

    pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
    options:
      hidden: true

    Required property
    """

    data_version: Required["Version"]
    r"""
    Version.

    Version (or release date) of the dataset

    minLength: 1
    options:
      hidden: true

    Required property
    """

    data_contacts: Required["Contacts"]
    r"""
    Contacts.

    Emails of the dataset contact(s). Should be registered in Mongo and OIDC, as they are going to be used to do the proper internal mappings

    minItems: 1
    uniqueItems: True

    Required property
    """

    participant_id: str
    r"""
    The id / name of the tool which generated this dataset.

    minLength: 1
    primary_key:
      members:
      - .
      name: participant_id_key
    """

    renamed_participant_id: str
    r"""
    The id / name of the tool which generated this dataset, to be used on OEB storage.

    minLength: 1
    """

    exclude: bool
    r"""
    Should participant be excluded?.

    default: False
    """

    participant_file: "ParticipantFileAssociated"
    r"""
    Participant file associated.

    Path or URI (e.g. DOI) to the participant file associated with the consolidated results

    readonly: True
    """

