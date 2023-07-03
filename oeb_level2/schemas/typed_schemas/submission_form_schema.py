# Automatically generated file from a JSON schema


from typing import Literal, Union, TypedDict, List
from typing_extensions import Required


class ConfigParams(TypedDict, total=False):
    """ Schema that defines the set of fields to be filled for a submission to OpenEBench database. """

    consolidated_oeb_data: Required[str]
    """
    Consolidated dataset path or URL.

    Path or publicly reachable URL to the consolidated dataset, coming from an OpenEBench standardized benchmarking workflow (e.g.https://github.com/inab/TCGA_benchmarking_workflow

    readonly: True

    Required property
    """

    data_visibility: Required["DatasetsVisibility"]
    """ Required property """

    benchmarking_event_id: Required[str]
    """
    Benchmarking event.

    The unique id of the benchmarking event the dataset belongs to, as is returned by the API

    pattern: ^OEBE[0-9]{3}[A-Z0-9]{7}$
    readonly: True

    Required property
    """

    participant_file: Required[str]
    """
    Participant file associated.

    Path or URI (e.g. DOI) to the participant file associated with the consolidated results

    readonly: True

    Required property
    """

    community_id: str
    """
    OEB community.

    The unique id of the community where the data should be uploaded. Should come from VRE workflow. (This parameter is not needed any more)

    pattern: ^OEBC[0-9]{3}$
    """

    tool_id: "ParticipantTool"
    tool_mapping: List["_ParticipantElements"]
    """
    List of tools.

    minItems: 1
    unevaluatedProperties: False
    """

    tool_selection: str
    """
    Participant Tool.

    The id of the tool used to generate the dataset. Should be selected by uploader, using API

    format: selectize
    """

    dataset_submission_id: str
    """
    Dataset submission id.

    The unique identifier of the dataset which will hold the identifiers of all the loaded/updated datasets. Beware, this id should be unique! If it is not set, a UUIDv1 is used
    """

    workflow_oeb_id: str
    """
    OEB Workflow.

    The id of the workflow (as a tool) used to compute the assessment metrics. Should be associated to VRE tool

    pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
    """

    data_model_repo: str
    """
    OpenEBench benchmarking data model repository.

    The git repository URI, needed to check out the data model

    format: uri
    minLength: 1
    default: https://github.com/inab/benchmarking-data-model.git
    """

    data_model_tag: str
    """
    Data model tag.

    Either the tag, branch or checkout hash needed to fetch the right version of the data model

    minLength: 1
    default: 6495b7317f830ad739591be2de1e279ea6c4c0d8
    """

    data_model_reldir: str
    """
    Data model relative path.

    Within a checked-out copy of the benchmarking data model repository, the relative path to the data model

    format: uri-reference
    default: json-schemas/1.0.x
    """

    fix_original_ids: bool
    """
    Fix non-conformant original ids.

    Fix original ids provided by the minimal datasets, so they conform the original id naming conventions

    default: True
    """

    data_storage_endpoint: str
    """
    Storage endpoint.

    Storage endpoint used to submit the assessment dataset to a perdurable storage which is able to provide a persistent id (e.g. a DOI)

    $comment: This key is only allowed for backward compatibility
    format: uri
    minLength: 1
    """



Contacts = List["_ContactsItem"]
"""
Contacts.

Emails of the dataset contact(s). Should be registered in Mongo and OIDC, as they are going to be used to do the proper internal mappings

minItems: 1
uniqueItems: True
"""



DATASETS_VISIBILITY_DEFAULT = 'public'
""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_visibility' """



DATA_MODEL_RELATIVE_PATH_DEFAULT = 'json-schemas/1.0.x'
""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_reldir' """



DATA_MODEL_TAG_DEFAULT = '6495b7317f830ad739591be2de1e279ea6c4c0d8'
""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_tag' """



DatasetsVisibility = Union[Literal["public"], Literal["community"], Literal["challenge"], Literal["participant"]]
"""
Datasets visibility.

The desired visibility of the submitted datasets, which must be acknowledged by the APIs

default: public
readonly: True
"""
DATASETSVISIBILITY_PUBLIC: Literal["public"] = "public"
"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_COMMUNITY: Literal["community"] = "community"
"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_CHALLENGE: Literal["challenge"] = "challenge"
"""The values for the 'Datasets visibility' enum"""
DATASETSVISIBILITY_PARTICIPANT: Literal["participant"] = "participant"
"""The values for the 'Datasets visibility' enum"""



FIX_NON_CONFORMANT_ORIGINAL_IDS_DEFAULT = True
""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database fix_original_ids' """



OPENEBENCH_BENCHMARKING_DATA_MODEL_REPOSITORY_DEFAULT = 'https://github.com/inab/benchmarking-data-model.git'
""" Default value of the field path 'Schema that defines the set of fields to be filled for a submission to OpenEBench database data_model_repo' """



ParticipantTool = str
"""
Participant Tool.

The id of the tool used to generate the dataset. Should be selected by uploader, using API

pattern: ^OEBT[0-9]{3}[A-Z0-9]{7}$
"""



Version = Union[str, Union[int, float]]
"""
Version.

Version (or release date) of the dataset

minLength: 1
"""



_ContactsItem = str
""" format: autocomplete """



class _ParticipantElements(TypedDict, total=False):
    tool_id: Required["ParticipantTool"]
    """ Required property """

    data_version: Required["Version"]
    """ Required property """

    data_contacts: Required["Contacts"]
    """ Required property """

    participant_id: str
    """
    The id / name of the tool which generated this dataset.

    minLength: 1
    """

