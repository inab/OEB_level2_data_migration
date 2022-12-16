#!/usr/bin/env python3

import os.path
from typing import TYPE_CHECKING
from extended_json_schema_validator.extensible_validator import ExtensibleValidator

if TYPE_CHECKING:
	from typing import (
		Tuple,
	)
	
	from extended_json_schema_validator.extensible_validator import ExtensibleValidatorConfig

AUTH_CONFIG_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/configuration-json-schema"

SUBMISSION_FORM_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/submission-form-json-schemas"

def create_validator_for_directory(schemas_dir: "str", config: "ExtensibleValidatorConfig" = {}) -> "Tuple[ExtensibleValidator, int]":
    schema_validators = ExtensibleValidator(config=config)
        
    numSchemas = schema_validators.loadJSONSchemas(schemas_dir, verbose=False)
    
    return schema_validators, numSchemas

def create_validator_for_oeb_level2() -> "Tuple[ExtensibleValidator, int]":
	return create_validator_for_directory(os.path.dirname(__file__))