#!/usr/bin/env python3

import os.path
from typing import TYPE_CHECKING
from extended_json_schema_validator.extensible_validator import ExtensibleValidator

if TYPE_CHECKING:
	from typing import (
		Sequence,
		Tuple,
		Union,
	)
	
	from extended_json_schema_validator.extensible_validator import ExtensibleValidatorConfig
	from extended_json_schema_validator.extensions.abstract_check import SchemaHashEntry

AUTH_CONFIG_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/configuration-json-schema"

SUBMISSION_FORM_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/submission-form-json-schemas"

MINIMAL_DATA_BLOCK_SCHEMA_ID = "https://github.com/inab/benchmarking/minimal-json-schemas"

SINGLE_METRIC_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/single-metric"

SERIES_METRIC_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/series-metric"

AGGREGATION_2D_PLOT_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/aggregation-2d-plot"

AGGREGATION_BAR_PLOT_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/aggregation-bar-plot"

AGGREGATION_BOX_PLOT_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/aggregation-data-series"

TYPE2SCHEMA_ID = {
	"2D-plot": AGGREGATION_2D_PLOT_SCHEMA_ID,
	"bar-plot": AGGREGATION_BAR_PLOT_SCHEMA_ID,
	"box-plot": AGGREGATION_BOX_PLOT_SCHEMA_ID,
}

ASSESSMENT_INLINE_SCHEMAS = [
	SINGLE_METRIC_SCHEMA_ID,
	SERIES_METRIC_SCHEMA_ID,
]

AGGREGATION_INLINE_SCHEMAS = [
	AGGREGATION_2D_PLOT_SCHEMA_ID,
	AGGREGATION_BAR_PLOT_SCHEMA_ID,
	AGGREGATION_BOX_PLOT_SCHEMA_ID,
]

LEVEL2_SCHEMA_IDS = [
	AUTH_CONFIG_SCHEMA_ID,
	SUBMISSION_FORM_SCHEMA_ID,
	MINIMAL_DATA_BLOCK_SCHEMA_ID,
	*ASSESSMENT_INLINE_SCHEMAS,
	*AGGREGATION_INLINE_SCHEMAS,
]

def create_validator(schemas_dir: "Union[str, Sequence[SchemaHashEntry]]", config: "ExtensibleValidatorConfig" = {}) -> "Tuple[ExtensibleValidator, int]":
	schema_validators = ExtensibleValidator(config=config)
	
	if isinstance(schemas_dir, list):
		schemas_param = schemas_dir
	else:
		schemas_param = [ schemas_dir ]
	
	numSchemas = schema_validators.loadJSONSchemas(*schemas_param, verbose=False)
	
	return schema_validators, numSchemas

def create_validator_for_oeb_level2() -> "Tuple[ExtensibleValidator, int]":
	return create_validator(os.path.dirname(__file__))