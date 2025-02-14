#!/usr/bin/env python3
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

import os.path
from typing import TYPE_CHECKING
from extended_json_schema_validator.extensible_validator import ExtensibleValidator

if TYPE_CHECKING:
	from typing import (
		Sequence,
		Set,
		Tuple,
		Union,
	)
	
	from typing_extensions import (
		Final,
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

AGGREGATION_DATA_SERIES_SCHEMA_ID = "https://github.com/inab/OEB_level2_data_migration/aggregation-data-series"

VIS_2D_PLOT: "Final[str]" = "2D-plot"
VIS_BAR_PLOT: "Final[str]" = "bar-plot"
VIS_BOX_PLOT: "Final[str]" = "box-plot"
VIS_LINE_PLOT: "Final[str]" = "line-plot"
VIS_RADAR_PLOT: "Final[str]" = "radar-plot"

VIS_AGG_DATA_SERIES: "Final[Set[str]]" = {
    VIS_BOX_PLOT,
    VIS_LINE_PLOT,
    VIS_RADAR_PLOT,
}

TYPE2SCHEMA_ID = {
	VIS_2D_PLOT: AGGREGATION_2D_PLOT_SCHEMA_ID,
	VIS_BAR_PLOT: AGGREGATION_BAR_PLOT_SCHEMA_ID,
	VIS_BOX_PLOT: AGGREGATION_DATA_SERIES_SCHEMA_ID,
	VIS_LINE_PLOT: AGGREGATION_DATA_SERIES_SCHEMA_ID,
	VIS_RADAR_PLOT: AGGREGATION_DATA_SERIES_SCHEMA_ID,
}

ASSESSMENT_INLINE_SCHEMAS = [
	SINGLE_METRIC_SCHEMA_ID,
	SERIES_METRIC_SCHEMA_ID,
]

AGGREGATION_INLINE_SCHEMAS = [
	AGGREGATION_2D_PLOT_SCHEMA_ID,
	AGGREGATION_BAR_PLOT_SCHEMA_ID,
	AGGREGATION_DATA_SERIES_SCHEMA_ID,
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

def get_oeb_level2_schemas_path() -> "str":
	return os.path.dirname(__file__)

def create_validator_for_oeb_level2() -> "Tuple[ExtensibleValidator, int, int]":
	return (*create_validator(get_oeb_level2_schemas_path()), len(LEVEL2_SCHEMA_IDS))