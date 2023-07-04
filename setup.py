#!/usr/bin/env python
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

import os
import re
import sys

import setuptools

# In this way, we are sure we are getting
# the installer's version of the library
# not the system's one
setupBaseDir = os.path.dirname(__file__)
sys.path.insert(0, setupBaseDir)

from oeb_level2 import version as oeb_level2_version

# Populating the long description
with open(os.path.join(setupBaseDir, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Populating the install requirements
with open(os.path.join(setupBaseDir, "requirements.txt"), "r", encoding="utf-8") as f:
    requirements = []
    egg = re.compile(r"#[^#]*egg=([^=&]+)")
    for line in f.read().splitlines():
        m = egg.search(line)
        requirements.append(line if m is None else m.group(1))


setuptools.setup(
    name="OEB_level2_data_migration",
    version=oeb_level2_version,
    scripts=[
        "push_data_to_oeb.py",
        "validate_community_challenges.py",
    ],
    author="José Mª Fernández",
    author_email="jose.m.fernandez@bsc.es",
    description="OpenEBench Scientific Level 2 data migration",
    license="GPLv3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/inab/OEB_level2_data_migration",
    project_urls={
        "Bug Tracker": "https://github.com/inab/OEB_level2_data_migration/issues"
    },
    packages=setuptools.find_packages(),
    package_data={
        "": [
            "oebdev_api_auth.json.template",
            "minimal_dataset_examples",
        ],
        "oeb_level2": [
            "py.typed",
            "schemas/*.json",
            "schemas/*.yaml",
        ],
    },
    include_package_data=True,
    install_requires=requirements,
#    entry_points={
#        "console_scripts": [
#            "ext-json-validate=extended_json_schema_validator.__main__:main",
#        ],
#    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
