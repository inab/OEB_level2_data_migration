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

#COLORED_LOGS_FMT='%(asctime)s %(hostname)s %(name)s[%(process)d]\n[%(levelname)s] %(message)s'
COLORED_LOGS_FMT='%(asctime)s %(hostname)s %(name)s\n[%(levelname)s] %(message)s'
COLORED_LOGS_FMT_BRIEF='%(asctime)s [%(levelname)s] %(message)s'

COLORED_LOGS_LEVEL_STYLES={
    'critical': {
        'bold': True,
        'background': 'red'
    },
    'debug': {
        'color': 'green'
    },
    'error': {
        'bold': True,
        'color': 'red'
    },
    'info': {
    },
    'notice': {
        'color': 'magenta'
    },
    'spam': {
        'color': 'green',
        'faint': True
    },
    'success': {
        'bold': True,
        'color': 'green'
    },
    'verbose': {
        'color': 'blue'
    },
    'warning': {
        'color': 'yellow'
    }
}

# Borrowed from WfExS-backend
LOGFORMAT = "%(asctime)-15s - [%(levelname)s] %(message)s"
VERBOSE_LOGFORMAT = "%(asctime)-15s - [%(name)s %(funcName)s %(lineno)d][%(levelname)s] %(message)s"
