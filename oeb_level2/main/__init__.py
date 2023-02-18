#!/usr/bin/env python

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
