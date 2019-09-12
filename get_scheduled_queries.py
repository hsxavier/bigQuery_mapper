#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input:
config - a string for a filename of a json file.

Runs the bash command 'gcloud auth print-access-token' to get the access token, 
then uses it to enter the BigQuery API via http requests and download all info about
scheduled queries, in particular their query codes. Saves them to directory specified
in the config.

Created by: Henrique S. Xavier, hsxavier@if.usp.br, 12/sep/2019.
"""

import sys
import download_bigquery_info as d

# Docstring output:
if len(sys.argv) > 1 + 1: 
    print(__doc__)
    sys.exit(1)

# Get input config:
elif len(sys.argv) == 1 + 1:
    config = sys.argv[1]
    
# Set default config:
else:
    config = {
        "credentials": "/home/skems/gabinete/projetos/keys-configs/gabinete-compartilhado.json",
        "printout": True,
        "get_views": True,
        "views_path":  "../views/",
        "scheduled_path": "../scheduled_queries/"
    }

# Run code:
d.get_scheduled_queries(config)
