#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This function prints to screen the list of BigQuery Tables and downloads the view's queries. 
It may be ran without input parameters (in which case it uses hard-coded defaults) or 
with a json file as config. 

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
d.list_tables(config)
