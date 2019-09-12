#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Input: 
config - filename of json file containing the configuration parameters.

If no input is passed, run with hard-coded config that does not download 
views and scheduled queries.

This function downloads all table names and types from BigQuery, 
optionally downloads view and scheduled queries or loads the previously
downloaded (and locally stored) views and scheduled queries, cleanes queries of comments,
classify tables into types EXTERNAL, TABLE, VIEW and SCHEDULED. It then creates a 
flowchart that is saved to a file.

Created by: Henrique S. Xavier, hsxavier@if.usp.br, 12/sep/2019.
"""

import sys
import create_flowchart_functions as cf
import download_bigquery_info as di
import json

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
        "printout": False,
        "get_views": False,
        "views_path":  "../views/",
        "get_scheduled": False,
        "scheduled_path": "../scheduled_queries/",
        "flowchart": True,
        "flowchart_file": "this_file.pdf"
    }

    
### MAIN CODE ###

# Load config from file:
if type(config)==str:
    with open(config, 'r') as f:
        config = json.load(f)    

if config['get_scheduled']:
    di.get_scheduled_queries(config)
    
if config['flowchart']:
    all_table_query_dict, all_table_type_dict = cf.structure_bigquery_data(config)
    all_tables_list = list(all_table_type_dict.keys())

    cf.create_flowchart(all_table_query_dict, all_table_type_dict, all_tables_list, config['flowchart_file'])
