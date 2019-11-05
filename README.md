# Backup & map BigQuery

This project has a set of executable scripts that:

* Lists the tables in BigQuery;
* Downloads the query codes for views and scheduled queries;
* Create a flowchart (using `pygraphviz`) of the BigQuery table relations.

**NOTE:** Besides python, this script requires the shell command `gcloud` to get
an access token to bigquery.

---

## Scripts

### `list_tables.py`

This script can print to the screen a list of tables, save the list to a file in BigQuery
and downloads the query codes from views.

### `get_scheduled_queries.py`

This script downloads non-disabled scheduled query codes.

### `create_flowchart.py`

The script can be executed with a input parameter (a JSON config file)
or without any parameters (in which case it used a hard-coded config).

The hard coded config reads the previously downloaded views and scheduled queries and
creates a flowchart to 'this_file.pdf'. The configuration passed to the
script can demand views and scheduled queries to be updated (i.e. downloaded) first.
In this case, old queries not present in BigQuery anymore are deleted.

## Configuration file

The configuration file is a JSON file with the following keywords:

* `credentials`     (str): path to JSON file containing Google Cloud credentials;
* `printout`       (bool): whether or not to print on screen information about the tables;
* `table_list_file` (str): path to file where to save the table information above (can be set to `null` to avoid saving this to a file);
* `get_views`      (bool): whether or not to update the local copy of the views (and delete old saved views);
* `views_path`      (str): path to a folder where to save view queries as `.sql` files;   
* `get_scheduled`  (bool): whether or not to update the local copy of the scheduled queries (and delete old saved ones);
* `scheduled_path`  (str): path to a folder where to save scheduled queries as `.sql` files;
* `flowchart`      (bool): whether or not to produce a flowchart of the tables and views dependencies;
* `flowchart_file`  (str): path to a PDF file where to save the flowchart.


## Authors

**Henrique S. Xavier** [@hsxavier](https://github.com/hsxavier)

