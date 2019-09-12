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

This script can print to the screen a list of tables in BigQuery and downloads
the query codes from views.

### `get_scheduled_queries.py`

This script downloads non-disabled scheduled query codes.

### `create_flowchart.py`

The script can be executed with a input parameter (a JSON config file)
or without any parameters (in which case it used a hard-coded config).

The hard coded config reads the previously downloaded views and scheduled queries and
creates a flowchart to 'this_file.pdf'. The configuration passed to the
script can demand views and scheduled queries to be updated (i.e. downloaded) first.

## Authors

**Henrique S. Xavier** [@hsxavier](https://github.com/hsxavier)

