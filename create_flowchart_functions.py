import re
import pygraphviz as pg
import download_bigquery_info as di
from subprocess import check_output


### Mapping connections ###

def skip_empty_strings(string_list):
    """
    Given a list of strings, remove the strings that are empty (zero length):
    """
    return list(filter(lambda s: len(s)>0, string_list))


def get_destination_table(query_file):
    """
    Given a path to a scheduled query file (which we assume contains the 
    destination table in the first line), returns this destination table.
    """
    with open(query_file, 'r') as f:
        raw = f.read()
    split = raw.split('\n')
    line  = split[0]
    query = '\n'.join(split[1:])
    return query, line.split(': ')[1]


def remove_all(pattern_list, string):
    """
    Given a list of compiled regex patterns 'pattern_list' and a 'string', 
    removes all matches from string. 
    """
    temp = string
    for p in pattern_list:
        temp = re.subn(p, '', temp)[0]
    return temp


def remove_comments(query_list):
    """
    Given a query list (a list of strings), returns a list with the same queries but 
    with comments removed (to avoid matching commented tables).
    """
    # Remove large comments:
    large_comment  = re.compile(r"/\*.*?\*/", re.DOTALL)
    # Remove hash-commented lines:
    hash_comment   = re.compile(r"#.*$", re.MULTILINE)
    # Remove commented lines:
    single_comment = re.compile(r"--.*$", re.MULTILINE)
    
    return [remove_all([large_comment, hash_comment, single_comment], q) for q in query_list]


def load_file(filename):
    """
    Given a filename (string), returns the content of the respective file as a string.
    """
    with open(filename, 'r') as f:
        content = f.read()
    return content


def get_referenced_tables(tables_list, query):
    """
    Given a list of strings 'tables_list' that contains the names of BigQuery tables and 
    a 'query' (string), returns a list of table names that are present in the query.
    """
    referenced_tables = []
    for t in tables_list:
        pattern = re.compile(t.replace('.','\.')+'`')
        if re.search(pattern, query) != None:
            referenced_tables.append(t)
    return referenced_tables


def crawl_dependencies(all_table_query_dict, one_table):
    """
    Given a dict 'all_table_query_dict' with all tables names as keys and 
    the respective queries that build them as values (if there is no query, 
    the value is None); and one string 'one_table' with the name of one table,
    return a set of all tables that are needed by 'one_table'.
    """
    # Initial condition:
    if all_table_query_dict[one_table] == None:
        return set()
    # Recursive response:
    else:
        referenced = get_referenced_tables(list(all_table_query_dict.keys()), all_table_query_dict[one_table])
        result = {one_table} | set(referenced)
        for ref in referenced:
            result = result | set(crawl_dependencies(all_table_query_dict, ref))
        return result


def structure_bigquery_data(config):
    """
    Input: 
    config - a dict or filename of json file containing the configuration parameters:

    This function downloads all table names and types from BigQuery, loads the previously
    downloaded (and locally stored) views and scheduled queries, cleanes queries of comments,
    classify tables into types EXTERNAL, TABLE, VIEW and SCHEDULED.

    Returns:
    a tuple of 2 dicts, the first one being table_name -> query, and the second
    one being table_name -> type.
    """
    
    # Downloads list all tables:
    list_raw        = di.list_tables(config)
    all_tables_list = [d['name'] for d in list_raw]
    
    # Load list of all views:
    views_dir  = config['views_path']
    views_list = skip_empty_strings(check_output(['ls', views_dir]).decode('utf-8').replace('.sql','').split('\n'))
    
    # Load list of all scheduled tables:
    sched_queries_dir  = config['scheduled_path']
    sched_queries_list = skip_empty_strings(check_output(['ls', sched_queries_dir]).decode('utf-8').split('\n'))
    sched_list = [get_destination_table(sched_queries_dir + q) for q in sched_queries_list]
    sched_tables_list = [s[1] for s in sched_list]
    sched_query_codes_list = [s[0] for s in sched_list]
    
    # Create list of external tables and bigquery static tables:
    ext_tables_list = list(set(all_tables_list) - set(views_list) - set(sched_tables_list))
    bg_tables_list  = [t['name'] for t in list_raw if t['type'] == 'TABLE' and t['name'] in set(ext_tables_list)]
    ext_tables_list = list(set(ext_tables_list) - set(bg_tables_list))
    
    # Remove comments from scheduled queries:
    clean_sched_queries = remove_comments(sched_query_codes_list)
    
    # Load views' codes and remove comments from them:
    views_code_list = [load_file(views_dir + v + '.sql') for v in views_list]
    clean_views     = remove_comments(views_code_list)
    
    # Build a dict: table_name -> creating query:
    ordered_tables_list  = ext_tables_list + bg_tables_list + sched_tables_list + views_list
    ordered_queries_list = [None] * len(ext_tables_list) + [None] * len(bg_tables_list) \
                           + clean_sched_queries + clean_views
    ordered_types        = ['EXT'] * len(ext_tables_list) + ['TABLE'] * len(bg_tables_list) + \
                           ['SCHED'] * len(sched_tables_list) + ['VIEWS'] * len(views_list)

    all_table_query_dict = dict(zip(ordered_tables_list, ordered_queries_list))
    all_table_type_dict  = dict(zip(ordered_tables_list, ordered_types))
    
    return all_table_query_dict, all_table_type_dict

    
### Creating flowcharts with pygraphviz ###

def link_tables_by_queries(graph, all_tables_list, tables_list, queries_list):
    """
    Given a list of all tables (strings) in a project 'all_tables_list', 
    a list of a sub-set of tables (strings) 'tables_list' and a list of queries 
    (strings) 'queries_list', add edges between the sub-set of tables and 
    the tables present in 'all_tables_list' that are mentioned in the list 
    of queries. Note that 'tables_list' are the destination of the queries and 
    it should be aligned with 'queries_list'.
    """
    arrow_color_list = ['black','gray','red','red4','skyblue','blue','green','darkgreen','yellow', 
                        'brown','purple','pink','orange']
    color_counter = 0
    
    # Loop over queries and their destination tables:
    for table, query in zip(tables_list, queries_list):
        color_counter = color_counter+1
        # Get all tables used by one query:
        origin_list = get_referenced_tables(all_tables_list, query)
        # Connect all used tables to the query's destination:
        for origin_table in origin_list:
            graph.add_edge(origin_table, table, color=arrow_color_list[color_counter % len(arrow_color_list)])


def create_flowchart(all_table_query_dict, all_table_type_dict, tables, filename):
    """
    Given:
    - a dict with name of tables as keys and queries as values 'all_table_query_dict';
    - a dict with name of tables as keys and their types (external, scheduled or view)
      as values 'all_table_type_dict';
    - a list or set of table names 'tables';
    - and a 'filename', 
    create a flowchart of 'tables' and save it to 'filename'.
    """
    
    # Group tables by type:
    ext_tables   = list(filter(lambda t: all_table_type_dict[t]=='EXT', tables))
    bg_tables    = list(filter(lambda t: all_table_type_dict[t]=='TABLE', tables))
    sched_tables = list(filter(lambda t: all_table_type_dict[t]=='SCHED', tables))
    views        = list(filter(lambda t: all_table_type_dict[t]=='VIEWS', tables))
    
    # Get queries:
    sched_queries = [all_table_query_dict[t] for t in sched_tables]
    view_queries  = [all_table_query_dict[t] for t in views]
    
    # Create a new graph:
    g = pg.AGraph(directed=True, ranksep='2')
    g.node_attr['fontsize']=20
    g.node_attr['style']='filled'
    
    # Place all tables (color-coded as scheduled, external and view) in the graph:
    g.add_nodes_from(ext_tables, color='palegreen3', fillcolor='palegreen', shape='cylinder')
    g.add_nodes_from(bg_tables, color='snow3', fillcolor='snow', shape='house')
    g.add_nodes_from(views, color='khaki3', fillcolor='khaki', shape='cds')
    g.add_nodes_from(sched_tables, color='skyblue3', fillcolor='skyblue', shape='ellipse')
    
    # Place connections in graph:
    link_tables_by_queries(g, list(tables), sched_tables,  sched_queries)
    link_tables_by_queries(g, list(tables), views,  view_queries)
    
    # Print graph to file:
    g.layout(prog='dot')
    g.draw(filename, prog='dot')
