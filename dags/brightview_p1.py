"""
******************************************************************
Author = Bootstaff          *                        
Date = '23 August 2022'     * 
Description = Send file from carelogic tables to snowflake * 
******************************************************************
"""

##other imports
import pandas as pd
import time
import json
import jaydebeapi
import jpype
import sys
import re
import gcsfs
from contextlib import closing
from datetime import datetime, timedelta,date

##airflow imports
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import models
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator 
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.models.variable import Variable

args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 23),
    'tags': ['etl'],
    'concurrency': 10,
    'email': ["mamtanainwal6@gmail.com", "sreedhar@bootstaff.co", "mamta@bootstaff.co"],
    'email_on_failure': True,
    #'params': {'snowflake_conn_id': 'snowflake_conn_id','gcp_conn_id' 'gcp_conn_id'},
    #'on_failure_callback': ??
}

##give name to dag
dag_name = "brightview_p1"


hive_config = Variable.get("carelogic_hive_db",deserialize_json=True)
dag_config = Variable.get("carelogic_to_snowflake",deserialize_json=True)
old_db = dag_config.get("old_db")
old_schema = dag_config.get("old_schema") 
destination_db = dag_config.get("destination_db")
destination_schema = dag_config.get("destination_schema")
metadata_table = dag_config.get("metadata_table")
metadata_schema = dag_config.get("metadata_schema")


def dag_init(**kwargs):
    print("hello there")

def connect_snowflake():
    snowflake_conn = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return snowflake_conn

def gcs_conn(gcp_conn_id):
    gcs_connection = S3Hook(gcp_conn_id=gcp_conn_id)
    return gcs_connection

def hive_conn():
    hive_connection = jaydebeapi.connect(
                                hive_config.get('driver_class'),
                                hive_config.get('jdbc_url'),
                                [hive_config.get('username'), hive_config.get('password')],
                                hive_config.get('driver_path'),
                                )
    return hive_connection
    

def execute_snowflake_query(query, with_cursor=False):
    """Execute snowflake query."""
    snowflake_conn = connect_snowflake()
    with closing(snowflake_conn.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(query)
            res = cur.fetchall()
            if with_cursor:
                return (res, cur)
            else:
                return res
            
def execute_hive_query(query, with_cursor=False):
    """Execute Hive query."""
    hive_connection = hive_conn()
    cur = hive_connection.cursor()
    cur.execute(query)
    res = cur.fetchall()
    if with_cursor:
        return (res, cur)
    else:
        return res
              
def get_metadata(query):
    """Execute snowflake query."""
    print("Execute snowflake meta data query")
    metadata_list = []
    records, cursor = execute_snowflake_query(query, True)
    columnNames = [column[0] for column in cursor.description]
    for record in records:
        metadata_list.append( dict( zip( columnNames , record ) ) )
    return metadata_list

def check_table_exist(table_name, **kwargs):
    query = f'''create table 
                if not exists "{destination_db}"."{destination_schema}".{table_name} 
                like "{old_db}"."{old_schema}".{table_name}'''
    records = execute_snowflake_query(query, False)
    print(records)

    
def create_query(table_meta, offset, limit):
    table = table_meta.get('table_name')
    load_type = table_meta.get('load_type')
    delta_column = table_meta.get('delta_column')
    delta_value = table_meta.get('delta_value')
    primary_key = table_meta.get('primary_key')
    limit = limit
    
    ##framing where clause
    if not delta_value:
        delta_value = "1900-01-01 00:00:00"
    if load_type == "full_table" or delta_column is None:
        where_clause = ""
    else:
        where_clause = f"WHERE {delta_column} >= '{delta_value}'"
        
    ##framing order by clause
    if primary_key == None:
        order_by = f"ORDER BY {delta_column} "
    else:
        order_by = f"ORDER BY {delta_column}, {primary_key} "
        
    rowlimit_clause = f"LIMIT {limit} OFFSET {offset}"
    
    source_query = f"""SELECT *
                       FROM {table}
                       {where_clause}
                       {order_by}
                       {rowlimit_clause}"""  

    return source_query

def get_table_schema(table_name, **kwargs):
    col_info_list = []
    query = f'''show columns 
                in table "{destination_db}"."{destination_schema}"."{table_name.upper()}"'''
    print("Table Schema Query is ",query)
    records, cursor = execute_snowflake_query(query, True)
    columnNames = [column[0] for column in cursor.description]
    for record in records:
        col_info_list.append( dict( zip( columnNames , record ) ) )
    column_list = [ col.get('column_name') for col in col_info_list ]
    print(column_list)
    return column_list
    
        
def get_hive_data(table_meta,**kwargs):
    column_list = kwargs['ti'].xcom_pull(task_ids='get_schema_{table}'.format(table=table_meta.get('table_name')))
    print(column_list)
    max_delta_value = ''
    Running = True
    offset = 0
    limit_rows = 10000
    while Running:
        query = create_query(table_meta, offset,limit_rows)
        print(f"Query running for source : {query}")
        result, cursor = execute_hive_query(query, with_cursor=True)
        if result:
            try:
                offset = offset + limit_rows + 1
                headers = list(map(lambda t: t[0].upper(), cursor.description))
                df = pd.DataFrame(result)
                df.columns = headers
                df['__LOADED_AT'] = datetime.now()    
                snowflake_conn = connect_snowflake()
                df_ord = df[column_list]
                print("Writing the data in snowflake")
                df_ord.to_sql(name = f"{table_meta.get('table_name')}",
                              schema = destination_schema,
                              if_exists='append', 
                              con=snowflake_conn.get_sqlalchemy_engine(), 
                              index=False)
            except Exception as e:
                print("Exception occurred while running the load ", e)
                query = f'''SELECT MAX("{table_meta.get('delta_column')}") 
                           FROM "{destination_db}"."{destination_schema}"."{table_meta.get('table_name')}" '''
                max_delta_value = execute_snowflake_query(query,False)[0][0] 
                return max_delta_value
        else:
            Running = False
    if offset != 0:
            max_delta_value = df_ord[table_meta.get('delta_column').upper()].max()
    return max_delta_value


def update_metadata(table_name, **kwargs):
    new_delta_value = kwargs['ti'].xcom_pull(task_ids=f"pull_records_{table_name}")
    query = f"""UPDATE "{destination_db}"."{metadata_schema}"."{metadata_table}" 
               SET "delta_value" = '{new_delta_value}',
               "last_updated" = '{datetime.now()}'
               WHERE "table_name" = '{table_name}'"""
    records = execute_snowflake_query(query, False)
    print(records)
    
    
def dag_exit(**kwargs):
    print("bye")
    
with DAG(dag_name, default_args=args, schedule_interval=dag_config.get('schedule'), catchup=False) as dag: 
    init_task=PythonOperator(
            task_id="init_dag",
            python_callable=dag_init
            )

    exit_task=PythonOperator(
            task_id="exit_dag",
            python_callable=dag_exit
            )
    
    required_table_meta = get_metadata(f"""SELECT * 
                                           FROM "{destination_db}"."{metadata_schema}"."{metadata_table}" 
                                           where "pid" = '{dag_name.split("_")[1]}'""")

    for table in required_table_meta:
        
        check_table_exist_or_not = PythonOperator(task_id='create_{table}'.format(table=table.get('table_name')),     
                                               python_callable=check_table_exist,
                                               op_kwargs={"table_name": table.get('table_name')},   
                                               dag=dag )
        
        get_schema_of_table = PythonOperator(task_id='get_schema_{table}'.format(table=table.get('table_name')),     
                                               python_callable=get_table_schema,
                                               op_kwargs={"table_name": table.get('table_name')},
                                               do_xcom_push=True,
                                               provide_context=True, 
                                               dag=dag )
        
        
        get_records_from_hive = PythonOperator(task_id='pull_records_{table}'.format(table=table.get('table_name')),     
                                    python_callable=get_hive_data,
                                    op_kwargs={"table_meta": table}, 
                                    do_xcom_push=True,
                                    provide_context=True, 
                                    dag=dag )
        
        update_table_metadata = PythonOperator(task_id='update_metadata_{table}'.format(table=table.get('table_name')),     
                                               python_callable=update_metadata,
                                               op_kwargs={"table_name": table.get('table_name')},   
                                               dag=dag )
       

        init_task  >> check_table_exist_or_not >> get_schema_of_table >> get_records_from_hive >> update_table_metadata >> exit_task
