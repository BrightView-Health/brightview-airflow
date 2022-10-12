from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from custom_helpers.utils import *

dag_config = Variable.get("carelogic-to-snowflake",deserialize_json=True)
retries=dag_config.get('retries')
retry_delay=dag_config.get('retry_delay')
args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 9, 25),
    'tags': ['etl'],
    'concurrency': 40,
    'email': ["simar@bootstaff.co", "mamta@bootstaff.co", "sreedhar@bootstaff.co"],
    'email_on_failure': True,
    'retries': retries,
    'retry_delay': timedelta(seconds=retry_delay),

}

##give name to dag (only variable for changing the priority of the job)
dag_name = "carelogic_to_snowflake_p35"
table_load_type = ""

#dag_config = Variable.get("carelogic-to-snowflake",deserialize_json=True)
old_db = dag_config.get("old_db")
old_schema = dag_config.get("old_schema") 
destination_db = dag_config.get("destination_db")
destination_schema = dag_config.get("destination_schema")
metadata_table = dag_config.get("metadata_table")
metadata_schema = dag_config.get("metadata_schema")
audit_table = dag_config.get('audit_table')

def dag_init(**kwargs):
    print("hello there")
    
def check_table_exist_func(table_name, **kwargs):
    records = check_table_exist(table_name, table_load_type)
    print(records)
    
def get_table_schema_func(table_name, **kwargs):
    column_list = get_table_schema(table_name)
    print(column_list)
    return column_list
    
def get_hive_data_func(table_meta, **kwargs):
    column_list = kwargs['ti'].xcom_pull(task_ids='get_schema_{table}'.format(table=table_meta.get('table_name')))
    max_delta_value = get_hive_data(table_meta = table_meta, column_list = column_list, table_load_type = table_load_type)
    return max_delta_value
    print(max_delta_value)
    
def update_metadata_func(table_name, **kwargs):
    new_delta_value = kwargs['ti'].xcom_pull(task_ids=f"pull_records_{table_name}")
    print(f"printing the delta value which will be updated for current run {str(new_delta_value)}")
    records = update_metadata(table_name, new_delta_value)
    print(records)
    
def update_audit_table_func(table_name, **kwargs):
    return_list = kwargs['ti'].xcom_pull(task_ids="validate_record_counts_{table}".format(table=table_name))
    update_audit_table(table_name, return_list, audit_table)
    
def validate_counts_func(table_meta, **kwargs):
    new_delta_value = kwargs['ti'].xcom_pull(task_ids="pull_records_{table_name}".format(table_name=table_meta.get('table_name')))
    print(f"printing the delta value which was updated for current run {str(new_delta_value)}")
    return_list = validate_record_count(table_meta, new_delta_value)
    return return_list
    
def dag_exit(**kwargs):
    print("bye")
    
    
with DAG(dag_name, default_args=args, schedule_interval=dag_config.get(f'schedule_{dag_name.split("_")[-1]}'), catchup=False) as dag: 
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
                                           where "pid" = '{dag_name.split("_")[-1]}' AND "table_active" = 'TRUE' """)

    for table in required_table_meta:
        
        check_table_exist_task = PythonOperator(task_id='create_{table}'.format(table=table.get('table_name')),     
                                               python_callable=check_table_exist_func,
                                               op_kwargs={"table_name": table.get('table_name')},
                                               dag=dag)
        
        get_schema_of_table = PythonOperator(task_id='get_schema_{table}'.format(table=table.get('table_name')),     
                                               python_callable=get_table_schema_func,
                                               op_kwargs={"table_name": table.get('table_name')},
                                               do_xcom_push=True,
                                               provide_context=True, 
                                               dag=dag )
        
        
        get_records_from_hive = PythonOperator(task_id='pull_records_{table}'.format(table=table.get('table_name')),     
                                    python_callable=get_hive_data_func,
                                    op_kwargs={"table_meta": table}, 
                                    do_xcom_push=True,
                                    provide_context=True, 
                                    dag=dag )
        
        update_table_metadata = PythonOperator(task_id='update_metadata_{table}'.format(table=table.get('table_name')),     
                                               python_callable=update_metadata_func,
                                               op_kwargs={"table_name": table.get('table_name')},   
                                               dag=dag )
        
        validate_record_counts = PythonOperator(task_id='validate_record_counts_{table}'.format(table=table.get('table_name')),     
                                               python_callable = validate_counts_func,
                                               op_kwargs={"table_meta": table}, 
                                               do_xcom_push=True,
                                               provide_context=True,
                                               dag=dag )
        
        update_audit_tables = PythonOperator(task_id='update_audit_{table}'.format(table=table.get('table_name')),     
                                               python_callable = update_audit_table_func,
                                               op_kwargs={"table_name": table.get('table_name')},   
                                               dag=dag )
        
        init_task  >> check_table_exist_task >> get_schema_of_table >> get_records_from_hive >> update_table_metadata  >>  validate_record_counts >> update_audit_tables >> exit_task
