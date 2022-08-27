# brightview-airflow
airflow dags and infra for brightview

# AIRFLOW DATA INGESTION ARCHITECTURE

***-- ExtractLoad : Airflow 
-- Transformation : DBT 
-- Datawarehouse : Snowflake***

# Data flow:

1. Data is pulled from carelogic hive tables into snowflake raw schema
2. Transformation is applied on the table that is read from raw schema and added to staging layer
3. final transformation is applied and the resultant table is stored in analytics layer ( ANALYTICS_PROD in snowflake )
4. Aggreation FCT table are created in DBT using the analytics layer tables created in point 3 above

