from datetime import datetime

from operators import CreateTableOperator

from airflow import DAG
# from helpers import load_dim_subdag
# from helpers import SqlQueries
# from operators import DataQualityOperator
# from operators import LoadFactOperator
# from operators import LoadStageOperator
# from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME = 'capstone_pipeline'

default_args = {
    'owner': 'jp',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'catchup': False,
}

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Load and transform data in postgres with Airflow',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)

create_tables = CreateTableOperator(
    task_id='Create_Tables',
    postgres_conn_id='postgres',
    dag=dag,
)

# load_demo_stage = LoadStageOperator(
#     task_id='Stage_Demographics',
#     table_name='stage_demo',
#     postgres_conn_id='postgres',
#     sql_query=SqlQueries.demo_stage_load,
#     file='TODO',
#     dag=dag,
# )

# load_temp_stage = LoadStageOperator(
#     task_id='Stage_Temperatures',
#     table_name='stage_temp',
#     postgres_conn_id='postgres',
#     sql_query=SqlQueries.temp_stage_load,
#     file='TODO',
#     dag=dag,
# )

# load_demo_fact = LoadFactOperator(
#     task_id='Fact_Demographics',
#     postgres_conn_id='postgres',
#     sql_query=SqlQueries.fact_demo_load,
#     dag=dag,
# )

# load_temp_fact = LoadFactOperator(
#     task_id='Fact_Temperatures',
#     postgres_conn_id='postgres',
#     sql_query=SqlQueries.fact_temp_load,
#     dag=dag,
# )

# load_city_dim_table = SubDagOperator(
#     subdag=load_dim_subdag(
#         parent_dag_name=DAG_NAME,
#         task_id='Dim_City',
#         postgres_conn_id='postgres',
#         start_date=default_args['start_date'],
#         sql_statement=SqlQueries.dim_city_load,
#         do_truncate=True,
#         table_name='dim_city',
#     ),
#     task_id='Dim_City',
#     dag=dag,
# )

# load_date_dim_table = SubDagOperator(
#     subdag=load_dim_subdag(
#         parent_dag_name=DAG_NAME,
#         task_id='Dim_Date',
#         postgres_conn_id='postgres',
#         start_date=default_args['start_date'],
#         sql_statement=SqlQueries.dim_date_load,
#         do_truncate=True,
#         table_name='date',
#     ),
#     task_id='Dim_Date',
#     dag=dag,
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     postgres_conn_id='postgres',
#     tables=['dim_date', 'dim_city', 'fact_temp', 'fact_demo'],
# )

# create_tables >> [load_demo_stage, load_temp_stage]

# load_demo_stage >> [load_city_dim_table, load_date_dim_table, load_demo_fact]
# load_temp_stage >> [load_city_dim_table, load_date_dim_table, load_temp_fact]

# [
#     load_demo_fact,
#     load_temp_fact,
#     load_city_dim_table,
#     load_date_dim_table,
# ] >> run_quality_checks

create_tables
