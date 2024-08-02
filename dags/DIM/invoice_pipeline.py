import importlib
import random
from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from DIM_pull.transfer_to_stg import src_to_stg
from airflow.models import Variable

with DAG(
    dag_id="invoice_tasks",
    start_date=datetime(2024, 1, 11),
    schedule_interval=None,
) as dag:
    
    start = DummyOperator(task_id='start', dag=dag)
    done_src_to_stg = DummyOperator(task_id='done_src_to_stg', dag=dag)
    version = 'v2'
    random.seed(10)
    runday = datetime.strptime(Variable.get('runday'),'%Y/%m/%d')

    # wait_for_sp_dag = ExternalTaskSensor(
    #     task_id='wait_for_sp_task',
    #     external_dag_id='dim_sp_tasks',
    #     external_task_id='end',
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     mode='poke',
    #     poke_interval=2,
    #     timeout=600, 
    # )
    
    # wait_for_kho_dag = ExternalTaskSensor(
    #     task_id='wait_for_kho_task',
    #     external_dag_id='dim_kho_tasks',
    #     external_task_id='end',
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     mode='poke',
    #     poke_interval=2,
    #     timeout=600, 
    # )
    # [wait_for_kho_dag, wait_for_sp_dag] >> start

    def create_task(table, version, runday):
        with TaskGroup(group_id=f"dim_raw_to_stg_{version}_{table}") as invoice_task:
            src_to_stg_task = PythonOperator(
                task_id=f"src_to_stg_{table.lower()}",
                python_callable= src_to_stg,
                op_kwargs={
                    'table_name': table,
                    'version'   : version,
                    'runday'    : runday
                },
                dag=dag
            )
            sleep_task = BashOperator(
                task_id=f"sleep_2s_{random.random()}",
                bash_command="sleep 2",
            )
            src_to_stg_task >> sleep_task
        return invoice_task

    tables = ['INVOICE','INVOICE_ITEMS']
    for version in ['v1','v2']:
        task_arr = []
        for table in tables:
            invoice_task = create_task(table, version=version, runday=runday)
            task_arr.append(invoice_task)
        for i in range(len(task_arr) - 1):
            task_arr[i] >> task_arr[i+1]
        start >> task_arr[0]
        task_arr[-1] >> done_src_to_stg
        
    load_dwh_task = PostgresOperator(
        task_id=f"load_to_dwh_invoice",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/INVOICE/load_to_dwh.sql",
        params = {'runday': Variable.get('runday')}
    )

    end = DummyOperator(task_id='end', dag=dag)

    done_src_to_stg >> load_dwh_task >> end
    # task_arr[-1] >> end
