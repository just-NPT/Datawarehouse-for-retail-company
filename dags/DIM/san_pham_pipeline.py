import importlib
import random
from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from DIM_pull.transfer_to_stg import src_to_stg
from airflow.models import Variable

with DAG(
    dag_id="dim_sp_tasks",
    start_date=datetime(2024, 1, 11),
    schedule_interval=None,
) as dag:
    tables = ['D_LOAI_HANG','D_THUONG_HIEU','D_PHAN_LOAI_NGANH_HANG',
              'D_PACKSIZE','D_DON_VI_TINH','D_DON_VI_DG','D_HT_DONG_GOI',
              'D_NGUON_GOC_SP','D_MO_HINH_MUA','D_TRANG_THAI_SP','D_NCC_SP',
              'D_NHOM_SAN_PHAM','D_GIA_BAN_VAT','D_SAN_PHAM']
    
    start = DummyOperator(task_id='start', dag=dag)
    random.seed(10)
    runday = datetime.strptime(Variable.get('runday'),'%Y/%m/%d')
    
    def create_task(table, version, runday):
        with TaskGroup(group_id=f"dim_raw_to_stg_{table}") as dim_task:
            src_to_stg_task = PythonOperator(
                task_id=f"src_to_stg_{table.lower()}",
                python_callable= src_to_stg,
                op_kwargs={
                    'table_name': table,
                    'version': version,
                    'runday': runday
                },
                dag=dag
            )
            sleep_task = BashOperator(
                task_id=f"sleep_2s_{random.random()}",
                bash_command="sleep 2",
            )
            src_to_stg_task >> sleep_task
        return dim_task
    
    def load(tables, version):
        task_arr = []
        for table in tables:
            dim_task = create_task(table, version='master_data', runday=runday)
            task_arr.append(dim_task)
            
        version >> [task_arr[0],task_arr[1],task_arr[2]] 
        for i in range(0, len(task_arr) - 6,4):
            [task_arr[i],task_arr[i+1],task_arr[i+2]] >> task_arr[i+3]
            task_arr[i+3] >> [task_arr[i+4], task_arr[i+5], task_arr[i+6]]
        [task_arr[i+4],task_arr[i+5],task_arr[i+6]] >> task_arr[i+7]

        for i in range(len(task_arr)-4,len(task_arr)-1):
            task_arr[i] >> task_arr[i+1]

        end_version1 = DummyOperator(task_id='end_version', dag=dag)
        task_arr[-1] >> end_version1

        return end_version1
    


    last_task = load(tables, start)

    runday_str = Variable.get('runday')

    update_cay_nhom_san_pham_task = PostgresOperator(
        task_id = f"update_cay_nhom_san_pham_task",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/DIM_PRODUCT/D_CAY_NHOM_SAN_PHAM/update.sql",
        params={"runday": runday_str},
    )

    update_stg_sp_task = PostgresOperator(
        task_id=f"update_stg_product",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/DIM_PRODUCT/D_SAN_PHAM/update.sql",
        params={"runday": runday_str},
    )

    load_dwh_sp_task = PostgresOperator(
        task_id=f"load_to_dwh_product",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/DIM_PRODUCT/D_SAN_PHAM/load_to_dwh.sql",
        params={"runday": runday_str},
    )

    update_stg_gia_task = PostgresOperator(
        task_id=f"update_stg_gia",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/DIM_PRODUCT/D_GIA_BAN_VAT/update.sql",
        params={"runday": runday_str},
    )
    
    load_dwh_price_task = PostgresOperator(
        task_id=f"load_to_dwh_price",
        postgres_conn_id="postgres_conn",
        sql=f"/sql/DIM_PRODUCT/D_GIA_BAN_VAT/load_to_dwh.sql",
        params={"runday": runday_str},
    )

    end = DummyOperator(task_id='end', dag=dag)

    last_task >> update_cay_nhom_san_pham_task >> update_stg_sp_task >> load_dwh_sp_task >> update_stg_gia_task >> load_dwh_price_task >> end
    # last_task >> end
