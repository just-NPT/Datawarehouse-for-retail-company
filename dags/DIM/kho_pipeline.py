import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from DIM_pull.transfer_to_stg import src_to_stg
from airflow.models import Variable


with DAG(
    dag_id="dim_kho_tasks",
    start_date=datetime(2024, 1, 11),
    schedule_interval=None,
) as dag:
    # tables = ['D_KHO', 'D_NGUON_GOC_SP', 'D_THUONG_HIEU', 'D_NGANH_HANG', 
    #           'D_NHOM_HANG', 'D_NHOM_SAN_PHAM', 'D_PHAN_LOAI_NGANH_HANG', 'D_TRANG_THAI_SP']
    
    start = DummyOperator(task_id='start', dag=dag)

    random.seed(10)
    tables = ['D_TRANG_THAI_KHO','D_KHU_VUC','D_KHO']
    runday = datetime.strptime(Variable.get('runday'),'%Y/%m/%d')

    def create_task(table, version, runday):
        with TaskGroup(group_id=f"dim_raw_to_stg_{table}") as kho_task:
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
        return kho_task
    
    def load(tables, version):
        task_arr = []
        for table in tables:
            dim_task = create_task(table, version='master_data', runday=runday)
            task_arr.append(dim_task)
            
        version >> [task_arr[0],task_arr[1],task_arr[2]] 
        end_version = DummyOperator(task_id='end_version', dag=dag)
        [task_arr[0],task_arr[1],task_arr[2]]  >> end_version

        return end_version

    tasks = []
    last_task = load(tables, start)
    levels = [0,1,2,3]
    for level in levels[:]:
        update_stg_sp_task = PostgresOperator(
            task_id=f"update_stg_kho_{level}",
            postgres_conn_id="postgres_conn",
            sql=f"/sql/DIM_KHO/update.sql",
            params={
                "runday": Variable.get('runday'),
                "level" : level
                    }
        )

        load_dwh_task = PostgresOperator(
            task_id=f"load_to_dwh_kho_{level}",
            postgres_conn_id="postgres_conn",
            sql=f"/sql/DIM_KHO/load_to_dwh.sql",
            params={
                "runday": runday,
                "level" : level
                }
        )
        tasks.append(update_stg_sp_task)
        tasks.append(load_dwh_task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
    
    end = DummyOperator(task_id='end', dag=dag)
    update_dwh_task = PostgresOperator(
        task_id="update_dwh_task",
        postgres_conn_id="postgres_conn",
        sql = """
            update dwh.D_KHO
                set kho_cha_bi_id = source.kho_cha_bi_id
                from (
                    select a.ma_kho,b.kho_bi_id as kho_cha_bi_id
                    from dwh.d_kho a
                    left join dwh.d_kho b on a.ma_kho_cha = b.ma_kho 
                    where a.kho_cha_bi_id is null and a.ma_kho_cha is not null
                ) as source
                where dwh.D_KHO.ma_kho = source.ma_kho;
        """
    )
    last_task >> tasks[0]
    tasks[-1] >> update_dwh_task >> end
    # last_task >> update_stg_sp_task  >> end
    # last_task >> end