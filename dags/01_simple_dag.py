import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="01_simple_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=[],
    params={},
) as dag:
    empty_task = EmptyOperator(
        task_id="empty_task",
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'Hello world'",
    )

    empty_task >> bash_task
