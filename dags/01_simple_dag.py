import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from src.python_runner import first_function, process_parameters

extra_packages = "{{ dag_run.conf.get('extra_packages') }}"

with DAG(
    dag_id="01_simple_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["level:lame", "usability:confusing"],
    render_template_as_native_obj=True,
    params={},
) as dag:
    empty_task = EmptyOperator(
        task_id="empty_task",
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="pip freeze"
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=process_parameters,
        op_args=[
            "{{ dag_run.conf['python_file_path'] }}",
            "{{ dag_run.conf['extra_packages'] }}",
            ]
    )

    empty_task >> bash_task >> python_task
