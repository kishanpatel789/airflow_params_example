import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from src.python_runner import process_parameters, run_python_file

with DAG(
    dag_id="01_simple_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["level:lame", "usability:low"],
    render_template_as_native_obj=True,
    params={},
) as dag:

    process_parameters_py = PythonOperator(
        task_id="process_parameters_py",
        python_callable=process_parameters,
        op_args=[
            "{{ dag_run.conf.get('python_file_path', '') }}",
            "{{ dag_run.conf.get('extra_packages', '') }}",
            "{{ dag_run.conf.get('kw_args', {}) }}",
        ],
    )

    run_python_file_py = PythonVirtualenvOperator(
        task_id="run_python_file_py",
        python_callable=run_python_file,
        requirements="{{task_instance.xcom_pull(task_ids='process_parameters_py', key='final_packages_str')}}",
        python_version="3.12",
        system_site_packages=False,
        op_args=[
            "{{ dag_run.conf['python_file_path'] }}",
            "{{ task_instance.xcom_pull(task_ids='process_parameters_py', key='final_packages') }}",
            "{{ dag_run.conf.get('kw_args', {}) }}",
        ],
        venv_cache_path="/home/airflow/venv-cache",
    )

    process_parameters_py >> run_python_file_py
