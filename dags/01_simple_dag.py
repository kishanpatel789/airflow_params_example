import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from src.python_runner import process_parameters, run_python_file, first_function

extra_packages = "{{ dag_run.conf.get('extra_packages') }}"

def debug_xcom_value(ti):
    xcom_value = ti.xcom_pull(task_ids='process_parameters_py', key='final_packages')
    print(f"XCom Value: {xcom_value}")
    print(f"XCom Type: {type(xcom_value)}")


with DAG(
    dag_id="01_simple_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["level:lame", "usability:confusing"],
    render_template_as_native_obj=True,
    params={},
) as dag:

    # first_function_py = PythonOperator(
    #     task_id="first_function_py",
    #     python_callable=first_function,
    #     op_args=[
    #         "{{task_instance.xcom_pull(task_ids='process_parameters_py', key='final_packages')}}",
    #     ],
    # )

    process_parameters_py = PythonOperator(
        task_id="process_parameters_py",
        python_callable=process_parameters,
        op_args=[
            "{{ dag_run.conf.get('python_file_path', '') }}",
            "{{ dag_run.conf.get('extra_packages', '') }}",
        ],
    )



    debug_task = PythonOperator(
        task_id='debug_xcom_value',
        python_callable=debug_xcom_value,
    )


    run_python_file_py = PythonVirtualenvOperator(
        task_id="run_python_file_py",
        python_callable=run_python_file,
        requirements="{{task_instance.xcom_pull(task_ids='process_parameters_py', key='final_packages_str')}}",
        # serializer="dill",
        python_version="3.12",
        system_site_packages=False,
        op_args=[
            "{{ dag_run.conf['python_file_path'] }}",
            "{{task_instance.xcom_pull(task_ids='process_parameters_py', key='final_packages')}}",
        ],
        venv_cache_path='/home/airflow/venv-cache',
        # use_dill=True,
    )

    process_parameters_py >> debug_task >> run_python_file_py

