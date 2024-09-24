def first_function(extra_arguments):
    print("hello world")
    print(type(extra_arguments))
    print(extra_arguments)

STANDARD_PACKAGES = ['boto3', 'pandas']

def process_parameters(python_file_path, extra_packages, ti):

    # validate python_file_path
    if not python_file_path.startswith("s3://"):
        raise ValueError(f"Parameter 'python_file_path' must start with 's3://'. Received '{python_file_path}'")
    if not python_file_path.endswith(".py"):
        raise ValueError(f"Parameter 'python_file_path' must end with '.py'. Received '{python_file_path}'")
    
    # generate final list of packages for virtualenv, removing duplicates
    seen = set(STANDARD_PACKAGES)
    final_packages = STANDARD_PACKAGES + [p for p in extra_packages if p not in seen and not seen.add(p)]

    print(f"python_file_path: {python_file_path}")
    print(f"extra_packages: {extra_packages}")
    print(f"final_packages: {final_packages}")

    # store finalized package list
    ti.xcom_push(key="final_packages", value=final_packages)