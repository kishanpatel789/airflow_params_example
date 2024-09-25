def first_function(extra_arguments):
    print("hello world")
    print(type(extra_arguments))
    print(extra_arguments)


STANDARD_PACKAGES = ["boto3", "pandas", "dill", "cloudpickle"]


def process_parameters(python_file_path: str, extra_packages: list, ti):

    # validate python_file_path
    if not python_file_path.startswith("s3://"):
        raise ValueError(
            f"Parameter 'python_file_path' must start with 's3://'. Received '{python_file_path}'"
        )
    if not python_file_path.endswith(".py"):
        raise ValueError(
            f"Parameter 'python_file_path' must end with '.py'. Received '{python_file_path}'"
        )

    # generate final list of packages for virtualenv, removing duplicates
    seen = set(STANDARD_PACKAGES)
    final_packages = STANDARD_PACKAGES + [
        p for p in extra_packages if p not in seen and not seen.add(p)
    ]
    final_packages_str = '\n'.join(final_packages)

    print(f"python_file_path: {python_file_path}")
    print(f"extra_packages: {extra_packages}")
    print(f"final_packages: {final_packages}")

    # store finalized package list
    ti.xcom_push(key="final_packages", value=final_packages)
    ti.xcom_push(key="final_packages_str", value=final_packages_str)


def run_python_file(python_file_path, final_packages):

    # print target and actual packages installed
    print(f"final_packages {final_packages}")

    import importlib.metadata

    packages = sorted(
        [f"{p.name}=={p.version}" for p in importlib.metadata.distributions()]
    )
    print("Here are the packages currently installed: ")
    print("\n    ".join(packages))

    # set up boto3 s3 client

    # get python file content

    # validate python file syntax

    # copy python file to local worker

    # run python file
