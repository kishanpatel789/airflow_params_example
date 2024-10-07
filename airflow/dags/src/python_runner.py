import logging

logger = logging.getLogger(__name__)
STANDARD_PACKAGES = ["boto3>=1.35.0", "pandas>=2.2.0"]


def process_parameters(python_file_path: str, extra_packages: list, kw_args: dict, ti):

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
    final_packages_str = "\n".join(final_packages)

    logger.info(f"python_file_path: {python_file_path}")
    logger.info(f"extra_packages: {extra_packages}")
    logger.info(f"final_packages: {final_packages}")
    logger.info(f"kw_args: {kw_args}")

    # store finalized package list
    ti.xcom_push(key="final_packages", value=final_packages)
    ti.xcom_push(key="final_packages_str", value=final_packages_str)


def run_python_file(python_file_path, final_packages, file_kw_args):

    import logging
    import importlib
    from urllib.parse import urlparse
    import boto3
    import botocore
    from pathlib import Path
    import sys

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # handler.setFormatter(formatter)
        logger.addHandler(handler)

    # print target and actual packages installed
    logger.info(f"final_packages {final_packages}")
    packages = sorted(
        [f"{p.name}=={p.version}" for p in importlib.metadata.distributions()]
    )
    logger.info("Here are the packages currently installed: ")
    logger.info("    " + "\n    ".join(packages))

    # get python file content - s3://bucket-name/somewhere/file/path.py
    parsed_url = urlparse(python_file_path)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path.lstrip("/")
    logger.info(f"Attempting to use file '{object_key}' in bucket '{bucket_name}'")

    python_file_path = Path.cwd() / "python_file_to_run.py"
    logger.info(f"Local python file path: {python_file_path}")

    # download python file content
    try:
        s3_client = boto3.client("s3")
        with open(python_file_path, "w+b") as f:
            s3_client.download_fileobj(bucket_name, object_key, f)
            logger.info(f"File '{object_key}' acquired: ")
            f.seek(0)
            logger.info(f.read())
    except botocore.exceptions.ClientError:
        logger.error(f"Failed to download file '{object_key}'")
        raise

    # validate python file syntax
    try:
        with open(python_file_path, "rb") as f:
            code = compile(f.read(), "<string>", "exec")
            logger.info("Successfully compiled script into code object")
    except SyntaxError:
        logger.error(f"File '{object_key}' does not contain compilable code")
        raise

    # ensure python file has main() function
    sys.path.append(str(python_file_path.parent.absolute()))
    module_to_run = importlib.import_module(python_file_path.stem)
    if "main" not in dir(module_to_run):
        raise SyntaxError("File does not have a main() function defined")

    # run python file
    try:
        logger.info("Running file...")
        module_to_run.main(**file_kw_args)
    except Exception:
        logger.error("Failed to execute file content")
        raise

    # remove downloaded python file from local worker
    try:
        python_file_path.unlink()
        logger.info("Removed file from local worker")
    except FileNotFoundError:
        logger.error("Failed to remove local python file")
        pass
