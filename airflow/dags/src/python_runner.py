import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def first_function(extra_arguments):
    print("hello world")
    print(type(extra_arguments))
    print(extra_arguments)


STANDARD_PACKAGES = ["boto3>=1.35.0", "pandas>=2.2.0"]


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
    final_packages_str = "\n".join(final_packages)

    logger.info(f"python_file_path: {python_file_path}")
    logger.info(f"extra_packages: {extra_packages}")
    logger.info(f"final_packages: {final_packages}")

    # store finalized package list
    ti.xcom_push(key="final_packages", value=final_packages)
    ti.xcom_push(key="final_packages_str", value=final_packages_str)


def run_python_file(python_file_path, final_packages):

    import importlib.metadata
    import logging
    from urllib.parse import urlparse
    import boto3
    import botocore
    from tempfile import TemporaryFile

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

    with TemporaryFile() as f:
        # download python file content
        try:
            s3_client = boto3.client("s3")
            s3_client.download_fileobj(bucket_name, object_key, f)
        except botocore.exceptions.ClientError:
            logger.error(f"Failed to download file '{object_key}'")

        # validate python file syntax
        f.seek(0)
        try:
            code = compile(f.read(), "<string>", "exec")
        except SyntaxError:
            logger.error(f"File '{object_key}' does not contain compilable code")
            raise

        # run file content
        try:
            exec(code)
        except Exception:
            logger.error("Failed to execute file content")
            raise
