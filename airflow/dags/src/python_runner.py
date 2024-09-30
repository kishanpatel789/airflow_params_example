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

    # print target and actual packages installed
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
            handler = logging.StreamHandler()
            # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            # handler.setFormatter(formatter)
            logger.addHandler(handler)

    logger.info(f"final_packages {final_packages}")


    packages = sorted(
        [f"{p.name}=={p.version}" for p in importlib.metadata.distributions()]
    )
    logger.info("Here are the packages currently installed: ")
    logger.info("\n    ".join(packages))

    # set up boto3 s3 client

    # get python file content

    # validate python file syntax

    # copy python file to local worker

    # run python file


def upload_file_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    Modified from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(str(file_name), bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True
