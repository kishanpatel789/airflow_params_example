"""
Scatch work to test python logic
"""

# %%
import logging
from urllib.parse import urlparse
import boto3
import botocore
import tempfile
from tempfile import TemporaryFile
from pathlib import Path

# %%
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# %%
python_file_path = "s3://airflow-params-example-jk7scjn6f65i/python_file_to_run.py"
parsed_url = urlparse(python_file_path)
bucket_name = parsed_url.netloc
object_key = parsed_url.path.lstrip("/")
logger.info(f"Attempting to use file '{object_key}' in bucket '{bucket_name}'")

# %%
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
        logger.error(f"File does not contain compilable code.")
        raise

    # run file content
    try:
        exec(code)
    except Exception:
        logger.error("Failed to execute file content.")
        raise


# %%
from airflow.operators.python import PythonVirtualenvOperator
print(PythonVirtualenvOperator.template_fields)
# %%
