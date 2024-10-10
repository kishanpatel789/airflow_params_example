# Airflow Params Example

**Objective**: This repo demonstrates how Airflow Params can be used to improve the usability of a DAG. Two Airflow DAGs used to test-run python scripts in AWS S3.

The first DAG is simple and does not provide much help to the user. The second DAG features Airflow Params and documentation, which improves usability. 

Project Start: 2024.09.19

Asssociated Blogpost: [Coming soon]

## Architecture
- Technologies Used
  - Apache Airflow
  - AWS: S3
  - Terraform
  - Docker
- Terraform is used to provision AWS infrastructure. Docker is used to run Apache Airflow locally.

## Setup Instructions
- Local Setup
  - Download or clone the repository to a local computer. 
- AWS
  - Create an [AWS account](https://aws.amazon.com/) or identify an existing account.
  - Ensure that credentials for an AWS user with full admin privileges is configured on the local computer to use the AWS CLI. This user should be associated with the `default` profile and have access keys defined in the file `~/.aws/credentials`: 
    ```toml
    [default]
    aws_access_key_id = <access-key-id>
    aws_secret_access_key = <secret-access-key>
    ```
    - These credentials will be used by Terraform when creating AWS resources and by Airflow when executing DAGs. 
- Terraform
  - In the directory `./terraform/` create a file `terraform.tfvars` with a single variable to identify the S3 bucket name: 
    ```bash
    # terraform.tfvars
    bucket_name = "<your-bucket-name-here>"
    ```
  - This bucket will host the python file to be run by the Airflow DAGs.
    - For customization, additional variables can be overridden in the module `./terraform/modules/aws/`.
  - In the directory `./terraform/`, execute the following commands to create the S3 bucket and upload the python file.
    ```bash
    terraform init
    terraform apply
    ```
- Airflow on Docker
  - The following instructions assume Docker is already installed on the local machine. 
  - In the directory `./airflow/` create additional airflow files to be used as volumes by Docker: 
    ```
    mkdir -p ./logs ./plugins ./config
    ```
  - In the directory `./airflow/` create a file `.env` by running the following commands: 
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env    
    ```
    - Docker will automatically pick up this `.env` file when building Airflow resources. 
  - Run the Airflow containers by executing following two commands in a terminal when in the directory `./airflow/`:
    ```bash
    docker compose up airflow-init
    docker compose up
    ```

## How to Run
- After completing setup described above, go to the Airflow UI at `localhost:8080`. 
- There should be two dags available. Both DAGs expect the same run configuration and execute the same tasks:
  - **01_simple_dag**: Provides no documentation or Params.  
  - **02_better_dag**: Provides DAG documentation and Params UI to receive run configuration.
