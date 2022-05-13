# gcp_utility
This repo is to learn GCP services easily


### Ref:
1. Auth: https://cloud.google.com/storage/docs/authentication
2. GCS: https://cloud.google.com/storage/docs/listing-buckets#storage-list-buckets-python
3. Composer 
    1. Google doc for DAGs: https://cloud.google.com/composer/docs/how-to/using/writing-dags
    2. Aiflow GIT: https://github.com/apache/airflow/tree/main/airflow


### Pre-requisite:
1. Enable the services
    ```shell
    gcloud services enable composer.googleapis.com \
    gcloud services enable dataflow.googleapis.com
    ```

### How to start
1. First clone the code from git repo:
    ```shell
    git clone https://github.com/buaindra/gcp_utility.git
    ```
2. Change the directory:
    ```shell
    cd gcp_utility
    ```
3. Activate the python virtual environment:
    ```shell
    source venv/bin/activate
    ```
