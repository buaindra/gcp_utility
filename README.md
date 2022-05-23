# gcp_utility
This repo is to learn GCP services easily


### Ref:
1. Auth: https://cloud.google.com/storage/docs/authentication
2. GCS: https://cloud.google.com/storage/docs/listing-buckets#storage-list-buckets-python
3. Composer 
    1. Google doc for DAGs: https://cloud.google.com/composer/docs/how-to/using/writing-dags
    2. Aiflow GIT: https://github.com/apache/airflow/tree/main/airflow
    3. Airflow Official Doc: https://airflow.apache.org/docs/apache-airflow/2.2.3/dag-run.html
4. Google Python API: https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob


### Pre-requisite:
1. Enable the services
    ```shell
    gcloud services enable composer.googleapis.com \
    gcloud services enable dataflow.googleapis.com
    ```
2. Provide below roles:
    1. provide "dataflow Worker" role to the default project compute service account


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
    #sudo apt-get install -y python3-venv
    #python3 -m venv venv
    source venv/bin/activate
    ```
4. Install below packages inside the Virtual Environment:
    ```shell
    pip install google-api-python-client
    pip install google-cloud-bigquery
    pip install google-cloud-storage
    pip install apache-beam[gcp]
    pip install google-auth
    ```
