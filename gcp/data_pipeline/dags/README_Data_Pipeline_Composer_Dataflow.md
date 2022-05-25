# GCP Data Pipeline:
1. Most of the client prefers those data pipeline services from any cloud providers which are based on open source products (e.g. **Google Cloud Composer** is based on apache Airflow, **Google Cloud Dataflow** is based on apache BEAM)


## Data Pipelines:
1. Composer:

2. Dataflow:


### Process:
1. Create Composer *Note: will share the terraform script
2. install below packages into the composer environment
    ```shell
    pip install apache-airflow-providers-microsoft-azure
    ```
3. Update the airflow connection of "wasb_default" to access azure blob storage
    1. airflow webserver -> Admin -> connections -> wasb_default
4. 

### Commands:
1. gsutil cp ~/gcp_utility/gcp/gcp_data_pipeline/dags/hybrid_cloud_storage_dag.py gs://us-central1-composer-env-01-8f4755f6-bucket/dags/
