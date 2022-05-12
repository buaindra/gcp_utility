# GCS Utility

### Ref:
1. Auth: https://cloud.google.com/storage/docs/authentication
2. GCS: https://cloud.google.com/storage/docs/listing-buckets#storage-list-buckets-python
3. Composer DAGs: https://cloud.google.com/composer/docs/how-to/using/writing-dags


### Pre-requisite:
1. Install VENV and Activate it
    ```shell
    python -m venv venv
    source venv/bin/activate
    ```
2. Install Required Packages:
    ```shell
    pip install google-cloud-storage
    ```

### Sample Code:
1. Print Bucket Lists and Blob lists
    ```shell
    python gcs_app.py
    ```