### Set Variable **GOOGLE_APPLICATION_CREDENTIALS**
```shell
export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"
```

## Google Cloud Storage
### Create a Cloud Storage bucket
```shell
export BUCKET=$DEVSHELL_PROJECT_ID
gsutil mb gs://$BUCKET
```

## Google Bigquery
### Create a BigQuery dataset
```shell
export PROJECT="$(gcloud config get-value project)"
export DATASET="dataflow_samples"
export TABLE="streaming_beam_sql"

bq mk --dataset "$PROJECT:$DATASET"
```