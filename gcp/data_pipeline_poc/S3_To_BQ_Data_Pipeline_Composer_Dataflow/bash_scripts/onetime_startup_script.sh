#!/bin/bash
source ~/GCP-POC-DataPipeline/bash_scripts/config.conf

# Environment Variable
export PROJECT_ID="$PROJECTID" \
export BILLING_ACCOUNT_ID=01F748-D68B6C-7BFEF3 \
export SERVICE_ACCOUNT_ID=sa-composer-dataflow \
export REGION="$CON_REGION" \
export GCS_BUCKET_01="$PROJECTID" \
export PUBSUB_TOPIC=pubsub-topic-poc-01 \
export PUBSUB_SUBSCRIPTION_01=pubsub-subscription-poc-01 \
export BIGQUERY_DATASET="$CON_BIGQUERY_DATASET" \
export BIGQUERY_TABLE_01="$CON_BIGQUERY_TABLE_Dataflow" \
export BIGQUERY_TABLE_02="$CON_BIGQUERY_TABLE_Composer" \
export COMPOSER_ENV_NAME=bq-composer-env \
export COMPOSER_IMAGE_VERSION=composer-1.17.7-airflow-2.1.4 \
export DAG_SOURCE_PATH=~/GCP-POC-DataPipeline/python_scripts/composer_dag/s3_to_gcs_bq_dag.py

echo "Variable Setup Done"

# Create the Project
echo "New Project creation started..@ $(date)"
gcloud projects create $PROJECT_ID --name 'Composer Dataflow POC'

# Enable billing for newly created project
echo "Billing account going to be assigned to the project..@ $(date)"
gcloud alpha billing projects link $PROJECT_ID --billing-account $BILLING_ACCOUNT_ID

# Set to the newly created Project
echo "Set the project @ $(date)"
gcloud config set project $PROJECT_ID

# Enable the API
echo "Enabling the APIs..@ $(date)"
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable composer.googleapis.com


# create the service account
echo "Service Account is going to be created..@ $(date)"
gcloud iam service-accounts create $SERVICE_ACCOUNT_ID \
--description='service account for POC' \
--display-name='sa-composer-dataflow'


# assign the role to that specific service account
echo "role is going to be assigned to that service account..@ $(date)"
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:$SERVICE_ACCOUNT_ID@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/editor"


# Create GCS Bucket
echo "GCS Bucket is going to be created..@ $(date)"
gsutil mb -c standard -b off -l $REGION gs://$GCS_BUCKET_01

sleep 10s
touch a.txt
gsutil cp a.txt gs://$GCS_BUCKET_01/input/
gsutil cp a.txt gs://$GCS_BUCKET_01/output/
gsutil cp a.txt gs://$GCS_BUCKET_01/temp/
gsutil cp a.txt gs://$GCS_BUCKET_01/stage/

#gsutil rm gs://$GCS_BUCKET_01/input/a.txt
#gsutil rm gs://$GCS_BUCKET_01/output/a.txt
#gsutil rm gs://$GCS_BUCKET_01/temp/a.txt
#gsutil rm gs://$GCS_BUCKET_01/stage/a.txt
gsutil cp ~/GCP-POC-DataPipeline/python_scripts/beam_pipeline/gcs_to_bq_beam_batch.py gs://$GCS_BUCKET_01/

# Create Pub/Sub Topic
#echo "pubsub topic is going to be created..@ $(date)"
#gcloud pubsub topics create $PUBSUB_TOPIC --project $PROJECT_ID


# Bigquery Dataset Creation
echo "Bigquery Dataset is going to be created..@ $(date)"
bq mk --location $REGION --description 'for demo poc' --dataset $PROJECT_ID:$BIGQUERY_DATASET

# Bigquery Table Creation
echo "Bigquery Table is going to be created..@ $(date)"
bq mk --location $REGION --table $PROJECT_ID:$BIGQUERY_DATASET.$BIGQUERY_TABLE_01
sleep 5s
bq mk --location $REGION --table $PROJECT_ID:$BIGQUERY_DATASET.$BIGQUERY_TABLE_02

#Create/Setup Composer Env 
#--env-variables GCP_PROJECTID=$PROJECT_ID,GCP_BUCKET=$GCP_BUCKET,AWS_BUCKET=$AWS_BUCKET,AWS_FILE_PREFIX=$AWS_FILE_PREFIX,BQ_DATASET=$BQ_DATASET,BQ_TABLE=$BQ_TABLE \
echo "Composer env is going to be created @ $(date)"
gcloud composer environments create $COMPOSER_ENV_NAME \
--location $REGION \
--image-version $COMPOSER_IMAGE_VERSION

#Move the DAG to the Composer Env
echo "Dag file is going to be moved to the composer environment @ $(date)"
#gcloud composer environments storage dags import --environment bq-composer-env --source dags/Composer_DAG_01.py --location us-central1
gcloud composer environments storage dags import --environment $COMPOSER_ENV_NAME --source $DAG_SOURCE_PATH --location $REGION

