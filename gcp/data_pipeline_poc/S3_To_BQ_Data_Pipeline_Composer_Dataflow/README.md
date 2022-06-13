# GCP-POC-DataPipeline
1. Using **Composer**, migrate the data from AWS S3 or Azure Blob Storage to Google Cloud Storage and then move to datastore
2. Using **Pubsub** for passing Barcode (Fact data)
3. Using **Datastore** for Product Catalog (Product Dimentions)
4. Using **Dataflow**, join data between Pubsub (streaming) and Cloud Datastore (Static Data) and do some transformation and load into Bigquery
5. Use **BigQuery** for data warehouse
---

### Step1: CLone the code from GIT Repo
	git clone git@github.com:buaindra/GCP-POC-DataPipeline.git
	cd GCP-POC-DataPipeline
	git pull origin main


### Step2: Execute the Onetime Startup Script
	bash bash ~/GCP-POC-DataPipeline/bash_scripts/onetime_startup_script.sh

### Step3: Setup AWS connection in Composer/Airflow env 
#### 1. On Airflow UI, go to Admin > Connections
#### 2. Create/Edit a new connection (aws_default) with the following attributes:
	Conn Id: aws_default
	Conn Type: amazon web services
	Extra: {"aws_access_key_id":"_your_aws_access_key_id_", "aws_secret_access_key": "_your_aws_secret_access_key_"}
	Leave all the other fields (Host, Schema, Login) blank.
### Step3. Click on newly created Composer Env from GCP Console -> PYPI PACKAGES -> Edit -> Add below packages
	boto3, ==1.18.65
	apache-airflow-providers-amazon, -
	apache-airflow-backport-providers-google, - (**not required)
	apache-airflow-backport-providers-sftp, - (**not required)
	apache-airflow-backport-providers-ssh, - (**not required)

### Step4: Create Python Virtual Environment and install the Apache Beam SDKs
	python3 -m virtualenv env
	source env/bin/activate
---
	pip install -m 'apache-beam[gcp]'

### Step5: Run the Beam Pipeline Locally for testing
	python3 gcs_to_bq_beam_batch.py --runner DirectRunner --project $DEVSHELL_PROJECT_ID --temp_location gs://poc01-330806/temp --staging_location gs://poc01-330806/output --region $REGION --job_name indianstock

### Step6: Then run the pipeline in dataflow runner
	python3 -m gcs_to_bq_beam_batch.py \
	--region $REGION \
	--job_name indianstock
	--runner DataflowRunner \
	--project $DEVSHELL_PROJECT_ID \
	--temp_location gs://poc01-330806/temp \
    --staging_location gs://poc01-330806/output

### Step7: Verify the output in Bigquery Table
    SELECT * FROM `indranil-24011994-04.poc_composer_dataflow.df_stock_details` LIMIT 1000

### Extras-
    #Dag file move to composer
    gcloud composer environments storage dags import --environment bq-composer-env --source ~/GCP-POC-DataPipeline/python_scripts/composer_dag/s3_to_gcs_bq_dag.py --location us-central1

    #Beam file move to GCS
    gsutil cp ~/GCP-POC-DataPipeline/python_scripts/beam_pipeline/gcs_to_bq_beam_batch.py gs://indranil-24011994-04/

    #Run the Beam from Cloudshell
    python3 -m gcs_to_bq_beam_batch.py \
    --region us-central1 \
    --job_name indianstock \
    --runner DataflowRunner \
    --project $DEVSHELL_PROJECT_ID \
    --temp_location gs://indranil-24011994-04/temp \
    --staging_location gs://indranil-24011994-04/output
---
    cp ~/GCP-POC-DataPipeline/python_scripts/beam_pipeline/gcs_to_bq_beam_batch.py ./

    python3 gcs_to_bq_beam_batch.py \
    --region us-central1 \
    --job_name indianstock \
    --runner DirectRunner \
    --project $DEVSHELL_PROJECT_ID \
    --temp_location gs://indranil-24011994-04/temp  



