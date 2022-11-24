"""
License: Free to use
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, WorkerOptions
import argparse
import json
import logging
from datetime import datetime


"""
**** Pipeline Description:
* Templated pipeline to read json output as response from rest api,
* apply a python UDF to it, and write it to Bigquery.

**** Pipeline has 3 required parameters, which need to be passed while executing:
* 1. input_api endpoint provide a api full url (sample: https://zoo-animal-api.herokuapp.com/animals/rand/5)
* 2. output bq table: provide bigquery sink details as Project_id: Dataset_id.Table
* 3. secret id api key provide secret details as PROJECT_ID: SECRET_NAME: SECRET_ID if the api requires key else provide None

* Note: SECRET NAME is the "api key name" to authenticate the api
        and SECRET ID is the ID created inside seceret manager to keep the "api key value" as secret.

**** Sample Public apis
#Sample public api without api-key
1. https://cataas.com/api/cats?tags=cute 
2. https://zoo-animal-api.herokuapp.com/animals/rand/5
3. https://api.ipify.org?format=json

#Sample public api with api-key
1. https://api.ebird.org/v2/data/obs/CA/recent (documentation: https://documenter.getpostman.com/view/664302/S1ENwy59)

* <pre>
* # Enable the services
* gcloud services enable dataflow.googleapis.com 
* gcloud services enable secretmanager.googleapis.com
*

# Install the python modules ( same has been provided inside requirement.txt **)
* python3 m pip install requests 
* pip install google-cloud-secret-manager

* # first export below variables, with your own values 
* # And test from execute 1 to execute 3, that beam pipeline is working on both as a directrunner and dataflowrunner

export PROJECT="$(gcloud config get-value project)"
export REGION="europe-west3" 
export DATASET="beam_dataset"
export TABLE="animal_api_json_table" 
export BUCKET_NAME="$(PROJECT)-temp"
export PIPELINE_FOLDER=gs://${BUCKET NAME}/dataflow/pipelines/api-to-biqquery-json-flex
export SECRET_NAME="X-eBirdApiToken"
export SECRET_ID="dataflow-api-key"

# execute 1 (directrunner)
export TABLE="animal_api_json_table"

python api_to_bigquery_json_flex.py \ 
--input_api_endpoint "https://zoo-animal-api.herokuapp.com/animals/rand/5" \
--output_bq_table "$PROJECT:$DATASET.$TABLE" \
--secret_id_api_key None \
--project ${PROJECT} \
--region europe-west3 \
--temp_location ${PIPELINE FOLDER}/temp \
--staging_location ${PIPELINE_FOLDER}/staging

# execute 2 (directrunner)
export TABLE="ebird_api_json_table"

python api_to_bigquery_json_flex.py \
--input_api_endpoint "https://api.ebird.org/v2/data/obs/CA/recent" \ 
--output bq table "$PROJECT:$DATASET.STABLE" 
--secret_id_api_key "$PROJECT:$SECRET_NAME:$SECRET_ID" \
--project ${PROJECT} \ 
--region europe-west3 \
--temp_location ${PIPELINE_FOLDER}/temp \
--staging_location ${PIPELINE_FOLDER}/staging \
--service_account_email test-sa@${PROJECT}.iam.gserviceaccount.com

## Please enable the setup.py option, as dataflow runner requires it to install the required python packages. 
## Note: Flex template uses requirements.txt and dataflow runner without template, uses setup.py 

# execute 3 (dataflowrunner, with only private ip enabled worker)
export TABLE="test_api_json_table"

python api_to_bigquery_json_flex.py \
--input_api_endpoint "https://api.ipify.org?format=json" \
--output_bq_table "$PROJECT:$DATASET.$TABLE" \
--secret_id_api_key "None" 
--project ${PROJECT} \
--region europe-west3 \
--temp_location ${PIPELINE FOLDER}/temp \
--staging_location ${PIPELINE FOLDER}/staging \
--service_account_email test-sa@${PROJECT}.iam.gserviceaccount.com \
--subnetwork regions/europe-west3/subnetworks/test-vpc-subnet-sappi-dataflow \
--no_use_public_ips \
--runner DataflowRunner


# Create dataflow flex template step by step

## contig the cloud build
export PROJECT="$(gcloud config get-value project)" 
gcloud config set builds/use_kaniko True

## build the template image and store into container registry 
## Also change the working directory to flex template files 
export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow/api_to_biqquery_json_flex:latest" 
gcloud builds submit --tag $TEMPLATE_IMAGE .

## build dataflow template from container registry image, and store it to gcs bucket 
export BUCKET_NAME="${PROJECT}-temp"
export PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/apt-to-biqquery-ison-flex 
export TEMPLATE_PATH "${PIPELINE_FOLDER}/template/api_to_bigquery_json_flex.json" 
gcloud beta dataflow flex-template build $TEMPLATE_PATH \
--image "$TEMPLATE IMAGE" \
--sdk-language "PYTHON" \
--metadata-file "metadata.json"


## flex execute 1 - variable setup

export JOB_NAME="animal-api-to-bigquery-json-flex-$(date +%Y%m%H%M$S)" 
export INPUT_API="https://zoo-animal-api.herokuapp.com/animals/rand/5" 
export BQ_TABLE="${PROJECT}:beam_dataset.animal_api_json_table" 
export SECRET_KEY="None"

## flex execute 2 - variable setup

export JOB_NAME="ebird-api-to-bigquery-json-flex-$(date +%Y%m%H%M$S)" 
export INPUT API="https://api.ebird.org/v2/data/obs/CA/recent" 
export BQ_TABLE="${PROJECT}:beam_dataset.ebird_api_json_table" 
export SECRET_KEY="$PROJECT:$SECRET_NAME:$SECRET_ID"

## flex execute 3 - variable setup

export JOB_NAME="endpoint-api-to-bigquery-json-flex-$(date +%Y%m%H%M$S)" 
export INPUT API="https://api.ipity.org?format=json" 
export BQ_TABLE="${PROJECT}:beam_dataset.endpoint_api_json_table" 
export SECRET_KEY="None"

# gcloud command to call the flex template 
export REGION="europe-west3"

gcloud beta dataflow flex-template run ${JOB_NAME} \
--region=$REGION \
--template-file-gcs-location ${TEMPLATE PATH} \
--service-account-email test-sa@${PROJECT}.iam.gserviceaccount.com \
--parameters "input_api_endpoint=${INPUT API}, output_bq_table=${BQ_TABLE}, secret_id_api_key=${SECRET_KEY}"


# Cleaning process:

## delete the container image
gcloud container images delete $TEMPLATE_IMAGE --force-delete-tags

## delete the template.json file (also you can delete the whole bucket) 
gsutil rm $TEMPLATE_PATH
"""

## defining the custom options for runtime parameter

#class CustomOptions(PipelineOptions):
#
#    # Use add value provider argument for arguments to be templatable
#    # Use add argument as usual for non-templatable arguments
#
#    @classmethod
#    def _add_argparse_args(cls, parser):
#        parser.add_value_provider_argument( 
#            '--output_bq_table',
#            type=str,
#            required=True, 
#            help='Biqquery Table for storing output results')
#
#        parser.add_value_provider_argument(
#            '--input_api_endpoint', 
#            type=str,
#            required-True,
#            help='Provide api endpoint base url')
#
#        parser.add_value_provider_argument( 
#            '--secret_id_api_key',
#            type=str, 
#            required=True,
#            help='Provide Secret ID, stored api key for authentication, project_id:secret_name:secrect_id or None')


# python ParDo Class to call rest api
class Call_Api(beam.DoFn):
    def __init__(self, base_url, method="GET", api_key=None, version_id="latest"): 
        self.headers = {
            "Content-Type": "application/json", 
            "Accept": "application/json"
        }

        self.payload = {}
        self.method = method 
        self.base_url = base_url
        self.api_key = api_key
        self.version_id = version_id

    def process(self, element): 
        import requests
        from google.cloud import secretmanager 
        import logging

        # print(f"self.api_key {self.api_key}")

        # if (self.api_key.get() and self.api_key.get().split(":")[0].upper() != "NONE"): 
        if (self.api_key and self.api_key.split(":")[0].upper() != "NONE"):
            self.project_id = self.api_key.split(":")[0] 
            self.api_key_name = self.api_key.split(":")[1] 
            self.secret_id= self.api_key.split(":")[2]

            self.client = secretmanager.SecretManagerServiceClient()
            self.name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/{self.version_id}" 
            self.response = self.client.access_secret_version(name=self.name)
            self.api_key_val = self.response.payload.data.decode('UTF-8') 
            self.headers (self.api key_name] = self.api_key_val
            
        response = requests.request(
            self.method, self.base_url, headers=self.headers, data=self.payload
        )

        rows = response.json()

        if type(rows) == list: 
            for row in rows:
                if type(row) == dict: 
                    yield row
                else:
                    yield {"sample_col": "data not able to parse to json"} 
        elif type(rows) == dict:
            logging.info("call_api_details: (rows)")
            yield rows
        else:
            yield {"sample_col": "No Data Found or data not able to parse to json"}


#Beam ParDo transformation 
class Add_Custom_Field(beam.DoFn): 
    def init__(self, api_base_url): 
        self.api_base_url = api_base_url

    def process(self, element):
        #logging.info(f"{element.get('id', 'None Object)} has been processing..")
        if type(element) == dict:
            element ["API_URL"] = self.api_base_url
        
        yield element


def run(argv=None):
    options=PipelineOptions()
    #custom_options = options.view_as(CustomOptions)

    parser = argparse.ArgumentParser() 
    parser.add_argument(
        "--output_bq_table", 
        help="Bigquery Table for storing output results as: " 
        "PROJECT: DATASET. TABLE or DATASET. TABLE" 
        #default=str(custom_options.output_bq_table)
    )

    parser.add_argument(
        "--input_api_endpoint",
        help="Provide api endpoint base url" 
        #default=str(custom_options.input_api_endpoint)
    )
    
    parser.add_argument(
        "--secret_id_api_key",
        help="Provide Secret ID, atored api key for authentication, project_idisecret_name:secrect_id" 
        #default=str(custom_options.secret_id_api_key)
    )

    args, beam args = parser.parse_known_args()

    # Preparing the Pipeline Options

    google_cloud_options = options.view_as(GoogleCloudoptions) 
    google_cloud_options.job_name = "api-to-bigquery-json-flex-" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    options.view_as(Workeroptions).num_workers = 1 
    options.view_as(WorkerOptions).max_num_workers = 3

    #options.view_as(SetupOptions).setup_file="./setup.py" 
    options.view_as(SetupOptions).save_main_session = True

    cur_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # PCollection and pipeline
    pipeline = beam.Pipeline(options=options)

    message_json = (pipeline | "Dummy Data" >> beam.Create(["Single API"]) 
                             | "Call Rest API" >> beam.ParDo(Call_Api(
                                                    base_url=args.input_api_endpoint,
                                                    method="GET",
                                                    api_key=args.secret_id _api_key))
                             | "Adding Custom Field" >> beam.ParDo(Add_Custom_Field(args.input_api_endpoint))
                             | "Adding timestamp" >> beam.Map(lambda x: {
                                                                **x,
                                                                "timestamp": cur_timestamp
                                                                }
                                                            )
                             | "Loaded into BO" >> bean.io.WriteToBigQuery(
                                                                args.output_bq_table, 
                                                                schema="SCHEMA_AUTODETECT", 
                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                            )
                    )
                    
    result = pipeline.run()
    # result.wait_until_finish()

if __name__ ==  "__main__": 
    log_name = 'api to bigquery_json_flex' 
    logging.getLogger(log_name).setLevel(logging.INFO) 
    logging.info(f"Beam pipeline started, log name: [log_name}") 
    run()