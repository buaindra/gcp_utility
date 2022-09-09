




License: Free to use

import apache_beam as beam

from apache_beam.options.pipeline options import PipelineOptions, SetupOptions

from apache_beam.options.pipeline_options import Standardoptions, GoogleCloudOptions, Workeroptions

import argparse

import json

import logging

from datetime import datetime

My Dmm

ERVIets Will

**** Pipeline Description:

Templated pipeline to read json output as response from rest api,

apply a python UDF to it, and write it to Bigquery.

**** Pipeline has 3 required parameters, which need to be passed while executing:

1. input_api endpoint provide a api full url (sample: https://zoo-animal-api.herokuapp.com/animals/rand/5)

2. output bq table: provide bigquery sink details as Project_id: Dataset_id.Table

3. secret id api key provide secret details as PROJECT_ID: SECRET_NAME: SECRET_ID if the api requires key else

provide None

**Note: SECRET NAME is the "api key name" to authenticate the api

and SECRET ID is the ID created inside seceret manager to keep the "api key value" as secret.

Sample Public apis

#Sample public api without api-key

1. https://cataas.com/api/cats?tags=cute 2. https://zoo-animal-api.herokuapp.com/animals/rand/5

3. https://api.ipify.org?format=json

#Sample public api with api-key

1. https://api.ebird.org/v2/data/obs/CA/recent (documentation: https://documenter.getpostman.com/view/664302/SIENwy59)

田

@

<pre>

Enable the services

gcloud services enable dataflow.googleapis.com gcloud services enable secretmanager.googleapis.com

# Install the python modules ( same has been provided inside requirement.txt **)

python3 m pip install requests pip install google-cloud-secret-manager

first export below variables, with your own values And test from execute 1 to execute 3, that beam pipeline is working on both as a directrunner and dataflow

runner

export PROJECT="$(gcloud config get-value project)"

export REGION="europe-west 3" export DATASET="beam dataset"

export TABLE="animal api json_table" export BUCKET_NAME="$(PROJECT) temp"

export PIPELINE FOLDER gs://$(BUCKET NAME)/dataflow/pipelines/api-to-biqquery-json-flex

export SECRET_NAME "X-eBirdApiToken"

export SECRET_ID="dataflow-api-key"

I execute 1 (directrunner)

export TABLE="animal_api_json_table"
python api to bigquery json flex.py\ --input_api_endpoint "https://zoo-animal-api.herokuapp.com/animals/rand/5"

secret_id_api_key None

--project $1 PROJECT)\

output bq table "$PROJECT:SDATASET.STABLE" --region europe-west3 staging location (PIPELINE_FOLDER)/staging

temp_location (PIPELINE FOLDER)/temp

+ execute 2 (directrunner)

export TABLE="ebird_api_json_table"

python api to bigquery_json_flex.py\

Input_api_endpoint "https://api.ebird.org/v2/data/obs/CA/recent"\ output bq table "$PROJECT: $DATASET.STABLE" secret id api key "$PROJECT: SSECRET NAME: $SECRET ID" V project ${PROJECT} \ -service account_email test-sa@$(PROJECT). Lam.gserviceaccount.com

--region europe-west3\

temp_location $PIPELINE FOLDER)/temp:\

staging location $(PIPELINE FOLDER)/staging

#Please enable the setup.py option, as dataflow runner requires it to install the required python packages. #Note: Flex template uses requirements.txt and dataflow runner without template, uses setup.py #execute 3 (dataflowrunner, with only private ip enabled worker) Export TABLE="test_api_json_table"

python api to bigquery json flex.py\

input_api_endpoint "https://api.ipify.org?format=json" --output bq table "SPROJECT:$DATASET.STABLE"

--secret_id_api_key "None" --project ${PROJECT} \

--region europe-west3\

-temp_location $(PIPELINE FOLDER)/temp\

staging location (PIPELINE FOLDER)/staging V

--service account email test-sa@$(PROJECT).iam.gserviceaccount.com

--subnetwork regions/europe-west3/subnetworks/test-vpc-subnet-sappi-dataflow

no_use_public_ips

runner DataflowRunner

# Create dataflow flex template step by step

Orive

#contig the cloud build

export PROJECT="$(gcloud config get-value project)" gcloud config set builds/use_kaniko True

# build the template image and store into container registry # Also change the working directory to flex template tiles export TEMPLATE IMAGE-"ger.lo/SPROJECT/dataflow/api to Biqquery json flex:latest" gcloud builds submittag STEMPLATE_IMAGE

P107 # build dataflow template from container registry image, and store it to gen bucket export BUCKET NAME="SPROJECT)-temp" Pet export PIPELINE FOLDER=gs://${BUCKET_NAME)/dataflow/pipelines/apt-to-biqquery-ison-fiex export TEMPLATE PATH "$(PIPELINE_FOLDER)/template/api_to_bigquery_json_flexedmon" gcloud beta dataflow flex-template build STEMPLATE_PATH

image "STEMPLATE IMAGE" --sdk-language "PYTHON"

--metadata file "metadata.json"

flex execute 1 variable setup

export JOB NAME "animal-api-to-bigquery-json-flex-$(date YmH&MSS)" export INPUT API="https://zoo-animal-api.herokuapp.com/animals/rand/5" export BO TABLE="$(PROJECT):beam dataset animal_api_json_table" export SECRET_KEY="None"

# flex execute 2 variable setup

export JOB NAME="ebird-api-to-bigquery-json-flex-$(date +\X\m\MSS)" export INPUT API="https://api.ebird.org/v2/data/obs/CA/recent" export BQ TABLE="$(PROJECT):beam dataset.ebird_api_son_table" export SECRET KEY="PROJECT:SSECRET_NAME: $SECRET_ID"

#flex execute 3 variable setup

export JOB NAME="endpoint-api-to-bigquery-json-flex-5 (date YSMS)" export INPUT API="https://api.ipity.org?format=json" export BQ TABLE="$(PROJECT):beam_dataset.endpoint_api_json_table" export SECRET_KEY="None"

+ geloud command to call the flex template export REGION="europe-west3"

gcloud bera dataflow flex-template run SJOB NAME) A

gcloud beta dataflow flex-template run $(JOB_NAME) A

region $REGION

--template-file-ges-location (TEMPLATE PATH)

-service-account-email test-sa@s (PROJECT). Lam.gserviceaccount.com --parameters "input_api_endpoint $(INPUT API), output_bq_table=$(BO_TABLE), secret_id_api_key=$(SECRET_KEY]

+ Cleaning process:

# delete the container image

gcloud container images delete 9TEMPLATE IMAGE --force-delete-tags

#i delete the template.json file (also you can delete the whole bucket) gautil rm STEMPLATE_PATH

#defining the custom options for runtime parameter

class CustomOptions (PipelineOptions):

Use add value provider argument for arguments to be templatable

Use add argument as usual for non-templatable arguments

@classmethod

def add argparse_args(cls, parser):

parser.add value provider_argument ( --output bq table'.

type str.

required-True, help 'Biqquery Table for storing output results')

parser.add value provider argument(

Input_api_endpoint", required-True,

type-str

help='Provide api endpoint base url')

parser.add_value_provider argument ( --secret_id_api_key",

type=str, required=True,

help='Provide Secret ID, stored api key for authentication, project_id:secret_name:secrect_id or
 None

python ParDo Class to call rest api elans Call Api (beam. DoFn):

def init__(self, base_url, method="GET", api_key=None, version_id="latest"): self.headers = (

"Content-Type": "application/json", "Accept": "application/json"

self.payload= ()

self.method = method self.base_url - base_url

self.api_key = api_key

self.version_id = version_id

def process (self, element): import requests

from google.cloud import secretmanager import logging

#print("self.api_key (self.api_key)")

if (self.api_key.get() and self.api_key.get().split(":")[0].upper() != "NONE"): if (self.api_key and self.api_key.split(":")[0].upper () "NONE") self.project_id self.ap! key.split(":")[0] self.api_key_name = self.api_key.split(":") [1] self.secret id= self.api_key.split(":") (2)

self.client secretmanager.SecretManagerServiceClient()

self.name = "projects/(self.project_id)/secrets/(self.secret_id)/versions/(self.version_id" self.response = self.client.access_secret_version (name=self.name)

self.api key_val = self.response.payload.data.decode('UTF-8') self.headers (self.api key_name] = self.api_key_val

response = requests.request(

self.method, self.base url, headers self.headers, data selt.payload

cows = response.json()

if type (rows) == list: for row in rows:
if type(row) dict: yield row

else:

yield ("sample_col": "data not able to parse to json") elif type(rows) == dict:

logging.info("call_api_details: (rows)")

yield rows

else:

yield ("sample_col": "No Data Found or data not able to parse to json")

Beam ParDo transformation class Add Custom Field (beam. DoFn): def init__(self, api base url): self.api base url = api base_url

def process (self, element):

#logging.info(" (element.get('id', 'None Object)) has been processing..")

If type (element) == dict:

element ["API_URL"] = self.api base_url

yield element

def run (argv=None):

options PipelineOptions()

#custom options options.view as (CustomOptions)

parser argparse. ArgumentParser()) parser.add argument( output bq table", help "Bigquery Table for storing output results as: " "PROJECT: DATASET. TABLE or DATASET. TABLE" #default str(custom options.output_bq_table)

parser.add_argument (

input_api_endpoint",

help="Provide api endpoint base url" #default-str(custom options.input_api_endpoint)

parser.add argument (
parser.add argument( senret id api key",

help-"Provide Secret ID, atored api key for authentication, project_idisecret_name:secrect_id" #default=str(custom options.secret id_api_key)

args, beam args = parser.parse_known_args()

Preparing the Pipeline Options

google cloud options options.view as (GoogleCloudoptions) google_cloud_options.job_name = "api-to-bigquery-json-flex-"datetime.now().strftime("%Y-\m-3d-\H÷3M-557)

options.view as (Workeroptions).num workers = 1 options, view as (WorkerOptions).max_num_workers = 3

toptions.view as (SetupOptions).setup_file="./setup.py" options, view as (SetupOptions).save main_session= True

cur timestamp - datetime.now().strftime("y-im-d-SH-AM-15")

PCollection and pipeline

pipeline beam. Pipeline (options-options)

message json (pipeline

| "Dummy Data">> beam.Create(["Single API") "Call Rest API" >> beam. ParDo (Call Api(

base url-args.input_api_endpoint. method-"GET",

api key args.secret id _api_key))

| "Adding Custom Field" >> beam. Parto (Add Custom Fieldlarga input api endpoint}}

"Adding timestamp">> beam.Map(lambda x

"timestamp": cur timestamp

"Loaded into BO" >> bean.io.WriteToBigQuery(

args.output bq table, schema "SCHEMA AUTODETECT", create disposition-beam.io.BigQueryDisposition.CREATE IF NEEDED. write disposition-beam.io.BigQueryDisposition.WRITE APPEND

result pipeline.run()
# result.wait_until_finish()

if name main ": log_name 'api to bigquery_json_flex' logging.getLogger (log_name).setLevel (logging. INFO) logging.info (f"Beam pipeline started, log name: [log_name}") run ()