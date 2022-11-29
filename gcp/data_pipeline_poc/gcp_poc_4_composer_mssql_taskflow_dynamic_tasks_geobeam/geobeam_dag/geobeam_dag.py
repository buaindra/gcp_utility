"""
Example pipeline that loads the NFHL (National Flood Hazard Layery Into
BigQuery.

** Ref **

1. bigquery: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
2. geobeam: https://github.com/GoogleCloudPlatform/dataflow-geobeam/tree/main/geobeam/examples 
3. geobeam: https://docplayer.net/208288685-Geobcam-release-feb-06-2021.html
4. geobeam: https://storage.googleapis.com/geobeam/docs/all.pdf?ref=morioh.com 
5. google custom docker doc: https://cloud.google.com/dataflow/docs/guides/using-custom-containers#python


** Pre-Requisite **

1. create Dockerfile
2. create requirements.txt

3. create virtual env
    >> python3 -m venv env
    >> source env/bin/activate
    >> pip install apache-beam[GCP]
    >> pip install geobeam

4. verify the gcp bucket
    >> gsutil ls -lf gs://geobeam/examples/
    >> gsutil du gs://geobeam/examples/510104_20170217.zip | wc -1

5. execure below gcloud command for cloud build where Dockertile, requirements.txt are present 
    >> #export TEMPLATE_IMAGE="gcr.io/<project_id>/geobeam"
    >> #gcloud builds submit --tag $TEMPLATE_IMAGE --timeout=3600s --machine-type=n1-highcpu-8

    >> export PROJECT=<project_id>
    >> export REPO=geobeam
    >> export TAG=latest
    >> export IMAGE_URI=gcr.io/$PROJECT/$REPO:$TAG 
    >> gcloud builds submit . --tag $IMAGE_URI

** Local build **
docker build -t gcr.io/<project_id>/geobeam 
docker push gcr.io/<project_id>/geobeam

6. execute the dataflow job

** locally tested **
python3-rametrics/dags/sample geobeam.pyx
--runner-PortableRunner A
--job_endpoint=embed\ 47 --environment type "DOCKER"

45 -environment config-"S(IMAGE_URI"

49 --temp location qs://tmp_geobean_bucket_1/

50 --service account email ee-geobeam-higquery@rametric-sbx-toc. Lam.gserviceaccount.com

51 -gas url gs://geobeam/examples/510104_20170217.zip \ 52 -layer name S FLD HAZ AR

53

dataset examples

54

tahle FLD HAS AR

55

56 python3/rametrics/dags/sample geobeam.py\

cunner DataflowRunner

--project rsmetric-sbx-toe A

59 --region us-centrall --temp-location gs://tmp_geobeam Bucket 1/

60

61 -sdk container image ger,lo/rametrio-abx-toc/geobeam V

62 --experiment use runner v2

63

service account email en-geobeam-bigquery@rametric-sbx-toclam.gserviceaccount.

A

64 --des prl gs://geobeam/examples/510104 26170217.zip

65 layer name S FLD HAZ AR

66 dataset examples \ table FLD HAZ AR V

machine type c2-standard-30)

length:
clean up ** gelaud container Amager delete gor.fo/cameraceabx-toc/quobeall --torce-delete-tagN

71 72

74

75 76

def

run (pipeline_args, known_args):

Invoked by the Beam runner

78

79

80 81

82 83

import apache beam as beam

from apache beam. io.gep.internal.clients import bigquery as beam bigquery

from apache beam.options.pipeline options import PipelineOptions, SetupOptions from apache beam.options.pipeline options import GooglecloudOptions

from geobeam. Lo import ShapefileSource from geobeam. fn import format record, make valid, filter invalid

from datetime import datetime

Ι

84

85

197

89 90

pipeline options = PipelineOptions([

--experiments, use beam bq sink,

1 + pipeline args)

03 91

pipeline options, view as (GooglecloudOptions), job_name "ged-beam-ipal-" V

+datetime.now().strftime("Y-1m-\d+3H-1M-13") pipeline options.view as (SetupOptions).save_main_session = True

95

917

with beam.Pipeline (options-pipeline options) as p:

98

99

400

101

102

"Read Shape file">> beam. 1o. Read (ShapefileSource (known_args.gus_url,

layer_name=known_args. layer name)) 'MakeValid >> beam. Map (make_valid)

FilterInvalid>> beam. Filter(filter invalid)

'FilterInvalid' >> beam. Filter (filter invalid)

I 'Format Records' >> beam.Map (format_record) I 'WriteToBigQuery' >> beam.io.WriteToBigQuery(

beam bigquery.TableReference (

datasetId=known_args.dataset, tableld-known_args.table),

method-beam. 1o. WriteToBigQuery.Method.FILE_LOADS,

schema "SCHEMA AUTODETECT",

write disposition-beam.io.BigQueryDisposition.WRITE TRUNCATE, create disposition-beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

112

113

114

115

if name

116

import logging

import argparse

logging.getLogger().setLevel (logging. INFO)

I

122

123

124

125

parser = argparse. Argument Parser()

parser.add_argument (--gcs_url')

parser.add_argument ('--dataset')

parser.add argument (--table')

parser.add_argument ('--layer name") parser.add argument('--in_epsg', type-int, default=None)

known_args, pipeline args = parser.parse_known_args()

126

127

128

129

run (pipeline args, known_args)

Ln: 52 Col 28 Pos: 1884

UTF-8

Unix (LF)

INS

length: