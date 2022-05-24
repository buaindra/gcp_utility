"""
python gcs_to_bq_classic_beam.py \
    --gcs_input_file gs://coherent-coder-346704/test1/yob1880.txt \
    --bq_dataset test_dataset \
    --bq_table test_table \
    --projectid coherent-coder-346704 \
    --runner DataflowRunner \
    --project coherent-coder-346704 \
    --staging_location gs://coherent-coder-346704/staging \
    --temp_location gs://coherent-coder-346704/temp \
    --template_location gs://coherent-coder-346704/templates/gcs_to_bq_classic_beam \
    --region us-central1 
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions
from google.cloud import storage
import argparse
# import json
import logging


def get_schema(projectid: str, gcs_blob_path: str, file_header: bool) -> str:
    bucket_nm = gcs_blob_path.split("/")[2]
    blob_nm = "/".join([str(item) for item in gcs_blob_path.split("/")[3:]])  # list comprehension
    logging.info(f"Source bucket name: {bucket_nm} and source blob name: {blob_nm}")
    storage_client = storage.Client(project=projectid)
    bucket = storage_client.bucket(bucket_nm)
    blob = bucket.blob(blob_nm)
    content = blob.download_as_text().strip()
    header = content.split("\r\n", 1)[0]
    header_list = header.split(",")
    #print("header:", header)
    data_type = "string"
    schema = ""
    if not file_header:
        for i in range(0, len(header_list), 1):
            schema = schema + "cl_" + str(i) + f":{data_type},"
    
    schema = schema[:-1]  # string slicing
    logging.info(f"generated bigquery schema: {schema}")
    return schema


# convert the lines (string element) into dict object
class ConvertToDict(beam.DoFn):
    # def __init__(self, templated_schema):
    #   self.templated_schema = templated_schema

    def process(self, element):
        #print(element)
        out_value_list = element.split(',')
        out_key_list = [ "cl_"+str(i) for i in range(0, len(out_value_list), 1)]
        out_dict = dict(zip(out_key_list, out_value_list))
        yield out_dict


# defining the custom options for runtime parameter
class CustomOptions(PipelineOptions):
    # Use add_value_provider_argument for arguments to be templatable
    # Use add_argument as usual for non-templatable arguments
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--projectid',
            type=str,
            required=True,
            help='')
        parser.add_value_provider_argument(
            '--gcs_input_file',
            dest='gcs_input_file',
            type=str,
            required=True,
            help='The gcs file path for the input csv to process. Example: "gs://<bucket name>/ \
            <prefix names>/<blob name>"')
        parser.add_value_provider_argument(
            '--bq_dataset',
            type=str,
            required=True,
            help='Bigquery dataset name')
        parser.add_value_provider_argument(
            '--bq_table',
            type=str,
            required=True,
            help='Bigquery table name')


def run(argv=None):
    beam_options = PipelineOptions()
    custom_options = beam_options.view_as(CustomOptions)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--projectid',
        type=str,
        required=True,
        help='provide projectid',
        default=str(custom_options.projectid)     
    )
    parser.add_argument(
        '--gcs_input_file',
        type=str,
        required=True,
        help='The gcs file path for the input csv to process. Example: "gs://<bucket name>/ \
        <prefix names>/<blob name>"',
        default=str(custom_options.gcs_input_file)     
    )
    parser.add_argument(
        '--bq_dataset',
        type=str,
        required=True,
        help='Bigquery dataset name',
        default=str(custom_options.bq_dataset)     
    )
    parser.add_argument(
        '--bq_table',
        type=str,
        required=True,
        help='Bigquery table name',
        default=str(custom_options.bq_table)     
    )

    # getting pipeline arguments
    static_options, beam_args = parser.parse_known_args(argv)

    # Preparing the Pipeline Options
    google_cloud_options = beam_options.view_as(GoogleCloudOptions)
    beam_options.view_as(StandardOptions).runner = "DataflowRunner"
    google_cloud_options.project = static_options.projectid  #'coherent-coder-346704'
    #google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = job_name
    #google_cloud_options.staging_location = 'gs://coherent-coder-346704/staging/'
    #google_cloud_options.temp_location = 'gs://coherent-coder-346704/temp/'
    #google_cloud_options.template_location = 'gs://coherent-coder-346704/templates/gcs_to_bq_classic_beam'
    #beam_options.view_as(SetupOptions).setup_file = "./setup.py"
    beam_options.view_as(SetupOptions).save_main_session = True

    # generate the bigquery table
    path = static_options.projectid + ":" + static_options.bq_dataset + "." + static_options.bq_table

    # generate the bigquery table schema  
    bq_schema = get_schema(static_options.projectid, static_options.gcs_input_file, False)

    ## debug
    #print("content:",content)
    # print("custom_options:", custom_options)
    # print("beam_args:", beam_args)
    # options_out=beam_options.get_all_options()
    # print("options_out:", options_out)

    # PCollection and pipeline 
    pipeline = beam.Pipeline(options=beam_options)
    lines = (
                pipeline | 'Read File' >> beam.io.textio.ReadFromText(static_options.gcs_input_file)
                         | 'Convert Each Line to Dict' >> beam.ParDo(ConvertToDict())
                         #| 'Convert Each Line to Dict' >> beam.ParDo(ConvertToDict(static_options.templated_schema)
                         #| 'Print the lines' >> beam.Map(print)
    )

    lines | 'Write To BQ' >> beam.io.gcp.bigquery.WriteToBigQuery(path,
                schema= bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == "__main__":
    job_name = 'gcs-to-bq-beam-python'
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f"Beam pipeline started, job name:{job_name}")
    run()