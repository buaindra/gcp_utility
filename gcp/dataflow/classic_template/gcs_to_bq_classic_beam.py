"""
python ~/gcp_utility/gcp/dataflow/classic_template/gcs_to_bq_classic_beam.py \
    --gcs_input_file gs://coherent-coder-346704/test1/yob1880.txt \
    --bq_dataset test_dataset \
    --bq_table test_table \
    --projectid coherent-coder-346704 \
    --project coherent-coder-346704 \
    --runner DirectRunner \
    --region us-central1 \
    --staging_location gs://coherent-coder-346704/staging/ \
    --temp_location gs://coherent-coder-346704/temp/ \
    --template_location gs://coherent-coder-346704/template/gcs_to_bq_classic_beam
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage
import argparse
# import json
import logging


def get_schema(bucket_nm, blob_nm):
    storage_client = storage.Client()
    bucket = 


# convert the lines (string element) into json object
class ConvertToJson(beam.DoFn):
    def process(self, element):
        out_value_list = element.split(',')
        out_dict = dict(out_list)
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
    pipeline = beam.Pipeline(options=beam_options)
    custom_options = beam_options.view_as(CustomOptions)
    beam_options.view_as(SetupOptions).save_main_session = True

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--projectid',
        type=str,
        required=True,
        help='',
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

    static_options, _ = parser.parse_known_args(argv)
    path = static_options.projectid + ":" + static_options.bq_dataset + "." + static_options.bq_table
    

    lines = (
                pipeline | 'Read File' >> beam.io.textio.ReadFromText(static_options.gcs_input_file)
                         | 'Call Pardo' >> beam.ParDo(ConvertToJson())
                         | 'Print the lines' >> beam.Map(print)
    )

    # lines | 'Write files' >> beam.io.gcp.bigquery.WriteToBigQuery(path,
    #             # schema=Schema,
    #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    #         )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    #logging.info("Beam pipeline started, job name:")
    run()