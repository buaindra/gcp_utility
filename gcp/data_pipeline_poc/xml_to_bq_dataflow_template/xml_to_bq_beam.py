"""
python3 xml_to_bq_beam.py  \
--xml_path gs://coherent-coder-346704/xml_data/sample_xml.xml  \
--project coherent-coder-346704  \
--region us-central1  \
--temp_location gs://coherent-coder-346704/temp

gs://coherent-coder-346704/test1/yob1880.txt
gs://coherent-coder-346704/xml_data/sample_xml.xml

pip install xmltodict
"""

# importing required python packages/modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions
import argparse
import xmltodict
import logging


class XML_Parser(beam.DoFn):
    def process(self, element):
        import xmltodict
        
        yield f"{element = }"



# fetching runtime parameter from flex template 
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--xml_path',
            dest='xmlpath',
            type=str,
            required=True,
            help='Provide the gcs location of single xml file path')


# main entry function to run dataflow pipeline
def run(argv=None):
    # pipeline option
    pipeline_options = PipelineOptions()
    custom_option = pipeline_options.view_as(CustomOptions)

    # beam pipeline parser argument
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--xml_path',
        dest='xmlpath',
        type=str,
        required=True,
        help='Provide the gcs location of single xml file path',
        default=str(custom_option.xmlpath)
    )

    # pipeline option specifying
    beam_args, _ = parser.parse_known_args(argv)

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = job_name
    pipeline_options.view_as(StandardOptions).runner = "DirectRunner"
    #pipeline_options.view_as(SetupOptions).setup_file = "./setup.py"
    pipeline_options.view_as(SetupOptions).save_main_session = True

    #debug
    print(f"{beam_args.xmlpath = }")

    # call beam pipeline 
    pipeline = beam.Pipeline(options=pipeline_options)
    xml_file_data = ( 
        pipeline | "Read XML File" >> beam.io.textio.ReadFromText(beam_args.xmlpath)
                 | "XML Parsing" >> beam.ParDo(XML_Parser())           
                 | "Print XML Content" >> beam.Map(print)
    )

    # execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == "__main__":
    job_name = "xml_to_bq_beam_job"
    #logging.getLogger().setLevel(logging.INFO)
    run()










