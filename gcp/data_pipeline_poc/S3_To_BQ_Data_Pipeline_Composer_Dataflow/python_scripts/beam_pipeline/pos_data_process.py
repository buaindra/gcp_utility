
# Created By: Indranil Pal

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

v_runner = "DirectRunner"
input_file = "gs://poc01-330806/input/bquxjob_5e14218b_17e28a4c242.csv"
output_file = "gs://poc01-330806/output/output.txt"
v_temp_location= "gs://poc01-330806/temp"
v_project = "indranil-24011994-03export"
dataflow_job_name = "poc-job-01"

beam_options = PipelineOptions(
    runner=v_runner,
    project=v_project,
    job_name=dataflow_job_name,
    temp_location=v_temp_location,
)

#pipeline object creation
with beam.Pipeline(options = beam_options) as pipeline:
    pass



