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
python3 ~/dags/geobeam_dag.py \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URI}" \
--temp_location qs://tmp_geobean_bucket_1/ \
--service_account_email geobeam-bigquery@project_id.iam.gserviceaccount.com \
--gcs_url gs://geobeam/examples/510104_20170217.zip \
--layer_name S_FLD_HAZ_AR \
--dataset examples \
--table FLD_HAS_AR


python3 ~/dags/geobeam_dag.py \
--runner=DataflowRunner \
--project <project_id> \
--region us-centrall \
--temp_location qs://tmp_geobean_bucket_1/ \
--sdk container image ger,lo/rametrio-abx-toc/geobeam V
--experiment use_runner_v2
--service_account_email geobeam-bigquery@project_id.iam.gserviceaccount.com \
--gcs_url gs://geobeam/examples/510104_20170217.zip \
--layer_name S_FLD_HAZ_AR \
--dataset examples \
--table FLD_HAS_AR \
--machine_type c2-standard-30


** clean up ** 
    >> gcloud container images delete gcr.io/<project_id>/geobeam --force-delete-tags
"""

def run (pipeline_args, known_args):
    """
    Invoked by the Beam runner
    """
    
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions 
    from apache_beam.options.pipeline options import GoogleCloudOptions
    from geobeam.io import ShapefileSource 
    from geobeam.fn import format_record, make_valid, filter_invalid
    from datetime import datetime

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    pipeline_options.view_as(GoogleCloudOptions).job_name = "geobeam-ipal-" \
            + datetime.now().strftime("%Y-%m-%d-%H-%M-%S") 
    pipeline options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(ShapefileSource(known_args.gcs_url,
             layer_name=known_args.layer_name))
         | 'MakeValid' >> beam.Map(make_valid)
         | 'FilterInvalid' >> beam.Filter(filter_invalid)
         | 'FormatRecords' >> beam.Map(format_record)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))


if __name__ == '__main__':
    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url')
    parser.add_argument('--dataset')
    parser.add_argument('--table')
    parser.add_argument('--layer_name')
    parser.add_argument('--in_epsg', type=int, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)