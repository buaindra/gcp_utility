# Google Cloud Dataflow 

## What is Dataflow?
1. Google Cloud Dataflow is based on open-source product Apache Beam.
2. *Apache Beam* is an open source, unified model for defining both batch and streaming data-parallel processing pipelines


## Dataflow fundamentals:
### Ref: 
1. https://cloud.google.com/dataflow/docs/concepts/beam-programming-model
2. https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline

### Beam Pipeline Overview:
1. **Pipeline:**
    > A Pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data
2. **PCollections:**
    > A PCollection represents a distributed data set that your Beam pipeline operates on.
    >
    > The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning it comes from a continuously updating source via a subscription or other mechanism.
3. **PTransform:**
    > A PTransform represents a data processing operation, or a step, in your pipeline.
    >
    > Every PTransform takes one or more PCollection objects as input, performs a processing function that you provide on the elements of that PCollection, and produces zero or more output PCollection objects.

### *A typical Beam driver program works as follows:*
1. First, create a Pipeline object 
2. Second, create initial PCollection data using I/O operation or Create transform
3. Apply PTransforms to each PCollection
4. Using I/O operation, write the output Pcollections to external sources.
5. Run the pipeline using Pipeline Runner.


## Dataflow basic operations:
1. I/O Operations
    1. Ref: https://beam.apache.org/documentation/io/built-in/
2. Pipeline Options:
    1. https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.options.pipeline_options.html?highlight=pipeline#module-apache_beam.options.pipeline_options
    


### Dataflow sample codes
1. Git:
    1. https://github.com/rishisinghal/BeamPipelineSamples


## How to execute Dataflow driver program code:
1. From local or cloud-shell
    ```shell
    python driver_program.py \
    --dataset dataset_name \
    --table table_name \
    --project project_name \
    --runner DataFlowRunner \
    --region region_name \
    --staging_location gs://bucket_name/staging \
    --temp_location gs://bucket_name/temp \
    ```

2. Programatically specify Pipeline Options:
    1. https://beam.apache.org/releases/pydoc/2.16.0/_modules/apache_beam/options/pipeline_options.html#GoogleCloudOptions

## How to create classic template for cloud dataflow?
### Ref:
1. Google Doc: https://cloud.google.com/dataflow/docs/guides/templates/creating-templates

### Steps:
1. Run below template creation script by providing "template_location" param
    ```shell
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
    ```
2. create *\<template_name\>_metadata* file and keep the file in same gcs location with the template_location
    ```shell
    gsutil cp ~/gcp_utility/gcp/dataflow/classic_template/gcs_to_bq_classic_beam_metadata gs://coherent-coder-346704/templates/
    ```
3. Execute that dataflow template using gcloud command
    ```shell
    gcloud dataflow jobs run [JOB_NAME] \
    --gcs-location gs://<template_location>
    ```

## Classic template vs Flex Template
1. The Dataflow runner does not support ValueProvider options for Pub/Sub topics and subscription parameters. If you require Pub/Sub options in your runtime parameters, switch to using Flex Templates.
2. Flex Template requires Docker files where you install all dependencies.
3. Flex is latest and supports both batch and stream.


## How to create Flex Template for cloud Dataflow?
### Ref:
1. Google Doc: 
    1. https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#python_6
2. Youtube:
    1. https://www.youtube.com/watch?v=S6rZDZdfPPY
3. Medium Blogs:
    1. https://medium.com/posh-engineering/how-to-deploy-your-apache-beam-pipeline-in-google-cloud-dataflow-3b9fe431c7bb

### Follow the below steps:
1. Create Project, Enable Billing, Create service account and key (*Check GoogleCloud_API_IAM_Learning.md)
    ```shell
    cd ~/gcp_utility/gcp/dataflow/flex_template/
    export BASE_DIR=$(pwd)
    export PROJECT_ID=coherent-coder-346704
    ```
2. Flex Template container image creation:
    1. Execute below script to speed up the subsequent builds. Kaniko caches build containers.
        ```shell
        gcloud config set builds/use_kaniko True
        ```
    2. we will create our Dockerfile
        1. **modify the line 6 and 7 in Dockerfile (copy and env variable) with our beam pipeline code.**
    3. use Cloud Build to build the container image.
        ```shell
        export TEMPLATE_IMAGE="gcr.io/$PROJECT_ID/dataflow/gcs_to_bq_flex_beam:latest"
        gcloud builds submit --tag $TEMPLATE_IMAGE .
        ```
        1. Images starting with gcr.io/PROJECT/ are saved into your project's Container Registry, where the image is accessible to other Google Cloud products. Although not shown in this tutorial, you can also store images in Artifact Registry.

    4. Then build and stage the actual template
        ```shell
        export TEMPLATE_PATH="gs://${PROJECT_ID}/templates/gcs_to_bq_flex_beam.json"
        # Will build and upload the template to GCS
        # You may need to opt-in to beta gcloud features
        gcloud beta dataflow flex-template build $TEMPLATE_PATH \
        --image "$TEMPLATE_IMAGE" \
        --sdk-language "PYTHON" \
        --metadata-file "metadata.json"
        ```
    5. call the flex template from gcloud command:
        ```shell
        export PROJECT_ID=$(gcloud config get-value project)
        export REGION='us-central1'
        export JOB_NAME=mytemplate-$(date +%Y%m%H%M$S)
        export TEMPLATE_LOC=gs://${PROJECT_ID}/templates/mytemplate.json
        export INPUT_PATH=gs://${PROJECT_ID}/events.json
        export OUTPUT_PATH=gs://${PROJECT_ID}-coldline/template_output/
        export BQ_TABLE=${PROJECT_ID}:logs.logs_filtered
        gcloud beta dataflow flex-template run ${JOB_NAME} \
        --region=$REGION \
        --template-file-gcs-location ${TEMPLATE_LOC} \
        --parameters "inputPath=${INPUT_PATH},outputPath=${OUTPUT_PATH},tableName=${BQ_TABLE}"
        ```
    6. Cleaning up 
        1. Stop the Dataflow pipeline
        2. Delete the template spec file from Cloud Storage.
            ```shell
            gsutil rm $TEMPLATE_PATH
            ```
        3. Delete the Flex Template container image from Container Registry
            ```shell
            gcloud container images delete $TEMPLATE_IMAGE --force-delete-tags
            ```