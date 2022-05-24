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
1. Build a Docker container image.
2. Create and run a Dataflow Flex Template.