# Google Cloud Dataflow 

## What is Dataflow?
1. Google Cloud Dataflow is based on open-source product Apache Beam.
2. *Apache Beam* is an open source, unified model for defining both batch and streaming data-parallel processing pipelines


## Dataflow fundamentals:
### Ref: 
	1. https://cloud.google.com/dataflow/docs/concepts/beam-programming-model
### Topic:
1. Pipeline:
2. PCollections:
3. PTransform


## Dataflow basic operations:
1. I/O Operations
    1. Ref: https://beam.apache.org/documentation/io/built-in/

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