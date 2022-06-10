# XML to BQ Beam

### Ref: 
1. https://kontext.tech/article/705/load-xml-file-into-bigquery
2. https://medium.com/google-cloud/how-to-load-xml-data-into-bigquery-using-python-dataflow-fd1580e4af48

## How to test the Pipeline
1. Clone the repo from git
2. Create Virtual Env, Activate it and install necessary packages
    1. install apache_beam[gcp]
2. Goto the working folder:
    ```shell
    cd ~/gcp_utility/gcp/data_pipeline_poc/xml_to_bq_dataflow_template/
    ```
3. Execute the python beam code from cloud-shell:
    ```shell
    python3 xml_to_bq_beam.py  \
    --xml_path gs://coherent-coder-346704/xml_data/sample_xml.xml  \
    --projectid coherent-coder-346704  \
    --region us-central1  \
    --temp_location gs://coherent-coder-346704/temp
    ```