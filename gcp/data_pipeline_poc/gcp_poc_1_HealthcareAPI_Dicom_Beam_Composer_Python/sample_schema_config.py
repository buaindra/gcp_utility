import os
import re
import json
import csv
import google
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
from io import StringIO
import configparser
from google.cloud.exceptions import NotFound

env = os.environ["ENV"]
config = configparser.ConfigParser()
config.read(f"/home/airflow/gcs/data/config_emr_{env.lower()}.properties")

project_id = config[env]["project_id"]
dataset_id = config[env]["dataset_id"]
bucket_name = config[env]["bucket_name"]
dest_bucket_name = config[env]["dest_bucket_name"]
dest_folder_schema = config[env]["dest_folder_schema"]
dest_folder_config = config[env]["dest_folder_config"]
file_prefix = config[env]["file_prefix"]

BLOB_CHUNK_SIZE = 3000
CARRIAGE_RETURN = "\r\n"
END_OF_LINE = "\n"
FILE_SUFFIX = ".csv"

def _read_header_data_in_chunks(blob, file_encoding, chunk_size=BLOB_CHUNK_SIZE):
    chunk_buffer = []
    position = 0
    file_processing = True
    while file_processing:
        start, end = position, position+chunk_size
        chunk = blob.download_as_string(start=start, end=end).decode(file_encoding)
        if CARRIAGE_RETURN in chunk:
            chunk_buffer.append(chunk.split(CARRIAGE_RETURN, 1)[0])
            file_processing = False
        elif END_OF_LINE in chunk:
            chunk_buffer.append(chunk.split(END_OF_LINE, 1)[0])
            file_processing = False
        elif end >= blob.size:
            chunk_buffer.append(chunk)
            file_processing = False
        else:
            chunk_buffer.append(chunk)
            position += chunk_size
    return chunk_buffer

def get_col_schema(col_name):
    json_dict = {}
    json_dict["name"] = col_name
    json_dict["type"] = "STRING"
    json_dict["mode"] = "NULLABLE"
    return json_dict

def check_table(bq_client, table_id, json_schema):
    try:
        bq_client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=json_schema["fields"])
        table = bq_client.create_table(table)
        print("table created {}".format(table.table_id))

def prepare_config():
    config_dict = {"table_list": []}
    gcs_client = storage.Client()
    bq_client = bigquery.Client()

    dest_bucket = gcs_client.bucket(dest_bucket_name)
    blobs = gcs_client.list_blobs(bucket_name, prefix=file_prefix)
    for blob in blobs:
        temp_dict = {"source_file": "", "target_table": "", "schema_uri": ""}
        if ".csv" in blob.name and "checkpoint" not in blob.name:
            header_string = _read_header_data_in_chunks(blob, "utf-8")
            headers = "".join(header_string).replace('"', '').split(',')
            json_schema = []
            json_schema = {"fields": [get_col_schema(col_name) for col_name in headers if col_name]}
            json_schema["fields"].append({"name": "hash_code", "type": "STRING", "mode": "NULLABLE"})
            json_schema["fields"].append({"name": "ingestion_date", "type": "DATETIME", "mode": "NULLABLE"})

            file_name = blob.name.split("/")[-1]
            if file_name.split(".")[1]=="csv":
                table_name = file_name.split(".")[0]
            else:
                table_name = re.sub(r"[\[\]]", "", file_name).replace(".csv","").replace(".","_")

            table_id = "{}.{}.{}".format(project_id,dataset_id,table_name)
            check_table(bq_client, table_id, json_schema)

            destination_schema_file_name = "{}/{}_schema.txt".format(dest_folder_schema, table_name)

            source_file = f"gs://{bucket_name}/{blob.name}"
            target_table = dataset_id + "." + table_name
            schema_uri = f"gs://{dest_bucket_name}/{destination_schema_file_name}"
            destination_config_file_name = f"{dest_folder_config}/table_list_config.json"

            temp_dict["source_file"] = source_file
            temp_dict["target_table"] = target_table
            temp_dict["schema_uri"] = schema_uri
            config_dict["table_list"].append(temp_dict)

            '''
            out_file = open(destination_file_name, "w")
            json.dump(json_schema, out_file, indent=2)
            out_file.close()
            '''

            upload_blob = dest_bucket.blob(destination_schema_file_name)
            upload_blob.upload_from_string(data=json.dumps(json_schema, indent=2), content_type="application/json")
    upload_blob = dest_bucket.blob(destination_config_file_name)
    upload_blob.upload_from_string(data=json.dumps(config_dict, indent=2), content_type="application/json")
    print("config file uploaded")

