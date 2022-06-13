import os
import re
import json
import csv
import argparse
import codecs
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.filesystems import FileSystems as beam_fs
from apache_beam import pvalue
from apache_beam.io import WriteToBigQuery
from google.cloud import storage
from apache_beam.coders import coders
from datetime import datetime
import hashlib
import logging
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler

def read_config(config_file_name):
    try:
        conf_file_path = config_file_name.replace("gs://", "")
        conf_bucket_name = conf_file_path.split(os.sep)[0]
        conf_file_name = conf_file_path.replace(conf_bucket_name + "/", "")
        storage_client = storage.Client()

        bucket = storage_client.bucket(conf_bucket_name)
        filedata = bucket.blob(conf_file_name)
        json_data = filedata.download_as_string()
        return json.loads(json_data)
    except Exception as e:
        logging.error(f"error {e}")

def get_schema(schema_uri):
    try:
        schema_file_path = schema_uri.replace("gs://","")
        schema_bucket_name = schema_file_path.split(os.sep)[0]
        schema_file_name = schema_file_path.replace(schema_bucket_name + "/","")
        storage_client = storage.Client()
        bucket = storage_client.bucket(schema_bucket_name)
        filedata = bucket.blob(schema_file_name)
        json_data = filedata.download_as_string()
        schema = json.loads(json_data)
        '''
        keys = []
        for item in schema["fields"]:
            for key, val in item.items():
                if key == "name" and val not in ("hash_code", "ingestion_date"):
                    keys.append(val)
        return keys, schema
        '''
        return schema
    except Exception as e:
        logging.error(f"error {e}")



class read_csv_lines(beam.DoFn):
    #OUTPUT_TAG_ERROR_RECORDS = "tag_error_records"
    #OUTPUT_TAG_SUCCESS_RECORDS = "tag_success_records"

    def process(self, element):
        # values = csv.reader([element], escapechar="\\").__next__()
        with beam_fs.open(element) as f:
            csv.field_size_limit(sys.maxsize)
            for row in csv.DictReader((line.replace("\0", "").strip()
                                      for line in codecs.iterdecode(f, "utf-8")), escapechar="\\"):
                record = dict(row)
                values = [val for key, val in record.items()]
                str2hash = "".join([str(elem) for elem in values])
                result = hashlib.md5(str2hash.encode())
                record["hash_code"] = result.hexdigest()
                record["ingestion_date"] = datetime.now().strftime("%Y-%m-%d- %H:%M:%S")
                yield record
                '''
                if row == "SUCCESS":
                    yield pvalue.TaggedOutput(self.OUTPUT_TAG_SUCCESS_RECORDS, row)
                else:
                    yield pvalue.TaggedOutput(self.OUTPUT_TAG_ERROR_RECORDS, row)
                '''

def run(argv=None, dest_mstr_tbl_id=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_name", help="",required=True)
    parser.add_argument("--bq_project_id", help="", required=True)
    parser.add_argument("--master_table_id", help="", required=True)

    args, beam_args = parser.parse_known_args(argv)
    beam_options = PipelineOptions(beam_args, streaming=False)

    setup_options = beam_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    tbl_lst_cfg = read_config(args.config_file_name)
    project_id = args.bq_project_id
    dest_master_tbl_id = args.master_table_id
    mstr_tbl_lst = []

    with beam.Pipeline(options=beam_options) as pipeline:
        for p in tbl_lst_cfg["table_list"]:
            input_file_path = p["source_file"]
            schema_uri = p["schema_uri"]
            schema = get_schema(schema_uri)
            dest_table_id = p["target_table"]
            dest_table_name = dest_table_id.split(".")[1]

            ( pipeline | "Get CSV File" + dest_table_name >> beam.Create([input_file_path])
                       | "Read CSV File" + dest_table_name >> beam.ParDo(read_csv_lines())
                       | "Write to BQ Table" + dest_table_name >> beam.io.WriteToBigQuery(
                                "{0}.{1}".format(project_id, dest_table_id),
                                schema = schema,
                                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE)
            )
            mstr_tbl_dict = {"table_name": dest_table_name, "last_loaded_date": datetime.now().strftime("%Y-%m-%d- %H:%M:%S")}
            mstr_tbl_lst.append(mstr_tbl_dict)

            '''
            records = (pipeline | "read from file" + dest_table_name >> beam.io.ReadFromText(input_file_path, 
                                    skip_header_lines=1, strip_trailing_newlines=True, coder=coders.StrUtf8Coder())
                                | "convert to BQ rows" + dest_table_name >> beam.ParDo(read_csv_lines(), 
                                    keys).with_outputs(read_csv_lines.OUTPUT_TAG_ERROR_RECORDS, 
                                                       read_csv_lines.OUTPUT_TAG_SUCCESS_RECORDS)
                       )
            success_records = records[read_csv_lines.OUTPUT_TAG_SUCCESS_RECORDS]
            error_records = records[read_csv_lines.OUTPUT_TAG_ERROR_RECORDS]
            
            (success_records | "write to bq"+dest_table_name >> beam.io.WriteToBigQuery(
                                f"{project_id}:{dest_table_id}",
                                schema = schema,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
            )
            (error_records | ....)
            '''

        dest_mstr_tbl_name = dest_master_tbl_id.split(".")[1]
        master_table = ( pipeline | "Read from Dict List" >> beam.Create(mstr_tbl_lst)
                                  | "write to BQ table "+ dest_mstr_tbl_name >> beam.io.WriteToBigQuery(
                                        "{0}.{1}".format(project_id, dest_mstr_tbl_id),
                                        schema = "table_name:STRING, last_loaded_date:DATETIME",
                                        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE)
                     )

if __name__ == "__main__":
    run()