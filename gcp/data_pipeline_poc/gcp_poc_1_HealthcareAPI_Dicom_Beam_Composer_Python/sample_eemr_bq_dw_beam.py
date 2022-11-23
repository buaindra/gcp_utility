
same as sample_eemr_gcs_bq_beam.py

project_number = config[env]["project_number"]
location =
bq_project =
bq_dataset =
nlp_col_table =
nlp_col_dataset =
master_table =

master_table_query = f"Select table_name from {bq_project}.{bq_dataset}.{master_table}"
def get_table_list(query):
    bq_client = bigquery.Client(project=bq_project)
    results = bq_client.query(query).result()
    tbl_lst = []
    for row in results:
        tbl_lst.append(row.table_name)
        return tbl_lst

def hash_code_mapping_table_merge(bq_client, table_name):
    for table_name in table_list:
        merge_query = ""
        bq_client.query(merge_query)

def mapTable(element):
    return (element["table_name"], element["column_name"], )

class ConvertToJsonText(beam.DoFn):
    def process(self, element, table_name, nlp_col_dict):
        from packages import nlp_lib
        nlp_columns = nlp_col_dict.get(table_name,"")
        for nlp_col in nlp_columns:
            medical_text = element.get(nlp_col,"")
            if not medical_text:
                element[f"NLPOutput_{nlp_col}"] = {}
            else:
                element[f"NLPOutput_{nlp_col}"] = nlp_lib.call_healthcare_nlp(medical_text)
        schema_element = {key: "string" for key, value in element.items()}
        schema_dict_element = json.dumps(dict(element), indent=2, default=str)
        yield {"schema_element": schema_dict_element, "data_element": dict_element}

class ImportToContentWarehouse(beam.DoFn):
    def process(self, element, table_name):
        from packages import doc_lib
        import time

        doc_name = table_name + str(time.time()) + ".txt"
        doc_json_text = str(element["data_element"])
        schema_json_text = str(element["schema_element"])

        documemt_schema = doc_lib.return_schemes_names(
            project_number=project_number, location=location, display_name=table_name
        )

        if not documemt_schema:
            document_schema = doc_lib.upload_document_schema(
                project_number=project_number, location=location, schema_name=table_name,
                schema_json_text=schema_json_text, endpoint_override=""
                )
            doc_lib.return_shemes_names.cache_clear()
        else:
            document_schema = documemt_schema[0]
        doc_lib.upload_document(
            project_number=project_number, location=location, endpoint_override="",
            document_schema=document_schema,
            doc_name=doc_name, doc_json_text=doc_json_text
        )

class getHashCode(beam.DoFn):
    def process(self, element):
        import logging
        yield {"hash_code": element["hash_code"], "ingestion_date": element["ingestion_date"]}

def run(argv=None):


    with beam.Pipeline(options=pipeline_options) as pipeline:
        bq_client = bigquery.Client()
        for table_name in table_list:
            query_create_hash_table = "create table if not exists `{}.{}.{}_hash_code_mapping`\
                                      (hash_code string, ingestion_date datetime)".format(bq_project, bq_dataset, bq_table)
            bq_client.query(query_create_hash_table)
            nlp_col_query = f"select table_name, column_name FROM `{bq_project}.{nlp_col_dataset}.{nlp_col_table}` where is_active='Y' and table_name='{table_name}'"
            nlp_cololumns = (
                pipeline | "read from Bigquery"+table_name >> beam.io.ReadFromBigQuery(
                            query=nlp_col_query,
                            use_standard_sql=True)
                         | "Get NLP Columns"+table_name >> beam.Map(mapTable)
                         | "group by Table"+table_name >> beam.GroupByKey()
                         | "Convert to Tuple"+table_name >> beam.MapTuple(lambda k, v: (k, list(v)))
            )
            delta_query = f"select * from `{bq_project}.{bq_dataset}.{table_name}_hash_code_mapping` as mt \
                            WHERE NOT EXISTS (SELECT 1 FROM `{bq_project}.{bq_dataset}.{table_name}_hash_code_mapping` as ht \
                             where mt.hash_code = ht.hash_code)"
            result = pipeline | "Read from Bigquery" >> beam.io.ReadFromBigQuery(query=delta_query, use_standard_sql=True)
            (result | beam.ParDo(ConvertToJsonText(), table_name, beam.pvalue.AsDict(nlp_cololumns))
                    | beam.ParDo(ImportToContentWarehouse(), table_name)
             )
            (result | beam.ParDo(getHashCode())
                    | beam.io.WriteToBigQuery(...)
             )

if __name__ == "__main__":
    table_list = get_table_list(master_table_query)
    run()
