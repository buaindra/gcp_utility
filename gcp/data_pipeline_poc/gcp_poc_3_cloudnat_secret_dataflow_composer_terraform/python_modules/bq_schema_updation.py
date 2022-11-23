Required Packages

1. pip install google-cloud-bigquery

**Ref

1. https://cloud.google.com/bigquery/docs/managing-table-schemas@add columns when you overwrite or append data

from google.cloud Import bigquery

#bigquery client
client bigquery.Client())

def ba schema change(table_id):
Make an API request

table client.get_table(table_id)

I

original schema table.schema

new_schema original schema[:] # Creates a copy of the schema.
new_schema.append(bigquery.SchemaField("age", "STRING"))

table.schema new schema
table client.update_table(table, ["schema"]) # Make an API request.

if len(table-schema) len(original_schema)+ 1 = len(new_schema):

print("A new column has been added.")

print("The column has not been added.")

def

print bg schema(table_id);
Make an API request

table client.get_table(table_id)
original schema table:schema

print("schema of (table_id) table is in (original schema)")


def ges_to_bq_csv(source_gcs_uni, bq_schema, dest_table_id):
job config

bigquery.LoadJobConfig(
create disposition bigquery.CreateDisposition.CREATE_IF NEEDED,

write_disposition bigquery.WriteDisposition.WRITE_APPEND,

schema_update_options = [

bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,

bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION

schema-bq schema,
#autodetect=True,

skip leading rows=1,
source_format-bigquery.SourceFormat.CSV,

# Make an API request.

load job client. load table_from_uri(
source_gcs_uri, dest_table_id, job_config-job_config

)

# Waits for the job to complete.

load job.result()
# Make an API request.

destination_table client.get_table(dest_table_id)
print("Loaded (destination_table.num_rows) rows into (dest table_id).") I

name

main":

source Rcs_uri = "gs://sappi-toc-sbx-temp/data/csv/test_csv.csv"

bq_schema = [

bigquery.SchemaField("name", "STRING", mode "NULLABLE"),
bigquery.SchemaField("gender", "STRING", mode="NULLABLE")

source_gcs_uri 81 - "gs://sappi-toc-sbx-temp/data/csv/test_csv_e1.csv"
bq schema_e1 [

bigquery.SchemaField("name", "STRING", mode="NULLABLE"),

bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),

bigquery. SchemaField("age", "INTEGER", mode="NULLABLE"),

dest_table_id = "sappi-toc-sbx.beam_dataset.emp_table"

gcs_to_bq_csv(source_gcs_uri, bq_schema, dest_table_id)