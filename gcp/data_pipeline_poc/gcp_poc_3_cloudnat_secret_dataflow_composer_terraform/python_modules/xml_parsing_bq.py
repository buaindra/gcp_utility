# https://github.com/bxparks/bigquery-schema-generator#schemageneratordeduce_schema

import xmltodict
from bigquery_schema_generator.generate_schema import SchemaGenerator
import json

xml_parse xmltodict.parse(xml_var)
#print (f" type of xml_parse: {type(xml_parse)}")

input_data = []
input_data.append(xml_parse)
generator = SchemaGenerator(input_format='dict')

schema_map, error_logs generator.deduce_schema(input_data=input_data)
schema = generator.flatten_schema (schema_map)
#print(schema)
bq_schema = json.dumps (schema)

print(bq schema)