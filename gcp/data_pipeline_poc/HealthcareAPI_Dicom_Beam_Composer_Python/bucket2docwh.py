import os
import csv
import json
from io import StringIO

from dotenv import load_dotenv
from google.cloud import storage

from libs.document_warehouse import upload_files
from libs.nlp_lib import call_healthcare_nlp, get_column_names

'''
please populate .env file with
GOOGLE_APPLICATION_CREDENTIALS="<service_key.json>"
GCP_PROJECT="<project_id>"
PROJECT_NUMBER=""
LOCATION="us"
'''

load_dotenv("dev.env")
project_id = os.environ.get("GCP_PROJECT")

def create_json(bucket_name: str, file_name:str) -> bool:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data_text = blob.download_as_text().strip()
    file_ = os.path.splitext(os.path.split(file_name)[-1])[0]
    nlp_list = get_column_names(file_)

    try:
        header, data_text = data_text.split("\r\n", 1)
        header = header.split(",")
        f = StringIO(data_text)

        data_list = csv.DictReader((line.replace("\0", "").strip()
                                    for line in f), fieldnames=header)
    except Exception as e:
        return f"error: {e}", blob.name

    count = -1
    dirpath = os.path.join("json_new", file_)
    os.makedirs(dirpath, exist_ok=False)
    file_ = os.path.join(dirpath, file_)
    for data in data_list:
        data = {key: str(data[key]).strip() for key in data}
        if count == -1:
            schema_data = {key: "string" for key in data}
            for nlp_col in nlp_list:
                schema_data[f"NLPOutput_{nlp_col}"] = "string"
            with open(f"{file_}_schema.txt", "w") as schema:
                schema.write(json.dumps(schema_data, indent=4))

        count += 1
        if count > 10:
            break
        data = {key: data[key].strip() for key in data}
        for nlp_col in nlp_list:
            medical_text = data.get(nlp_col, "")
            if not medical_text:
                data[f"NLPOutput_{nlp_col}"] = ""
            else:
                nlp_request = {"documentContent": medical_text}
                nlp_output = call_healthcare_nlp(json.dumps(nlp_request))
                data[f"NLPOutput_{nlp_col}"] = nlp_output
        with open(f"{file_}_{count}.txt", "w") as json_file:
            json_file.write(json.dumps(data, indent=4))
        return True

def download_process(bucket_list, filelist_json):
    with open(filelist_json) as fp:
        data = fp.read()
        lst = json.loads(data)["bucket_folders"]
    for bucket_name in bucket_list:
        for file_name in lst:
            create_json(bucket_name, f"{file_name}.csv")
    print("Completed Download process...")

"""  
def get_immediate_subdirectories(a_dir):
    return (name for name in os.listdir(a_dir) 
            if os.path.isdir(os.path.join(a_dir, name)))

def get_files(folder):
    print("")
    return [x for x in os.listdir(folder) if x.endswith(".txt") and not x.endswith(("_schema.txt")]

def upload_files(base_data_folder, project_number, location, endpoint_override):
    for folder in get_immediate_subdirectories(base_data_folder):
        schema_file = os.path.join(base_data_folder, folder, f"{folder}_schema.txt")
        if os.path.exists(schema_file):
            document_schema = schema_file.rsplit("_",1)[0].rsplit("/", 1)[-1]
            returned_schemas = returned_schemes_names(
                project_number, location, document_schema, key_file="")
            if not returned_schemas:
                document_schema = create_document_schema(project_number, location, 
                                                         endpoint_override, schema_file)
            else:
                document_schema = returned_schemas[0]
            file_list = [os.path.join(base_data_folder, folder, data_file) 
                         for data_file in get_files(os.path.join(base_data_folder, folder))]
            partial_create_doc = partial(create_document, project_number, document_schema, 
                                         key_file="",
                                         location=location, endpoint_override=endpoint_override)
            with Pool(5) as pool:
                pool.map(partial_create_doc, file_list)
                
def cleanup(folder):
    dirpath = Path(folder)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
        
def print_progress(count):
    ERASE_LINE = "\xlb[2k"
    CURSOR_UP_ONE = "\xlb[1A"
    sys.stdout.write(CURSOR_UP_ONE)
    sys.stdout.write(ERASE_LINE)
    print(f"processed {count} files..")
"""

if __name__ == "__main__":
    config = get_arguments("config.json")
    cleanup("json_new")
    download_process(cinfig.get("bucket_list", []),
                     config.get("filelist_json", []))
    project_number = config.get("PROJECT_NUMBER", "<>")
    location = config.get("LOCATION", "us")
    endpoint_override = ""
    upload_files("json_new", project_number, location, endpoint_override)
