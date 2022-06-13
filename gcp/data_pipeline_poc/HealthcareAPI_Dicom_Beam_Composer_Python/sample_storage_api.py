import csv
from google.cloud import storage
from io import StringIO
import os
import json

project_id = ""
storage_client = storage.Client(project=project_id)

def list_buckets():
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    bucket_list = []
    for bucket in buckets:
        bucket_list.append(bucket.name)
    return bucket_list

def get_bucket_files_count(bucket_name, prefix="", delimiter=""):
    """
    gsutil ls gs://bucket/** | wc -lcket Name
    """
    options = {"prefix": prefix, "delimiter": delimiter}
    try:
        for bkt in storage_client.list_blobs(bucket_name, **options):
            if "landing" not in bkt.name and not bkt.name.endswith("/"):
                #if bkt.size < 60000
                data = bkt.download_as_text().strip()

                try:
                    f = StringIO(data)
                    data = list(csv.reader((line.replace("\0", "").strip() for line in f)))
                except Exception as e:
                    yield f"error: {e}", bkt.name
                #else:
                #    count = 0
                #print(count, count<=1)
                #if count <= 1:
                #    empty_file = 1
                #else:
                #   empty_file = -1
                #val = str(bkt.download_as_bytes()).strip().count("\n")
                #print(f"{bkt.name} - {val}")
                #break
                #empty_file = True
            #yield empty_file, bkt.name
    except Exception as e:
        print("-"*60)
        traceback.print_exec(file=sys.stdout)
    print(f">>> Got all the list of files in {bucket_name}")

def get_bucket_files_withoutlanding(bucket_name):
    bucket = storage_client.bucket(bucket_name)
    try:
        for bkt in bucket.list_blobs():
            #does not contain landing
            if "landing" not in bkt.name and not bkt.name.endswith("/"):
                yield bkt.name
    except Exception as e:
        print(f"error: {e}")
    print(f">>> Got all the list of files in {bucket_name}")

def create_header(file_name):
    with open(file_name, "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter = ",", quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(("bucket name", "file name", "file ext"))

def create_bucket_files(bucket_name):
    #d = {"EMR System": {}, "EMR_ENM": {}}
    d = ["EMR System"]
    with open(f"csv_file_detailed.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for prefix in d:
            for flg, file_name in get_bucket_files_count(bucket_name, prefix):
                print(f"bucket_name: {prefix} - {file_name} - {flg}")
                csvwriter.writerow((prefix, file_name, flg))
    print(f"Done for {bucket_name} with data: {d}")

def list_blobs_csv(project_id, bucket_name):
    storage_client = storage.Client(project=project_id)
    blobs = storage_client.list_blobs(bucket_name)
    count = 0
    for blob in blobs:
        name = blob.name
        if name.endswith(".csv"):
            count += 1
    return count

def list_blobs(project_id, bucket_name, folder="/"):
    storage_client = storage.Client(project=project_id)
    blobs = storage_client.list_blobs(bucket_name)
    file_types = {}
    for blob in blobs:
        name = blob.name
        if not name.endswith("/"):
            if "." in name:
                _, ext = name.rsplit(".", 1)
            else:
                ext = "None"
            file_types[ext] = file_types.get(ext, 0) + 1
    return file_types

def list_blobs_01(bucket_name):
    storage_client = storage.Client()
    for folder in ("EEMR_System", "EEMR_ENM"):
        blobs = storage_client.list_blobs(bucket_name, prefix=f"collaborator_16/{folder}")

        for blob in blobs:
            if blob.name.endswith(".csv"):
                yield blob.name

def get_datatype(data):
    if isinstance(data, int):
        return "int"
    elif isinstance(data, str):
        if is_date(data):
            return "DateTime"
        count = len(data.strip().split())
        return f"str:{count}"
    return None

def process_csv(bucket_name, file_name):
    file_name = f"gs://{bucket_name}/{file_name}"
    try:
        df = pd.read_csv(file_name, nrows=10, on_bad_lines="skip")
        result = {key: None for key in df.keys()}
        for index, row in df.iterrows():
            for name, data in row.iteritems():
                data_type = get_datatype(data)
                print(result.get(name, None))
                if result.get(name, None) is None and data_type:
                    result[name] = data_type
    except Exception as e:
        return {file_name: f"error: {e}"}
    return {file_name: result}
all_data = []
for file_name in list_blobs("notebook-emr"):
    all_data.append(process_csv("notebook-emr", file_name)
with open("datatypes.txt", "w") as fp:
    fp.write(json.dumps(all_data, indent=4)



def create_json(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data_text = blob.download_as_text().strip()
    try:
        header, data_text = data_text.split("\r\n", 1)
        header = header.split(",")
        f = StringIO(data_text)
        data_list = csv.DictReader((line.replace('\0', '').strip() for line in f), fieldname=header)
    except Exception as e:
        print("error:", e)
        return f"error: {e}", blob.name
    count = -1
    file_ = os.path.splittext(os.path.split(file_name)[-1])[0]
    dirpath = os.path.join("json_new", file_)
    os.makedirs(dirpath, exist_ok=False)
    file_ = os.path.join(dirpath, file_)
    for data  in data_list:
        data = {key: str(data[key]).strip() for key in data}
        if count == -1:
            schema_data = {key: "string" for key in data}
            with open(f"{file_}_schema.txt", "w") as schema:
                schema.write(json.dumps(schema_data, indent=4))
        count += 1
        data = {key: data[key].strip() for key in data}
        with open(f"{file_}_{count}.txt", "w") as json_file:
            json_file.write(json.dumps(data, indent=4))

if __name__ == "__main__":
    bucket_list = ["notebook-emr"] #list of buckets
    with open("bucket_folders.json") as fp:
        data = fp.read()
        lst = json.loads(data)["bucket_folders"]
        print(lst)
    for bucket_name in bucket_list:
        for file_name in lst:
            create_json(bucket_name, f"collaborator_16/{file_name}.csv")

    for bucket_name in bucket_list:
        create_bucket_files(bucket_name)

    folder = ["collaborator_07"]
    for bucket in folder:
        data = list_blobs(project_name, bucket)
        for d in data:
            print(d)



