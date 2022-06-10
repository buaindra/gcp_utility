"""
pip install 'cloud-sql-python-connector[pg8000]'
pip install SQLAlchemy
pip install google-cloud-storage
"""


from google.cloud.sql.connector import Connector
import sqlalchemy
from google.cloud import storage

# initialize Connector object
connector = Connector()

# configure Cloud SQL Python Connector properties
def getconn():
    conn = connector.connect(
        "xxxxxxxx:us-central1:testpg",
        "pg8000",
        user="postgres",
        password="YOUR_PASSWORD",
        db="YOUR_DB"
    )
    return conn

# create connection pool to re-use connections
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

# query or insert into Cloud SQL database
super_set_table = ""
with pool.connect() as db_conn:
    # query database
    result = db_conn.execute("SELECT * from my_table").fetchall()

    # Do something with the results
    for row in result:
        super_set_table = super_set_table + str(row[0]) + ", "
		
prop_file_content = f"[DEV]\ntables_name = {super_set_table[:-2]}"

with open(r"Config.properties") as f:
	# writer = csv.writer(f)
	# writer.writerow(lines)
	f.write(prop_file_content)
	
storage_client = storage.Client()
bucket = storage_client.get_bucket("bucket_name")
blob = bucket.blob("prefix/blob_name")
# blob.download_to_filename("Config.properties")
blob.upload_from_filename(Config.properties)
print("successful")


	
		
		
		
		
		
		
		
		
		
		
