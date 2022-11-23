"""
pip install 'cloud-sql-python-connector[pg8000]'
pip install SQLAlchemy
pip install google-cloud-storage
"""


from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
from google.cloud import storage

# initialize Connector object
connector = Connector()

# configure Cloud SQL Python Connector properties
def getconn():
    conn = connector.connect(
        "xxxxxxxx:us-central1:testpg",
        "pg8000",
        # ip_type=IPTypes.PRIVATE,  # use only private ip
        user="postgres",
        password="YOUR_PASSWORD",
        db="YOUR_DB"
    )
    return conn


def set_configfile():
    # create connection pool to re-use connections
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    pool.dialect.description_encoding = None  # solve the pg8000 version error
    
    create_stmt = sqlalchemy.text("create table if not exists config_db(table_name varchar(100), loadtype varchar(50))")  
    insert_stmt = sqlalchemy.text("insert into config_db (table_name, loadtype) values (:table_name, :loadtype)", )
    
    # query or insert into Cloud SQL database
    super_set_table = ""
    with pool.connect() as db_conn:
    
        db_conn.execute(create_stmt)
        db_conn.execute(insert_stmt, table_name="hr_table", loadtype="incremental")
        
        # query database
        result = db_conn.execute("SELECT table_name from my_table").fetchall()

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


	
		
		
		
		
		
		
		
		
		
		
