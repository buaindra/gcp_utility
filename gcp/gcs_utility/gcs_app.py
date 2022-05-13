from gcs_utility import GCS_Utility

project_id = input("enter the project no:")

# initiate the GCS_Utility object
gcs_obj = GCS_Utility(project_id=project_id)

# get list of buckets
list_buckets = gcs_obj.get_list_buckets()
print(f"{list_buckets = }")

# get list of blobs
list_blobs = gcs_obj.get_list_blobs()
print(f"{list_blobs = }")

