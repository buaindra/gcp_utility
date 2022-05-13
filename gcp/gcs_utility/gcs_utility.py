# import libraries
from google.cloud import storage

class GCS_Utility(object):

    # initialization
    def __init__(self, project_id):
        """
        Initialize the GCS client to access buckets, blobs
        Parameters:
        1. project_id (required): specify the project id from where you want to get the list 
            of buckets and blobs
        """
        self.storage_client = storage.Client(project=project_id)


    # list buckets
    def get_list_buckets(self):
        """
        Will return lists of all buckets. 
        """
        buckets = self.storage_client.list_buckets()
        buckets_list = []
        for bucket in buckets:
            buckets_list.append(bucket.name)     
        return buckets_list


    # list blobs
    def get_list_blobs(self, bucket_nm=None):
        """
        Will return lists of all blobs. 
        Parameter:
        1. bucket_nm (optional): can specify the particular bucket also to get list of blobs 
            of that buckets.
        """
        blobs_dict = {}
        if bucket_nm is None:
            buckets_list = self.get_list_buckets()
            for bucket in buckets_list:
                blobs_dict[bucket] = []
                blobs = self.storage_client.list_blobs(bucket)
                for blob in blobs:
                    blobs_dict[bucket].append(blob.name)
        else:
            blobs_dict[bucket_nm] = []
            blobs = self.storage_client.list_blobs(bucket_nm)
            for blob in blobs:
                blobs_dict[bucket_nm].append(blob.name)
        return blobs_dict


    def get_blob_metadata(self, bucket_nm, blob_nm):
        pass