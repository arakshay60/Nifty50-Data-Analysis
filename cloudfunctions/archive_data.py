from google.cloud import storage
import functions_framework
import os


def move_parquet_file():
    # Initialize the GCS client
    client = storage.Client()

    # Define the bucket and folder names
    bucket_name = "nsestock"
    raw_folder = "raw"
    archive_folder = "archive"

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # List files in the raw folder
    blobs = bucket.list_blobs(prefix=f"{raw_folder}/")

    # Find the .parquet file in the raw folder
    parquet_file = None
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            parquet_file = blob
            break
    
    new_blob_name = f"{archive_folder}/{os.path.basename(parquet_file.name)}"

    # Copy the file to the archive folder
    new_blob = bucket.copy_blob(parquet_file, bucket, new_blob_name)

    # Delete the original file from the raw folder
    parquet_file.delete()
    return ("success",200)

@functions_framework.http
def hello_http(request):
    return move_parquet_file()
