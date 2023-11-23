import logging
import os 
from azure.storage.blob import BlobServiceClient

def delete_blob(containerName, blobName ):
    logging.warning(f"Deleting the blob after reading its content. BlobName = {blobName}")
    connectionString = os.environ["personalstoragehector_STORAGE"]
    blob_service_client = BlobServiceClient.from_connection_string(connectionString)
    container_client = blob_service_client.get_container_client(containerName)
    blob_client = container_client.get_blob_client(blobName)
    blob_client.delete_blob()