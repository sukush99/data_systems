import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
from config import config

def upload_blob(
    fileNames: str):

    

def main():
    try:
        print("Azure Blob Storage Python quickstart sample")
        blob_service_client = BlobServiceClient.from_connection_string(config.azure_storage_connection_string)

    # Quickstart code goes here

    except Exception as ex:
        print('Exception:')
        print(ex)


if __name__ == "__main__":
    main()
