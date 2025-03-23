import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
from azure_comp.azure_config import config
import pandas as pd


class ConnectToAzure:
    def __init__(self, fileName: str, data: dict):
        self.fileName = fileName
        self.data = data
        self.upload_blob(fileName, data)

    def upload_blob(self, fileName: str, data: dict):
        uploadable_data = self.convert_to_csv(data)
        try:
            blob_service_client = BlobServiceClient.from_connection_string(config.azure_storage_connection_string)
            blob_client = blob_service_client.get_blob_client(container=config.container_name, blob=fileName)
            blob_client = blob_service_client.get_blob_client(container=config.container_name, blob=fileName)
            blob_client.upload_blob(uploadable_data)
        except Exception as e:
            print(f"Error uploading data to Azure Blob Storage: {e}")

        
    def convert_to_csv(self, data: dict):
        df = pd.DataFrame(data)
        return df.to_csv(index=False, header=True, encoding='utf-8')

    def check_archive(self, fileName: str):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(config.azure_storage_connection_string)
            blob_client = blob_service_client.get_blob_client(container=config.archive_container_name, blob=fileName)
            if blob_client.exists():
                return True
            else:
                return False
        except Exception as e:
            print(f"Error downloading data from Azure Blob Storage: {e}")
            return False
