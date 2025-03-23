from transform import PrepareTransformData
from loguru import logger
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io


def main(file_list: list,
        company_list: list,
        container_name: str,
        connection_string: str):

        for company in company_list:
            for file in file_list:
                logger.info(f"Processing {company} {file}")
                search_file = f"{company}_{file}.csv"
                logger.info(f"Searching for {search_file} in {container_name}")

                #download file from azure blob storage
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                blob_full_name = f"{company}/{search_file}"
                logger.debug(f"Blob full name: {blob_full_name}")
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_full_name)

                # Download the blob data as bytes
                downloaded_blob = blob_client.download_blob()
                blob_data = downloaded_blob.readall()

                # Decode the bytes to a string
                blob_string = blob_data.decode('utf-8')

                # Use io.StringIO to treat the string as a file-like object
                csv_file = io.StringIO(blob_string)

                # Read the CSV data into a pandas DataFrame
                df = pd.read_csv(csv_file)
                
                try:
                    PrepareTransformData(df, company, file)
                except Exception as e:
                    logger.error(f"This isn't balance sheet data")
                    logger.error(e)

if __name__ == "__main__":
    from config import config
    main(
        file_list=config.file_list,
        company_list=config.company_list,
        container_name=config.search_container_name,
        connection_string=config.connection_string
    )
