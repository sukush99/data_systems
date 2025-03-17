import io
import pandas as pd
from collections import defaultdict
from azure.storage.blob import BlobServiceClient
from loguru import logger
from edgar import *
from tqdm import tqdm as p_bar

def process_company(company_symbol, container_client, form="10-K", filing_date="2020-01-01:"):
    """
    Process filings for a single company and upload the resulting CSV files to Azure Blob Storage.

    For each filing, the function:
      - Groups DataFrames by the statement's name (using statement.name, if available).
      - Joins DataFrames of the same statement (across filings) using an outer join.
      - Writes each joined DataFrame as CSV into an in-memory buffer.
      - Uploads the CSV content as a blob to Azure Blob Storage with low latency settings.

    It also creates an index CSV blob that lists all generated CSV blob names.

    Parameters:
      company_symbol: String representing the company's ticker symbol (e.g. "AAPL").
      container_client: An instance of ContainerClient (from azure-storage-blob) connected to your container.
      form: Filing form type (default "10-K").
      filing_date: Filing date range (default "2020-01-01:" which means filings on/after Jan 1, 2020).

    Returns:
      A tuple containing:
        - A list of CSV blob names (one per statement type).
        - The index blob name.
    """
    logger.info(f"\nProcessing {company_symbol} filings...")
    filings = Company(company_symbol).get_filings(form=form, filing_date=filing_date)

    # Use a defaultdict to group DataFrames by statement name.
    statements_dict = defaultdict(list)
    for filing_index in p_bar(range(0, len(filings), 2), desc=f"{company_symbol}: Processing Filings"):
        try:
            xbrl_data = filings[filing_index].xbrl()
        except Exception as e:
            logger.debug(f"Error retrieving xbrl data for filing {filing_index} of {company_symbol}: {e}")
            continue

        statements = xbrl_data.statements
        for statement_index, statement in enumerate(p_bar(statements, desc=f"{company_symbol}: Processing Statements", leave=False)):
            try:
                # Convert the statement to a DataFrame.
                df = statement.get_dataframe()
                # Reset and then set the index to the 'concept' column.
                df_no_index = df.reset_index(drop=True)
                df_indexed = df_no_index.set_index('concept')

                # Use the statement name if available; otherwise default to a numbered name.
                statement_name = getattr(statement, 'name', None)
                if statement_name is None:
                    statement_name = f"statement_{statement_index}"
                else:
                    # Replace spaces with underscores for a file-friendly name.
                    statement_name = statement_name.replace(" ", "_")

                # Append the DataFrame to the list for this statement.
                statements_dict[statement_name].append(df_indexed)
            except AttributeError as e:
                logger.debug(f"AttributeError for statement {statement} in filing {filing_index}: {e}")
                continue
            except Exception as e:
                logger.debug(f"General error for statement {statement} in filing {filing_index}: {e}")
                continue

    csv_blobs = []

    # For each statement group, join the DataFrames and upload as a CSV blob.
    for statement_name, df_list in statements_dict.items():
        if df_list:
            # Start with the first DataFrame as the base.
            df_joined_outer = df_list[0].copy()
            for i in range(1, len(df_list)):
                df_joined_outer = df_joined_outer.join(
                    df_list[i],
                    how='outer',
                    lsuffix=f'_{i-1}',
                    rsuffix=f'_{i}'
                )
            # Define a blob name that includes the company symbol and statement name.
            blob_path = f"{company_symbol}/{company_symbol}_{statement_name}.csv"
            # Write the CSV content to an in-memory string buffer.
            csv_buffer = io.StringIO()
            df_joined_outer.to_csv(csv_buffer)
            csv_data = csv_buffer.getvalue()
            # Upload the CSV content to Azure Blob Storage.
            blob_client = container_client.get_blob_client(blob_path)
            # The max_concurrency parameter can help reduce latency for uploads.
            blob_client.upload_blob(csv_data, overwrite=True, max_concurrency=4)
            csv_blobs.append(blob_path)
            #logger.info(f"Uploaded blob: {blob_path}")
        else:
            logger.debug(f"No DataFrames collected for statement '{statement_name}' in {company_symbol}.")

    # Create an index CSV blob that lists all the CSV blob names.
    index_blob_name = f"{company_symbol}_Index.csv"
    index_df = pd.DataFrame({"CSV Files": csv_blobs})
    index_csv_buffer = io.StringIO()
    index_df.to_csv(index_csv_buffer, index=False)
    index_csv_data = index_csv_buffer.getvalue()
    index_blob_client = container_client.get_blob_client(index_blob_name)
    index_blob_client.upload_blob(index_csv_data, overwrite=True, max_concurrency=4)
    print(f"Uploaded index blob: {index_blob_name}\n")

    return csv_blobs, index_blob_name

def process_companies(companies, azure_storage_connection_string, container_name, form="10-K", filing_date="2020-01-01:"):
    """
    Process one or more companies and upload their CSV files to Azure Blob Storage.

    Parameters:
      companies: Either a single company symbol (string) or a list of company symbols.
      connection_string: Your Azure Storage connection string.
      container_name: The name of the container in Azure Blob Storage.
      form: Filing form type (default "10-K").
      filing_date: Filing date range (default "2020-01-01:").

    Returns:
      A dictionary mapping each company symbol to a dict containing:
         { "csv_files": list of blob names, "index_file": index blob name }
    """
    # If companies is a single string, convert it into a list.
    if isinstance(companies, str):
        companies = [companies]

    # Create a BlobServiceClient and then a ContainerClient.
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    all_indexes = {}
    for company in p_bar(companies, desc="Processing Companies"):
        csv_blobs, index_blob = process_company(company, container_client, form=form, filing_date=filing_date)
        all_indexes[company] = {"csv_files": csv_blobs, "index_file": index_blob}

    return all_indexes



def main(
    company_name: str,
    azure_storage_connection_string: str,
    container_name: str
    ):
    set_identity("sukushkunwar99@gmail.com")
    process_companies(company_name, azure_storage_connection_string, container_name)

if __name__ == "__main__":
    from config import config
    main(
        company_name = config.company_name,
        azure_storage_connection_string = config.azure_storage_connection_string,
        container_name = config.container_name
    )