from loguru import logger
from config import config
from api_response import fetch_stock_data
from tqdm import tqdm as p_bar

def main(
        company_list: list,
        data_source: str,
) -> None:
    '''
    Main function to run the script,
    This is where we receive the API response data
    Args:
        company_list: list of companies to fetch data for
        data_source: source of data, live or backfill
    Returns:
        None
    '''
    # Log the start of the main function
    logger.info(f"Starting data fetch for companies: {company_list} using data source: {data_source}")
    
    # Initialize tqdm for the progress bar
    for company in p_bar(company_list, desc="Processing Companies", ncols=100):
        # Log each company being processed
        logger.info(f"Fetching stock data for company: {company} from source: {data_source}")
        
        try:
            
            # Fetch stock data
            fetch_stock_data(
                access_key=config.api_access_key, symbol=company
            )

        
        except Exception as e:
            # Log error if something goes wrong
            logger.error(f"Error fetching stock data for company: {company} from source: {data_source}. Error: {str(e)}")
            continue  # Skip to the next company in case of error
        

    # Log the completion of the main function
    logger.info(f"Completed data fetch for all companies using data source: {data_source}")


if __name__ == "__main__":
    # Set up logging configuration to log to file and console
    logger.add("logfile.log", level="INFO")  # Log to file, change level as needed
    
    # Start the main function
    logger.info("Starting the main process.")
    main(
        company_list=config.company_list,
        data_source=config.data_source,
    )

