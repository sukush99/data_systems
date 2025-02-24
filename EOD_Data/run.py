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
            # Log before making the API call
            logger.debug(f"Making API call for company: {company} with access_key: {config.api_access_key}")
            
            # Fetch stock data
            stock_data = fetch_stock_data(
                access_key=config.api_access_key, symbol=company
            )
            
            if stock_data:
                # Log successful data fetch
                logger.info(f"Successfully fetched data for company: {company}")
            else:
                # Log warning if no data is returned
                logger.warning(f"No data returned for company: {company} from source: {data_source}")
        
        except Exception as e:
            # Log error if something goes wrong
            logger.error(f"Error fetching stock data for company: {company} from source: {data_source}. Error: {str(e)}")
            continue  # Skip to the next company in case of error
        
        # Log that the company data has been processed
        logger.info(f"Completed processing for company: {company}")

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
    logger.info("Main process finished.")
