from loguru import logger
from config import config

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

if __name__ == "__main__":
    main(
        company_list=config.company_list,
        data_source=config.data_source,
    )
