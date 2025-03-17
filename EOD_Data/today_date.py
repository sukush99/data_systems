from datetime import datetime
from loguru import logger
from dim_timestamp import TimestampHandler

class InsertDate:
    '''
    This small program is used to get the timestamp in milliseconds for the start of today.
    Every day it run to populates the dim_timestamp table.

    author: @sukush
    date: 2025-03-17
    '''
    def __init__(self):
        pass

    def get_today_timestamp_ms(self):
        """Returns the timestamp in milliseconds for the start of today."""
        timestamp_handler = TimestampHandler()
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"Today's date: {today_start}")
        today_timestamp_ms = int(today_start.timestamp() * 1000)
        logger.info(f"Today's timestamp in milliseconds: {today_timestamp_ms}")
        timestamp_handler.timestamp_handler(today_timestamp_ms)
    
insert_date = InsertDate()
insert_date.get_today_timestamp_ms()