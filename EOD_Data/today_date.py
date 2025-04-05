# from datetime import datetime, timedelta
# from loguru import logger
# from dim_timestamp import TimestampHandler

# class InsertDate:
#     '''
#     This small program is used to get the timestamp in milliseconds for the start of each day.
#     It can be run to populate the dim_timestamp table for a specific period.

#     author: @sukush
#     date: 2025-03-17
#     modified: 2025-04-06 (for backfill)
#     '''
#     def __init__(self):
#         self.timestamp_handler = TimestampHandler()

#     def get_day_start_timestamp_ms(self, date_obj):
#         """Returns the timestamp in milliseconds for the start of a given date."""
#         day_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
#         logger.info(f"Processing date: {day_start.strftime('%Y-%m-%d')}")
#         day_timestamp_ms = int(day_start.timestamp() * 1000)
#         logger.info(f"Timestamp in milliseconds: {day_timestamp_ms}")
#         return day_timestamp_ms

#     def populate_timestamp_table(self, timestamp_ms):
#         """Populates the dim_timestamp table with the given timestamp."""
#         self.timestamp_handler.timestamp_handler(timestamp_ms)

#     def backfill_timestamp_data(self, num_days=365):
#         """Backfills the dim_timestamp table for the last specified number of days."""
#         end_date = datetime.now().date()
#         start_date = end_date - timedelta(days=num_days - 1)  # Subtract 1 to include the start date

#         current_date = start_date
#         while current_date <= end_date:
#             timestamp_ms = self.get_day_start_timestamp_ms(datetime.combine(current_date, datetime.min.time()))
#             self.populate_timestamp_table(timestamp_ms)
#             current_date += timedelta(days=1)
#         logger.info(f"Backfill completed for {num_days} days, from {start_date} to {end_date}.")

#     def get_today_timestamp_ms(self):
#         """Populates the dim_timestamp table for the start of today."""
#         today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
#         logger.info(f"Today's date: {today_start}")
#         today_timestamp_ms = int(today_start.timestamp() * 1000)
#         logger.info(f"Today's timestamp in milliseconds: {today_timestamp_ms}")
#         self.populate_timestamp_table(today_timestamp_ms)


# insert_date = InsertDate()
# insert_date.backfill_timestamp_data(num_days=365)



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