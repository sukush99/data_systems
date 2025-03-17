from loguru import logger
from azure_comp.azure_sql import MainModel
import datetime
import calendar

class TimestampHandler:
    def __init__(self):
        pass


    def check_public_holiday(self, dt: datetime.datetime) -> bool:
        """
        Check if a given date is a public holiday.
        """
        import holidays
        us_holidays = holidays.US()
        if dt.date() in us_holidays:
            return True
        else:
            return False

    def timestamp_handler(self, timestamp_ms: int):
        """
        Convert a timestamp in milliseconds to a datetime object.
        timestamp_ms
        """
        timestamp_table = []
        timestamp_s = timestamp_ms / 1000 #convert to seconds
        dt = datetime.datetime.fromtimestamp(timestamp_s)

        #extract components of the timestamp
        date_str = dt.strftime("%Y-%m-%d")
        day_of_week_num = dt.weekday()
        day_of_week_str = calendar.day_name[day_of_week_num]
        month = dt.month
        year = dt.year
        quarter = (month - 1) // 3 + 1
        
        #calculate fiscal year
        if month>= 7:
            fiscal_year = year + 1
        else:
            fiscal_year = year
        
        is_weekend = day_of_week_str in ["Saturday", "Sunday"]
        is_public_holiday = self.check_public_holiday(dt)

        timestamp_table.append({
            "timestamp_ms": timestamp_ms,
            "date": date_str,
            "day_of_the_week": day_of_week_str,
            "month": month,
            "year": year,
            "quarter": quarter,
            "fiscal_year": fiscal_year,
            "is_weekend": is_weekend,
            "is_public_holiday": is_public_holiday
        })

        db = MainModel()
        logger.info(f"Inserting timestamp in database: {timestamp_ms}")
        db.insert_timestamp(timestamp=timestamp_table)


