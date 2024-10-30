import unittest
from unittest.mock import patch, MagicMock, Mock

from datetime import datetime
from typing import Dict, Optional

from pytz import timezone
from pytz.tzinfo import BaseTzInfo


import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.utils.datetime_management import DatetimeManagement


class TestDateTimeManagement(unittest.TestCase):
    
    def setUp(self):
        
        #configuracion de variables
        self.TIMEZONE_DEFAULT = "America/Bogota"
        self.TIMESTAMP_FORMAT = "%Y%m%d%H%M%S.%f"
        self.DATE_FORMAT = "%d/%m/%Y"
        self.TIME_FORMAT = "%I:%M %p"
        
        self.mock_datetime_management = Mock(DatetimeManagement)
        
    def test_get_datetime_succes(self):
        
        time_zone = self.TIMEZONE_DEFAULT
        
        response = self.mock_datetime_management.get_datetime(time_zone)
        
        self.assertIsNotNone(response)
        
    def test_convert_string_to_date_succes(self):
        
        date_str = '20240802100340'
        date_format = self.TIMESTAMP_FORMAT
        
        response = self.mock_datetime_management.convert_string_to_date(date_str, date_format)  
        self.assertIsNotNone(response)  
        
    def test_convert_date_to_string_succes(self):
        
        date_now = datetime.now
        date =  date_now
        date_format = self.TIMESTAMP_FORMAT
        
        response = self.mock_datetime_management.convert_string_to_date(date, date_format)
        
        self.assertIsNotNone(response)
         
        

if __name__ == '__main__':
    unittest.main()        