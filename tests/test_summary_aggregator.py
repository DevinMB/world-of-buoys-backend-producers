import warnings
from urllib3.exceptions import NotOpenSSLWarning
import unittest
from unittest.mock import patch
import fakeredis
from buoy_summary_aggregator.summary_aggregator import SummaryAggregator


warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

class TestSummaryAggregator(unittest.TestCase):
    
    def setUp(self):
        self.redis_conn = fakeredis.FakeStrictRedis()
        self.aggregator = SummaryAggregator(self.redis_conn)

    @patch('requests.get')
    def test_fetch_and_store_buoy_data(self, mock_get):
        
        mock_get.return_value.text = (
            "13001|PR|Atlas Buoy|PM-595|NE Extension||12.000 N 23.000 W|| |\n"
            "13002|PR|Atlas Buoy||NE Extension||21.000 N 23.000 W|| |\n"
        )

        self.aggregator.fetch_and_store_buoy_data()
        
        buoy_ids = self.redis_conn.smembers(SummaryAggregator.STATION_SET_KEY)
        buoy_ids = {buoy_id.decode('utf-8') for buoy_id in buoy_ids}
        self.assertIn("13001", buoy_ids)
        self.assertIn("13002", buoy_ids)
        
        buoy_13001_summary = {k.decode('utf-8'): v.decode('utf-8') for k, v in self.redis_conn.hgetall("buoy:13001:summary").items()}
        buoy_13002_summary = {k.decode('utf-8'): v.decode('utf-8') for k, v in self.redis_conn.hgetall("buoy:13002:summary").items()}

        self.assertEqual(buoy_13001_summary, {
            "owner": "PR",
            "ttype": "Atlas Buoy",
            "hull": "PM-595",
            "name": "NE Extension",
            "payload": "",
            "location": "12.000 N 23.000 W",
            "timezone": "",
            "forecast": "",
            "note": ""
        })

        self.assertEqual(buoy_13002_summary, {
            "owner": "PR",
            "ttype": "Atlas Buoy",
            "hull": "",
            "name": "NE Extension",
            "payload": "",
            "location": "21.000 N 23.000 W",
            "timezone": "",
            "forecast": "",
            "note": ""
        })

if __name__ == "__main__":
    unittest.main()
