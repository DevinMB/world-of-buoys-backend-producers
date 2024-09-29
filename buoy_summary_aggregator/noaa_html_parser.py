from html.parser import HTMLParser
import requests

class NOAAParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.station_files = {}
    
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr_name, attr_value in attrs:
                if attr_name == 'href' and '.' in attr_value:
                    station_id, file_type = attr_value.split('.')[:2]
                    
                    if station_id not in self.station_files:
                        self.station_files[station_id] = []
                    
                    self.station_files[station_id].append(file_type)

    def fetch_directory_file_list():
        directory_url = "https://www.ndbc.noaa.gov/data/realtime2/"
        try:
            response = requests.get(directory_url)
            response.raise_for_status()
            parser = NOAAParser()
            parser.feed(response.text)
            return parser.station_files

        except Exception as e:
            print(f"Error fetching directory file list: {e}")
            return {}
