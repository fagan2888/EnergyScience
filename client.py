import requests
import pandas as pd
from utilities import get_building_id, get_location_id, get_lat_lng, starting_date, time_delta, year

class TemperatureClient(object):
    def __init__(self, url):
        self.base_url = url
        self.token = '0b42d5bd80493103378180d6526c6ef2'
        self.cache = {}

    def get_data(self, path, params=None):
        url = self.base_url+'/'+self.token+'/'+path
        response = requests.get(url)
        return response.json()

    def cache_data(self, key, value):
        self.cache[key] = value

    def get_cache(self):
        return self.cache

    def parse_data(self, response, props):
        for prop in props:
            if self.cache.get(prop, ''):
                self.cache.get(prop).append(response.get('daily').get('data')[0].get(prop))
                continue
            self.cache[prop]=[response.get('daily').get('data')[0].get(prop)]

if __name__ == '__main__':
    temperature_client = TemperatureClient('https://api.darksky.net/forecast')
    properties = ['time', 'sunriseTime', 'sunsetTime', 'temperatureHigh', 'dewPoint', 'humidity', 'windSpeed', 'cloudCover']
    start_date = starting_date()
    end_date = start_date + year()
    # locations = ['USA_AK_FAIRBANKS', 'USA_CA_LOS_ANGELES', 'USA_IL_CHICAGO-OHARE', 'USA_MN_MINNEAPOLIS', 'USA_TX_HOUSTON', 'USA_WA_SEATTLE']
    # locations = ['USA_AK_FAIRBANKS','USA_CA_LOS_ANGELES', 'USA_IL_CHICAGO-OHARE', 'USA_MN_MINNEAPOLIS', 'USA_TX_HOUSTON', 'USA_WA_SEATTLE']
    locations = ['USA_NV_LAS_VEGAS', 'USA_CA_SAN_FRANCISCO', 'USA_AZ_PHOENIX', 'USA_GA_ATLANTA', 'USA_MD_BALTIMORE', 'USA_CO_BOULDER']
    for location in locations:
        temperature_client.cache = {}
        coordinates = get_lat_lng(location)
        current_date = start_date
        while current_date <= end_date:
            response = temperature_client.get_data(coordinates+','+str(current_date))
            temperature_client.parse_data(response, properties)
            current_date = current_date + time_delta()
        data = temperature_client.get_cache()
        temperature_data = pd.DataFrame.from_dict(data)
        temperature_data.to_csv('data/'+location+'_temperature_usage.csv')
