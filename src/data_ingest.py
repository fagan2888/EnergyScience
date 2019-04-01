#
# The Data ingest module is responsible for reading from csv files
# and splitting the files by Site ID
###############################################################################
import os
import csv
import pandas as pd
import pkg_resources
import logging
import argparse
import re
from pathlib import Path
import mysql.connector

from db_functions import create_dataset_tables, insert_rows


SCRIPT_PATH = os.path.dirname(__file__)  # Script directory
HOLIDAY_READ_PATH = os.path.join(SCRIPT_PATH, '../data/raw/power-laws-forecasting-energy-consumption-holidays.csv')
WEATHER_READ_PATH = os.path.join(SCRIPT_PATH, '../data/raw/power-laws-forecasting-energy-consumption-weather.csv')
WRITE_PATH = os.path.join(SCRIPT_PATH, '../data/interim')

class DataIngest(object):
    """
    Read/Query from csv/api to generate interim files that contain a subset of
    the data
    """
    def __init__(self, logger=None):
        self.file_prefix = 'site'
        self.file_extension = 'csv'
        self.logger = logger
        self.count_interim_files = 0
        self.data_holidays = None
        self.data_weather = None
        self.connection = None

    def connect_to_database(self, schema):
        self.connection = mysql.connector.connect(host='35.227.50.121',
                                                  database=schema,
                                                  user='ebuser1',
                                                  passwd='ebuser1')

    def convert_to_date(self, date_in_some_format):
        """
        :param date_in_some_format: datetime
        """
        date_as_string = str(date_in_some_format)
        year_as_string = date_as_string.split('T')[0] # last four characters
        return year_as_string

    def generate_file_name(self, site_id, data_type):
        return "site-{}-{}.csv".format(site_id, data_type)

    def write_records_to_file(self, data, file_name):
        """
        :param data: Data by Site ID
        :param file_name: file name
        """
        write_location = "{path}/{file}".format(path=WRITE_PATH, file=file_name)
        with open(write_location, 'w+') as file:
            data.to_csv(file, sep=',', encoding='utf-8')

    def ingest(self):
        """
        Ingest from csv
        :param site_id: The Site ID
        """
        self.connect_to_database('esschema')
        create_dataset_tables(self.connection)
        self.data_holidays = pd.read_csv(HOLIDAY_READ_PATH, sep=';')
        # data_holiday_headers = ','.join(list(self.data_holidays.columns))
        data_holiday_rows = [tuple(row) for row in self.data_holidays.values]
        insert_rows(self.connection, 'esschema', 'holiday1', 'Holiday_date, Holiday, SiteId',
                    data_holiday_rows, '%s, %s, %s', logger=self.logger)
        self.data_weather = pd.read_csv(WEATHER_READ_PATH, sep=';')
        data_weather_headers = ','.join(list(self.data_weather.columns))
        self.logger.debug(data_weather_headers)
        weather_data_rows = [tuple(row) for row in self.data_weather.values]
        insert_rows(self.connection, 'esschema', 'weather1', data_weather_headers,
                    weather_data_rows, '%s, %s, %s, %s', logger=self.logger)
        self.connection.close()


    def wrangle(self):
        """
        Wrangle holiday and weather datasets
        """
        for site in range (0, 2):
            holiday_data_by_site = self.data_holidays[self.data_holidays['SiteId'] == site]
            weather_data_by_site = self.data_weather[self.data_weather['SiteId'] == site]
            self.logger.debug("Adding the date column to weather dataset")
            if not holiday_data_by_site.empty and not weather_data_by_site.empty:
                weather_data_by_site['Date'] = weather_data_by_site['Timestamp'].apply(self.convert_to_date)
                merged_dataset = pd.merge(holiday_data_by_site, weather_data_by_site,
                                          on='Date', how='outer')
                self.count_interim_files+=1
                self.logger.debug('making intertim data set from raw data')
                self.logger.debug(holiday_data_by_site)
                self.logger.debug(weather_data_by_site)
                holiday_file_name = self.generate_file_name(site, 'holiday')
                weather_file_name = self.generate_file_name(site, 'weather')
                merged_file_name = self.generate_file_name(site, 'weather-holiday')
                self.write_records_to_file(holiday_data_by_site, holiday_file_name)
                self.write_records_to_file(weather_data_by_site, weather_file_name)
                self.write_records_to_file(merged_dataset, merged_file_name)

    @property
    def sites_count(self):
        return self.count_interim_files

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)
    logger = logging.getLogger(__name__)
    data_ingest = DataIngest(logger=logger)
    data_ingest.ingest()
    #data_ingest.wrangle()
    logger.info("Sites count: {}".format(data_ingest.sites_count))
