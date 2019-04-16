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
TRAINING_READ_PATH = os.path.join(SCRIPT_PATH, '../data/raw/power-laws-forecasting-energy-consumption-training-data.csv')
# WEATHER_READ_PATH = os.path.join(SCRIPT_PATH, '../data/interim/site-1-weather.csv')
WRITE_PATH = os.path.join(SCRIPT_PATH, '../data/interim/')

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
        self.training_data = None
        self.dataset = None
        self.filenames = []
        self.connection = None
        self.holiday_codes = {'Christmas Day': 0, 'Columbus Day': 1,
                              'Fraternal Day': 1, 'American Indian Heritage Day': 1,
                              'Veterans Day': 2, 'Confederate Memorial Day': 2,
                              'Memorial Day': 3, 'Labor Day': 4, 'George Washington': 5,
                              "Washington's Birthday": 5, 'Christmas Eve (Observed)': 13,
                              'Thomas Jefferson Birthday': 5, 'Robert E. Lee': 6,
                              'Martin Luther King Birthday': 6, 'Independence Day': 7,
                              'Thanksgiving Day': 8, 'Jefferson Davis Birthday': 9,
                              'Christmas Day (Observed)': 10, 'New year': 11,
                              'Independence Day (Observed)': 12, 'Election Day': 14,
                              'New Years Eve (Observed)': 15, 'New Years Eve': 16,
                              "Birthday of Martin Luther King, Jr.": 6,
                              'New Years Eve Shift': 17, 'Christmas Eve shift': 18,
                              'Mardi Gras': 19, 'Good Friday': 20}

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

    def get_holiday_code(self, holiday):
        """
        :param holiday:
        """
        return self.holiday_codes.get(holiday, -1)

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
        #self.connect_to_database('esschema')
        #create_dataset_tables(self.connection)
        self.data_holidays = pd.read_csv(HOLIDAY_READ_PATH, sep=';')
        # data_holiday_headers = ','.join(list(self.data_holidays.columns))
        data_holiday_rows = [tuple(row) for row in self.data_holidays.values]
        #insert_rows(self.connection, 'esschema', 'holiday1', 'Holiday_date, Holiday, SiteId',
        #            data_holiday_rows, '%s, %s, %s', logger=self.logger)
        self.data_weather = pd.read_csv(WEATHER_READ_PATH, sep=';')
        #data_weather_headers = ','.join(list(self.data_weather.columns))
        weather_data_rows = [tuple(row) for row in self.data_weather.values]
        self.training_data = pd.read_csv(TRAINING_READ_PATH, sep=";")
        training_data_rows = [tuple(row) for row in self.training_data.values]
        # self.logger.debug(weather_data_rows)
        #insert_rows(self.connection, 'esschema', 'weather1', 'Timestamp,Temperature,Distance,SiteId',
        #            weather_data_rows, '%s, %s, %s, %s', logger=self.logger)
        #self.connection.close()


    def wrangle(self):
        """
        Wrangle holiday and weather datasets
        """
        for site in range (0, 350):
            holiday_data_by_site = self.data_holidays[self.data_holidays['SiteId'] == site]
            weather_data_by_site = self.data_weather[self.data_weather['SiteId'] == site]
            training_data_by_site = self.training_data[self.training_data['SiteId'] == site]
            if not holiday_data_by_site.empty and not weather_data_by_site.empty and not training_data_by_site.empty:
                self.logger.info("Wrangling Site ID {}".format(site))
                weather_data_by_site['Date'] = weather_data_by_site['Timestamp'].apply(self.convert_to_date)
                training_data_by_site['Date'] = training_data_by_site['Timestamp']
                merged_dataset = pd.merge(holiday_data_by_site, weather_data_by_site,
                                          on='Date', how='outer')
                self.dataset = pd.merge(merged_dataset, training_data_by_site,
                                        on='Date', how='outer')
                self.dataset = self.dataset[pd.isna(self.dataset['Value']) == False]
                self.dataset['HolidayCode'] = self.dataset['Holiday'].apply(self.get_holiday_code)
                holiday_file_name = self.generate_file_name(site, 'holiday')
                weather_file_name = self.generate_file_name(site, 'weather')
                training_file_name = self.generate_file_name(site, 'training')
                merged_file_name = self.generate_file_name(site, 'training-weather-holiday')
                self.write_records_to_file(holiday_data_by_site, holiday_file_name)
                self.write_records_to_file(weather_data_by_site, weather_file_name)
                self.write_records_to_file(training_data_by_site, training_file_name)
                self.write_records_to_file(self.dataset, merged_file_name)

    def merge_csvs(self):
        """
        Merge all CSVs
        """
        site_ids  = []
        for site in range(0, 350):
            filename = self.generate_file_name(site, 'training-weather-holiday')
            if os.path.isfile(WRITE_PATH+filename):
                self.count_interim_files+=1
                site_ids.append(site)
                self.filenames.append(filename)
        # self.logger.debug(site_ids)
        # self.logger.debug(self.filenames)
        combined_csv = pd.concat([pd.read_csv(WRITE_PATH+f, sep=',') for f in self.filenames])
        combined_csv.to_csv(WRITE_PATH+"site-all-training-weather-holiday.csv", index=False)

    @property
    def sites_count(self):
        return self.count_interim_files

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)
    logger = logging.getLogger(__name__)
    data_ingest = DataIngest(logger=logger)
    data_ingest.ingest()
    data_ingest.wrangle()
    data_ingest.merge_csvs()
    logger.info("Sites count: {}".format(data_ingest.sites_count))
