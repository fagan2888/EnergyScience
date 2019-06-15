#!/usr/bin/env python
# coding: utf-8

import warnings
warnings.filterwarnings('ignore')
import os
import pandas as pd
import numpy as np
import sys
module_path = os.path.abspath(os.path.join(os.pardir, os.pardir))
if module_path not in sys.path:
    sys.path.append(module_path)

from store_data import DataStore
from utilities import (get_building_id, get_location_id,
                       get_lat_lng, starting_date, time_delta,
                       year, get_date, timestamp_to_string, epoch_to_date)


class DataIngestionWrangling(object):

    def __init__(self,raw_data_path, interim_data_path, processed_data_path):
        self.raw = raw_data_path
        self.interim = interim_data_path
        self.processed = processed_data_path
        self.dirpath = None

    def ingestion(self):
        """
        Ingest raw data sources
        """
        print("===========Data ingestion=============")
        for dirpath, dirnames, filenames in os.walk(self.raw):
            self.dirpath = dirpath
            self.filenames = filenames
            print(dirpath)
        print("Reading holidays from {}".format(self.raw))
        self.holidays = pd.read_csv(self.raw+"Holidays.csv")

    def wrangling(self):
        """
        Processing the Data
        """
        print("=============Data Wrangling============")
        energystats = pd.DataFrame()
        for file_ in self.filenames:
            if '2004' in file_ and 'USA_' in file_:
                df = pd.read_csv(self.dirpath+file_)
                building_type = file_[:file_.index("2004")]
                location = file_[file_.index("USA_"):file_.index(".csv")]
                df['building_type'] = building_type
                df['location'] = location
            else:
                continue
            energystats = pd.concat((df, energystats), axis=0)

        energystats['location_id'] = energystats['location'].apply(get_location_id)
        energystats['building_id'] = energystats['building_type'].apply(get_building_id)
        energystats['Date'] = energystats['Date/Time'].apply(get_date)
        energystats.Date = energystats.Date.astype(str)
        # holiday wrangling
        energystats_holidays = pd.merge(energystats, self.holidays,
                                        on='Date', how='outer')
        # temperature wrangling
        temperaturestats = pd.DataFrame()
        locations = ['USA_AK_FAIRBANKS', 'USA_CA_LOS_ANGELES', 'USA_IL_CHICAGO-OHARE', 'USA_MN_MINNEAPOLIS', 'USA_TX_HOUSTON', 'USA_WA_SEATTLE',
                     'USA_NV_LAS_VEGAS', 'USA_CA_SAN_FRANCISCO', 'USA_AZ_PHOENIX', 'USA_GA_ATLANTA', 'USA_MD_BALTIMORE', 'USA_CO_BOULDER']
        for location in locations:
            loc_temp_usage = pd.read_csv(self.interim+location+'_temperature_usage.csv')
            loc_temp_usage['Date'] = loc_temp_usage['time'].apply(epoch_to_date)
            loc_temp_usage['location'] = location
            temperaturestats = pd.concat((loc_temp_usage, temperaturestats), axis=0)
        temperaturestats.drop(columns=['Unnamed: 0'], inplace=True)
        print("Storing interim datasets under: {}".format(self.interim))
        temperaturestats.to_csv(self.interim+'All_locations_temperature_usage.csv')
        temperature_usage = pd.read_csv(self.interim+"All_locations_temperature_usage.csv")
        dataset = pd.merge(energystats_holidays, temperature_usage,
                           on=['Date','location'], how='inner')
        dataset = dataset.drop(columns=['Unnamed: 0'])
        dataset_seattle = dataset[dataset['location'] == 'USA_WA_SEATTLE']
        dataset_fairbanks = dataset[dataset['location'] == 'USA_AK_FAIRBANKS']
        dataset_phoenix = dataset[dataset['location'] == 'USA_AZ_PHOENIX']
        dataset_los_angeles = dataset[dataset['location'] == 'USA_CA_LOS_ANGELES']
        dataset_san_francisco = dataset[dataset['location'] == 'USA_CA_SAN_FRANCISCO']
        dataset_boulder = dataset[dataset['location'] == 'USA_CO_BOULDER']
        dataset_chicago = dataset[dataset['location'] == 'USA_IL_CHICAGO-OHARE']
        dataset_baltimore = dataset[dataset['location'] == 'USA_MD_BALTIMORE']
        dataset_minneapolis = dataset[dataset['location'] == 'USA_MN_MINNEAPOLIS']
        dataset_vegas = dataset[dataset['location'] == 'USA_NV_LAS_VEGAS']
        dataset_houston = dataset[dataset['location'] == 'USA_TX_HOUSTON']
        dataset_atlanta = dataset[dataset['location'] == 'USA_GA_ATLANTA']
        self.datasets = [dataset_seattle, dataset_fairbanks, dataset_phoenix, dataset_los_angeles, dataset_san_francisco,
                         dataset_boulder, dataset_chicago, dataset_baltimore, dataset_minneapolis, dataset_vegas,
                         dataset_houston, dataset_atlanta]

    def store_engineered_features(self):
        """
        Store engineered features
        """
        print("===========Feature Engineering============")
        for data in self.datasets:
            data['Electricity:Facility [kW](Hourly)_lag'] = data['Electricity:Facility [kW](Hourly)'].shift(24)
            data.drop(data.head(24).index,inplace=True)
            data['Electricity:Facility [kW](Hourly)_future'] = data['Electricity:Facility [kW](Hourly)'].shift(-24)
            data.drop(data.tail(24).index,inplace=True)
            data.drop(columns=['Electricity:Facility [kW](Monthly)', 'Gas:Facility [kW](Monthly)'], axis=1, inplace=True)
            data["InteriorEquipment:Gas [kW](Hourly)"].fillna(0, inplace=True)
            data["Water Heater:WaterSystems:Gas [kW](Hourly)"].fillna(0, inplace=True)
            data['Electricity:Facility_delta_current_lag'] = data['Electricity:Facility [kW](Hourly)'].sub(data['Electricity:Facility [kW](Hourly)_lag'], axis = 0)
        # Store processed data by location
        print("Storing Processed datasets under: {}".format(self.processed))
        self.datasets[0].to_csv(self.processed+'dataset_seattle.csv')
        self.datasets[1].to_csv(self.processed+'dataset_fairbanks.csv')
        self.datasets[2].to_csv(self.processed+'dataset_phoenix.csv')
        self.datasets[3].to_csv(self.processed+'dataset_los_angeles.csv')
        self.datasets[4].to_csv(self.processed+'dataset_san_francisco.csv')
        self.datasets[5].to_csv(self.processed+'dataset_boulder.csv')
        self.datasets[6].to_csv(self.processed+'dataset_chicago.csv')
        self.datasets[7].to_csv(self.processed+'dataset_baltimore.csv')
        self.datasets[8].to_csv(self.processed+'dataset_minneapolis.csv')
        self.datasets[9].to_csv(self.processed+'dataset_vegas.csv')
        self.datasets[10].to_csv(self.processed+'dataset_houston.csv')
        self.datasets[11].to_csv(self.processed+'dataset_atlanta.csv')
        dataset_all = pd.DataFrame()
        filenames = ['dataset_seattle.csv', 'dataset_fairbanks.csv', 'dataset_phoenix.csv',
                     'dataset_los_angeles.csv', 'dataset_san_francisco.csv', 'dataset_boulder.csv',
                     'dataset_chicago.csv', 'dataset_baltimore.csv', 'dataset_minneapolis.csv',
                     'dataset_vegas.csv', 'dataset_houston.csv', 'dataset_atlanta.csv']
        for file in filenames:
            df = pd.read_csv(self.processed+file)
            dataset_all = pd.concat((df, dataset_all), axis=0)
        dataset_all.to_csv(self.processed+'dataset.csv')

if __name__ == "__main__":
    energy = DataIngestionWrangling('../data/data1/raw/', '../data/data1/interim/', '../data/data1/processed/')
    energy.ingestion()
    energy.wrangling()
    energy.store_engineered_features()
