import pandas as pd
import datetime

def convert_to_date(date_in_some_format):
    """
    :param date_in_some_format: datetime
    """
    date_as_string = str(date_in_some_format)
    year_as_string = date_as_string.split('T')[0] # last four characters
    return year_as_string

def get_year(date):
    """
    Returns the year
    :param: Date
    """
    return date.split('-')[0]

def get_month(date):
    """
    Return the month
    :param: Date
    """
    return date.split('-')[1]

def string_to_timestamp(timestamp):
    """
    Return timestamp
    :param: timestamp
    """
    return pd.to_datetime(timestamp)


def timestamp_to_string(timestamp):
    """
    """
    return timestamp.strftime('%m/%d  %H:%M:%S')

def get_building_id(building_type):
    """
    :param: building_type
    :return: building_id
    """
    building_types = {
        'RefBldgFullServiceRestaurantNew': 1,
        'RefBldgHospitalNew': 2,
        'RefBldgLargeHotelNew': 3,
        'RefBldgLargeOfficeNew': 4,
        'RefBldgMediumOfficeNew': 5,
        'RefBldgMidriseApartmentNew': 6,
        'RefBldgOutPatientNew': 7,
        'RefBldgPrimarySchoolNew': 8,
        'RefBldgQuickServiceRestaurantNew': 9,
        'RefBldgSecondarySchoolNew': 10,
        'RefBldgSmallHotelNew': 11,
        'RefBldgSmallOfficeNew': 12,
        'RefBldgStand-aloneRetailNew': 13,
        'RefBldgStripMallNew': 14,
        'RefBldgSuperMarketNew': 15,
        'RefBldgWarehouseNew': 16
    }
    return building_types.get(building_type, 0)

def get_location_id(location):
    """
    :param: location
    :return: location ID
    """
    locations = {
        'USA_AK_FAIRBANKS': 1,
        'USA_CA_LOS_ANGELES': 2,
        'USA_IL_CHICAGO-OHARE': 3,
        'USA_MN_MINNEAPOLIS': 4,
        'USA_TX_HOUSTON': 5,
        'USA_WA_SEATTLE': 6
    }
    return locations.get(location, 0)

def get_lat_lng(location):
    locations = {
        'USA_AK_FAIRBANKS': '64.83778, -147.71639',
        'USA_CA_LOS_ANGELES': '34.05223, -118.24368',
        'USA_IL_CHICAGO-OHARE': '41.85003, -87.65005',
        'USA_MN_MINNEAPOLIS': '44.97997, -93.26384',
        'USA_TX_HOUSTON': '29.76328, -95.36327',
        'USA_WA_SEATTLE': '47.60621, -122.33207',
        'USA_NV_LAS_VEGAS': '36.17497, -115.13722',
        'USA_CA_SAN_FRANCISCO': '37.77493, -122.41942',
        'USA_AZ_PHOENIX': '33.44838, -112.07404',
        'USA_GA_ATLANTA': '33.749, -84.38798',
        'USA_MD_BALTIMORE': '39.29038, -76.61219',
        'USA_CO_BOULDER': '40.01499, -105.27055'
    }
    return locations.get(location)

def timezone_mapper(location):
    timezones = {
        'USA_AK_FAIRBANKS': 'America/Anchorage' ,
        'USA_CA_LOS_ANGELES': 'America/Los_Angeles',
        'USA_IL_CHICAGO-OHARE': 'America/Chicago',
        'USA_MN_MINNEAPOLIS': 'America/Chicago',
        'USA_TX_HOUSTON': 'America/Chicago',
        'USA_WA_SEATTLE': 'America/Los_Angeles'
    }
    return timezones.get(location)

def starting_date():
    return 1072947600

def time_delta():
    return 86400

def year():
    return 31622400

def epoch_to_date(epoch):
    return datetime.datetime.utcfromtimestamp(epoch).strftime('%m/%d')

def get_date(timestamps):
    dates = timestamps.split('  ')[0]
    return dates.replace(' ','')
