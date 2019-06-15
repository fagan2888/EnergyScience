import pandas as pd

from sqlalchemy import create_engine
import mysql.connector


class DataStore(object):
    """
    Connects to the database and stores data
    """
    def __init__(self, database_ip, logger=None):
        self.engine = None
        self.ip = database_ip # 35.227.50.121

    def connect_to_database(self):
        """
        Connect to the database
        """
        eng_str = 'mysql+mysqldb://{0}:{1}@{2}/{3}'.format('****',
                                                           '****',
                                                            self.ip,
                                                           'esschema')
        self.engine = create_engine(eng_str, pool_recycle=3600, echo=False)

    def chunker(self, seq, size):
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def store_data(self, dataframe):
        """
        Store dataframe in a table
        :param dataframe: Name of the dataframe and the MySQL table
        """
        chunksize = int(len(dataframe) / 1000)
        for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
            replace = "replace" if i == 0 else "append"
            dataframe.to_sql(con=self.engine, name='energystats',
                             if_exists=replace, index=False)
