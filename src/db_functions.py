#
# Database functions
################################################################################
import mysql.connector
from mysql.connector.errors import IntegrityError, ProgrammingError

def create_dataset_tables(connection, tables=('training_data1', 'holiday1', 'weather1'), logger=None):
    """Creates the Weather, Holidays and Training data tables"""

    cursor = connection.cursor()

    create_tables = {
        'holiday1': '''create table if not exists holiday1(
                    Holiday_date DATE,
                    Holiday VARCHAR(255),
                    SiteId INT)''',
        'weather1': '''create table if not exists weather1(
                      Timestamp VARCHAR(255) PRIMARY KEY,
                      Temperature INT,
                      Distance INT,
                      SiteId INT)''',
        'training_data1': '''create table if not exists training_data1(
                            ObsId INT PRIMARY KEY,
                            SiteId INT,
                            Timestamp Date,
                            ForecastId INT,
                            Value INT)'''

    }
    for table in tables:
        if table in create_tables.keys():
            cursor.execute(create_tables[table])
            connection.commit()

def insert_rows(connection, schema, table, fields, values,
                format_specifier, logger=None):
    """Insert rows into the table"""
    inserted_row_count = 0
    cursor = connection.cursor(buffered=True)
    select_data = "select count(*) from {schema}.{table}".format(schema=schema, table=table)
    insert_data = "insert into {schema}.{table}({fields}) values({format_specifier})".format(
        schema=schema, table=table, fields=fields, format_specifier=format_specifier)
    if logger:
        logger.info(insert_data)
        logger.info(select_data)
    try:
        cursor.execute(select_data)
        row_count = cursor.fetchall()[0][0]
        logger.debug("{} Row count {}".format(table, row_count))
        if row_count == 0:
            cursor.executemany(insert_data, values)
            connection.commit()
    except ProgrammingError as p_e:
        logger.error(p_e)
        if p_e.errno == 1146:
            create_dataset_tables(connection, [table])
            cursor.executemany(insert_data, values)
    except IntegrityError as i_e:
        logger.error(i_e)
        connection.rollback()
    except Exception as e:
        logger.error(e)
    finally:
        cursor.close()
        inserted_row_count += 1
    return inserted_row_count
