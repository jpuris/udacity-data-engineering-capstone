import argparse
import logging as log
import os
import sys
from io import StringIO
from time import time

import pandas as pd
import psycopg2 as pg
import yaml
from dotenv import load_dotenv
from lib.sql_queries import LOAD_DIM_CITY
from lib.sql_queries import LOAD_DIM_DATE
from lib.sql_queries import LOAD_FACT_DEMO
from lib.sql_queries import LOAD_FACT_TEMP
from lib.sql_queries import TABLE_ROW_COUNT
from psycopg2.extensions import AsIs


def get_config(conf_file: str, section: str = None) -> dict:
    """Reads yaml configuration file and returns dict with its content

    Args:
        conf_file (str): Config file path
        section (str, optional): root node in yaml file to read

    Returns:
        dict: Read configuration
    """
    try:
        with open(conf_file) as file:
            log.debug("Loading configuration from file '%s'", conf_file)
            conf = yaml.load(file, Loader=yaml.FullLoader)
            log.debug("Configuration file '%s' loaded", conf_file)
    except FileNotFoundError:
        log.error("Configuration file ['%s'] not found. Exiting..", conf_file)
        sys.exit(1)

    if not section:
        return conf

    return conf[section]


def init_db_conn(db_params: dict) -> pg.connect:
    """Creates PostgreSQL database connection object

    Args:
        db_params (dict): Database parameters. Must be compatible with
        psycopg2 connect params.

    Returns:
        psycopg2.connect: connect object
    """
    log.debug('Connecting to database')
    conn = pg.connect(**db_params)
    conn.set_session(autocommit=True)
    log.debug('Database params: "%s"', db_params)
    log.info('Connected to database')
    return conn


def setup_logging(log_level: str):
    """Sets logging package log level according to the param given.
    Log levels in decremental value of verbosity:
    - CRITICAL
    - ERROR
    - WARNING
    - INFO
    - DEBUG
    - NOTSET

    log_level (str): Desired log level
    """
    log_numeric_level = getattr(log, log_level.upper(), None)
    _format = '%(name)s - %(levelname)s - %(message)s'
    if not isinstance(log_numeric_level, int):
        log.error("Invalid log level: ['%s']. Exiting..", log_level)
        sys.exit(1)
    log.basicConfig(level=log_numeric_level, format=_format)
    log.info("Log level has been set to '%s'", log_level.upper())


def create_tables(conn) -> None:
    """Runs a hardcoded SQL file to recreates project's database structure.

    Args:
        conn (psycopg2.connect): PostgreSQL database connection object
    """
    func_start_time = time()
    sql_file = os.path.join(os.path.dirname(__file__), 'lib/create_tables.sql')
    log.debug('Running create table SQL script, file: "%s"', sql_file)
    with open(sql_file) as sql:
        with conn.cursor() as cur:
            cur.execute(sql.read())
    log.info(
        'Creating table objects completed in %s seconds',
        round(time() - func_start_time, 3),
    )


def bulk_load_df(data: pd.DataFrame, table_name: str, conn: pg.connect):
    """Bulk inserts a pandas dataframe into PostgreSQL table

    Args:
        data (pandas.Dataframe): Data for insertion
        table_name (str): Table name for logging purposes
        conn (psycopg2.connect): PostgreSQL database connection
    """
    buffer = StringIO()
    buffer.write(data.to_csv(index=None, header=None, na_rep=''))
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_from(
            buffer,
            table_name,
            columns=data.columns,
            sep=',',
            null='',
        )
        conn.commit()


def load_stage_demo(conn: pg.connect, filepath: str) -> None:
    """Loads data from specified JSON file into stage_demo table in PostgreSQL

    Args:
        conn (psycopg2.connect): PostgreSQL database connection
        filepath (str): File path
    """
    func_start_time = time()
    log.info('Loading demo stage, file: "%s"', filepath)
    # Load file data into pandas dataframe
    df_data = pd.read_json(filepath)

    # Explode the JSON column 'fields'
    df_data = pd.concat(
        [
            pd.json_normalize(df_data['fields']),
            df_data.drop('fields', axis=1),
        ], axis=1,
    )

    # Select only relevant data
    df_data = df_data[[
        'city',
        'record_timestamp',
        'number_of_veterans',
        'male_population',
        'foreign_born',
        'average_household_size',
        'median_age',
        'total_population',
        'female_population',
    ]]

    df_data['record_timestamp'] = '2015-01-01'

    bulk_load_df(df_data, 'public.stage_demo', conn)

    log.info(
        'Bulk loading demo stage "%s" completed in: %s seconds',
        filepath,
        round(time() - func_start_time, 3),
    )


def load_stage_temp(conn, filepath):
    """Loads data from specified JSON file into stage_temp table in PostgreSQL

    Args:
        conn (psycopg2.connect): PostgreSQL database connection
        filepath (str): File path
    """
    func_start_time = time()
    log.debug('Bulk loading temp stage, file: "%s"', filepath)
    # Load file data into pandas dataframe
    df_data = pd.read_csv(filepath)

    # Only interested in US data
    df_data = df_data[df_data['Country'] == 'United States']

    # Convert 'dt' column values to datetime type
    df_data['dt'] = pd.to_datetime(df_data['dt'])

    # Select only relevant data
    df_data = df_data[['dt', 'AverageTemperature', 'City']]

    # Clean up the column names
    column_names = {
        'dt': 'dt',
        'AverageTemperature': 'avg_temp',
        'City': 'city',
    }

    df_data.rename(columns=column_names, inplace=True)

    bulk_load_df(df_data, 'public.stage_temp', conn)

    log.info(
        'Bulk loading temp stage "%s" completed in: %s seconds',
        filepath,
        round(time() - func_start_time, 3),
    )


def run_sql_etl(sql: str, conn: pg.connect, table_name: str):
    """Runs given SQL query on the provided PostgreSQL connection obj.

    Args:
        sql (str): SQL script to run
        conn (psycopg2.connect): PostgreSQL database connection
        table_name (str): Table name for logging purposes
    """
    func_start_time = time()
    log.debug('Running SQL ETL for "%s" table', table_name)
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log.info(
        'SQL ETL for table "%s" completed in: %s seconds',
        table_name,
        round(time() - func_start_time, 3),
    )


def dq_check_fact_table_row_count(
        stage_table: str,
        fact_table: str,
        conn: pg.connect,
) -> bool:
    """Compares row count between the stage and fact tables

    Args:
        stage_table (str): Target stage table name
        fact_table (str): Target fact table name
        conn (pg.connect): Psycopg2 connect obj

    Returns:
        bool: True, if checks have passed. False, if failed.
    """
    func_start_time = time()
    log.debug(
        'Running row consistency checks on "%s" and "%s" tables',
        stage_table,
        fact_table,
    )

    with conn.cursor() as cur:
        cur.execute(TABLE_ROW_COUNT, {'table': AsIs(stage_table)})
        stage_row_count = cur.fetchone()[0]
        cur.execute(TABLE_ROW_COUNT, {'table': AsIs(fact_table)})
        fact_row_count = cur.fetchone()[0]

    if not stage_row_count == fact_row_count:
        log.warning('Table consistency checks have failed')
        log.warning('Table %s row count is %s', stage_table, stage_row_count)
        log.warning('Table %s row count is %s', fact_table, fact_row_count)
    else:
        log.debug('Table %s row count is %s', stage_table, stage_row_count)
        log.debug('Table %s row count is %s', fact_table, fact_row_count)

    log.info(
        'Consistency checks on "%s" and "%s" tables finished in %s seconds',
        stage_table,
        fact_table,
        round(time() - func_start_time, 3),
    )


def main():
    """Main ETL function"""
    etl_start_time = time()
    default_config_file = f'{os.path.dirname(os.path.realpath(__file__))}' \
                          f'/config/config.yaml'

    # Script argument configuration
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', help='Configuration file',
        default=default_config_file,
    )
    parser.add_argument(
        '-l', '--log-level', help='Log level',
        choices=[
            'notset', 'debug', 'info',
            'warning', 'error', 'critical',
        ],
        default='info',
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    log.info('Application start')
    config = get_config(args.config)

    envfile = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.isfile(envfile):
        log.info('Loaded .env file')
        load_dotenv(envfile)
    else:
        log.info('Did not find .env file, fallback to env vars')

    # Accessing variables.
    config['database']['password'] = os.getenv('PG_PASSWORD')

    # Initialise DB connection and cursor objects
    try:
        conn = init_db_conn(config['database'])
    except pg.OperationalError as exception:
        config['database']['password'] = '*****'
        log.error(
            'Failed to open connection to %s database. Error: %s',
            config['database'], exception,
        )
        sys.exit(1)

    # Sources
    source_demo = config['data']['demographic']
    source_temp = config['data']['temperature']

    # Sanity checks, before ETL execution
    log.debug('Checking, if source files exist')
    for source_file in [source_temp, source_demo]:
        if not os.path.isfile(source_file):
            log.info('Configured source file "%s" does not exist', source_file)
            sys.exit(1)
        else:
            log.debug('Source file "%s" exists', source_file)

    # ETL Jobs
    create_tables(conn)
    # Stage
    load_stage_demo(conn, source_demo)
    load_stage_temp(conn, source_temp)
    # Dimension
    run_sql_etl(LOAD_DIM_DATE, conn, 'dim_date')
    run_sql_etl(LOAD_DIM_CITY, conn, 'dim_city')
    # Fact
    run_sql_etl(LOAD_FACT_DEMO, conn, 'fact_demo')
    run_sql_etl(LOAD_FACT_TEMP, conn, 'fact_temp')
    # Checks
    dq_check_fact_table_row_count('stage_demo', 'fact_demo', conn)
    dq_check_fact_table_row_count('stage_temp', 'fact_temp', conn)

    log.info('Disconnected from database')
    conn.close()

    log.info(
        'ETL job completed successfully in %s seconds',
        round(time() - etl_start_time, 3),
    )


if __name__ == '__main__':
    main()
