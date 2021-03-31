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
# from lib.sql_queries import SqlQueries


def get_config(conf_file: str, section: str = None):
    """
    TODO
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


def init_db_conn(db_params: dict):
    """
    TODO
    """

    log.info('Connecting to database')
    conn = pg.connect(**db_params)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    log.debug('Database params: "%s"', db_params)
    return conn, cur


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


def create_tables(cur):
    """
    TODO
    """
    func_start_time = time()
    sql_file = os.path.join(os.path.dirname(__file__), 'lib/create_tables.sql')
    log.info('Running create table SQL script, file: "%s"', sql_file)
    with open(sql_file) as sql:
        cur.execute(sql.read())
    log.info(
        'Job completed in: %s seconds',
        round(time() - func_start_time, 3),
    )


def bulk_load_df(df, table_name, conn):
    buffer = StringIO()
    buffer.write(df.to_csv(index=None, header=None, na_rep=''))
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_from(
            buffer,
            table_name,
            columns=df.columns,
            sep=',',
            null='',
        )
        conn.commit()


def load_stage_demo(conn, filepath):
    """
    TODO
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
        'state',
        'record_timestamp',
        'number_of_veterans',
        'male_population',
        'foreign_born',
        'average_household_size',
        'median_age',
        'total_population',
        'female_population',
    ]]

    bulk_load_df(df_data, 'public.stage_demo', conn)

    log.info(
        'Job completed in: %s seconds',
        round(time() - func_start_time, 3),
    )


def load_stage_temp(conn, filepath):
    """
    TODO
    """
    func_start_time = time()
    log.info('Bulk loading temp stage, file: "%s"', filepath)
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
        'Job completed in: %s seconds',
        round(time() - func_start_time, 3),
    )


def load_dim_date(sql, conn):
    """
    TODO
    """


def load_dim_city(sql, conn):
    """
    TODO
    """


def main():
    """
    TODO
    """

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
        conn, cur = init_db_conn(config['database'])
    except pg.OperationalError as exception:
        config['database']['password'] = '*****'
        log.error(
            'Failed to open connection to %s database. Error: %s',
            config['database'], exception,
        )
        sys.exit(1)

    # ETL Jobs
    create_tables(cur)
    load_stage_demo(conn, config['data']['demographic'])
    load_stage_temp(conn, config['data']['temperature'])

    log.info('Successfully disconnected from database')
    conn.close()


if __name__ == '__main__':
    main()