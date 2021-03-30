from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """
    Runs the provided SQL file to create sparkify database tables.
    File is '/opt/airflow/plugins/helpers/create_tables.sql

    Keyword arguments:
    postgres_conn_id  -- Airflow connection name for PostgreSQL detail
    """

    ui_color = '#e67e22'

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        self.log.info('Creating Postgres SQL Hook')
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info('Executing creating tables in PostgreSQL.')
        queries = open(
            '/opt/airflow/plugins/helpers/create_tables.sql',
        ).read()
        postgres.run(queries)

        self.log.info('Tables created ')
