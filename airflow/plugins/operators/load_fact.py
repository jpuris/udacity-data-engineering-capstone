from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Runs provided SQL query on PostgreSQL cluster.

    Keyword arguments:
    postgres_conn_id  -- Airflow connection name for PostgreSQL detail
    sql_query         -- Query to run
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        sql_query: str,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        postgres_hook.run(self.sql_query)
