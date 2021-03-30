from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadStageOperator(BaseOperator):
    """
    TODO
    """

    ui_color = '#00aae4'

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        table_name: str,
        sql_query: str,
        file: str,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table_name
        self.sql_query = sql_query
        self.file = file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
        TODO
        """
