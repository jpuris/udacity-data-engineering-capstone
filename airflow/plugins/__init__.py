import helpers
import operators

from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    """
    Capstone plugin...
    """
    name = 'capstone_plugin'
    operators = [
        operators.CreateTableOperator,
        operators.LoadStageOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
    ]
    helpers = [
        helpers.SqlQueries,
    ]
