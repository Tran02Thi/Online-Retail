from typing import Any
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
import psycopg2
from sqlalchemy import create_engine
from .register_stored import *

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = get_db_connector('postgresql')(**config)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        # TODO: your code here
        schema, table = 'gold', context.asset_key.path[-1]
        obj.to_sql(
                name=f"{table}",
                con=self._config,
                schema=schema,
                if_exists="replace",
                index=False
            )

    def load_input(self, context: InputContext) -> Any:
        pass