from typing import Any
from dagster import asset, IOManager, OutputContext, InputContext
from .register_stored import *
from minio import Minio
import pandas as pd
import os
import io


class MinIOManager(IOManager):
    def __init__(self, config) -> None:
        self._config = get_db_connector('minio')(config['endpoint_url'], config['aws_access_key_id'], config['aws_secret_access_key'])
        self.info_config = config

    def __str__(self):
        return self._config


    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        #  Encode the CSV string to UTF-8 bytes
        df = obj.to_csv(index=False).encode('utf-8')

        bucket_name = self.info_config['bucket']
        file_name = context.asset_key.path

        self._config.put_object(
            bucket_name,
            f"{file_name[-3]}/{file_name[-2]}/{file_name[-1]}.csv",
            io.BytesIO(df),
            len(df),
            content_type='text/csv'
        )

    def load_input(self, context: InputContext) -> [pd.DataFrame]:
        bucket_name = self.info_config['bucket']
        file_name = context.upstream_output.asset_key.path
        data = self._config.get_object(bucket_name,
                                              object_name=f"{file_name[-3]}/{file_name[-2]}/{file_name[-1]}.csv")

        df = pd.read_csv(data)
        return df