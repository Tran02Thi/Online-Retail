import pandas as pd
from dagster import asset, Output
import os
from pathlib import Path


COMPUTE_KIND = "Pandas"
LAYER = "bronze"
SCHEMA = "ecom"
adr_data = r"D:\Build TNTT\etl_pipeline\E-Commerce Retail"
YEAR = "2010__2011"
zone_data = "e-commerce_retail"


def create_dataset(address, name_asset):

    @asset(
        name=name_asset,
        io_manager_key="minio_io_manager",
        key_prefix=[LAYER, SCHEMA],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER,
    )
    def bronze_dataset(context) -> Output[pd.DataFrame]:
        df = pd.read_csv(address, encoding='unicode_escape')
        return Output(
            df,
            metadata={
                "directory": address,
                "table": name_asset,
                "records count": len(df),
            }
        )
    return bronze_dataset

# ad = os.path.join(Path(__file__).parents[2], 'E-Commerce Retail/OnlineRetail_2010__2011.csv')
address = os.path.join(Path(__file__).parents[2], 'E-Commerce Retail/OnlineRetail_2010__2011.csv')
name_asset = "bronze_OnlineRetail_2010__2011"
silver_e_commerce_retail_2010_2011 = create_dataset(address, name_asset)



